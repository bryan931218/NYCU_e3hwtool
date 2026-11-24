"""Supplement student REST responses with the same user's E3 class totals.

Moodle's grading summary is teacher-only. E3 exposes these totals to enrolled
students on its course-extension page. Use Moodle's private-token handoff to
read that page, never another user's cache or a teacher-only API.
"""
import hashlib
import logging
import re
import threading
import time
from collections import OrderedDict
from urllib.parse import parse_qs, urlsplit

import requests

from e3_tracker.assignments.domain.parsing import gather_assign_links_from_list_page
from e3_tracker.assignments.services.http import (
    E3LoginError, _authenticated_principal, apply_cookie, configure_tls, safe_request,
)
from e3_tracker.assignments.services.mobile_auth import API_HEADERS, _json_post
from e3_tracker.platform.constants import HEADERS

_entries = OrderedDict()
_entries_lock = threading.Lock()
_MAX_ENTRIES = 256
_IDLE_SECONDS = 3600
_RETRY_SECONDS = 480  # Moodle normally permits one handoff every six minutes.
_CREDENTIAL = re.compile(r"[A-Za-z0-9_-]{16,256}\Z")
logger = logging.getLogger(__name__)


def _entry(options):
    # Neither API tokens nor private tokens are retained as dictionary keys.
    key = hashlib.sha256((options.base_url + str(options.moodle_user_id)
                          + options.moodle_token + options.moodle_private_token).encode()).digest()
    now = time.monotonic()
    with _entries_lock:
        for old_key, value in list(_entries.items()):
            if now - value["used"] > _IDLE_SECONDS:
                _entries.pop(old_key, None)
        if key not in _entries:
            if len(_entries) >= _MAX_ENTRIES:
                _entries.popitem(last=False)
            _entries[key] = {"lock": threading.Lock(), "cookie": None, "retry_at": 0}
        entry = _entries[key]
        entry["used"] = now
        _entries.move_to_end(key)
        return entry


def _dashboard(sess, base, timeout):
    return safe_request(sess, "GET", base + "/my/", headers=HEADERS,
                        timeout=timeout, allow_redirects=False)


def _open_session(sess, options, entry, timeout):
    base = options.base_url.rstrip("/")
    if entry["cookie"]:
        apply_cookie(sess, base, entry["cookie"])
        if _authenticated_principal(_dashboard(sess, base, timeout), base) == str(options.moodle_user_id):
            return True
        entry["cookie"] = None
        sess.cookies.clear()
    now = time.monotonic()
    if entry["retry_at"] > now:
        return False
    entry["retry_at"] = now + _RETRY_SECONDS
    handoff = _json_post(sess, base + "/webservice/rest/server.php", {
        "wstoken": options.moodle_token, "privatetoken": options.moodle_private_token,
        "wsfunction": "tool_mobile_get_autologin_key", "moodlewsrestformat": "json",
    }, step="handoff", timeout=timeout,
        headers={**API_HEADERS, "User-Agent": "E3hwtool/1.0 (MoodleMobile API client)"})
    if (not isinstance(handoff, dict) or handoff.get("autologinurl") != base + "/admin/tool/mobile/autologin.php"
            or not isinstance(handoff.get("key"), str) or not _CREDENTIAL.fullmatch(handoff["key"])):
        raise E3LoginError("upstream_response_invalid", auth_step="handoff")
    # Do not follow upstream redirects carrying credentials. Inspect the known
    # dashboard separately and verify that the web principal matches REST.
    safe_request(sess, "POST", handoff["autologinurl"], data={
        "userid": str(options.moodle_user_id), "key": handoff["key"], "urltogo": base + "/my/",
    }, headers=HEADERS, timeout=timeout, allow_redirects=False)
    if _authenticated_principal(_dashboard(sess, base, timeout), base) != str(options.moodle_user_id):
        raise E3LoginError("identity_mismatch", auth_step="web_session")
    cookies = [c.value for c in sess.cookies if c.name == "MoodleSession" and c.value]
    if len(cookies) != 1:
        raise E3LoginError("session_cookie_missing", auth_step="web_session")
    entry["cookie"] = cookies[0]
    return True


def fetch_submission_counts(options, courses):
    """Return fresh totals by course-module id; a missing total stays unknown."""
    if not options.moodle_private_token:
        return {}, "requires_relogin"
    if options.base_url.rstrip("/") != "https://e3p.nycu.edu.tw":
        return {}, "unavailable"
    entry = _entry(options)
    totals = {}
    failed = False
    timeout = min(options.timeout, 8)
    with entry["lock"], requests.Session() as sess:
        configure_tls(sess, cafile=options.cafile, insecure=options.insecure)
        try:
            if not _open_session(sess, options, entry, timeout):
                return {}, "unavailable"
        except (requests.RequestException, RuntimeError) as exc:
            # Fixed codes only: upstream exceptions can contain credential URLs.
            logger.warning("E3 submission totals web session unavailable (reason=%s code=%s)",
                           exc.reason if isinstance(exc, E3LoginError) else "request_failed",
                           exc.api_code if isinstance(exc, E3LoginError) else "none")
            entry["cookie"] = None
            return {}, "unavailable"
        base = options.base_url.rstrip("/")
        for course in courses:
            try:
                response = safe_request(sess, "GET", base + "/local/courseextension/index.php",
                    params={"courseid": course["id"], "scope": "assignment"}, headers=HEADERS,
                    timeout=timeout, allow_redirects=False)
                if response.status_code != 200:
                    failed = True
                    continue
                for _, url, _, submitted, participants in gather_assign_links_from_list_page(response.text, base):
                    target = urlsplit(url)
                    query = parse_qs(target.query)
                    if (target.scheme, target.netloc, target.path) != ("https", "e3p.nycu.edu.tw", "/mod/assign/view.php"):
                        continue
                    ids = query.get("id", [])
                    if (len(ids) == 1 and ids[0].isdigit() and type(submitted) is int
                            and type(participants) is int and 0 <= submitted <= participants):
                        totals[(course["id"], int(ids[0]))] = (submitted, participants)
            except (requests.RequestException, RuntimeError):
                failed = True
    return totals, "partial" if failed else "available"
