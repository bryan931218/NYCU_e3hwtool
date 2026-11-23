"""Moodle's password-token and one-use session handoff for the NYCU API client."""

import re
from urllib.parse import urljoin, urlsplit

from e3_tracker.assignments.services.http import E3LoginError, _authenticated_principal, safe_request
from e3_tracker.platform.constants import HEADERS


# Identify this client and the Moodle mobile API protocol it implements.
API_HEADERS = {"User-Agent": "E3hwtool/1.0 (MoodleMobile API client)", "Accept": "application/json"}
SERVICE = "moodle_mobile_app"
_CREDENTIAL = re.compile(r"[A-Za-z0-9_-]{16,256}\Z")


def _json_post(sess, url, data, *, step, timeout):
    response = safe_request(sess, "POST", url, data=data, headers=API_HEADERS,
                            timeout=timeout, allow_redirects=False)
    if response.status_code != 200:
        raise E3LoginError("upstream_response_invalid", auth_step=step)
    try:
        result = response.json()
    except ValueError:
        raise E3LoginError("upstream_response_invalid", auth_step=step) from None
    if not isinstance(result, dict):
        raise E3LoginError("upstream_response_invalid", auth_step=step)
    if result.get("exception") or result.get("error"):
        code = result.get("errorcode")
        reason, notice = "mobile_session_unavailable" if step == "handoff" else "mobile_api_unavailable", "unknown"
        if code == "invalidlogin":
            reason, notice = "login_rejected", "credentials"
        elif code == "autologinkeygenerationlockout":
            reason, notice = "session_handoff_limited", "rate_limit"
        elif code in {"passwordisexpired", "usernotconfirmed", "usernotfullysetup", "forcepasswordchangenotice", "restoredaccountresetpassword"}:
            reason, notice = "account_action_required", "verification"
        raise E3LoginError(reason, auth_step=step, api_code=code, notice=notice)
    return result


def _credential(value, step):
    if not isinstance(value, str) or not _CREDENTIAL.fullmatch(value):
        raise E3LoginError("upstream_response_invalid", auth_step=step)
    return value


def _local_url(value, base_url, *, allowed_paths, step):
    if not isinstance(value, str) or not value or any(c in value for c in "\r\n\\"):
        raise E3LoginError("upstream_response_invalid", auth_step=step)
    target, base = urlsplit(urljoin(base_url + "/", value)), urlsplit(base_url)
    if ((target.scheme, target.netloc) != (base.scheme, base.netloc) or target.username or target.password
            or target.path not in allowed_paths or target.fragment):
        raise E3LoginError("upstream_response_invalid", auth_step=step)
    return target.geturl()


def login_with_mobile_api(sess, base_url, username, password, *, timeout=20):
    base_url = base_url.rstrip("/")
    token = private_token = key = None
    try:
        result = _json_post(sess, base_url + "/login/token.php", {
            "username": username, "password": password, "service": SERVICE,
        }, step="token", timeout=timeout)
        token = _credential(result.get("token"), "token")
        private_token = _credential(result.get("privatetoken"), "token")
        endpoint = base_url + "/webservice/rest/server.php"
        info = _json_post(sess, endpoint, {
            "wstoken": token, "wsfunction": "core_webservice_get_site_info", "moodlewsrestformat": "json",
        }, step="identity", timeout=timeout)
        user_id = info.get("userid")
        site = urlsplit(str(info.get("siteurl") or ""))
        base = urlsplit(base_url)
        if (type(user_id) is not int or user_id <= 0
                or not isinstance(info.get("username"), str)
                or info["username"].casefold() != username.strip().casefold()
                or (site.scheme, site.netloc, site.path.rstrip("/")) != (base.scheme, base.netloc, base.path.rstrip("/"))):
            raise E3LoginError("identity_mismatch", auth_step="identity")
        handoff = _json_post(sess, endpoint, {
            "wstoken": token, "privatetoken": private_token,
            "wsfunction": "tool_mobile_get_autologin_key", "moodlewsrestformat": "json",
        }, step="handoff", timeout=timeout)
        key = _credential(handoff.get("key"), "handoff")
        handoff_url = _local_url(handoff.get("autologinurl"), base_url,
                                 allowed_paths={"/admin/tool/mobile/autologin.php"}, step="handoff")
        if urlsplit(handoff_url).query:
            raise E3LoginError("upstream_response_invalid", auth_step="handoff")
        # Credentials remain in POST bodies and memory. Only the ordinary Moodle
        # session cookie is retained by the caller's existing encrypted storage.
        response = safe_request(sess, "POST", handoff_url, data={
            "userid": str(user_id), "key": key, "urltogo": base_url + "/my/",
        }, headers=HEADERS, timeout=timeout, allow_redirects=False)
        for _ in range(4):
            if response.status_code not in {301, 302, 303}:
                break
            target = _local_url(response.headers.get("Location"), base_url,
                                allowed_paths={"/", "/my/", "/my/index.php", "/login/index.php"}, step="web_session")
            response = safe_request(sess, "GET", target, headers=HEADERS, timeout=timeout, allow_redirects=False)
        if _authenticated_principal(response, base_url) != str(user_id):
            raise E3LoginError("mobile_session_unavailable", auth_step="web_session")
        if not any(cookie.name == "MoodleSession" and cookie.value for cookie in sess.cookies):
            raise E3LoginError("session_cookie_missing", auth_step="web_session")
    finally:
        token = private_token = key = None
