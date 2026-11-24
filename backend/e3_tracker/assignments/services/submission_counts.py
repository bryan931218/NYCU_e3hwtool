"""Read E3's student-visible totals using a verified companion web session."""
from urllib.parse import parse_qs, urlsplit

import requests

from e3_tracker.assignments.domain.parsing import gather_assign_links_from_list_page
from e3_tracker.assignments.services.http import (
    _authenticated_principal, apply_cookie, configure_tls, safe_request,
)
from e3_tracker.platform.constants import HEADERS

def fetch_submission_counts(options, courses):
    if not options.moodle_session:
        return {}, "requires_relogin"
    if options.base_url.rstrip("/") != "https://e3p.nycu.edu.tw":
        return {}, "unavailable"
    totals, failed = {}, False
    timeout = min(options.timeout, 8)
    base = options.base_url.rstrip("/")
    with requests.Session() as sess:
        configure_tls(sess, cafile=options.cafile, insecure=options.insecure)
        apply_cookie(sess, base, options.moodle_session)
        try:
            dashboard = safe_request(sess, "GET", base + "/my/", headers=HEADERS,
                                     timeout=timeout, allow_redirects=False)
            if _authenticated_principal(dashboard, base) != str(options.moodle_user_id):
                return {}, "requires_relogin"
        except (requests.RequestException, RuntimeError):
            return {}, "unavailable"
        for course in courses:
            try:
                response = safe_request(sess, "GET", base + "/local/courseextension/index.php",
                    params={"courseid": course["id"], "scope": "assignment"}, headers=HEADERS,
                    timeout=timeout, allow_redirects=False)
                if response.status_code != 200:
                    failed = True
                    continue
                found = False
                for _, url, _, submitted, participants in gather_assign_links_from_list_page(response.text, base):
                    target = urlsplit(url)
                    query = parse_qs(target.query)
                    if (target.scheme, target.netloc, target.path) != ("https", "e3p.nycu.edu.tw", "/mod/assign/view.php"):
                        continue
                    ids = query.get("id", [])
                    if (len(ids) == 1 and ids[0].isdigit() and type(submitted) is int
                            and type(participants) is int and 0 <= submitted <= participants):
                        totals[(course["id"], int(ids[0]))] = (submitted, participants)
                        found = True
                failed = failed or not found
            except (requests.RequestException, RuntimeError):
                failed = True
    return totals, "partial" if failed else "available"
