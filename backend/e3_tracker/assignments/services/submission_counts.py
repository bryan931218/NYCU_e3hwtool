"""Read class totals only from the REST user's verified web session."""
from urllib.parse import parse_qs, urlsplit

import requests

from e3_tracker.assignments.domain.parsing import gather_assign_links_from_list_page
from e3_tracker.assignments.services.http import apply_cookie, configure_tls, safe_request
from e3_tracker.assignments.services.server_sso import verified_cookie
from e3_tracker.platform.constants import HEADERS


def apply_class_counts(result, totals):
    # A refresh started just before SSO finishes can still receive fresh totals
    # before it writes its result, avoiding an older refresh clearing the counts.
    items = list(result.get("all_assignments", []))
    for course in result.get("courses", []):
        items.extend(course.get("assignments", []))
    for item in items:
        target = urlsplit(str(item.get("url") or ""))
        ids = parse_qs(target.query).get("id", [])
        if ((target.scheme, target.netloc, target.path) != ("https", "e3p.nycu.edu.tw", "/mod/assign/view.php")
                or len(ids) != 1 or not ids[0].isdigit()):
            continue
        counts = totals.get((item.get("course_id"), int(ids[0])))
        if counts:
            item["submitted_count"], item["participant_count"] = counts


def class_submission_counts(options, courses):
    totals = {}
    if not options.moodle_session or options.base_url.rstrip("/") != "https://e3p.nycu.edu.tw":
        return totals
    try:
        if not verified_cookie(options.moodle_session, options.moodle_user_id, min(options.timeout, 8)):
            return totals
    except (RuntimeError, requests.RequestException):
        return totals
    with requests.Session() as sess:
        configure_tls(sess, cafile=options.cafile, insecure=options.insecure)
        apply_cookie(sess, options.base_url, options.moodle_session)
        for course in courses:
            try:
                response = safe_request(sess, "GET", options.base_url.rstrip("/") + "/local/courseextension/index.php",
                    params={"courseid": course["id"], "scope": "assignment"}, headers=HEADERS,
                    timeout=min(options.timeout, 8), allow_redirects=False)
                if response.status_code != 200:
                    continue
                for _, url, _, submitted, participants in gather_assign_links_from_list_page(response.text, options.base_url):
                    target = urlsplit(url)
                    ids = parse_qs(target.query).get("id", [])
                    if ((target.scheme, target.netloc, target.path) == ("https", "e3p.nycu.edu.tw", "/mod/assign/view.php")
                            and len(ids) == 1 and ids[0].isdigit() and type(submitted) is int
                            and type(participants) is int and 0 <= submitted <= participants):
                        totals[(course["id"], int(ids[0]))] = (submitted, participants)
            except (RuntimeError, requests.RequestException):
                continue
    return totals
