"""Authenticate an explicitly bound Moodle principal, independently of metadata."""

import re
from urllib.parse import parse_qs, urljoin, urlsplit

from bs4 import BeautifulSoup

from e3_tracker.assignments.services.http import safe_request
from e3_tracker.platform.constants import HEADERS


def authenticated_moodle_user_id(sess, base_url, *, timeout=8):
    """Read the server-generated own-user menu; never accept a submitted user ID."""
    base = urlsplit(base_url)
    url = base_url.rstrip("/") + "/my/"
    for _ in range(3):
        response = safe_request(sess, "GET", url, headers=HEADERS, timeout=min(timeout, 8), allow_redirects=False)
        target = urlsplit(response.url)
        if (target.scheme, target.netloc) != (base.scheme, base.netloc) or target.path not in {"/my/", "/my/index.php"}:
            return ""
        if response.status_code in {301, 302, 303, 307, 308}:
            target = urlsplit(urljoin(url, response.headers.get("Location", "")))
            if ((target.scheme, target.netloc) != (base.scheme, base.netloc)
                    or target.path not in {"/my/", "/my/index.php"} or target.query or target.fragment):
                return ""
            url = target.geturl()
            continue
        if response.status_code != 200:
            return ""
        soup = BeautifulSoup(response.text, "html.parser")
        if soup.select_one('input[name="password"], input[name="logintoken"]'):
            return ""
        owners = set()
        logout = False
        for link in soup.select('.usermenu a[href]'):
            target = urlsplit(urljoin(base_url, link["href"]))
            if (target.scheme, target.netloc) != (base.scheme, base.netloc) or target.username or target.password:
                continue
            query = parse_qs(target.query, keep_blank_values=True)
            if target.path == "/user/profile.php":
                ids = query.get("id", [])
                if len(ids) != 1 or not re.fullmatch(r"[1-9][0-9]*", ids[0]):
                    return ""
                owners.add(ids[0])
            elif target.path == "/login/logout.php" and query.get("sesskey") and all(query["sesskey"]):
                logout = True
        return owners.pop() if len(owners) == 1 and logout else ""
    return ""
