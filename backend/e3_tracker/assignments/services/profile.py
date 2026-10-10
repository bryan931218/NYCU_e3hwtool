"""Read the signed-in user's name from E3's own profile page."""
import re
from urllib.parse import parse_qs, urljoin, urlsplit

import requests
from bs4 import BeautifulSoup

from e3_tracker.platform.constants import HEADERS
from e3_tracker.platform.services.profile_names import is_academic_unit_name
from e3_tracker.assignments.services.http import safe_request


COMPOUND_SURNAMES = frozenset((
    "歐陽", "司馬", "上官", "諸葛", "夏侯", "司徒", "司空", "皇甫",
    "尉遲", "公孫", "慕容", "宇文", "令狐", "長孫", "南宮", "東方",
    "西門", "獨孤", "聞人", "澹臺", "申屠", "仲孫", "軒轅", "端木",
))


def parse_profile_name(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    heading = soup.select_one("#page-header h1")
    if not heading:
        return ""
    # E3 may put the department before or after the person's name.
    labels = {"登入", "登出", "關於我", "個人資料", "使用者資料", "焦點綜覽", "錯誤", "錯誤訊息"}
    candidates = re.findall(
        r"(?<![\u3400-\u9fff])([\u3400-\u9fff]{2,8})(?![\u3400-\u9fff])",
        heading.get_text(" ", strip=True),
    )
    names = list(dict.fromkeys(name for name in candidates
                             if name not in labels and not is_academic_unit_name(name)))
    return names[0] if len(names) == 1 else ""


def profile_surname(name: str) -> str:
    return name[:2] if name[:2] in COMPOUND_SURNAMES else name[:1]


def parse_profile_surname(html: str) -> str:
    return profile_surname(parse_profile_name(html))


def fetch_profile_name(sess, base_url: str, *, timeout: int = 8) -> str:
    dashboard = safe_request(sess, "GET", f"{base_url.rstrip('/')}/my/",
                             headers=HEADERS, timeout=timeout)
    soup = BeautifulSoup(dashboard.text, "html.parser")
    link = soup.select_one('.usermenu a[href*="/user/profile.php"]')
    if not link:
        return ""
    profile_url = urljoin(base_url + "/", link.get("href", ""))
    base, target = urlsplit(base_url), urlsplit(profile_url)
    if (target.scheme, target.netloc) != (base.scheme, base.netloc):
        return ""
    response = safe_request(sess, "GET", profile_url, headers=HEADERS, timeout=timeout)
    return parse_profile_name(response.text)


def fetch_profile_surname(sess, base_url: str, *, timeout: int = 8) -> str:
    return profile_surname(fetch_profile_name(sess, base_url, timeout=timeout))


def parse_student_number(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    field = soup.select_one('input#id_idnumber[name="idnumber"]')
    # Moodle's auth-locked text fields use readonly without necessarily using disabled.
    if not field or not field.has_attr("readonly"):
        return ""
    value = str(field.get("value") or "").strip()
    return value if re.fullmatch(r"[0-9]{9}", value) else ""


def _own_profile_url(base_url: str, href: str, path: str, user_id=None, *, implicit_own=False) -> str:
    url = urljoin(base_url.rstrip("/") + "/", href)
    base, target = urlsplit(base_url), urlsplit(url)
    query = parse_qs(target.query, keep_blank_values=True)
    ids = query.get("id", [])
    if (
        (target.scheme, target.netloc) != (base.scheme, base.netloc)
        or target.username or target.password or target.path != path
        or target.fragment
    ):
        return ""
    if path == "/user/edit.php":
        # Only read profile forms; do not follow GET actions such as email cancellation.
        if set(query) - {"id", "course", "returnto"}:
            return ""
        if "course" in query and (len(query["course"]) != 1 or not re.fullmatch(r"[0-9]+", query["course"][0])):
            return ""
        if "returnto" in query and (len(query["returnto"]) != 1 or query["returnto"][0] not in {"profile", "preferences", ""}):
            return ""
    if not ids and implicit_own and user_id is not None:
        return url
    if len(ids) != 1 or not re.fullmatch(r"[0-9]+", ids[0]) or (user_id is not None and ids[0] != user_id):
        return ""
    return url


def fetch_session_identity(sess, base_url: str, *, timeout: int = 8, diagnostics=None) -> dict:
    """Follow only the authenticated menu's own profile and its locked idnumber."""
    empty = {"name": "", "student_number": ""}

    def fail(reason, name=""):
        if diagnostics is not None:
            diagnostics["reason"] = reason
        return {"name": name, "student_number": ""}

    def read(url, stage):
        try:
            response = safe_request(sess, "GET", url, headers=HEADERS,
                                    timeout=timeout, allow_redirects=False)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if diagnostics is not None:
                diagnostics["reason"] = "access_denied" if status == 403 else stage + "_unavailable"
            return None
        if response.status_code != 200:
            if diagnostics is not None:
                location = response.headers.get("Location", "")
                target = urlsplit(urljoin(url, location)) if isinstance(location, str) else None
                diagnostics["reason"] = "login_required" if target and target.path == "/login/index.php" else stage + "_unavailable"
            return None
        soup = BeautifulSoup(response.text, "html.parser")
        if soup.select_one('form[action*="/login/index.php"]') or soup.select_one('input[name="logintoken"]'):
            if diagnostics is not None:
                diagnostics["reason"] = "login_required"
            return None
        return soup

    dashboard = read(base_url.rstrip("/") + "/my/", "dashboard")
    if dashboard is None:
        return empty
    link = dashboard.select_one('.usermenu a[href*="/user/profile.php"]') if dashboard else None
    profile_url = _own_profile_url(base_url, link.get("href", ""), "/user/profile.php") if link else ""
    if not profile_url:
        return fail("profile_link_missing")
    user_id = parse_qs(urlsplit(profile_url).query)["id"][0]
    profile = read(profile_url, "profile")
    if profile is None:
        return empty
    name = parse_profile_name(str(profile))
    # Never construct an edit URL from a caller-provided Moodle id.
    found_edit = False
    visited = set()
    for edit in profile.select('a[href*="/user/edit.php"]'):
        edit_url = _own_profile_url(base_url, edit.get("href", ""), "/user/edit.php", user_id, implicit_own=True)
        if edit_url:
            if edit_url in visited:
                continue
            if len(visited) >= 3:
                break
            visited.add(edit_url)
            found_edit = True
            page = read(edit_url, "edit")
            if page is None:
                continue
            # Check the real form owner, not an idnumber injected elsewhere in the page.
            form = None
            for candidate in page.select("form"):
                action = _own_profile_url(base_url, candidate.get("action", ""), "/user/edit.php", user_id, implicit_own=True)
                owners = candidate.select('input[name="id"]')
                if (action and len(owners) == 1 and owners[0].get("type", "").lower() == "hidden"
                        and owners[0].get("value") == user_id):
                    form = candidate
                    break
            if form is None:
                fail("edit_owner_mismatch", name)
                continue
            fields = form.select('input#id_idnumber[name="idnumber"]')
            if len(fields) != 1:
                fail("student_number_field_missing", name)
                continue
            if not fields[0].has_attr("readonly"):
                fail("student_number_unlocked", name)
                continue
            number = parse_student_number(str(form))
            if number:
                if diagnostics is not None:
                    diagnostics["reason"] = "success"
                return {"name": name, "student_number": number}
            fail("student_number_invalid", name)
    if not found_edit:
        return fail("edit_link_missing", name)
    return {"name": name, "student_number": ""}
