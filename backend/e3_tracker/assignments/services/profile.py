"""Read the signed-in user's name from E3's own profile page."""
import re
import time
import unicodedata
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
    heading = soup.select_one("#page-header h1, .page-context-header h1, #page-header h2")
    if not heading:
        return ""
    # E3 may put the department before or after the person's name.
    labels = {"登入", "登出", "關於我", "個人資料", "使用者資料", "焦點綜覽", "錯誤", "錯誤訊息", "姓名"}
    text = unicodedata.normalize('NFKC', heading.get_text(" ", strip=True))
    candidates = re.findall(
        r"(?<![\u3400-\u9fff])([\u3400-\u9fff]{2,8})(?![\u3400-\u9fff])",
        text,
    )
    names = list(dict.fromkeys(name for name in candidates
                             if name not in labels and not is_academic_unit_name(name)
                             and not re.fullmatch(r'[一二三四五六七八九十0-9]+年級', name)))
    if names:
        return names[0] if len(names) == 1 else ""
    # International students may have no Chinese name at all.
    if (re.fullmatch(r"[A-Za-z\u00c0-\u02af][A-Za-z\u00c0-\u02af .,'’-]{1,126}", text)
            and text.casefold() not in {'login', 'logout', 'profile', 'user profile', 'error', 'dashboard'}):
        return text
    return ""


def profile_surname(name: str) -> str:
    return name[:2] if name[:2] in COMPOUND_SURNAMES else name[:1]


def parse_profile_surname(html: str) -> str:
    return profile_surname(parse_profile_name(html))


def fetch_profile_name(sess, base_url: str, *, timeout: int = 8) -> str:
    return fetch_session_identity(sess, base_url, timeout=timeout, include_number=False)['name']


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
    if path == '/user/profile.php' and set(query) - {'id', 'course'}:
        return ''
    if path == "/user/edit.php":
        # Only read profile forms; do not follow GET actions such as email cancellation.
        if set(query) - {"id", "course", "returnto"}:
            return ""
        if "course" in query and (len(query["course"]) != 1 or not re.fullmatch(r"[0-9]+", query["course"][0])):
            return ""
        if "returnto" in query and (len(query["returnto"]) != 1 or query["returnto"][0] not in {"profile", "preferences", ""}):
            return ""
    if not ids and implicit_own and (user_id is not None or path == '/user/profile.php'):
        return url
    if len(ids) != 1 or not re.fullmatch(r"[0-9]+", ids[0]) or (user_id is not None and ids[0] != user_id):
        return ""
    return url


def fetch_session_identity(sess, base_url: str, *, timeout: int = 8, diagnostics=None, include_number=True) -> dict:
    """Follow only the authenticated menu's own profile and its locked idnumber."""
    empty = {"name": "", "student_number": ""}
    deadline = time.monotonic() + min(max(timeout, 1) * 3, 16)

    def fail(reason, name=""):
        if diagnostics is not None:
            diagnostics["reason"] = reason
        return {"name": name, "student_number": ""}

    def read(url, stage):
        for _ in range(3):
            budget = deadline - time.monotonic()
            if budget <= 0:
                fail('upstream_timeout')
                return None
            try:
                response = safe_request(sess, "GET", url, headers=HEADERS,
                                        timeout=min(timeout, budget), allow_redirects=False)
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                fail("access_denied" if status == 403 else stage + "_unavailable")
                return None
            except requests.Timeout:
                fail('upstream_timeout')
                return None
            except (requests.RequestException, RuntimeError):
                fail('upstream_request')
                return None
            if response.status_code not in {301, 302, 303, 307, 308}:
                break
            location = response.headers.get("Location", "")
            target = urlsplit(urljoin(url, location)) if isinstance(location, str) and location else None
            if target and target.path == '/login/index.php':
                fail('login_required')
                return None
            current = urlsplit(url)
            valid = False
            if target and (target.scheme, target.netloc) == (current.scheme, current.netloc) and not target.fragment:
                if stage == 'dashboard':
                    valid = target.path in {'/my/', '/my/index.php'} and not target.query
                else:
                    owner = parse_qs(current.query).get('id', [user_id])[0]
                    valid = bool(_own_profile_url(base_url, target.geturl(), current.path, owner, implicit_own=True))
                    if owner is None and parse_qs(target.query).get('id'):
                        valid = False
            if not valid or target.geturl() == url:
                fail(stage + '_unavailable')
                return None
            url = target.geturl()
        else:
            fail(stage + '_unavailable')
            return None
        if response.status_code != 200:
            if diagnostics is not None:
                diagnostics["reason"] = stage + "_unavailable"
            return None
        soup = BeautifulSoup(response.text, "html.parser")
        if soup.select_one('form[action*="/login/index.php"]') or soup.select_one('input[name="logintoken"]'):
            if diagnostics is not None:
                diagnostics["reason"] = "login_required"
            return None
        if soup.select_one('.errorcode, .errorbox, [data-region="error-message"]'):
            fail(stage + '_unavailable')
            return None
        return soup

    dashboard = read(base_url.rstrip("/") + "/my/", "dashboard")
    if dashboard is None:
        return empty
    profile_links = [url for link in dashboard.select('.usermenu a[href*="/user/profile.php"]')
                     if (url := _own_profile_url(base_url, link.get('href', ''), '/user/profile.php', implicit_own=True))]
    profile_url = next((url for url in profile_links if parse_qs(urlsplit(url).query).get('id')), profile_links[0] if profile_links else '')
    if not profile_url:
        return fail("profile_link_missing")
    user_id = parse_qs(urlsplit(profile_url).query).get('id', [None])[0]
    profile = read(profile_url, "profile")
    if profile is None:
        return empty
    name = parse_profile_name(str(profile))
    if not include_number:
        if diagnostics is not None:
            diagnostics['reason'] = 'success' if name else 'name_missing'
        return {"name": name, "student_number": ""}
    if user_id is None:
        return fail('profile_owner_missing', name)
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
                # E3 repeats Moodle's hidden owner field. Accept duplicates only
                # when every field independently identifies this same profile.
                if action and owners and all(
                    owner.get("type", "").lower() == "hidden"
                    and owner.get("value") == user_id for owner in owners
                ):
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
