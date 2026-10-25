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
from e3_tracker.assignments.domain.student_directory import academic_year


COMPOUND_SURNAMES = frozenset((
    "歐陽", "司馬", "上官", "諸葛", "夏侯", "司徒", "司空", "皇甫",
    "尉遲", "公孫", "慕容", "宇文", "令狐", "長孫", "南宮", "東方",
    "西門", "獨孤", "聞人", "澹臺", "申屠", "仲孫", "軒轅", "端木",
))


PROFILE_HEADINGS = '#page-header h1, .page-context-header h1, #page-header h2, .userprofile h1'
_CJK = '\u3400-\u9fff\U00020000-\U0002fa1f'


def _name_from_text(value):
    # E3 may put the department before or after the person's name.
    labels = {"登入", "登出", "關於我", "個人資料", "使用者資料", "焦點綜覽", "錯誤", "錯誤訊息", "姓名", "學士後", "學士", "碩士", "博士", "研究所"}
    text = unicodedata.normalize('NFKC', value).strip()
    program_heading = re.fullmatch(rf'[{_CJK}]{{2,30}}\s*/\s*[A-Z][A-Z0-9_-]{{1,30}}(?:\s+(.+))?', text)
    if program_heading:
        return _name_from_text(program_heading.group(1) or '')
    candidates = re.findall(
        rf"(?<![{_CJK}])([{_CJK}]{{2,12}})(?![{_CJK}])",
        text,
    )
    names = list(dict.fromkeys(name for name in candidates
                             if name not in labels and not is_academic_unit_name(name)
                             and not re.fullmatch(r'(?:[一二三四五六七八九十0-9]+年級|[大碩博研][一二三四五六七八九十]|(?:學士|碩士|博士)(?:班|在職專班))', name)))
    if names:
        return names[0] if len(names) == 1 else ""
    if re.search(rf'[{_CJK}]', text):
        # A foreign name may be separated from the Chinese department by '/'.
        parts = [_name_from_text(part) for part in re.split(r'[/|()（）]', text)
                 if part.strip() and not re.search(rf'[{_CJK}]', part)]
        parts = list(dict.fromkeys(part for part in parts if part))
        return parts[0] if len(parts) == 1 else ''
    # International students may have no Chinese name at all.
    if (2 <= len(text) <= 128 and sum(char.isalpha() for char in text) >= 2
            and all(char.isalpha() or char in " .,'’-" for char in text)
            and text.casefold() not in {'login', 'logout', 'profile', 'user profile', 'error', 'dashboard'}
            and not re.match(r'(?i)(department|college|school|institute)\b', text)):
        return text
    return ""


def _profile_fields(soup):
    # Stay in the person's profile tree, not course descriptions or arbitrary
    # footer/sidebar text. Only a small allowlist of labels is consumed below.
    for field in soup.select('.profile_tree dt, .userprofile dt'):
        value = field.find_next_sibling('dd')
        if value:
            yield unicodedata.normalize('NFKC', field.get_text(' ', strip=True)).strip(' :').casefold(), value.get_text(' ', strip=True)


def parse_profile_name(html: str) -> str:
    soup = BeautifulSoup(html, 'html.parser')
    explicit = {_name_from_text(value) for label, value in _profile_fields(soup)
                if label in {'姓名', '全名', 'name', 'full name'}} - {''}
    if explicit:
        return explicit.pop() if len(explicit) == 1 else ''
    headings = [node for node in soup.select(PROFILE_HEADINGS) if not node.find_parent(class_='description')]
    for heading in headings:
        name = _name_from_text(heading.get_text(' ', strip=True))
        if name:
            return name
    return ''


def parse_profile_metadata(html):
    """Only explicit own-profile department/year; never infer these from courses."""
    soup = BeautifulSoup(html, 'html.parser')
    details = {}
    grade_labels = {'年級', '就讀年級', 'year of study', 'study year', 'grade'}
    department_labels = {'系所', '科系', '系級', '院系', '所屬系所', 'department', 'department / institute'}
    chinese = {ch: index + 1 for index, ch in enumerate('一二三四五六七八九')}
    for label, raw in _profile_fields(soup):
        value = unicodedata.normalize('NFKC', raw).strip()
        if label in grade_labels | {'系級'}:
            grade_value = re.sub(r'^.*(?:學系|研究所|系|學位學程)\s*', '', value) if label == '系級' else value
            match = re.fullmatch(r'(?:(學士後|學士|碩士|博士|在職碩|大|碩|博|研)\s*)?([1-9]|1[0-5]|[一二三四五六七八九])(?:\s*(?:年級|年|st year|nd year|rd year|th year))?', grade_value, re.I)
            if match:
                prefix, grade = match.groups()
                details.update(grade=chinese.get(grade) or int(grade), academic_year=academic_year())
                degree = {'學士後':'2','學士':'1','大':'1','碩士':'3','碩':'3','研':'3','博士':'4','博':'4','在職碩':'5'}.get(prefix)
                if degree:
                    details['degree_code'] = degree
        if label in department_labels:
            unit = re.split(r'\s*[/|]\s*', value)[0].strip()
            unit = re.sub(r'(?:[大碩博研][一二三四五六七八九十]|[一二三四五六七八九十0-9]+年級)$', '', unit).strip()
            if is_academic_unit_name(unit) and len(unit) <= 128:
                details['department'] = unit
    return details


def _own_edit_profile_name(form):
    parts = []
    for key in ('firstname', 'lastname'):
        fields = form.select(f'input[name="{key}"]')
        if len(fields) != 1:
            return ''
        parts.append(str(fields[0].get('value') or '').strip())
    # Names may be editable in Moodle. This is a GET of the already verified
    # person's form, never a submission. Only idnumber must be auth-locked.
    # Some E3 programs use a short Chinese unit + English code as lastname,
    # with the complete personal name in firstname. The short unit may not
    # end in 系/所 and must not be treated as a second candidate person.
    if re.fullmatch(rf'[{_CJK}]{{2,30}}\s*/\s*[A-Z][A-Z0-9_-]{{1,30}}', parts[1]):
        return _name_from_text(parts[0])
    # Chinese Moodle names are lastname + firstname, while Latin names retain
    # firstname + lastname. Some E3 accounts put a department in the last name.
    cjk = any(re.search(rf'[{_CJK}]', value) for value in parts)
    order = parts[::-1] if cjk else parts
    order = [part for part in order if part and not is_academic_unit_name(part)]
    return _name_from_text(('' if cjk else ' ').join(order))


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


def fetch_session_identity(sess, base_url: str, *, timeout: int = 8, diagnostics=None, include_number=True, profile_metadata=None) -> dict:
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
    if profile_metadata is not None:
        profile_metadata.update(parse_profile_metadata(str(profile)))
    if not include_number and name:
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
            if not name:
                name = _own_edit_profile_name(form)
            if not include_number:
                if name:
                    if diagnostics is not None:
                        diagnostics['reason'] = 'success'
                    return {'name':name, 'student_number':''}
                fail('name_missing')
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
        return fail("edit_link_missing" if include_number else "name_missing", name)
    return {"name": name, "student_number": ""}
