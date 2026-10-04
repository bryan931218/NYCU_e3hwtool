"""Read the signed-in user's name from E3's own profile page."""
import re
from urllib.parse import parse_qs, urljoin, urlsplit

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
    if not field or not field.has_attr("disabled") or not field.has_attr("readonly"):
        return ""
    value = str(field.get("value") or "").strip()
    return value if re.fullmatch(r"[0-9]{9}", value) else ""


def _own_profile_url(base_url: str, href: str, path: str, user_id=None) -> str:
    url = urljoin(base_url.rstrip("/") + "/", href)
    base, target = urlsplit(base_url), urlsplit(url)
    ids = parse_qs(target.query).get("id", [])
    if (
        (target.scheme, target.netloc) != (base.scheme, base.netloc)
        or target.username or target.password or target.path != path
        or len(ids) != 1 or not re.fullmatch(r"[0-9]+", ids[0])
        or (user_id is not None and ids[0] != user_id)
    ):
        return ""
    return url


def fetch_session_identity(sess, base_url: str, *, timeout: int = 8) -> dict:
    """Follow only the authenticated menu's own profile and its locked idnumber."""
    empty = {"name": "", "student_number": ""}

    def read(url):
        response = safe_request(sess, "GET", url, headers=HEADERS,
                                timeout=timeout, allow_redirects=False)
        return BeautifulSoup(response.text, "html.parser") if response.status_code == 200 else None

    dashboard = read(base_url.rstrip("/") + "/my/")
    link = dashboard.select_one('.usermenu a[href*="/user/profile.php"]') if dashboard else None
    profile_url = _own_profile_url(base_url, link.get("href", ""), "/user/profile.php") if link else ""
    if not profile_url:
        return empty
    user_id = parse_qs(urlsplit(profile_url).query)["id"][0]
    profile = read(profile_url)
    if profile is None:
        return empty
    name = parse_profile_name(str(profile))
    # Never construct an edit URL from a caller-provided Moodle id.
    for edit in profile.select('a[href*="/user/edit.php"]'):
        edit_url = _own_profile_url(base_url, edit.get("href", ""), "/user/edit.php", user_id)
        if edit_url:
            form = read(edit_url)
            number = parse_student_number(str(form)) if form is not None else ""
            return {"name": name, "student_number": number}
    return {"name": name, "student_number": ""}
