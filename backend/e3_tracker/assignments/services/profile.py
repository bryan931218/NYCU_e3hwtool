"""Read the signed-in user's name from E3's own profile page."""
import re
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup

from e3_tracker.platform.constants import HEADERS
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
    # E3 prefixes the name with department text, e.g. "資工系 / DCP 王小明".
    name = heading.get_text(" ", strip=True).rsplit("/", 1)[-1].strip()
    match = re.search(r"(?:^|\s)([\u3400-\u9fff]{2,8})$", name)
    if not match:
        return ""
    name = match.group(1)
    if name in {"登入", "登出", "關於我", "個人資料", "使用者資料", "焦點綜覽", "錯誤", "錯誤訊息"}:
        return ""
    return name


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
