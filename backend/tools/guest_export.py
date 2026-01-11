"""
執行前請先安裝必要套件：
    pip install requests beautifulsoup4 python-dateutil pytz
"""

import getpass
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from pytz import timezone

TAIPEI_TZ = timezone("Asia/Taipei")
DEFAULT_BASE_URL = "https://e3p.nycu.edu.tw"
COURSE_LINK_RE = re.compile(r"course/view\.php\?id=(\d+)")
ASSIGN_LINK_RE = re.compile(r"/mod/assign/view\.php\?id=\d+")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
}
DUE_LABELS = [
    "截止",
    "截止日",
    "截止日期",
    "截止時間",
    "到期日",
    "結束時間",
    "Due date",
    "Due",
    "End date",
    "End Date",
    "End",
    "Cut-off date",
    "截止時間（Due）",
]
DUE_TEXT_SELECTORS = [
    ".assign-dates .value",
    ".assign-dates time",
    ".submissionduedate",
    ".duedate",
    ".date",
    "time[datetime]",
]
COMPLETED_KEYWORDS = [
    "已繳交",
    "已提交",
    "已送交",
    "已評分",
    "已提交評分",
    "submitted",
    "submitted for grading",
    "assignment was submitted",
    "was submitted",
    "submitted on",
    "graded",
]
INCOMPLETE_KEYWORDS = [
    "尚未繳交",
    "未提交",
    "尚未交",
    "草稿",
    "需要重交",
    "no submissions have been made yet",
    "no submission",
    "draft",
    "requires resubmission",
    "not submitted",
    "not yet submitted",
    "not submitted for grading",
]


@dataclass
class CollectOptions:
    base_url: str
    scope: str = "assignment"
    include_completed: bool = False
    username: Optional[str] = None
    password: Optional[str] = None
    moodle_session: Optional[str] = None
    timeout: int = 20


def safe_request(sess: requests.Session, method: str, url: str, **kwargs) -> requests.Response:
    resp = sess.request(method, url, **kwargs)
    resp.raise_for_status()
    return resp


def need_login_redirect(html_text: str) -> bool:
    return ("login/index.php" in html_text) or ("尚未登入" in html_text) or ("You are not logged in" in html_text)


def login_with_password(sess: requests.Session, base_url: str, username: str, password: str, *, timeout: int = 20) -> None:
    login_url = f"{base_url}/login/index.php"
    resp = safe_request(sess, "GET", login_url, headers=HEADERS, timeout=timeout)
    soup = BeautifulSoup(resp.text, "html.parser")
    token_input = soup.find("input", {"name": "logintoken"})
    token = token_input["value"] if token_input and token_input.has_attr("value") else ""
    payload = {"username": username, "password": password, "logintoken": token, "anchor": ""}
    resp = safe_request(sess, "POST", login_url, data=payload, headers=HEADERS, timeout=timeout)
    if need_login_redirect(resp.text):
        raise RuntimeError("登入失敗：請確認帳密是否正確，或是否啟用 2FA。")


def apply_cookie(sess: requests.Session, base_url: str, moodle_session_value: str) -> None:
    domain = re.sub(r"^https?://", "", base_url).split("/")[0]
    sess.cookies.set("MoodleSession", moodle_session_value, domain=domain)


def gather_my_courses(sess: requests.Session, base_url: str, *, timeout: int = 20) -> List[Dict[str, Any]]:
    pages = [
        f"{base_url}/my/",
        f"{base_url}/my/courses.php",
        f"{base_url}/course/index.php?mycourses=1",
    ]
    found: Dict[int, Dict[str, str]] = {}
    for url in pages:
        try:
            resp = safe_request(sess, "GET", url, headers=HEADERS, timeout=timeout)
            soup = BeautifulSoup(resp.text, "html.parser")
            for a_tag in soup.find_all("a", href=True):
                match = COURSE_LINK_RE.search(a_tag["href"])
                if not match:
                    continue
                cid = int(match.group(1))
                title = extract_text(a_tag)
                if cid not in found or (title and len(title) > len(found[cid]["title"])):
                    course_url = a_tag["href"] if a_tag["href"].startswith("http") else base_url.rstrip("/") + "/" + a_tag["href"].lstrip("/")
                    found[cid] = {"id": cid, "title": title or f"課程 {cid}", "url": course_url}
        except Exception:
            continue
    current_labels = _current_term_labels()
    filtered = []
    for idx in sorted(found.keys()):
        item = found[idx]
        title = item.get("title", "")
        normalized = title.strip()
        if not normalized.startswith("【"):
            continue
        if not any(lbl.lower() in normalized.lower() for lbl in current_labels):
            continue
        filtered.append(item)
    return filtered


def _current_term_labels(now: Optional[datetime] = None) -> Sequence[str]:
    now = now or datetime.now(TAIPEI_TZ)
    y = now.year
    m = now.month
    if m >= 8:
        roc = y - 1911
        seasons = ["上", "Fall", "Autumn"]
    elif m == 1:
        roc = (y - 1) - 1911
        seasons = ["上", "Fall", "Autumn"]
    else:
        roc = (y - 1) - 1911
        seasons = ["下", "Spring"]
    tags = []
    for s in seasons:
        tags.append(f"【{roc}{s}】")
        tags.append(f"【{roc} {s}】")
    return tags


def extract_text(el) -> str:
    return re.sub(r"\s+", " ", el.get_text(strip=True)) if el else ""


def _is_placeholder_title(title: str) -> bool:
    if not title:
        return True
    normalized = title.strip().lower()
    if len(normalized) <= 2:
        return True
    compact = re.sub(r"[\s\[\]]+", "", normalized)
    return compact in {"view", "檢視", "查看", "assignment", "作業"}


def parse_due_text_to_dt(due_text: Optional[str]):
    if not due_text:
        return None
    try:
        value = dtparser.parse(due_text, dayfirst=False, fuzzy=True)
        if value.tzinfo is None:
            return TAIPEI_TZ.localize(value)
        return value.astimezone(TAIPEI_TZ)
    except Exception:
        return None


def gather_assign_links_from_list_page(html: str, base_url: str) -> List[Tuple[str, str, Optional[str]]]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[Tuple[str, str, Optional[str]]] = []
    date_pattern = re.compile(r"\d{4}[/-]\d{1,2}[/-]\d{1,2}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?")

    for table in soup.find_all("table"):
        header_row = table.find("tr")
        headers: Sequence[str] = [extract_text(th).strip() for th in header_row.find_all(["th", "td"])] if header_row else []
        due_col_idx = None
        for idx, header in enumerate(headers):
            if any(lbl.lower() in header.lower() for lbl in DUE_LABELS):
                due_col_idx = idx
                break

        for tr in table.find_all("tr"):
            target = None
            for a_tag in tr.find_all("a", href=True):
                if ASSIGN_LINK_RE.search(a_tag["href"]):
                    target = a_tag
                    break
            if not target:
                continue
            href = target["href"]
            url = href if href.startswith("http") else base_url.rstrip("/") + "/" + href.lstrip("/")
            title = extract_text(target) or "未命名作業"
            if _is_placeholder_title(title):
                alt_title = target.get("data-activityname") or target.get("aria-label") or target.get("title")
                if alt_title:
                    title = extract_text(BeautifulSoup(str(alt_title), "html.parser"))
            cells = tr.find_all(["td", "th"])
            if _is_placeholder_title(title):
                if cells:
                    guessed = extract_text(cells[0])
                    if guessed:
                        title = guessed
            due_text = None
            if due_col_idx is not None:
                if len(cells) > due_col_idx:
                    due_text = extract_text(cells[due_col_idx])
            if not due_text:
                row_text = extract_text(tr)
                match = date_pattern.search(row_text)
                if match:
                    due_text = match.group(0)
            links.append((title, url, due_text))
    return links


def _find_due_text_from_html(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    for selector in DUE_TEXT_SELECTORS:
        for el in soup.select(selector):
            text = extract_text(el)
            if text:
                return text
    # look for labels inside text nodes
    label_pattern = re.compile("|".join(re.escape(lbl) for lbl in DUE_LABELS), re.IGNORECASE)
    for node in soup.find_all(string=label_pattern):
        parent = node.parent
        if not parent:
            continue
        # try sibling with actual date value
        sibling = parent.find_next_sibling()
        if sibling:
            text = extract_text(sibling)
            if text:
                return text
        # or next td in same row
        if parent.name in {"th", "td"}:
            row = parent.parent
            if row:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    idx = cells.index(parent)
                    if idx + 1 < len(cells):
                        text = extract_text(cells[idx + 1])
                        if text:
                            return text
    return None


def find_due_and_status_from_assign_page(html: str) -> Tuple[bool, Optional[bool], Optional[datetime], str]:
    soup = BeautifulSoup(html, "html.parser")
    status_cell_text = ""
    due_str = None

    for tr in soup.find_all("tr"):
        th = tr.find(["th", "td"])
        tds = tr.find_all("td")
        label = extract_text(th).lower() if th else ""
        if any(lbl in label for lbl in ["submission status", "繳交狀態", "提交狀態"]):
            status_cell_text = extract_text(tds[-1]) if tds else extract_text(tr)
        if any(lbl.lower() in label for lbl in [s.lower() for s in DUE_LABELS]):
            if tds:
                due_str = extract_text(tds[-1])

    if not status_cell_text:
        for dt in soup.find_all("dt"):
            label = extract_text(dt).lower()
            if any(lbl in label for lbl in ["submission status", "繳交狀態", "提交狀態"]):
                status_cell_text = extract_text(dt.find_next_sibling("dd"))

    low_status = status_cell_text.lower()
    completed = any(word.lower() in low_status for word in COMPLETED_KEYWORDS)
    incomplete = any(word.lower() in low_status for word in INCOMPLETE_KEYWORDS)
    due_dt = parse_due_text_to_dt(due_str)
    return completed, incomplete if not completed else False, due_dt, status_cell_text or "未知"


def _format_time_diff(due_dt: datetime, now: datetime) -> str:
    diff_seconds = int((due_dt - now).total_seconds())
    overdue = diff_seconds < 0
    secs = abs(diff_seconds)
    days, rem = divmod(secs, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}天")
    if hours:
        parts.append(f"{hours}小時")
    if not days and minutes:
        parts.append(f"{minutes}分鐘")
    if not parts:
        parts.append("不足1分鐘")
    if overdue:
        return "已逾期 " + " ".join(parts)
    return "剩餘 " + " ".join(parts)


def collect_assignments(options: CollectOptions) -> Dict[str, Any]:
    sess = requests.Session()
    if options.moodle_session:
        apply_cookie(sess, options.base_url, options.moodle_session)
    elif options.username and options.password:
        login_with_password(sess, options.base_url, options.username, options.password, timeout=options.timeout)
    else:
        raise RuntimeError("請提供帳密或 MoodleSession。")

    courses = gather_my_courses(sess, options.base_url, timeout=options.timeout)
    all_results: List[Dict[str, Any]] = []
    per_course: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for course in courses:
        cid = course["id"]
        list_url = f"{options.base_url}/local/courseextension/index.php?courseid={cid}&scope={options.scope}"
        assign_links: List[Tuple[str, str, Optional[str]]] = []
        try:
            resp = safe_request(sess, "GET", list_url, headers=HEADERS, timeout=options.timeout, allow_redirects=True)
            if need_login_redirect(resp.text):
                raise RuntimeError("尚未登入或 Cookie 過期")
            assign_links = gather_assign_links_from_list_page(resp.text, options.base_url)
        except Exception as exc:
            errors.append({"course_id": cid, "course_title": course["title"], "message": f"取得列表失敗：{exc}"})
            continue

        if not assign_links:
            fallback_urls = [
                f"{options.base_url}/course/view.php?id={cid}",
                f"{options.base_url}/mod/assign/index.php?id={cid}",
            ]
            for url in fallback_urls:
                try:
                    resp = safe_request(sess, "GET", url, headers=HEADERS, timeout=options.timeout)
                    more = gather_assign_links_from_list_page(resp.text, options.base_url)
                    assign_links.extend(more)
                except Exception:
                    continue
            dedup: Dict[str, Tuple[str, str, Optional[str]]] = {}
            for title, url, due in assign_links:
                dedup[url] = (title, url, due)
            assign_links = list(dedup.values())

        now = datetime.now(TAIPEI_TZ)
        course_results: List[Dict[str, Any]] = []
        for idx, (title, url, due_text) in enumerate(assign_links, start=1):
            try:
                resp = safe_request(sess, "GET", url, headers=HEADERS, timeout=options.timeout)
                is_complete, is_incomplete, due_dt, raw_status = find_due_and_status_from_assign_page(resp.text)
                if not due_dt and due_text:
                    due_dt = parse_due_text_to_dt(due_text)
                if not due_dt:
                    fallback_due = _find_due_text_from_html(resp.text)
                    if fallback_due:
                        due_dt = parse_due_text_to_dt(fallback_due)
                if is_complete and not options.include_completed:
                    continue
                if (is_incomplete is None) or (not is_incomplete and not is_complete):
                    is_incomplete = True
                if is_incomplete or (is_complete and options.include_completed):
                    due_str = due_dt.astimezone(TAIPEI_TZ).strftime("%Y-%m-%d %H:%M") if due_dt else ""
                    overdue = bool(due_dt and due_dt < now)
                    due_ts = int(due_dt.timestamp()) if due_dt else None
                    raw_status_text = raw_status
                    if due_dt:
                        raw_status_text = _format_time_diff(due_dt, now)
                    item = {
                        "course_id": cid,
                        "course_title": course["title"],
                        "title": title,
                        "url": url,
                        "due_at": due_str,
                        "due_ts": due_ts,
                        "overdue": overdue,
                        "completed": bool(is_complete),
                        "raw_status_text": raw_status_text,
                    }
                    course_results.append(item)
                    all_results.append(item)
            except Exception as exc:
                errors.append(
                    {"course_id": cid, "course_title": course["title"], "assignment_title": title, "message": f"解析失敗：{exc}"}
                )

        per_course.append(
            {
                "id": cid,
                "title": course["title"],
                "url": course.get("url"),
                "assignments": course_results,
                "detected_assign_links": len(assign_links),
            }
        )

    return {
        "courses": per_course,
        "all_assignments": all_results,
        "errors": errors,
        "login_method": "guest_export",
    }


def interactive_prompt() -> CollectOptions:
    print("=== E3 作業匯出工具 ===")
    print(f"已預設使用站台：{DEFAULT_BASE_URL}")
    username = input("E3 帳號： ").strip()
    password = getpass.getpass("E3 密碼： ")
    if not username or not password:
        raise SystemExit("帳號或密碼不可為空。")
    return CollectOptions(base_url=DEFAULT_BASE_URL, username=username, password=password, include_completed=True)


def main():
    try:
        options = interactive_prompt()
        result = collect_assignments(options)
        payload = {
            "mode": "guest_export_v1",
            "generated_at": datetime.now(TAIPEI_TZ).isoformat(),
            "base_url": options.base_url,
            "scope": options.scope,
            "include_completed": options.include_completed,
            "result": result,
            "excel_data": None,
        }
        out_path = Path("guest_payload.json")
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n匯出完成！檔案已儲存為 {out_path.resolve()}")
        print("接著可回到訪客模式，直接上傳此 JSON 檔即可。")
        wait_before_exit()
    except Exception as exc:
        print(f"發生錯誤：{exc}", file=sys.stderr)
        wait_before_exit()
        sys.exit(1)


def wait_before_exit():
    try:
        input("\n按 Enter 鍵關閉視窗...")
    except EOFError:
        pass


if __name__ == "__main__":
    main()
