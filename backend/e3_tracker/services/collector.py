from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Set

import requests
from bs4 import BeautifulSoup

from ..shared.constants import COURSE_LINK_RE, HEADERS, TAIPEI_TZ
from .http import (
    apply_cookie,
    configure_tls,
    login_with_password,
    need_login_redirect,
    safe_request,
)
from ..shared.parsing import (
    extract_text,
    find_due_and_status_from_assign_page,
    gather_assign_links_from_list_page,
    parse_due_text_to_dt,
)
from ..shared.utils import cleanup_debug_glob


@dataclass
class CollectOptions:
    base_url: str
    scope: str = "assignment"
    course_id: Optional[int] = None
    include_completed: bool = False
    all_courses: bool = False
    all_courses_all_terms: bool = False
    username: Optional[str] = None
    password: Optional[str] = None
    moodle_session: Optional[str] = None
    cafile: Optional[str] = None
    insecure: bool = False
    timeout: int = 20
    debug: bool = False


def _current_term_labels(now: Optional[datetime] = None) -> Sequence[str]:
    now = now or datetime.now(TAIPEI_TZ)
    y = now.year
    m = now.month
    if m >= 8:
        roc = y - 1911
        sem = "上"
        seasons = ["上", "Fall", "Autumn"]
    elif m == 1:
        roc = (y - 1) - 1911
        sem = "上"
        seasons = ["上", "Fall", "Autumn"]
    else:
        roc = (y - 1) - 1911
        sem = "下"
        seasons = ["下", "Spring"]
    tags = []
    for s in seasons:
        tags.append(f"【{roc}{s}】")
        tags.append(f"【{roc} {s}】")
    return tags


def gather_my_courses(
    sess: requests.Session,
    base_url: str,
    *,
    timeout: int = 20,
    only_current_term: bool = True,
) -> List[Dict[str, Any]]:
    pages = [
        f"{base_url}/my/",
        f"{base_url}/my/courses.php",
        f"{base_url}/course/index.php?mycourses=1",
    ]
    found: Dict[int, Dict[str, str]] = {}
    current_tags = _current_term_labels()
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
                if only_current_term and not any(tag.lower() in title.lower() for tag in current_tags):
                    continue
                if cid not in found or (title and len(title) > len(found[cid]["title"])):
                    course_url = a_tag["href"] if a_tag["href"].startswith("http") else base_url.rstrip("/") + "/" + a_tag["href"].lstrip("/")
                    found[cid] = {"id": cid, "title": title, "url": course_url}
        except Exception:
            continue
    return [found[idx] for idx in sorted(found.keys())]


def _course_sort_key(item: Dict[str, Any]):
    if not item.get("due_ts"):
        return (1, float("inf"))
    return (0, item["due_ts"])


def _global_sort_key(item: Dict[str, Any]):
    if not item.get("due_ts"):
        return (item.get("course_title", ""), 1, float("inf"))
    return (item.get("course_title", ""), 0, item["due_ts"])


def _save_debug_file(path: str, text: str, created_paths: Set[str]) -> None:
    try:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)
        created_paths.add(path)
    except Exception:
        pass


def collect_assignments(options: CollectOptions) -> Dict[str, Any]:
    sess = requests.Session()
    created_debug: Set[str] = set()

    configure_tls(sess, cafile=options.cafile, insecure=options.insecure)

    if options.debug:
        cleanup_debug_glob("debug_*.html")

    login_method = None
    if options.moodle_session:
        apply_cookie(sess, options.base_url, options.moodle_session)
        login_method = "cookie"
    elif options.username and options.password:
        login_with_password(
            sess,
            options.base_url,
            options.username,
            options.password,
            timeout=options.timeout,
        )
        login_method = "password"
    else:
        raise RuntimeError("請提供 MoodleSession 或帳密以登入 E3。")

    if options.all_courses or not options.course_id:
        only_current = not bool(options.all_courses_all_terms)
        courses = gather_my_courses(
            sess,
            options.base_url,
            timeout=options.timeout,
            only_current_term=only_current,
        )
        if not courses:
            raise RuntimeError("無法取得課程清單")
    else:
        courses = [
            {
                "id": options.course_id,
                "title": f"Course {options.course_id}",
                "url": f"{options.base_url}/course/view.php?id={options.course_id}",
            }
        ]

    per_course = []
    all_results: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for course in courses:
        cid = course["id"]
        ctitle = course.get("title", f"Course {cid}")
        list_url = f"{options.base_url}/local/courseextension/index.php?courseid={cid}&scope={options.scope}"
        assign_links: List[Any] = []
        try:
            resp = safe_request(
                sess,
                "GET",
                list_url,
                headers=HEADERS,
                timeout=options.timeout,
                allow_redirects=True,
            )
            if options.debug:
                _save_debug_file(f"debug_list_{cid}.html", resp.text, created_debug)
            if need_login_redirect(resp.text):
                raise RuntimeError("尚未登入或 Cookie 過期：請重新提供有效的 MoodleSession 或帳密。")
            assign_links = gather_assign_links_from_list_page(resp.text, options.base_url)
        except RuntimeError:
            raise
        except Exception as exc:
            errors.append({"course_id": cid, "course_title": ctitle, "message": f"取得作業列表失敗：{exc}"})

        if not assign_links:
            fallback_urls = [
                f"{options.base_url}/course/view.php?id={cid}",
                f"{options.base_url}/mod/assign/index.php?id={cid}",
            ]
            for idx, url in enumerate(fallback_urls):
                try:
                    resp = safe_request(
                        sess,
                        "GET",
                        url,
                        headers=HEADERS,
                        timeout=options.timeout,
                    )
                    if options.debug:
                        _save_debug_file(f"debug_fallback_{cid}_{idx+1}.html", resp.text, created_debug)
                    more = gather_assign_links_from_list_page(resp.text, options.base_url)
                    assign_links.extend(more)
                except RuntimeError:
                    raise
                except Exception:
                    pass
            assign_links = list({url: (title, url, due) for title, url, due in assign_links}.values())

        now = datetime.now(TAIPEI_TZ)
        course_results: List[Dict[str, Any]] = []
        for idx, (title, url, due_text_from_list) in enumerate(assign_links, start=1):
            try:
                resp = safe_request(
                    sess,
                    "GET",
                    url,
                    headers=HEADERS,
                    timeout=options.timeout,
                )
                if options.debug:
                    _save_debug_file(f"debug_assign_{cid}_{idx}.html", resp.text, created_debug)
                is_complete, is_incomplete, due_dt, raw_status = find_due_and_status_from_assign_page(resp.text)
                if not due_dt and due_text_from_list:
                    due_dt = parse_due_text_to_dt(due_text_from_list)

                if is_complete and not options.include_completed:
                    continue
                if (is_incomplete is None) or (not is_incomplete and not is_complete):
                    is_incomplete = True

                if is_incomplete or (is_complete and options.include_completed):
                    if not due_dt:
                        continue
                    due_str = due_dt.astimezone(TAIPEI_TZ).strftime("%Y-%m-%d %H:%M")
                    overdue = bool(due_dt and due_dt < now)
                    due_ts = int(due_dt.timestamp())
                    item = {
                        "course_id": cid,
                        "course_title": ctitle,
                        "title": title,
                        "url": url,
                        "due_at": due_str,
                        "due_ts": due_ts,
                        "overdue": overdue,
                        "completed": bool(is_complete),
                        "raw_status_text": raw_status,
                    }
                    course_results.append(item)
                    all_results.append(item)
            except RuntimeError:
                raise
            except Exception as exc:
                all_results.append(
                    {
                        "course_id": cid,
                        "course_title": ctitle,
                        "title": title,
                        "url": url,
                        "due_at": "",
                        "overdue": "",
                        "raw_status_text": f"解析失敗：{exc}",
                    }
                )
                errors.append(
                    {
                        "course_id": cid,
                        "course_title": ctitle,
                        "assignment_title": title,
                        "message": f"解析失敗：{exc}",
                    }
                )

        course_results.sort(key=_course_sort_key)
        per_course.append(
            {
                "id": cid,
                "title": ctitle,
                "url": course.get("url"),
                "assignments": course_results,
                "detected_assign_links": len(assign_links),
            }
        )

    all_results.sort(key=_global_sort_key)

    return {
        "courses": per_course,
        "all_assignments": all_results,
        "debug_files": created_debug,
        "errors": errors,
        "login_method": login_method,
    }
