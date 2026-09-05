from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from html import unescape
import json
import re
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
    semester_keys: Optional[Sequence[str]] = None
    username: Optional[str] = None
    password: Optional[str] = None
    moodle_session: Optional[str] = None
    cafile: Optional[str] = None
    insecure: bool = False
    timeout: int = 20
    debug: bool = False


def current_semester_key(now: Optional[datetime] = None) -> str:
    now = now or datetime.now(TAIPEI_TZ)
    if now.month >= 8:
        return f"{now.year - 1911}-1"
    if now.month == 1:
        return f"{now.year - 1912}-1"
    return f"{now.year - 1912}-2"


def parse_course_semester(title: Any, now: Optional[datetime] = None) -> Dict[str, Any]:
    text = str(title or "").strip()
    current_key = current_semester_key(now)
    year: Optional[int] = None
    term: Optional[str] = None

    roc_match = re.search(
        r"(?<!\d)(?:[\[【(（]\s*)?(\d{2,3})(?!\d)\s*(上|下|暑)(?:學期)?\s*(?:[\]】)）])?",
        text,
        re.IGNORECASE,
    )
    if roc_match:
        year = int(roc_match.group(1))
        term = {"上": "1", "下": "2", "暑": "summer"}.get(roc_match.group(2))
    if year is None:
        academic_match = re.search(
            r"(?<!\d)(\d{2,3})\s*學年度\s*(?:第\s*)?([12])\s*學期",
            text,
            re.IGNORECASE,
        )
        if academic_match:
            year = int(academic_match.group(1))
            term = academic_match.group(2)
    if year is None:
        roc_season_match = re.search(
            r"(?<!\d)(?:[\[【(（]\s*)?(\d{2,3})(?!\d)\s*(Fall|Autumn|Spring|Summer)\b",
            text,
            re.IGNORECASE,
        )
        if roc_season_match:
            year = int(roc_season_match.group(1))
            season = roc_season_match.group(2).lower()
            term = "1" if season in {"fall", "autumn"} else ("2" if season == "spring" else "summer")
    if year is None:
        numeric_match = re.search(r"(?<!\d)(\d{2,3})\s*[-/]\s*([12])(?!\d)", text)
        if numeric_match:
            year = int(numeric_match.group(1))
            term = numeric_match.group(2)
    if year is None:
        western_match = re.search(r"(?<!\d)(20\d{2})\s*[-/]?\s*(Fall|Autumn|Spring|Summer)\b", text, re.IGNORECASE)
        if western_match:
            calendar_year = int(western_match.group(1))
            season = western_match.group(2).lower()
            year = calendar_year - 1911 if season in {"fall", "autumn"} else calendar_year - 1912
            term = "1" if season in {"fall", "autumn"} else ("2" if season == "spring" else "summer")

    if year is None or term is None:
        return {
            "key": "other",
            "label": "其他課程",
            "sort_key": (-1, -1),
            "is_current": False,
        }

    term_label = {"1": "上學期", "2": "下學期", "summer": "暑期"}[term]
    key = f"{year}-{term}"
    term_order = {"1": 1, "2": 2, "summer": 3}[term]
    return {
        "key": key,
        "label": f"{year} {term_label}",
        "sort_key": (year, term_order),
        "is_current": key == current_key,
    }


def normalize_semester_keys(value: Any) -> List[str]:
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, (list, tuple, set)):
        return []
    normalized: List[str] = []
    for item in value:
        key = str(item or "").strip().lower()
        if key != "other" and not re.fullmatch(r"\d{2,3}-(?:1|2|summer)", key):
            continue
        if key not in normalized:
            normalized.append(key)
    return normalized[:12]


def _semester_catalog(courses: Sequence[Dict[str, Any]], now: Optional[datetime] = None) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for course in courses:
        semester = parse_course_semester(course.get("title"), now)
        course.update(semester_key=semester["key"], semester_label=semester["label"])
        entry = grouped.setdefault(
            semester["key"],
            {
                "key": semester["key"],
                "label": semester["label"],
                "course_count": 0,
                "is_current": semester["is_current"],
                "sort_key": semester["sort_key"],
            },
        )
        entry["course_count"] += 1
    ordered = sorted(grouped.values(), key=lambda item: item["sort_key"], reverse=True)
    for item in ordered:
        item.pop("sort_key", None)
    return ordered


def annotate_result_semesters(
    result: Optional[Dict[str, Any]],
    *,
    selected_keys: Optional[Sequence[str]] = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(result, dict):
        return result
    courses = result.get("courses")
    if not isinstance(courses, list):
        courses = []
    catalog = _semester_catalog([course for course in courses if isinstance(course, dict)])
    course_semesters = {
        str(course.get("id")): (course.get("semester_key"), course.get("semester_label"))
        for course in courses
        if isinstance(course, dict)
    }
    for item in result.get("all_assignments") or []:
        if not isinstance(item, dict):
            continue
        key, label = course_semesters.get(str(item.get("course_id")), (None, None))
        if not key:
            semester = parse_course_semester(item.get("course_title"))
            key, label = semester["key"], semester["label"]
        item["semester_key"] = key
        item["semester_label"] = label
    if not isinstance(result.get("available_semesters"), list) or (not result.get("available_semesters") and catalog):
        result["available_semesters"] = catalog
    selected = normalize_semester_keys(selected_keys)
    if not selected:
        selected = normalize_semester_keys(result.get("selected_semesters"))
    if not selected:
        selected = [item["key"] for item in catalog]
    result["selected_semesters"] = selected
    return result


def _extract_moodle_sesskey(html_text: str) -> Optional[str]:
    if not html_text:
        return None
    decoded_html = unescape(html_text)
    patterns = (
        r'"sesskey"\s*:\s*"([^"\\]+)"',
        r"'sesskey'\s*:\s*'([^'\\]+)'",
        r"data-sesskey\s*=\s*[\"']([^\"']+)[\"']",
        r"[?&]sesskey=([A-Za-z0-9_-]+)",
    )
    for pattern in patterns:
        match = re.search(pattern, decoded_html, re.IGNORECASE)
        if match:
            return unescape(match.group(1)).strip()
    soup = BeautifulSoup(decoded_html, "html.parser")
    sesskey_input = soup.find("input", attrs={"name": "sesskey"})
    if sesskey_input and sesskey_input.get("value"):
        return str(sesskey_input["value"]).strip()
    return None


def _merge_course(
    found: Dict[int, Dict[str, Any]],
    *,
    course_id: Any,
    title: Any,
    url: Any,
    base_url: str,
) -> None:
    try:
        cid = int(course_id)
    except (TypeError, ValueError):
        return
    normalized_title = unescape(str(title or "")).strip()
    normalized_url = str(url or "").strip()
    if not normalized_url:
        normalized_url = f"{base_url.rstrip('/')}/course/view.php?id={cid}"
    elif not normalized_url.startswith("http"):
        normalized_url = base_url.rstrip("/") + "/" + normalized_url.lstrip("/")
    existing = found.get(cid)
    if existing and len(str(existing.get("title") or "")) > len(normalized_title):
        return
    found[cid] = {
        "id": cid,
        "title": normalized_title or (str(existing.get("title")) if existing else f"Course {cid}"),
        "url": normalized_url,
    }


def _gather_timeline_courses(
    sess: requests.Session,
    base_url: str,
    sesskey: str,
    *,
    timeout: int,
) -> List[Dict[str, Any]]:
    found: Dict[int, Dict[str, Any]] = {}

    def collect_payload(payload: Any) -> None:
        if not isinstance(payload, list):
            return
        for result in payload:
            if not isinstance(result, dict) or result.get("error"):
                continue
            data = result.get("data")
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except (TypeError, ValueError):
                    continue
            if isinstance(data, dict):
                courses = data.get("courses") or []
            elif isinstance(data, list):
                courses = data
            else:
                courses = []
            for course in courses:
                if not isinstance(course, dict):
                    continue
                _merge_course(
                    found,
                    course_id=course.get("id") or course.get("courseid"),
                    title=(
                        course.get("fullname")
                        or course.get("displayname")
                        or course.get("fullnamedisplay")
                        or course.get("shortname")
                    ),
                    url=course.get("viewurl") or course.get("url"),
                    base_url=base_url,
                )

    def call_ajax(method_name: str, args: Dict[str, Any]) -> None:
        endpoint = f"{base_url.rstrip('/')}/lib/ajax/service.php?sesskey={sesskey}"
        response = safe_request(
            sess,
            "POST",
            endpoint,
            headers={**HEADERS, "Accept": "application/json", "Content-Type": "application/json"},
            json=[{"index": 0, "methodname": method_name, "args": args}],
            timeout=timeout,
        )
        collect_payload(response.json())

    timeline_method = "core_course_get_enrolled_courses_by_timeline_classification"
    common_args = {"offset": 0, "limit": 0, "sort": "fullname"}
    try:
        call_ajax(timeline_method, {**common_args, "classification": "allincludinghidden"})
    except Exception:
        pass
    if found:
        return list(found.values())

    for classification in ("all", "past", "inprogress", "future", "hidden"):
        try:
            call_ajax(timeline_method, {**common_args, "classification": classification})
        except Exception:
            continue

    try:
        call_ajax(
            "core_course_get_recent_courses",
            {"userid": 0, "limit": 0, "offset": 0, "sort": "fullname"},
        )
    except Exception:
        pass
    return list(found.values())


def _gather_embedded_courses(html_text: str, base_url: str) -> List[Dict[str, Any]]:
    if not html_text:
        return []
    found: Dict[int, Dict[str, Any]] = {}
    soup = BeautifulSoup(html_text, "html.parser")
    for attr_name in ("data-course-id", "data-courseid"):
        for element in soup.find_all(attrs={attr_name: True}):
            title = (
                element.get("data-course-name")
                or element.get("data-fullname")
                or element.get("aria-label")
                or extract_text(element)
            )
            _merge_course(
                found,
                course_id=element.get(attr_name),
                title=title,
                url=None,
                base_url=base_url,
            )

    decoded_html = unescape(html_text)
    json_course_pattern = re.compile(
        r'["\'](?:id|courseid)["\']\s*:\s*["\']?(\d+)["\']?'
        r'.{0,600}?["\'](?:fullname|displayname|shortname)["\']\s*:\s*["\']([^"\']+)',
        re.IGNORECASE | re.DOTALL,
    )
    for match in json_course_pattern.finditer(decoded_html):
        title = match.group(2).replace(r"\/", "/")
        if parse_course_semester(title)["key"] == "other":
            continue
        _merge_course(
            found,
            course_id=match.group(1),
            title=title,
            url=None,
            base_url=base_url,
        )
    return list(found.values())


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
    if not only_current_term:
        pages.extend(
            [
                f"{base_url}/my/courses.php?classification=all",
                f"{base_url}/my/courses.php?classification=past",
                f"{base_url}/my/courses.php?classification=hidden",
                f"{base_url}/grade/report/overview/index.php",
                f"{base_url}/calendar/view.php?view=month",
                f"{base_url}/calendar/export.php",
            ]
        )
    found: Dict[int, Dict[str, Any]] = {}
    current_key = current_semester_key()
    sesskey: Optional[str] = None
    for url in pages:
        try:
            resp = safe_request(sess, "GET", url, headers=HEADERS, timeout=timeout)
            if not sesskey:
                sesskey = _extract_moodle_sesskey(resp.text)
            for course in _gather_embedded_courses(resp.text, base_url):
                course_title = str(course.get("title") or "")
                if only_current_term and parse_course_semester(course_title)["key"] != current_key:
                    continue
                _merge_course(
                    found,
                    course_id=course.get("id"),
                    title=course_title,
                    url=course.get("url"),
                    base_url=base_url,
                )
            soup = BeautifulSoup(resp.text, "html.parser")
            for a_tag in soup.find_all("a", href=True):
                match = COURSE_LINK_RE.search(a_tag["href"])
                if not match:
                    continue
                cid = int(match.group(1))
                title = extract_text(a_tag)
                if only_current_term and parse_course_semester(title)["key"] != current_key:
                    continue
                _merge_course(
                    found,
                    course_id=cid,
                    title=title,
                    url=a_tag["href"],
                    base_url=base_url,
                )
        except Exception:
            continue
    if not only_current_term and sesskey:
        try:
            for course in _gather_timeline_courses(sess, base_url, sesskey, timeout=timeout):
                _merge_course(
                    found,
                    course_id=course.get("id"),
                    title=course.get("title"),
                    url=course.get("url"),
                    base_url=base_url,
                )
        except Exception:
            pass
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

    available_semesters = _semester_catalog(courses)
    requested_semesters = normalize_semester_keys(options.semester_keys)
    if options.semester_keys is not None:
        selected_set = set(requested_semesters)
        selected_courses = [course for course in courses if course.get("semester_key") in selected_set]
        if not selected_courses and requested_semesters == [current_semester_key()]:
            fallback = next((item["key"] for item in available_semesters if item["key"] != "other"), None)
            if fallback:
                requested_semesters = [fallback]
                selected_set = {fallback}
                selected_courses = [course for course in courses if course.get("semester_key") in selected_set]
        courses = selected_courses
    else:
        requested_semesters = [item["key"] for item in available_semesters]

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
                    # When falling back, we need to merge results carefully
                    existing_urls = {link[1] for link in assign_links}
                    assign_links.extend([link for link in more if link[1] not in existing_urls])
                except RuntimeError:
                    raise
                except Exception:
                    pass
        
        now = datetime.now(TAIPEI_TZ)
        course_results: List[Dict[str, Any]] = []
        for idx, (title, url, due_text_from_list, submitted_count, participant_count) in enumerate(
            assign_links, start=1
        ):
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
                
                is_complete, is_incomplete, due_dt, raw_status, grade_text, submitted_dt, remaining_text = find_due_and_status_from_assign_page(resp.text)
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
                    submitted_ts = int(submitted_dt.timestamp()) if submitted_dt else None
                    item = {
                        "course_id": cid,
                        "course_title": ctitle,
                        "semester_key": course.get("semester_key", "other"),
                        "semester_label": course.get("semester_label", "其他課程"),
                        "title": title,
                        "url": url,
                        "due_at": due_str,
                        "due_ts": due_ts,
                        "submitted_at": submitted_dt.astimezone(TAIPEI_TZ).strftime("%Y-%m-%d %H:%M") if submitted_dt else None,
                        "submitted_ts": submitted_ts,
                        "remaining_text": remaining_text,
                        "overdue": overdue,
                        "completed": bool(is_complete),
                        "raw_status_text": raw_status,
                        "grade_text": grade_text,
                        "submitted_count": submitted_count,
                        "participant_count": participant_count,
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
                        "semester_key": course.get("semester_key", "other"),
                        "semester_label": course.get("semester_label", "其他課程"),
                        "title": title,
                        "url": url,
                        "due_at": "",
                        "due_ts": None,
                        "submitted_at": None,
                        "submitted_ts": None,
                        "remaining_text": None,
                        "overdue": False,
                        "completed": False,
                        "raw_status_text": f"解析失敗：{exc}",
                        "grade_text": None,
                        "submitted_count": None,
                        "participant_count": None,
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
                "semester_key": course.get("semester_key", "other"),
                "semester_label": course.get("semester_label", "其他課程"),
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
        "available_semesters": available_semesters,
        "selected_semesters": requested_semesters,
    }


def merge_current_semester_cache(
    previous_result: Optional[Dict[str, Any]],
    refreshed_result: Optional[Dict[str, Any]],
    *,
    selected_keys: Optional[Sequence[str]] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    previous = deepcopy(previous_result) if isinstance(previous_result, dict) else {}
    refreshed = deepcopy(refreshed_result) if isinstance(refreshed_result, dict) else {}
    annotate_result_semesters(previous)
    annotate_result_semesters(refreshed)
    current_key = current_semester_key(now)

    merged_by_id: Dict[int, Dict[str, Any]] = {}
    for course in previous.get("courses") or []:
        if not isinstance(course, dict) or course.get("semester_key") == current_key:
            continue
        try:
            course_id = int(course.get("id"))
        except (TypeError, ValueError):
            continue
        merged_by_id[course_id] = course

    for course in refreshed.get("courses") or []:
        if not isinstance(course, dict):
            continue
        try:
            course_id = int(course.get("id"))
        except (TypeError, ValueError):
            continue
        semester = parse_course_semester(course.get("title"), now)
        course["semester_key"] = semester["key"]
        course["semester_label"] = semester["label"]
        merged_by_id[course_id] = course

    def course_order(course: Dict[str, Any]) -> Any:
        semester = parse_course_semester(course.get("title"), now)
        year, term = semester["sort_key"]
        return (-year, -term, str(course.get("title") or ""))

    courses = sorted(merged_by_id.values(), key=course_order)
    all_assignments: List[Dict[str, Any]] = []
    for course in courses:
        semester_key = str(course.get("semester_key") or "other")
        semester_label = str(course.get("semester_label") or "其他課程")
        course_id = course.get("id")
        course_title = str(course.get("title") or f"Course {course_id}")
        assignments = [item for item in (course.get("assignments") or []) if isinstance(item, dict)]
        for item in assignments:
            item["course_id"] = course_id
            item["course_title"] = course_title
            item["semester_key"] = semester_key
            item["semester_label"] = semester_label
        assignments.sort(key=_course_sort_key)
        course["assignments"] = assignments
        all_assignments.extend(assignments)
    all_assignments.sort(key=_global_sort_key)

    catalog = _semester_catalog(courses, now)
    available_keys = {item["key"] for item in catalog}
    selected = normalize_semester_keys(selected_keys)
    if not selected:
        selected = normalize_semester_keys(previous.get("selected_semesters"))
    selected = [key for key in selected if key in available_keys]
    if not selected:
        selected = [current_key] if current_key in available_keys else [item["key"] for item in catalog[:1]]

    result = dict(refreshed)
    result["courses"] = courses
    result["all_assignments"] = all_assignments
    result["available_semesters"] = catalog
    result["selected_semesters"] = selected
    return result
