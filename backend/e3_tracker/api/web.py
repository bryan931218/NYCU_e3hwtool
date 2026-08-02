import base64
import copy
import io
import json
import math
import os
import re
import secrets
import shutil
import threading
import time
import hashlib
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from difflib import SequenceMatcher
from functools import wraps
from pathlib import Path
from statistics import median
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlsplit, urlunsplit

import requests
from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageFont, ImageOps
from flask import Flask, Response, flash, redirect, render_template_string, request, send_file, session, url_for, has_request_context
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.http import http_date
from werkzeug.utils import secure_filename

from ..services.collector import CollectOptions, collect_assignments
from ..services.google_calendar import (
    GOOGLE_CALENDAR_SCOPE,
    GoogleUnauthorizedError,
    build_google_authorize_url,
    compute_expiry,
    exchange_code_for_google_token,
    refresh_google_token,
    sync_assignments_to_google_calendar,
)
from ..services.http import login_with_password
from ..shared.config import (
    DEFAULT_OPENAI_MODEL,
    load_env_defaults,
    normalize_openai_reasoning_effort,
)
from ..shared.constants import TAIPEI_TZ
from ..shared.storage import PersistentStorage
from ..shared.study_plan_data import STUDY_PLAN_VIDEO_INVENTORY
from ..shared.source_localization import (
    SOURCE_BBOX_VERSION,
    SOURCE_PAGE_INDEX_VERSION,
    assign_transcription_to_source_sections,
    build_source_page_geometry,
    canonicalize_source_text,
    collapse_source_refs_by_image,
    detect_source_horizontal_separator_candidates,
    estimate_source_page_content_bounds,
    estimated_source_line_count,
    literal_source_evidence,
    match_source_evidence_to_lines,
    match_source_evidence_to_sections,
    match_source_evidence_via_page_alignment,
    resolve_source_evidence_page,
    source_bbox_span_is_plausible,
    source_bbox_from_lines,
    source_line_match_is_candidate,
    source_line_match_is_verified,
    source_page_alignment_match_is_candidate,
    source_page_alignment_match_is_verified,
    source_section_match_is_candidate,
    source_section_match_is_verified,
    validated_source_bbox,
)
from ..shared.study_math import (
    is_pure_math_expression,
    repair_math_delimiters,
    wrap_bare_math_candidate,
)
from ..shared.excel import build_excel
from ..shared.utils import json_safe

PASSIVE_TRAFFIC_ACTIONS = {"heartbeat", "refresh_assignments"}

ROOT_DIR = Path(__file__).resolve().parents[3]
FRONTEND_TEMPLATE_DIR = ROOT_DIR / "frontend" / "templates"
TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "web.html"
WEB_TEMPLATE = TEMPLATE_PATH.read_text(encoding="utf-8")
LOGIN_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "login.html"
LOGIN_TEMPLATE = LOGIN_TEMPLATE_PATH.read_text(encoding="utf-8")
TRAFFIC_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "admin_traffic.html"
TRAFFIC_TEMPLATE = TRAFFIC_TEMPLATE_PATH.read_text(encoding="utf-8")
ANNOUNCEMENTS_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "admin_announcements.html"
ANNOUNCEMENTS_TEMPLATE = ANNOUNCEMENTS_TEMPLATE_PATH.read_text(encoding="utf-8")
HOME_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "home.html"
HOME_TEMPLATE = HOME_TEMPLATE_PATH.read_text(encoding="utf-8")
PRIVACY_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "privacy.html"
PRIVACY_TEMPLATE = PRIVACY_TEMPLATE_PATH.read_text(encoding="utf-8")
TERMS_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "terms.html"
TERMS_TEMPLATE = TERMS_TEMPLATE_PATH.read_text(encoding="utf-8")
FEEDBACK_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "feedback.html"
FEEDBACK_TEMPLATE = FEEDBACK_TEMPLATE_PATH.read_text(encoding="utf-8")
ADMIN_FEEDBACK_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "admin_feedback.html"
ADMIN_FEEDBACK_TEMPLATE = ADMIN_FEEDBACK_TEMPLATE_PATH.read_text(encoding="utf-8")
STUDY_PLAN_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "admin_study_plan.html"
STUDY_PLAN_TEMPLATE = STUDY_PLAN_TEMPLATE_PATH.read_text(encoding="utf-8")
STUDY_SETTINGS_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "admin_study_settings.html"
STUDY_SETTINGS_TEMPLATE = STUDY_SETTINGS_TEMPLATE_PATH.read_text(encoding="utf-8")
STUDY_HOME_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "admin_study_home.html"
STUDY_HOME_TEMPLATE = STUDY_HOME_TEMPLATE_PATH.read_text(encoding="utf-8")
PUBLIC_STUDY_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "public_study_progress.html"
PUBLIC_STUDY_TEMPLATE = PUBLIC_STUDY_TEMPLATE_PATH.read_text(encoding="utf-8")
STUDY_RECALL_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "study_recall.html"
STUDY_RECALL_TEMPLATE = STUDY_RECALL_TEMPLATE_PATH.read_text(encoding="utf-8")
STUDY_UPLOAD_TRACKER_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "_study_upload_tracker.html"
STUDY_UPLOAD_TRACKER_TEMPLATE = STUDY_UPLOAD_TRACKER_TEMPLATE_PATH.read_text(encoding="utf-8")

STUDY_PLAN_BLOCKS = (
    {"subject": "線性代數", "weeks": 4, "total_minutes": 4107.8, "lesson_targets": (11, 22, 32, 42)},
    {"subject": "離散數學", "weeks": 4, "total_minutes": 4770.4, "lesson_targets": (6, 12, 17, 23)},
    {"subject": "資料結構", "weeks": 5, "total_minutes": 6590.0, "lesson_targets": (13, 26, 40, 53, 67)},
    {"subject": "演算法", "weeks": 2, "total_minutes": 1610.5, "lesson_targets": (8, 16)},
    {"subject": "作業系統", "weeks": 3, "total_minutes": 5478.3, "lesson_targets": (19, 39, 58)},
    {"subject": "計算機組織", "weeks": 5, "total_minutes": 8633.8, "lesson_targets": (17, 34, 51, 68, 78)},
)
STUDY_PLAN_START = "2026-06-29"
STUDY_PLAN_INTERLEAVED_START = "2026-07-27"
STUDY_PLAN_END = "2026-12-03"
STUDY_PLAN_SUBJECTS = ("線性代數", "離散數學", "資料結構", "作業系統", "計算機組織", "演算法")
STUDY_PLAN_DAILY_VIDEO_SECONDS = 3.5 * 60 * 60
STUDY_PLAN_PHASE_ONE_SUBJECTS = ("離散數學", "資料結構")
STUDY_PLAN_PHASE_TWO_SUBJECTS = ("作業系統", "計算機組織", "演算法")
STUDY_PLAN_WEEKEND_VIDEO_HOUR_CAP = 4.0
STUDY_PLAN_DAILY_LABELS = (
    "週一",
    "週二",
    "週三",
    "週四",
    "週五",
    "週六",
    "週日",
)
STUDY_PLAN_COMPLETE_TOLERANCE_SECONDS = 5.0
STUDY_PLAN_COMPLETE_RATIO = 0.995
STUDY_PLAN_DAY_CUTOFF_HOUR = 8
STUDY_NOTE_MAX_IMAGE_BYTES = 2 * 1024 * 1024
STUDY_NOTE_MAX_REQUEST_BYTES = 16 * 1024 * 1024
STUDY_NOTE_AI_BATCH_SIZE = 8
STUDY_NOTE_STAGING_TTL_SECONDS = 6 * 60 * 60
STUDY_UPLOAD_ANALYSIS_START_PROGRESS = 10
STUDY_UPLOAD_BATCHES_END_PROGRESS = 94


def _study_upload_time_weighted_progress(
    local_progress: int,
    *,
    batch_index: int,
    batch_count: int,
) -> int:
    """Map one batch's expensive AI work onto most of the visible progress bar."""
    safe_batch_count = max(1, int(batch_count))
    safe_batch_index = min(max(0, int(batch_index)), safe_batch_count - 1)
    local_ratio = min(1.0, max(0.0, (int(local_progress) - 20) / 80))
    overall_ratio = (safe_batch_index + local_ratio) / safe_batch_count
    progress_span = STUDY_UPLOAD_BATCHES_END_PROGRESS - STUDY_UPLOAD_ANALYSIS_START_PROGRESS
    return STUDY_UPLOAD_ANALYSIS_START_PROGRESS + round(overall_ratio * progress_span)


def _offset_study_note_batch_analysis(analysis: Dict[str, Any], image_offset: int) -> None:
    if image_offset <= 0:
        return

    def offset_image_index(item: Any) -> None:
        if not isinstance(item, dict):
            return
        try:
            item["image_index"] = int(item.get("image_index") or 0) + image_offset
        except (TypeError, ValueError):
            pass

    for page in analysis.get("source_transcription") or []:
        offset_image_index(page)
    for fragment in analysis.get("uncertain_fragments") or []:
        offset_image_index(fragment)
    for record in analysis.get("correction_records") or []:
        offset_image_index(record)
    for concept in analysis.get("key_concepts") or []:
        if not isinstance(concept, dict):
            continue
        for source_ref in concept.get("source_refs") or []:
            offset_image_index(source_ref)
            bbox = source_ref.get("bbox") if isinstance(source_ref, dict) else None
            if isinstance(bbox, dict):
                try:
                    bbox["source_image_index"] = (
                        int(bbox.get("source_image_index") or 0) + image_offset
                    )
                except (TypeError, ValueError):
                    pass
        concept["coverage_ids"] = [
            re.sub(
                r"^p(\d+)b",
                lambda match: f"p{int(match.group(1)) + image_offset}b",
                str(coverage_id),
            )
            for coverage_id in concept.get("coverage_ids") or []
        ]


def _study_plan_business_date(now: Optional[datetime] = None) -> date:
    current = now or datetime.now(TAIPEI_TZ)
    return (current - timedelta(hours=STUDY_PLAN_DAY_CUTOFF_HOUR)).date()


def _study_plan_business_day_from_timestamp(value: Any) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        # Storage timestamps are UTC even when they do not carry an explicit offset.
        # Normalising first keeps the 08:00 Taipei learning-day cutoff consistent with
        # activity events and daily snapshots.
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        parsed = parsed.astimezone(TAIPEI_TZ).replace(tzinfo=None)
        return (parsed - timedelta(hours=STUDY_PLAN_DAY_CUTOFF_HOUR)).date().isoformat()
    except ValueError:
        if len(raw) >= 10:
            return raw[:10]
        return None


def _study_plan_nonnegative_number(value: Any) -> float:
    try:
        parsed = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(parsed):
        return 0.0
    return max(0.0, parsed)


def _study_plan_video_completion(duration_seconds: Any, watched_seconds: Any) -> float:
    duration = _study_plan_nonnegative_number(duration_seconds)
    watched = _study_plan_nonnegative_number(watched_seconds)
    if duration <= 0:
        return 0.0
    if _study_plan_video_is_complete(duration, watched):
        return 100.0
    return min(100.0, watched / duration * 100)


def _study_plan_video_is_complete(duration_seconds: Any, watched_seconds: Any) -> bool:
    duration = _study_plan_nonnegative_number(duration_seconds)
    watched = _study_plan_nonnegative_number(watched_seconds)
    if duration <= 0:
        return False
    return watched >= duration - STUDY_PLAN_COMPLETE_TOLERANCE_SECONDS or watched / duration >= STUDY_PLAN_COMPLETE_RATIO


def _parse_youtube_url(value: Any) -> Optional[Dict[str, str]]:
    raw = str(value or "").strip()
    if not raw:
        return {"video_id": "", "playlist_id": "", "url": ""}
    try:
        parsed = urlsplit(raw)
    except ValueError:
        return None
    host = (parsed.hostname or "").lower().removeprefix("www.")
    if parsed.scheme not in {"http", "https"} or host not in {"youtube.com", "m.youtube.com", "youtu.be"}:
        return None
    query = parse_qs(parsed.query)
    video_id = ""
    if host == "youtu.be":
        video_id = parsed.path.strip("/").split("/", 1)[0]
    else:
        video_id = (query.get("v") or [""])[0]
        if not video_id:
            path_parts = [part for part in parsed.path.split("/") if part]
            if len(path_parts) >= 2 and path_parts[0] in {"embed", "shorts", "live"}:
                video_id = path_parts[1]
    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id or ""):
        return None
    playlist_id = (query.get("list") or [""])[0]
    if playlist_id and not re.fullmatch(r"[A-Za-z0-9_-]{10,128}", playlist_id):
        playlist_id = ""
    return {"video_id": video_id, "playlist_id": playlist_id, "url": raw}


def _study_plan_total_is_complete(target_seconds: Any, watched_seconds: Any) -> bool:
    target = _study_plan_nonnegative_number(target_seconds)
    watched = _study_plan_nonnegative_number(watched_seconds)
    if target <= 0:
        return False
    return watched >= target - STUDY_PLAN_COMPLETE_TOLERANCE_SECONDS or watched / target >= STUDY_PLAN_COMPLETE_RATIO


def _study_plan_completion_percent(target_seconds: Any, watched_seconds: Any, *, complete_override: bool = False) -> float:
    target = _study_plan_nonnegative_number(target_seconds)
    watched = _study_plan_nonnegative_number(watched_seconds)
    if target <= 0:
        return 0.0
    if complete_override or _study_plan_total_is_complete(target, watched):
        return 100.0
    return min(100.0, watched / target * 100)


def _study_plan_progress_summary(videos: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    video_rows = list(videos)
    total_target_seconds = 0.0
    total_watched_seconds = 0.0
    completed_videos = 0
    recorded_videos = 0
    for item in video_rows:
        duration_seconds = _study_plan_nonnegative_number(item.get("duration_seconds"))
        watched_seconds = min(
            _study_plan_nonnegative_number(item.get("watched_seconds")),
            duration_seconds,
        )
        total_target_seconds += duration_seconds
        total_watched_seconds += watched_seconds
        if _study_plan_video_is_complete(duration_seconds, watched_seconds):
            completed_videos += 1
        if watched_seconds > 0 or bool(str(item.get("notes") or "").strip()):
            recorded_videos += 1

    total_videos = len(video_rows)
    all_videos_complete = bool(video_rows) and completed_videos == total_videos
    return {
        "total_target_seconds": total_target_seconds,
        "total_watched_seconds": total_watched_seconds,
        "total_target": total_target_seconds / 60,
        "total_watched": total_watched_seconds / 60,
        "completion": _study_plan_completion_percent(
            total_target_seconds,
            total_watched_seconds,
            complete_override=all_videos_complete,
        ),
        "completed_videos": completed_videos,
        "recorded_videos": recorded_videos,
        "total_videos": total_videos,
        "video_completion": min(
            100.0,
            (completed_videos / total_videos * 100) if total_videos else 0.0,
        ),
        "all_videos_complete": all_videos_complete,
    }


def _study_plan_progress_week(week_rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Return the first scheduled week that the learner has not completed yet."""
    rows = list(week_rows)
    if not rows:
        return {}

    scheduled_rows = [
        row
        for row in rows
        if _study_plan_nonnegative_number(row.get("target_seconds")) > 0
    ]
    for row in scheduled_rows:
        if not _study_plan_total_is_complete(
            row.get("target_seconds"),
            row.get("watched_seconds"),
        ):
            return row

    return scheduled_rows[-1] if scheduled_rows else rows[-1]


def _study_plan_subject_is_complete(target_seconds: Any, watched_seconds: Any) -> bool:
    """Subject progress is complete only after every target second is watched."""
    target = _study_plan_nonnegative_number(target_seconds)
    watched = _study_plan_nonnegative_number(watched_seconds)
    return target > 0 and watched >= target


def _study_plan_subject_status(
    subject_weeks: Iterable[Dict[str, Any]],
    today: date,
    *,
    subject_is_complete: bool,
) -> Tuple[str, str]:
    if subject_is_complete:
        return "complete", "已達標"

    weeks = list(subject_weeks)
    today_key = today.isoformat()
    overdue_incomplete = [
        row
        for row in weeks
        if str(row.get("end") or "")
        and str(row.get("end") or "") < today_key
        and float(row.get("completion") or 0) < 100
    ]
    if overdue_incomplete:
        return "behind", "待補"

    active_week = next(
        (
            row
            for row in weeks
            if str(row.get("start") or "") <= today_key <= str(row.get("end") or "")
        ),
        None,
    )
    if active_week:
        return str(active_week.get("state") or "active"), str(active_week.get("state_label") or "進行中")

    if any(str(row.get("start") or "") > today_key for row in weeks):
        return "upcoming", "未開始"
    return "behind", "待補"


def _study_plan_progress_race(
    watched_minutes: Any,
    target_minutes_by_today: Any,
    total_target_minutes: Any,
    today_target_minutes: Any = 0,
) -> Dict[str, Any]:
    watched = _study_plan_nonnegative_number(watched_minutes)
    target = _study_plan_nonnegative_number(target_minutes_by_today)
    total = _study_plan_nonnegative_number(total_target_minutes)
    today_target = _study_plan_nonnegative_number(today_target_minutes)
    actual_percent = min(100.0, (watched / total * 100) if total else 0.0)
    target_percent = min(100.0, (target / total * 100) if total else 0.0)
    delta_minutes = watched - target
    delta_hours = delta_minutes / 60

    def format_duration(minutes: float) -> str:
        full_hours, remainder_minutes = divmod(int(round(abs(minutes))), 60)
        if full_hours and remainder_minutes:
            return f"{full_hours} 小時 {remainder_minutes} 分鐘"
        if full_hours:
            return f"{full_hours} 小時"
        return f"{remainder_minutes} 分鐘"

    delta_label = format_duration(delta_minutes)
    today_target_label = format_duration(today_target)
    within_daily_allowance = bool(
        delta_minutes < -1
        and today_target > 1
        and abs(delta_minutes) < today_target
    )
    is_behind = bool(
        delta_minutes < -1
        and not within_daily_allowance
    )

    if is_behind:
        state = "behind"
        state_label = "落後計畫"
        status_label = f"落後 {delta_label}"
        status_detail = "補足這段時間即可回到今天應有的進度"
        headline_message = "目前進度落後，"
        headline_unit = "小時待補"
    elif delta_minutes > 1:
        state = "early"
        state_label = "超前計畫"
        status_label = f"領先 {delta_label}"
        status_detail = "已超過今天應看的時間，可保留作後續緩衝"
        headline_message = "目前進度超前，"
        headline_unit = "小時領先"
    elif within_daily_allowance:
        state = "active"
        state_label = "進度正常"
        status_label = f"差距 {delta_label}"
        status_detail = f"小於今日安排的 {today_target_label}，不列為落後"
        headline_message = "目前差距仍在今天安排的時數內。"
        headline_unit = "小時今日差距"
    else:
        state = "active"
        state_label = "進度同步"
        status_label = "與計畫同步"
        status_detail = "目前已達到今天應有的觀看進度"
        headline_message = "目前與計畫進度同步。"
        headline_unit = "小時差距"

    runner_position = min(97.5, max(2.5, actual_percent))
    plan_position = min(97.5, max(2.5, target_percent))
    return {
        "watched_hours": round(watched / 60, 1),
        "target_hours": round(target / 60, 1),
        "actual_percent": round(actual_percent, 1),
        "target_percent": round(target_percent, 1),
        "delta_hours": round(delta_hours, 1),
        "delta_minutes": round(delta_minutes, 1),
        "absolute_delta_hours": round(abs(delta_hours), 1),
        "delta_label": delta_label,
        "today_target_hours": round(today_target / 60, 1),
        "today_target_label": today_target_label,
        "within_daily_allowance": within_daily_allowance,
        "state": state,
        "state_label": state_label,
        "status_label": status_label,
        "status_detail": status_detail,
        "headline_message": headline_message,
        "headline_value": f"{abs(delta_hours):.1f}",
        "headline_unit": headline_unit,
        # Keep the runner and date marker fully visible at both track edges.
        "runner_position": round(runner_position, 1),
        "plan_position": round(plan_position, 1),
        "gap_start": round(min(runner_position, plan_position), 1),
        "gap_width": round(abs(runner_position - plan_position), 1),
    }


def _study_plan_daily_recommendations(
    subject: str,
    target_seconds: float,
    watched_seconds: float,
    week_start: date,
    today: date,
    *,
    week_is_complete: bool = False,
) -> Tuple[float, float, List[Dict[str, Any]]]:
    video_hours = target_seconds / 3600 if target_seconds else 0.0
    weekly_hours = video_hours
    average_hours = weekly_hours / 7 if weekly_hours else 0.0
    weekend_hours = min(average_hours, STUDY_PLAN_WEEKEND_VIDEO_HOUR_CAP)
    weekday_hours = max(0.0, (weekly_hours - weekend_hours * 2) / 5) if weekly_hours else 0.0
    daily_targets = [weekday_hours] * 5 + [weekend_hours] * 2
    daily_rows: List[Dict[str, Any]] = []
    remaining_seconds = max(0.0, watched_seconds)
    for index, label in enumerate(STUDY_PLAN_DAILY_LABELS):
        target_hours = daily_targets[index]
        daily_target_seconds = target_hours * 3600
        if week_is_complete:
            credited_seconds = daily_target_seconds
            completion = 100.0 if daily_target_seconds else 0.0
        else:
            credited_seconds = min(max(remaining_seconds, 0.0), daily_target_seconds)
            completion = min(100.0, (credited_seconds / daily_target_seconds * 100) if daily_target_seconds else 0.0)
        remaining_seconds -= daily_target_seconds
        current_day = week_start + timedelta(days=index)
        if completion >= 100:
            if today < current_day:
                state = "early"
                state_label = "提早完成"
            else:
                state = "complete"
                state_label = "完成"
        elif completion > 0:
            if today < current_day:
                state = "early"
                state_label = "超前"
            else:
                state = "partial"
                state_label = "部分"
        elif current_day == today:
            state = "active"
            state_label = "進行中"
        elif today > current_day:
            state = "behind"
            state_label = "待補"
        else:
            state = "upcoming"
            state_label = "未開始"
        daily_rows.append(
            {
                "label": label,
                "date": current_day.isoformat(),
                "short_date": current_day.strftime("%m/%d"),
                "focus": "看影片",
                # Seconds remain the source of truth. Hours exist only for display.
                "target_seconds": daily_target_seconds,
                "credited_seconds": credited_seconds,
                "hours": round(target_hours, 2),
                "credited_hours": round(credited_seconds / 3600, 2),
                "completion": round(completion, 1),
                "state": state,
                "state_label": state_label,
            }
        )
    return round(video_hours, 1), round(weekly_hours, 1), daily_rows


def _study_plan_week_start(day: date) -> date:
    return day - timedelta(days=day.weekday())


def _study_plan_schedule_definitions(
    videos: Iterable[Dict[str, Any]],
    replan_settings: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Build fixed daily targets without moving unused time between subjects."""
    video_rows = list(videos)
    videos_by_subject: Dict[str, List[Dict[str, Any]]] = {subject: [] for subject in STUDY_PLAN_SUBJECTS}
    for video in video_rows:
        videos_by_subject.setdefault(str(video.get("subject") or ""), []).append(video)
    for subject_videos in videos_by_subject.values():
        subject_videos.sort(key=lambda item: int(item.get("sequence") or 0))

    days: Dict[str, Dict[str, Any]] = {}

    def add_day(day: date, allocations: Dict[str, float], focus: str = "") -> None:
        days[day.isoformat()] = {
            "date": day,
            "allocations": {
                subject: max(0.0, float(seconds))
                for subject, seconds in allocations.items()
                if float(seconds) > 0
            },
            "focus": focus,
        }

    # Preserve the completed linear-algebra portion of the original plan.
    linear_targets = next(
        block["lesson_targets"] for block in STUDY_PLAN_BLOCKS if block["subject"] == "線性代數"
    )
    prior_target = 0
    cursor = datetime.strptime(STUDY_PLAN_START, "%Y-%m-%d").date()
    linear_videos = videos_by_subject.get("線性代數", [])
    for lesson_target in linear_targets:
        week_videos = [
            item
            for item in linear_videos
            if prior_target < int(item.get("sequence") or 0) <= int(lesson_target)
        ]
        week_seconds = sum(_study_plan_nonnegative_number(item.get("duration_seconds")) for item in week_videos)
        remaining_week_seconds = week_seconds
        for index in range(7):
            remaining_days = 7 - index
            target_seconds = remaining_week_seconds / remaining_days if remaining_days else 0.0
            add_day(cursor + timedelta(days=index), {"線性代數": target_seconds})
            remaining_week_seconds -= target_seconds
        cursor += timedelta(days=7)
        prior_target = int(lesson_target)

    interleaved_start = datetime.strptime(STUDY_PLAN_INTERLEAVED_START, "%Y-%m-%d").date()
    linear_complete = bool(linear_videos) and all(
        _study_plan_video_is_complete(item.get("duration_seconds"), item.get("watched_seconds"))
        for item in linear_videos
    )
    transition_focus = "線代階段結束" if linear_complete else "補線代未完成影片"
    for day_offset in range((interleaved_start - cursor).days):
        add_day(cursor + timedelta(days=day_offset), {}, transition_focus)

    subject_totals = {
        subject: sum(
            _study_plan_nonnegative_number(item.get("duration_seconds"))
            for item in videos_by_subject.get(subject, [])
        )
        for subject in (*STUDY_PLAN_PHASE_ONE_SUBJECTS, *STUDY_PLAN_PHASE_TWO_SUBJECTS)
    }
    day = interleaved_start
    for phase_subjects in (STUDY_PLAN_PHASE_ONE_SUBJECTS, STUDY_PLAN_PHASE_TWO_SUBJECTS):
        phase_total = sum(subject_totals[subject] for subject in phase_subjects)
        if phase_total <= 0.001:
            continue
        phase_day_count = max(1, math.ceil(phase_total / STUDY_PLAN_DAILY_VIDEO_SECONDS))
        daily_allocations = {
            subject: subject_totals[subject] / phase_day_count
            for subject in phase_subjects
            if subject_totals[subject] > 0.001
        }
        for _ in range(phase_day_count):
            add_day(day, daily_allocations)
            day += timedelta(days=1)

    replan_start: Optional[date] = None
    replan_end: Optional[date] = None
    replan_baselines: Dict[str, float] = {}
    if replan_settings:
        try:
            replan_start = datetime.strptime(str(replan_settings.get("start_date") or ""), "%Y-%m-%d").date()
            replan_end = datetime.strptime(str(replan_settings.get("end_date") or ""), "%Y-%m-%d").date()
        except (TypeError, ValueError):
            replan_start = None
            replan_end = None
        if replan_start and replan_end and replan_end >= replan_start:
            replan_baselines = {
                str(subject): _study_plan_nonnegative_number(seconds)
                for subject, seconds in dict(replan_settings.get("baseline_by_subject") or {}).items()
                if str(subject) in STUDY_PLAN_SUBJECTS
            }
            subject_targets = {
                str(subject): _study_plan_nonnegative_number(seconds)
                for subject, seconds in dict(replan_settings.get("subject_targets") or {}).items()
                if str(subject) in STUDY_PLAN_SUBJECTS and _study_plan_nonnegative_number(seconds) > 0
            }
            weekday_seconds = max(
                60.0,
                _study_plan_nonnegative_number(replan_settings.get("weekday_minutes")) * 60,
            )
            weekend_seconds = max(
                60.0,
                _study_plan_nonnegative_number(replan_settings.get("weekend_minutes")) * 60,
            )
            for day_key in list(days):
                if days[day_key]["date"] >= replan_start:
                    del days[day_key]
            replan_days: List[date] = []
            cursor_day = replan_start
            while cursor_day <= replan_end:
                replan_days.append(cursor_day)
                cursor_day += timedelta(days=1)
            total_target = sum(subject_targets.values())
            total_weight = sum(
                weekend_seconds if item.weekday() >= 5 else weekday_seconds
                for item in replan_days
            )
            load_ratio = (total_target / total_weight) if total_weight > 0 else 0.0
            remaining_by_subject = dict(subject_targets)
            study_phases = [
                ("線性代數",),
                tuple(STUDY_PLAN_PHASE_ONE_SUBJECTS),
                tuple(STUDY_PLAN_PHASE_TWO_SUBJECTS),
            ]
            phase_index = 0
            for index, scheduled_day in enumerate(replan_days):
                base_capacity = weekend_seconds if scheduled_day.weekday() >= 5 else weekday_seconds
                day_target = base_capacity * load_ratio
                if index == len(replan_days) - 1:
                    day_target = sum(remaining_by_subject.values())
                allocations: Dict[str, float] = {}
                remaining_day = max(0.0, day_target)
                while remaining_day > 0.001 and phase_index < len(study_phases):
                    phase_subjects = [
                        subject
                        for subject in study_phases[phase_index]
                        if remaining_by_subject.get(subject, 0.0) > 0.001
                    ]
                    phase_total = sum(remaining_by_subject[subject] for subject in phase_subjects)
                    if phase_total <= 0.001:
                        phase_index += 1
                        continue
                    phase_amount = min(remaining_day, phase_total)
                    distributed = 0.0
                    for subject_index, subject in enumerate(phase_subjects):
                        if subject_index == len(phase_subjects) - 1:
                            amount = phase_amount - distributed
                        else:
                            amount = phase_amount * remaining_by_subject[subject] / phase_total
                            distributed += amount
                        amount = min(amount, remaining_by_subject[subject])
                        if amount > 0.001:
                            allocations[subject] = allocations.get(subject, 0.0) + amount
                            remaining_by_subject[subject] = max(0.0, remaining_by_subject[subject] - amount)
                    remaining_day -= phase_amount
                    if sum(remaining_by_subject.get(subject, 0.0) for subject in study_phases[phase_index]) <= 0.001:
                        phase_index += 1
                add_day(scheduled_day, allocations)
                days[scheduled_day.isoformat()]["replanned"] = True
        else:
            replan_start = None
            replan_end = None

    first_day = datetime.strptime(STUDY_PLAN_START, "%Y-%m-%d").date()
    last_scheduled_day = replan_end or max(item["date"] for item in days.values())
    last_week_end = _study_plan_week_start(last_scheduled_day) + timedelta(days=6)
    day = first_day
    while day <= last_week_end:
        if day.isoformat() not in days:
            add_day(day, {}, "本期影片完成")
        day += timedelta(days=1)

    weeks: List[Dict[str, Any]] = []
    week_cursor = _study_plan_week_start(first_day)
    number = 1
    while week_cursor <= last_week_end:
        daily_targets = [days[(week_cursor + timedelta(days=index)).isoformat()] for index in range(7)]
        subject_targets: Dict[str, float] = {}
        for daily_target in daily_targets:
            for subject, seconds in daily_target["allocations"].items():
                subject_targets[subject] = subject_targets.get(subject, 0.0) + seconds
        subjects = [subject for subject in STUDY_PLAN_SUBJECTS if subject_targets.get(subject, 0.0) > 0]
        weeks.append(
            {
                "number": number,
                "start": week_cursor,
                "end": week_cursor + timedelta(days=6),
                "subjects": subjects,
                "subject_targets": subject_targets,
                "daily_targets": daily_targets,
                "credit_baselines": (
                    dict(replan_baselines)
                    if replan_start and week_cursor >= replan_start
                    else {}
                ),
                "is_replanned": bool(replan_start and week_cursor >= replan_start),
            }
        )
        number += 1
        week_cursor += timedelta(days=7)
    return weeks


def _study_plan_replan_preview(settings: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not settings:
        return None
    try:
        start_day = datetime.strptime(str(settings.get("start_date") or ""), "%Y-%m-%d").date()
        end_day = datetime.strptime(str(settings.get("end_date") or ""), "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None
    if end_day < start_day:
        return None
    weekday_minutes = max(1.0, _study_plan_nonnegative_number(settings.get("weekday_minutes")))
    weekend_minutes = max(1.0, _study_plan_nonnegative_number(settings.get("weekend_minutes")))
    subject_targets = {
        str(subject): _study_plan_nonnegative_number(seconds)
        for subject, seconds in dict(settings.get("subject_targets") or {}).items()
        if _study_plan_nonnegative_number(seconds) > 0
    }
    weekday_count = 0
    weekend_count = 0
    cursor_day = start_day
    while cursor_day <= end_day:
        if cursor_day.weekday() >= 5:
            weekend_count += 1
        else:
            weekday_count += 1
        cursor_day += timedelta(days=1)
    total_target_seconds = sum(subject_targets.values())
    total_capacity_seconds = (
        weekday_count * weekday_minutes + weekend_count * weekend_minutes
    ) * 60
    load_ratio = total_target_seconds / total_capacity_seconds if total_capacity_seconds else 0.0
    if load_ratio <= 0.8:
        state = "comfortable"
        state_label = "安排寬裕"
    elif load_ratio <= 1.0:
        state = "balanced"
        state_label = "負荷剛好"
    elif load_ratio <= 1.25:
        state = "tight"
        state_label = "需要加速"
    else:
        state = "overloaded"
        state_label = "目標偏緊"
    return {
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "day_count": (end_day - start_day).days + 1,
        "weekday_count": weekday_count,
        "weekend_count": weekend_count,
        "remaining_hours": round(total_target_seconds / 3600, 1),
        "weekday_hours": round(weekday_minutes / 60 * load_ratio, 1),
        "weekend_hours": round(weekend_minutes / 60 * load_ratio, 1),
        "load_percent": round(load_ratio * 100),
        "state": state,
        "state_label": state_label,
        "subjects": [
            {
                "name": subject,
                "hours": round(subject_targets.get(subject, 0.0) / 3600, 1),
            }
            for subject in STUDY_PLAN_SUBJECTS
            if subject_targets.get(subject, 0.0) > 0
        ],
    }


def _study_plan_today_progress_days(
    week_rows: Iterable[Dict[str, Any]],
    videos: Iterable[Dict[str, Any]],
    activity_events: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Map today's positive viewing gains onto their scheduled study segments."""
    watched_after_by_subject: Dict[str, float] = {}
    for video in videos:
        subject = str(video.get("subject") or "")
        if not subject:
            continue
        duration_seconds = _study_plan_nonnegative_number(video.get("duration_seconds"))
        watched_seconds = min(
            _study_plan_nonnegative_number(video.get("watched_seconds")),
            duration_seconds,
        )
        watched_after_by_subject[subject] = (
            watched_after_by_subject.get(subject, 0.0) + watched_seconds
        )

    delta_by_subject: Dict[str, float] = {}
    for event in activity_events:
        subject = str(event.get("subject") or "")
        if not subject:
            continue
        delta_seconds = float(event.get("delta_seconds") or 0)
        if not math.isfinite(delta_seconds):
            continue
        delta_by_subject[subject] = delta_by_subject.get(subject, 0.0) + delta_seconds

    watched_before_by_subject = {
        subject: max(
            0.0,
            watched_after_by_subject.get(subject, 0.0)
            - delta_by_subject.get(subject, 0.0),
        )
        for subject in set(watched_after_by_subject) | set(delta_by_subject)
    }
    planned_before_by_subject: Dict[str, float] = {}
    progress_days: List[Dict[str, Any]] = []

    for week in week_rows:
        week_target_seconds = max(0.0, float(week.get("target_seconds") or 0))
        week_before_seconds = 0.0
        week_after_seconds = 0.0
        gained_days: List[Dict[str, Any]] = []

        for day in week.get("daily_recommendations") or []:
            day_date = str(day.get("date") or "")
            allocations = {
                str(subject): _study_plan_nonnegative_number(seconds)
                for subject, seconds in (day.get("allocations") or {}).items()
                if str(subject or "") and _study_plan_nonnegative_number(seconds) > 0
            }
            day_before_seconds = 0.0
            day_after_seconds = 0.0
            for subject, target_seconds in allocations.items():
                planned_before = planned_before_by_subject.get(subject, 0.0)
                day_before_seconds += min(
                    max(watched_before_by_subject.get(subject, 0.0) - planned_before, 0.0),
                    target_seconds,
                )
                day_after_seconds += min(
                    max(watched_after_by_subject.get(subject, 0.0) - planned_before, 0.0),
                    target_seconds,
                )
                planned_before_by_subject[subject] = planned_before + target_seconds

            week_before_seconds += day_before_seconds
            week_after_seconds += day_after_seconds
            gained_seconds = max(0.0, day_after_seconds - day_before_seconds)
            gained_minutes = round(gained_seconds / 60, 1)
            day_target_seconds = sum(allocations.values())
            if gained_minutes > 0 and day_target_seconds > 0:
                gained_days.append(
                    {
                        "week_number": int(week.get("number") or 0),
                        "subject": str(week.get("subject") or ""),
                        "label": str(day.get("label") or ""),
                        "date": day_date,
                        "minutes": gained_minutes,
                        "day_target_minutes": round(day_target_seconds / 60, 1),
                        "before_completion": round(
                            min(100.0, day_before_seconds / day_target_seconds * 100),
                            1,
                        ),
                        "after_completion": round(
                            min(100.0, day_after_seconds / day_target_seconds * 100),
                            1,
                        ),
                    }
                )

        week_before_completion = (
            min(100.0, week_before_seconds / week_target_seconds * 100)
            if week_target_seconds
            else 0.0
        )
        week_after_completion = (
            min(100.0, week_after_seconds / week_target_seconds * 100)
            if week_target_seconds
            else 0.0
        )
        for item in gained_days:
            item["week_before_completion"] = round(week_before_completion, 1)
            item["week_after_completion"] = round(week_after_completion, 1)
            progress_days.append(item)

    return progress_days


class TrafficTracker:
    def __init__(
        self,
        activity_window: int = 60,
        count_interval: int = 3600,
        storage_path: Optional[Path] = None,
        log_path: Optional[Path] = None,
        max_events: int = 200,
        state_loader: Optional[Callable[[], Optional[Dict[str, Any]]]] = None,
        state_saver: Optional[Callable[[Dict[str, Any]], None]] = None,
        event_loader: Optional[Callable[[int], List[Dict[str, Any]]]] = None,
        event_writer: Optional[Callable[[Dict[str, Any]], None]] = None,
        event_clearer: Optional[Callable[[], None]] = None,
    ) -> None:
        self._activity_window = activity_window
        self._count_interval = count_interval
        self._active_ips: Dict[str, float] = {}
        self._last_total_increment: Dict[str, float] = {}
        self._ip_total_hits: Dict[str, int] = {}
        self._ip_users: Dict[str, str] = {}
        self._active_users: Dict[str, float] = {}
        self._user_total_hits: Dict[str, int] = {}
        self._user_last_count: Dict[str, float] = {}
        self._user_last_seen: Dict[str, float] = {}
        self._user_flags: Dict[str, bool] = {}
        self._total_hits = 0
        self._recent_events: List[Dict[str, Any]] = []
        self._max_events = max_events
        self._version = 0
        self._version = 0
        self._lock = threading.Lock()
        self._storage_path = Path(storage_path) if storage_path else None
        self._log_path = Path(log_path) if log_path else None
        self._concurrent_history: List[Dict[str, Any]] = []
        self._concurrent_history: List[Dict[str, Any]] = []
        self._hourly_buckets: Dict[int, Set[str]] = {}
        self._hourly_series: List[Dict[str, Any]] = []
        self._state_loader = state_loader
        self._state_saver = state_saver
        self._event_loader = event_loader
        self._event_writer = event_writer
        self._event_clearer = event_clearer
        if self._storage_path:
            self._storage_path.parent.mkdir(parents=True, exist_ok=True)
        if self._log_path:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
        if self._state_loader:
            self._load_from_backend()
        elif self._storage_path:
            self._load_from_disk()
        if self._event_loader:
            try:
                self._recent_events = self._event_loader(self._max_events) or []
            except Exception:
                self._recent_events = []
        elif self._log_path:
            self._load_recent_events()

    def _purge_expired(self, now: float) -> bool:
        expired_ips = [ip for ip, ts in self._active_ips.items() if now - ts > self._activity_window]
        for ip in expired_ips:
            self._active_ips.pop(ip, None)
        expired_users = [user for user, ts in self._active_users.items() if now - ts > self._activity_window]
        for user in expired_users:
            self._active_users.pop(user, None)
        return bool(expired_ips or expired_users)

    def remove_user_stats(self, username: str) -> bool:
        """Remove all tracked state for a specific username."""
        if not username:
            return False
        changed = False
        with self._lock:
            if username in self._user_total_hits:
                self._user_total_hits.pop(username, None)
                changed = True
            if username in self._user_last_count:
                self._user_last_count.pop(username, None)
                changed = True
            if username in self._user_last_seen:
                self._user_last_seen.pop(username, None)
                changed = True
            if username in self._active_users:
                self._active_users.pop(username, None)
                changed = True
            if username in self._user_flags:
                self._user_flags.pop(username, None)
                changed = True
            # detach IP mappings pointing to this user
            ips_to_clear = [ip for ip, user in self._ip_users.items() if user == username]
            for ip in ips_to_clear:
                self._ip_users.pop(ip, None)
            if ips_to_clear:
                changed = True
            # prune hourly buckets and recalc series counts
            if self._hourly_buckets:
                for ts, members in list(self._hourly_buckets.items()):
                    if username in members:
                        members.discard(username)
                        changed = True
                        self._hourly_buckets[ts] = members
                # rebuild hourly_series counts
                rebuilt = []
                for ts, members in self._hourly_buckets.items():
                    rebuilt.append({"ts": ts, "count": len(members)})
                self._hourly_series = sorted(rebuilt, key=lambda x: x["ts"])
            if changed:
                self._version += 1
                self._save_to_disk()
        return changed

    def _is_guest_user(self, username: Optional[str]) -> bool:
        if not username:
            return False
        normalized = str(username)
        if normalized in self._user_flags:
            return bool(self._user_flags[normalized])
        return normalized.startswith("訪客")

    def record_visit(
        self,
        ip: Optional[str],
        *,
        action: Optional[str] = None,
        status: str = "success",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not action:
            return
        action_lc = str(action).lower()
        now = time.time()
        with self._lock:
            prev_ts = self._active_ips.get(ip) if ip else None
            previously_online_ip = bool(prev_ts and now - prev_ts <= self._activity_window)
            if ip:
                self._active_ips[ip] = now
            username = None
            is_guest_user = False
            if metadata:
                username = metadata.get("username")
                is_guest_user = bool(metadata.get("is_guest"))
            if username:
                username = str(username)
                if ip:
                    self._ip_users[ip] = username
                self._user_flags[username] = is_guest_user
                self._user_last_seen[username] = now
                if not is_guest_user:
                    self._active_users[username] = now
                if not is_guest_user and action_lc not in PASSIVE_TRAFFIC_ACTIONS:
                    self._update_hourly(username, now)
            last_hit = self._last_total_increment.get(ip, 0) if ip else 0
            stats_changed = True
            if ip and now - last_hit >= self._count_interval:
                self._total_hits += 1
                self._last_total_increment[ip] = now
                self._ip_total_hits[ip] = self._ip_total_hits.get(ip, 0) + 1
                if username and not is_guest_user:
                    self._user_total_hits[username] = self._user_total_hits.get(username, 0) + 1
                    self._user_last_count[username] = now
            elif ip and ip not in self._ip_total_hits:
                self._ip_total_hits[ip] = 0
            if action_lc not in PASSIVE_TRAFFIC_ACTIONS:
                self._append_event(
                    {"ts": now, "ip": ip, "action": action, "status": status, "meta": metadata or {}}
                )
            self._purge_old_total_entries(now)
            previously_online_user = False
            if username and not is_guest_user:
                last_seen = self._active_users.get(username)
                previously_online_user = bool(last_seen and now - last_seen <= self._activity_window)
            if self._purge_expired(now) or not previously_online_ip or not previously_online_user:
                stats_changed = True
            if stats_changed:
                self._version += 1
                self._store_concurrent_snapshot(now)
            self._save_to_disk()

    def snapshot(self) -> Dict[str, int]:
        now = time.time()
        with self._lock:
            self._purge_expired(now)
            user_count, _ = self._online_counts(now)
            unique_users: Set[str] = set()
            for username in self._user_total_hits.keys():
                if not self._is_guest_user(username):
                    unique_users.add(username)
            for username in self._user_last_seen.keys():
                if not self._is_guest_user(username):
                    unique_users.add(username)
            for username in self._active_users.keys():
                if not self._is_guest_user(username):
                    unique_users.add(username)
            cutoff = now - 86400
            daily_users: Set[str] = set()
            for ev in self._recent_events:
                ts = ev.get("ts")
                if not ts or ts < cutoff:
                    continue
                meta = ev.get("meta") or {}
                username = meta.get("username")
                if username and not meta.get("is_guest"):
                    daily_users.add(str(username))
            return {
                "online": user_count,
                "total": self._total_hits,
                "total_users": len(unique_users),
                "daily_users": len(daily_users),
                "online_users": user_count,
            }

    def version(self) -> int:
        with self._lock:
            return self._version

    def _load_from_backend(self) -> None:
        if not self._state_loader:
            return
        try:
            data = self._state_loader() or {}
        except Exception:
            data = {}
        self._apply_state_payload(data)

    def _load_from_disk(self) -> None:
        if not self._storage_path or not self._storage_path.exists():
            return
        try:
            data = json.loads(self._storage_path.read_text(encoding="utf-8"))
        except Exception:
            return
        self._apply_state_payload(data)

    def _apply_state_payload(self, data: Optional[Dict[str, Any]]) -> None:
        if not isinstance(data, dict):
            return
        try:
            self._total_hits = int(data.get("total", 0))
        except Exception:
            self._total_hits = 0
        try:
            self._version = int(data.get("version", int(self._total_hits)))
        except Exception:
            self._version = int(self._total_hits)
        active = data.get("active") or {}
        cleaned: Dict[str, float] = {}
        if isinstance(active, dict):
            for ip, ts in active.items():
                try:
                    cleaned[str(ip)] = float(ts)
                except (TypeError, ValueError):
                    continue
        self._active_ips = cleaned
        last_total = data.get("last_total") or {}
        cleaned_total: Dict[str, float] = {}
        if isinstance(last_total, dict):
            for ip, ts in last_total.items():
                try:
                    cleaned_total[str(ip)] = float(ts)
                except (TypeError, ValueError):
                    continue
        self._last_total_increment = cleaned_total
        ip_totals = data.get("ip_totals") or {}
        cleaned_hits: Dict[str, int] = {}
        if isinstance(ip_totals, dict):
            for ip, count in ip_totals.items():
                try:
                    cleaned_hits[str(ip)] = int(count)
                except (TypeError, ValueError):
                    continue
        self._ip_total_hits = cleaned_hits
        ip_users = data.get("ip_users") or {}
        cleaned_users: Dict[str, str] = {}
        if isinstance(ip_users, dict):
            for ip, name in ip_users.items():
                try:
                    cleaned_users[str(ip)] = str(name)
                except Exception:
                    continue
        self._ip_users = cleaned_users
        active_users = data.get("active_users") or {}
        cleaned_active_users: Dict[str, float] = {}
        if isinstance(active_users, dict):
            for username, ts in active_users.items():
                try:
                    cleaned_active_users[str(username)] = float(ts)
                except (TypeError, ValueError):
                    continue
        self._active_users = cleaned_active_users
        user_totals = data.get("user_totals") or {}
        cleaned_user_totals: Dict[str, int] = {}
        if isinstance(user_totals, dict):
            for username, count in user_totals.items():
                try:
                    cleaned_user_totals[str(username)] = int(count)
                except (TypeError, ValueError):
                    continue
        self._user_total_hits = cleaned_user_totals
        user_last_count = data.get("user_last_count") or {}
        cleaned_last_count: Dict[str, float] = {}
        if isinstance(user_last_count, dict):
            for username, ts in user_last_count.items():
                try:
                    cleaned_last_count[str(username)] = float(ts)
                except (TypeError, ValueError):
                    continue
        self._user_last_count = cleaned_last_count
        user_last_seen = data.get("user_last_seen") or {}
        cleaned_last_seen: Dict[str, float] = {}
        if isinstance(user_last_seen, dict):
            for username, ts in user_last_seen.items():
                try:
                    cleaned_last_seen[str(username)] = float(ts)
                except (TypeError, ValueError):
                    continue
        self._user_last_seen = cleaned_last_seen
        user_flags = data.get("user_flags") or {}
        cleaned_flags: Dict[str, bool] = {}
        if isinstance(user_flags, dict):
            for username, flag in user_flags.items():
                try:
                    cleaned_flags[str(username)] = bool(flag)
                except Exception:
                    continue
        self._user_flags = cleaned_flags
        history = data.get("concurrent") or []
        cleaned_history: List[Dict[str, Any]] = []
        if isinstance(history, list):
            for item in history:
                if not isinstance(item, dict):
                    continue
                try:
                    ts = float(item.get("ts"))
                except (TypeError, ValueError):
                    continue
                try:
                    count = int(item.get("count") or 0)
                except (TypeError, ValueError):
                    continue
                cleaned_history.append({"ts": ts, "count": count})
        self._concurrent_history = cleaned_history[-(self._max_events * 3) :]
        hourly_series = data.get("hourly_series") or []
        cleaned_hourly_series: List[Dict[str, Any]] = []
        if isinstance(hourly_series, list):
            for item in hourly_series:
                if not isinstance(item, dict):
                    continue
                try:
                    ts = int(item.get("ts"))
                except (TypeError, ValueError):
                    continue
                try:
                    count = int(item.get("count") or 0)
                except (TypeError, ValueError):
                    count = 0
                cleaned_hourly_series.append({"ts": ts, "count": count})
        cleaned_hourly_series = sorted(cleaned_hourly_series, key=lambda x: x["ts"])
        max_hourly = self._max_events * 24
        if len(cleaned_hourly_series) > max_hourly:
            cleaned_hourly_series = cleaned_hourly_series[-max_hourly:]
        self._hourly_series = cleaned_hourly_series
        hourly_buckets = data.get("hourly_buckets") or {}
        cleaned_buckets: Dict[int, Set[str]] = {}
        if isinstance(hourly_buckets, dict):
            for ts, members in hourly_buckets.items():
                try:
                    bucket_ts = int(ts)
                except (TypeError, ValueError):
                    continue
                bucket_set: Set[str] = set()
                if isinstance(members, (list, set, tuple)):
                    for m in members:
                        if m is None:
                            continue
                        try:
                            bucket_set.add(str(m))
                        except Exception:
                            continue
                cleaned_buckets[bucket_ts] = bucket_set
        self._hourly_buckets = cleaned_buckets
        for entry in list(self._hourly_series):
            ts = entry.get("ts")
            if ts not in self._hourly_buckets:
                self._hourly_buckets[ts] = set()
        self._purge_expired(time.time())

    def _persist_state_payload(self, payload: Dict[str, Any]) -> None:
        if self._state_saver:
            try:
                self._state_saver(payload)
            except Exception:
                pass
            return
        if not self._storage_path:
            return
        try:
            self._storage_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    def _save_to_disk(self) -> None:
        payload = {
            "total": self._total_hits,
            "active": self._active_ips,
            "last_total": self._last_total_increment,
            "ip_totals": self._ip_total_hits,
            "version": self._version,
            "ip_users": self._ip_users,
            "active_users": self._active_users,
            "user_totals": self._user_total_hits,
            "user_last_count": self._user_last_count,
            "user_last_seen": self._user_last_seen,
            "user_flags": self._user_flags,
            "concurrent": self._concurrent_history,
            "hourly_series": self._hourly_series,
            "hourly_buckets": {ts: list(names) for ts, names in self._hourly_buckets.items()},
        }
        self._persist_state_payload(payload)

    def _purge_old_total_entries(self, now: float) -> None:
        expire_after = self._count_interval * 2
        stale = [ip for ip, ts in self._last_total_increment.items() if now - ts > expire_after]
        for ip in stale:
            self._last_total_increment.pop(ip, None)

    def _online_counts(self, now: float) -> Tuple[int, int]:
        active_usernames = [
            user
            for user, ts in self._active_users.items()
            if ts and now - ts <= self._activity_window and not self._is_guest_user(user)
        ]
        active_ips = [
            ip for ip, ts in self._active_ips.items() if ts and now - ts <= self._activity_window
        ]
        return len(active_usernames), len(active_ips)

    def ip_summary(self) -> Dict[str, int]:
        now = time.time()
        with self._lock:
            self._purge_expired(now)
            unique_ips = set(self._ip_total_hits.keys()) | set(self._active_ips.keys())
            online_ips = sum(
                1 for ts in self._active_ips.values() if ts and now - ts <= self._activity_window
            )
            return {
                "unique": len(unique_ips),
                "online": online_ips,
                "total": self._total_hits,
            }

    def reset(self) -> None:
        with self._lock:
            self._active_ips.clear()
            self._last_total_increment.clear()
            self._ip_total_hits.clear()
            self._ip_users.clear()
            self._active_users.clear()
            self._user_total_hits.clear()
            self._user_last_count.clear()
            self._user_last_seen.clear()
            self._user_flags.clear()
            self._recent_events = []
            self._concurrent_history = []
            self._concurrent_history = []
            self._total_hits = 0
            self._version += 1
            self._save_to_disk()
            if self._event_clearer:
                try:
                    self._event_clearer()
                except Exception:
                    pass
            elif self._log_path:
                try:
                    self._log_path.write_text("", encoding="utf-8")
                except Exception:
                    pass

    def _append_event(self, event: Dict[str, Any]) -> None:
        cleaned = {
            "ts": event.get("ts"),
            "ip": event.get("ip"),
            "action": event.get("action"),
            "status": event.get("status") or "info",
            "meta": event.get("meta") or {},
        }
        self._recent_events.append(cleaned)
        if len(self._recent_events) > self._max_events:
            self._recent_events = self._recent_events[-self._max_events :]
        if self._event_writer:
            try:
                self._event_writer(cleaned)
            except Exception:
                pass
            return
        if not self._log_path:
            return
        try:
            with self._log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(cleaned, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def _load_recent_events(self) -> None:
        if self._event_loader:
            try:
                self._recent_events = self._event_loader(self._max_events) or []
            except Exception:
                self._recent_events = []
            return
        if not self._log_path or not self._log_path.exists():
            return
        try:
            lines = self._log_path.read_text(encoding="utf-8").splitlines()
        except Exception:
            return
        events: List[Dict[str, Any]] = []
        for raw in lines[-self._max_events :]:
            try:
                event = json.loads(raw)
            except Exception:
                continue
            if isinstance(event, dict):
                events.append(event)
        self._recent_events = events

    def recent_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            subset = self._recent_events[-limit:]
            return list(subset)

    def concurrent_history(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._concurrent_history)

    def hourly_series(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._hourly_series)

    def hourly_buckets(self) -> Dict[int, Set[str]]:
        with self._lock:
            return {ts: set(names) for ts, names in self._hourly_buckets.items()}

    def _store_concurrent_snapshot(self, now: float) -> None:
        user_count, _ = self._online_counts(now)
        entry = {"ts": float(now), "count": user_count}
        if self._concurrent_history and now - self._concurrent_history[-1]["ts"] < 60:
            self._concurrent_history[-1] = entry
        else:
            self._concurrent_history.append(entry)
        max_len = self._max_events * 3
        if len(self._concurrent_history) > max_len:
            self._concurrent_history = self._concurrent_history[-max_len:]

    def _update_hourly(self, username: str, now: float) -> None:
        if not username or self._is_guest_user(username):
            return
        bucket_dt = datetime.fromtimestamp(now, tz=TAIPEI_TZ).replace(minute=0, second=0, microsecond=0)
        bucket_ts = int(bucket_dt.timestamp())
        bucket = self._hourly_buckets.setdefault(bucket_ts, set())
        before = len(bucket)
        bucket.add(username)
        if len(bucket) != before:
            # update series entry for this bucket
            self._hourly_series = [entry for entry in self._hourly_series if entry.get("ts") != bucket_ts]
            self._hourly_series.append({"ts": bucket_ts, "count": len(bucket)})
            self._hourly_series = sorted(self._hourly_series, key=lambda x: x["ts"])
            max_len = self._max_events * 24
            if len(self._hourly_series) > max_len:
                drop = len(self._hourly_series) - max_len
                old = self._hourly_series[:drop]
                self._hourly_series = self._hourly_series[-max_len:]
                for entry in old:
                    ts = entry.get("ts")
                    if ts in self._hourly_buckets:
                        self._hourly_buckets.pop(ts, None)
    def user_breakdown(self) -> List[Dict[str, Any]]:
        now = time.time()
        with self._lock:
            aggregated: Dict[str, Dict[str, Any]] = {}
            def _entry(username: str) -> Dict[str, Any]:
                return aggregated.setdefault(
                    username,
                    {"username": username, "count": 0, "last_seen": 0.0, "last_counted": None, "online": False},
                )

            for username, count in self._user_total_hits.items():
                if self._is_guest_user(username):
                    continue
                entry = _entry(username)
                entry["count"] = count
                if username in self._user_last_count:
                    entry["last_counted"] = self._user_last_count[username]
                entry["last_seen"] = max(
                    entry.get("last_seen") or 0.0,
                    self._user_last_seen.get(username, 0.0),
                    self._user_last_count.get(username, 0.0) or 0.0,
                )

            for username, last_seen in self._active_users.items():
                if self._is_guest_user(username):
                    continue
                entry = _entry(username)
                entry["last_seen"] = max(
                    entry.get("last_seen") or 0.0,
                    last_seen or 0.0,
                    self._user_last_seen.get(username, 0.0),
                )
                if last_seen and now - last_seen <= self._activity_window:
                    entry["online"] = True
                if username in self._user_last_count:
                    entry["last_counted"] = self._user_last_count[username]

            for username, last_seen in self._user_last_seen.items():
                if self._is_guest_user(username):
                    continue
                entry = _entry(username)
                entry["last_seen"] = max(
                    entry.get("last_seen") or 0.0,
                    last_seen or 0.0,
                    self._user_last_count.get(username, 0.0),
                )
                if username in self._user_total_hits:
                    entry["count"] = self._user_total_hits.get(username, entry.get("count", 0))

            entries = list(aggregated.values())
            entries.sort(key=lambda item: item["count"], reverse=True)
            return entries

    def ip_breakdown(self) -> List[Dict[str, Any]]:
        now = time.time()
        with self._lock:
            entries = []
            for ip, count in self._ip_total_hits.items():
                last_seen = self._active_ips.get(ip)
                entries.append(
                    {
                        "ip": ip,
                        "count": count,
                        "last_seen": last_seen,
                        "last_counted": self._last_total_increment.get(ip),
                        "online": bool(last_seen and now - last_seen <= self._activity_window),
                        "username": self._ip_users.get(ip),
                    }
                )
            entries.sort(key=lambda item: item["count"], reverse=True)
            return entries

    def guest_summary(self) -> Dict[str, int]:
        now = time.time()
        with self._lock:
            self._purge_expired(now)
            total_guests = sum(1 for flag in self._user_flags.values() if flag)
            active_guests: Set[str] = set()
            for ip, ts in self._active_ips.items():
                if not ts or now - ts > self._activity_window:
                    continue
                username = self._ip_users.get(ip)
                if username and self._is_guest_user(username):
                    active_guests.add(username)
            return {"total": total_guests, "online": len(active_guests)}


def _env_flag_truthy(value: Optional[str]) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")



def create_app(*, default_base_url: Optional[str] = None, default_scope: str = "assignment", default_timeout: int = 30) -> Flask:
    env_defaults = load_env_defaults()

    def _ensure_private_dir(path: Path) -> Path:
        path.mkdir(parents=True, exist_ok=True)
        try:
            if os.name != "nt":
                os.chmod(path, 0o700)
        except Exception:
            pass
        return path

    configured_cache_dir = env_defaults.get("cache_dir")
    if configured_cache_dir:
        data_root = Path(configured_cache_dir).expanduser()
    else:
        data_root = ROOT_DIR / ".localdata"
    _ensure_private_dir(data_root)
    database_url = env_defaults.get("database_url") or ""
    if database_url:
        db_location = database_url
    else:
        db_location = str((data_root / "e3_tracker.sqlite3").resolve())
    storage = PersistentStorage(db_location)
    storage.sync_study_plan_videos(STUDY_PLAN_VIDEO_INVENTORY)

    app = Flask(__name__)
    app.secret_key = env_defaults["web_secret"]
    app.extensions["e3_storage"] = storage
    app.jinja_env.globals["study_upload_tracker"] = STUDY_UPLOAD_TRACKER_TEMPLATE
    session_cookie_secure = _env_flag_truthy(env_defaults.get("session_cookie_secure"))
    session_cookie_samesite = env_defaults.get("session_cookie_samesite") or "Lax"
    app.config.update(
        PERMANENT_SESSION_LIFETIME=timedelta(days=1),
        SESSION_COOKIE_SECURE=session_cookie_secure,
        SESSION_COOKIE_SAMESITE=session_cookie_samesite,
        SESSION_COOKIE_HTTPONLY=True,
        PREFERRED_URL_SCHEME="https",
        MAX_CONTENT_LENGTH=STUDY_NOTE_MAX_REQUEST_BYTES,
    )

    @app.get("/favicon.ico")
    def favicon():
        return Response(status=204)
    base_url = default_base_url or env_defaults["base_url"]
    default_scope = default_scope or env_defaults["scope"]
    default_moodle_session = env_defaults["session"]
    cafile = env_defaults.get("cafile") or None
    insecure_tls = _env_flag_truthy(env_defaults.get("insecure_tls"))
    google_client_id = env_defaults.get("google_client_id")
    google_client_secret = env_defaults.get("google_client_secret")
    google_redirect_uri = env_defaults.get("google_redirect_uri")
    google_calendar_id = env_defaults.get("google_calendar_id") or "primary"
    admin_user_id = (env_defaults.get("admin_user_id") or "112550103").strip()
    canonical_host = (env_defaults.get("canonical_host") or "").strip()
    if canonical_host == "":
        canonical_host = None
    support_email = (env_defaults.get("support_email") or "support@e3hwtool.space").strip()
    if not support_email:
        support_email = "support@e3hwtool.space"
    app_home_url = (env_defaults.get("app_home_url") or "https://www.e3hwtool.space/").strip()
    if app_home_url and not app_home_url.startswith(("http://", "https://")):
        app_home_url = f"https://{app_home_url.lstrip('/')}"
    if not app_home_url:
        app_home_url = "https://www.e3hwtool.space/"
    if not app_home_url.endswith("/"):
        app_home_url = f"{app_home_url}/"
    legal_entity_name = env_defaults.get("legal_entity_name") or "E3 Homework Tracker Project"
    openai_api_key = (env_defaults.get("openai_api_key") or "").strip()
    openai_model = (env_defaults.get("openai_model") or DEFAULT_OPENAI_MODEL).strip()
    configured_upload_dir = (env_defaults.get("study_upload_dir") or "").strip()
    study_upload_root = Path(configured_upload_dir).expanduser() if configured_upload_dir else data_root / "study_note_images"
    _ensure_private_dir(study_upload_root)
    study_upload_staging_root = _ensure_private_dir(study_upload_root / "_staging")
    legal_effective_date = env_defaults.get("legal_effective_date") or "2024-11-19"
    traffic_event_limit = 500
    traffic_tracker = TrafficTracker(
        activity_window=300,
        count_interval=3600,
        storage_path=None,
        log_path=None,
        max_events=traffic_event_limit,
        state_loader=storage.load_traffic_state,
        state_saver=storage.save_traffic_state,
        event_loader=lambda limit: storage.recent_traffic_events(limit),
        event_writer=lambda event: storage.append_traffic_event(event, traffic_event_limit),
        event_clearer=storage.clear_traffic_events,
    )

    def _is_study_upload_request() -> bool:
        return request.headers.get("X-E3-Study-Upload") == "1"

    def _study_upload_error(message: str, status_code: int = 400):
        if _is_study_upload_request():
            return {"ok": False, "error": message}, status_code
        flash(message, "error")
        return redirect(url_for("admin_study_recall"))

    @app.errorhandler(RequestEntityTooLarge)
    def handle_request_entity_too_large(_error: RequestEntityTooLarge):
        if request.path.startswith("/admin/study-recall/upload"):
            return _study_upload_error("單張筆記照片超過傳輸限制，請縮小到 2MB 後再試。", 413)
        return Response("Request Entity Too Large", status=413, mimetype="text/plain")

    DEFAULT_PREFERENCES = {
        "view_mode": "due",
        "status_filter": ["pending"],
        "include_ignored_overdue": False,
        "show_overdue": False,
        "show_completed": False,
        "show_graded": False,
        "ignored_overdue_uids": [],
    }
    NEW_ASSIGNMENT_WINDOW_SECONDS = 5 * 60
    refresh_jobs_lock = threading.Lock()
    refresh_jobs: Dict[str, Dict[str, Any]] = {}
    study_upload_jobs_lock = threading.Lock()
    study_upload_jobs: Dict[str, Dict[str, Any]] = {}
    study_source_jobs_lock = threading.Lock()
    study_source_jobs: Dict[str, Dict[str, Any]] = {}
    study_upload_context = threading.local()
    study_relation_rebuild_lock = threading.Lock()
    study_progress_context_lock = threading.Lock()
    study_progress_context_cache: Dict[str, Any] = {"expires_at": 0.0, "context": None}

    def _study_upload_staging_directory(upload_id: str) -> Optional[Path]:
        token = str(upload_id or "").strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]{20,80}", token):
            return None
        root = study_upload_staging_root.resolve()
        directory = (root / token).resolve()
        if directory.parent != root:
            return None
        return directory

    def _read_study_upload_manifest(upload_id: str, username: str) -> Tuple[Optional[Dict[str, Any]], Optional[Path]]:
        directory = _study_upload_staging_directory(upload_id)
        if directory is None:
            return None, None
        manifest_path = directory / "manifest.json"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return None, directory
        if not isinstance(manifest, dict) or str(manifest.get("username") or "") != username:
            return None, directory
        return manifest, directory

    def _write_study_upload_manifest(directory: Path, manifest: Dict[str, Any]) -> None:
        temporary = directory / "manifest.tmp"
        temporary.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
        temporary.replace(directory / "manifest.json")

    def _remove_study_upload_staging(upload_id: str, username: str) -> bool:
        manifest, directory = _read_study_upload_manifest(upload_id, username)
        if manifest is None or directory is None or not directory.is_dir():
            return False
        try:
            shutil.rmtree(directory)
        except OSError:
            return False
        return True

    def _cleanup_expired_study_upload_staging() -> None:
        cutoff = time.time() - STUDY_NOTE_STAGING_TTL_SECONDS
        try:
            directories = list(study_upload_staging_root.iterdir())
        except OSError:
            return
        for directory in directories:
            if not directory.is_dir():
                continue
            try:
                manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
                updated_at = float(manifest.get("updated_at") or manifest.get("created_at") or 0)
            except (OSError, ValueError, TypeError):
                updated_at = 0
            if updated_at >= cutoff:
                continue
            try:
                shutil.rmtree(directory)
            except OSError:
                pass

    class _StudyUploadCancelled(RuntimeError):
        pass

    def _raise_if_study_upload_cancelled() -> None:
        cancel_event = getattr(study_upload_context, "cancel_event", None)
        if isinstance(cancel_event, threading.Event) and cancel_event.is_set():
            raise _StudyUploadCancelled("筆記處理已取消。")

    def _study_upload_retry_wait(seconds: float) -> None:
        cancel_event = getattr(study_upload_context, "cancel_event", None)
        if isinstance(cancel_event, threading.Event):
            if cancel_event.wait(max(0.0, seconds)):
                raise _StudyUploadCancelled("筆記處理已取消。")
            return
        time.sleep(max(0.0, seconds))

    def _set_study_upload_job(job_id: str, **changes: Any) -> None:
        with study_upload_jobs_lock:
            job = study_upload_jobs.get(job_id)
            if job is None:
                return
            if job.get("status") == "cancelled" and changes.get("status") != "cancelled":
                return
            if "progress" in changes and job.get("status") == "running":
                try:
                    changes["progress"] = max(
                        int(job.get("progress") or 0),
                        int(changes.get("progress") or 0),
                    )
                except (TypeError, ValueError):
                    changes.pop("progress", None)
            job.update(changes)
            job["updated_at"] = time.time()

    def _set_study_source_job(job_id: str, **changes: Any) -> None:
        with study_source_jobs_lock:
            job = study_source_jobs.get(job_id)
            if job is None:
                return
            job.update(changes)
            job["updated_at"] = time.time()

    def _active_study_upload_job(username: str) -> Optional[str]:
        cutoff = time.time() - 24 * 60 * 60
        with study_upload_jobs_lock:
            expired = [job_id for job_id, job in study_upload_jobs.items() if float(job.get("updated_at") or 0) < cutoff]
            for job_id in expired:
                study_upload_jobs.pop(job_id, None)
            for job_id, job in study_upload_jobs.items():
                if job.get("username") == username and job.get("status") == "running":
                    return job_id
        return None

    def _refresh_job_state(username: str) -> Optional[Dict[str, Any]]:
        if not username:
            return None
        with refresh_jobs_lock:
            job = refresh_jobs.get(username)
            if not job:
                return None
            started_at = float(job.get("started_at") or 0)
            finished_at = float(job.get("finished_at") or 0)
            if finished_at and time.time() - finished_at > 300:
                refresh_jobs.pop(username, None)
                return None
            if not finished_at and started_at and time.time() - started_at > 600:
                refresh_jobs.pop(username, None)
                return None
            return dict(job)

    def _mark_refresh_job_started(username: str) -> bool:
        if not username:
            return False
        with refresh_jobs_lock:
            job = refresh_jobs.get(username)
            started_at = float(job.get("started_at") or 0) if job else 0
            finished_at = float(job.get("finished_at") or 0) if job else 0
            if started_at and not finished_at and time.time() - started_at <= 600:
                return False
            refresh_jobs[username] = {"started_at": time.time(), "status": "running"}
            return True

    def _mark_refresh_job_done(username: str, *, status: str = "success", error: Optional[str] = None) -> None:
        if not username:
            return
        with refresh_jobs_lock:
            job = refresh_jobs.get(username) or {"started_at": time.time()}
            job["status"] = status
            job["finished_at"] = time.time()
            if error:
                job["error"] = str(error)
            else:
                job.pop("error", None)
            refresh_jobs[username] = job

    def load_cache_from_disk(username: str) -> Optional[Dict[str, Any]]:
        return storage.load_user_cache(username)

    def save_cache_to_disk(username: str, payload: Dict[str, Any]) -> None:
        storage.save_user_cache(username, payload)

    def _start_web_session(username: str, *, moodle_session: Optional[str], is_guest: bool, is_admin: bool, permanent: bool) -> None:
        session.clear()
        session_token = secrets.token_urlsafe(24)
        storage.save_web_session(session_token, username)
        session["username"] = username
        session["session_token"] = session_token
        session["moodle_session"] = moodle_session
        session["is_guest"] = is_guest
        session["is_admin"] = is_admin
        session.permanent = permanent

    def current_user() -> Optional[Dict[str, Any]]:
        username = session.get("username")
        session_token = session.get("session_token")
        if username and session_token and storage.is_valid_web_session(session_token, username):
            return {
                "username": username,
                "moodle_session": session.get("moodle_session"),
                "is_guest": bool(session.get("is_guest")),
                "is_admin": bool(session.get("is_admin")),
            }
        if username or session_token:
            session.clear()
            session.modified = True
        return None

    def login_required(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not current_user():
                return redirect(url_for("login"))
            return fn(*args, **kwargs)

        return wrapper

    def admin_required(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            user = current_user()
            if not user:
                return redirect(url_for("login"))
            if not user.get("is_admin"):
                flash("僅限管理員使用讀書計畫。", "error")
                return redirect(url_for("index"))
            return fn(*args, **kwargs)

        return wrapper

    def _coerce_bool(value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "yes", "on"}:
                return True
            if lowered in {"0", "false", "no", "off"}:
                return False
            return None
        if isinstance(value, (int, float)):
            return bool(value)
        return None

    def _sanitize_preferences(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        clean: Dict[str, Any] = {}
        if not isinstance(raw, dict):
            return clean
        view_mode = raw.get("view_mode")
        if view_mode is None:
            view_mode = raw.get("viewMode")
        if isinstance(view_mode, str):
            lowered = view_mode.strip().lower()
            if lowered in {"course", "due"}:
                clean["view_mode"] = lowered
        valid_status_filters = ("pending", "completed", "graded", "overdue")

        def _normalize_status_filters(value: Any) -> List[str]:
            if isinstance(value, str):
                stripped = value.strip()
                if not stripped:
                    return []
                try:
                    parsed = json.loads(stripped)
                except Exception:
                    parsed = None
                if isinstance(parsed, list):
                    value = parsed
                else:
                    value = [stripped]
            if not isinstance(value, list):
                return []
            normalized: List[str] = []
            seen: Set[str] = set()
            for item in value:
                lowered = str(item or "").strip().lower()
                if lowered == "all":
                    return list(valid_status_filters)
                if lowered in valid_status_filters and lowered not in seen:
                    seen.add(lowered)
                    normalized.append(lowered)
            return normalized

        status_filter_provided = False
        status_filter = raw.get("status_filter")
        if "status_filter" in raw:
            status_filter_provided = True
        if status_filter is None:
            status_filter = raw.get("statusFilter")
            if "statusFilter" in raw:
                status_filter_provided = True
        if status_filter is None:
            status_filter = raw.get("statusFilters")
            if "statusFilters" in raw:
                status_filter_provided = True
        normalized_status_filters = _normalize_status_filters(status_filter)
        if status_filter_provided:
            clean["status_filter"] = normalized_status_filters
        include_ignored_overdue = raw.get("include_ignored_overdue")
        if include_ignored_overdue is None:
            include_ignored_overdue = raw.get("includeIgnoredOverdue")
        coerced_include = _coerce_bool(include_ignored_overdue)
        if coerced_include is not None:
            clean["include_ignored_overdue"] = coerced_include
        for key, alias in (
            ("show_overdue", "showOverdue"),
            ("show_completed", "showCompleted"),
            ("show_graded", "showGraded"),
        ):
            value = raw.get(key)
            if value is None and alias:
                value = raw.get(alias)
            coerced = _coerce_bool(value)
            if coerced is not None:
                clean[key] = coerced
        ignored_overdue_uids = raw.get("ignored_overdue_uids")
        if ignored_overdue_uids is None:
            ignored_overdue_uids = raw.get("ignoredOverdueUids")
        if isinstance(ignored_overdue_uids, list):
            clean["ignored_overdue_uids"] = [
                str(item).strip()
                for item in ignored_overdue_uids
                if str(item).strip()
            ][:500]
        return clean

    def _selected_view_username(raw_username: Optional[str], *, actor: Optional[Dict[str, Any]] = None) -> Optional[str]:
        user = actor or current_user()
        if not user:
            return None
        candidate = (raw_username or "").strip()
        if user.get("is_admin") and candidate:
            return candidate
        return user["username"]

    def _request_view_username() -> Optional[str]:
        raw = request.args.get("view_user")
        if raw is None and request.method != "GET":
            raw = request.form.get("view_user")
        return raw

    def get_viewed_username(*, actor: Optional[Dict[str, Any]] = None) -> Optional[str]:
        return _selected_view_username(_request_view_username(), actor=actor)

    def is_admin_viewing_other_user(*, actor: Optional[Dict[str, Any]] = None, viewed_username: Optional[str] = None) -> bool:
        user = actor or current_user()
        if not user or not user.get("is_admin"):
            return False
        target_username = (viewed_username or get_viewed_username(actor=user) or "").strip()
        return bool(target_username and target_username != user["username"])

    def list_admin_view_options(limit: int = 500) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        guest_prefix = f"{chr(0x8A2A)}{chr(0x5BA2)}_"
        for raw in storage.list_cached_users(limit=limit):
            username = str(raw.get("username") or "").strip()
            if not username:
                continue
            if username.startswith(guest_prefix) or username.startswith("Session-"):
                continue
            fetched_ts = raw.get("fetched_ts")
            fetched_label = "尚未更新"
            if fetched_ts:
                try:
                    fetched_label = datetime.fromtimestamp(int(fetched_ts), TAIPEI_TZ).strftime("%Y-%m-%d %H:%M")
                except Exception:
                    fetched_label = str(fetched_ts)
            try:
                assignment_count = int(raw.get("assignment_count") or 0)
            except (TypeError, ValueError):
                assignment_count = 0
            try:
                course_count = int(raw.get("course_count") or 0)
            except (TypeError, ValueError):
                course_count = 0
            items.append(
                {
                    "username": username,
                    "is_admin": bool(raw.get("is_admin")),
                    "fetched_ts": fetched_ts,
                    "fetched_label": fetched_label,
                    "assignment_count": assignment_count,
                    "course_count": course_count,
                }
            )
        return items

    def get_user_preferences(username: Optional[str] = None) -> Dict[str, Any]:
        prefs = dict(DEFAULT_PREFERENCES)
        resolved_username = _selected_view_username(username)
        if not resolved_username:
            return prefs
        stored = storage.load_user_preferences(resolved_username)
        prefs.update(_sanitize_preferences(stored))
        return prefs

    def update_user_preferences(partial: Dict[str, Any], *, username: Optional[str] = None) -> Dict[str, Any]:
        prefs = get_user_preferences(username)
        sanitized = _sanitize_preferences(partial)
        prefs.update(sanitized)
        resolved_username = _selected_view_username(username)
        if not resolved_username:
            return prefs
        storage.save_user_preferences(resolved_username, prefs)
        return prefs

    def get_assign_cache(username: Optional[str] = None) -> Optional[Dict[str, Any]]:
        resolved_username = _selected_view_username(username)
        if not resolved_username:
            return None
        return load_cache_from_disk(resolved_username)

    def _annotate_new_assignments(
        result: Optional[Dict[str, Any]],
        *,
        username: Optional[str],
        readonly: bool,
        now_ts: int,
    ) -> None:
        if not username or not isinstance(result, dict):
            return
        assignments = result.get("all_assignments")
        if not isinstance(assignments, list) or not assignments:
            return
        assignment_uids: List[str] = []
        for item in assignments:
            if not isinstance(item, dict):
                continue
            try:
                course_id = int(item.get("course_id"))
            except (TypeError, ValueError):
                continue
            uid = storage.assignment_uid(course_id, str(item.get("title") or "").strip(), item.get("url"))
            if not uid.strip():
                continue
            item["assignment_uid"] = uid
            assignment_uids.append(uid)
        if not assignment_uids:
            return
        first_seen_map = (
            storage.load_assignment_view_map(username, assignment_uids)
            if readonly
            else storage.mark_assignment_views(username, assignment_uids, seen_ts=now_ts)
        )
        for item in assignments:
            uid = str(item.get("assignment_uid") or "").strip()
            first_seen_ts = first_seen_map.get(uid)
            is_new = bool(first_seen_ts is not None and now_ts - int(first_seen_ts) <= NEW_ASSIGNMENT_WINDOW_SECONDS)
            item["first_seen_ts"] = first_seen_ts
            item["is_new"] = is_new
            item["new_until_ts"] = (int(first_seen_ts) + NEW_ASSIGNMENT_WINDOW_SECONDS) if first_seen_ts is not None else None
        for course in result.get("courses") or []:
            if not isinstance(course, dict):
                continue
            for item in course.get("assignments") or []:
                if not isinstance(item, dict):
                    continue
                try:
                    course_id = int(item.get("course_id"))
                except (TypeError, ValueError):
                    course_id = None
                uid = storage.assignment_uid(
                    course_id,
                    str(item.get("title") or "").strip(),
                    item.get("url"),
                ) if course_id is not None else ""
                first_seen_ts = first_seen_map.get(uid)
                item["assignment_uid"] = uid
                item["first_seen_ts"] = first_seen_ts
                item["is_new"] = bool(first_seen_ts is not None and now_ts - int(first_seen_ts) <= NEW_ASSIGNMENT_WINDOW_SECONDS)
                item["new_until_ts"] = (int(first_seen_ts) + NEW_ASSIGNMENT_WINDOW_SECONDS) if first_seen_ts is not None else None

    def set_assign_cache_for_user(username: str, result: Dict[str, Any], excel_data: Optional[str]) -> None:
        if not username:
            return
        existing = load_cache_from_disk(username) or {}
        slim = dict(result)
        slim.pop("debug_files", None)
        slim.pop("login_method", None)
        payload = {
            "result": json_safe(slim),
            "excel_data": excel_data,
            "ts": int(datetime.now(TAIPEI_TZ).timestamp()),
        }
        stored_prefs = _sanitize_preferences(existing.get("preferences"))
        if stored_prefs:
            payload["preferences"] = stored_prefs
        save_cache_to_disk(username, payload)

    def set_assign_cache(result: Dict[str, Any], excel_data: Optional[str]) -> None:
        user = current_user()
        if not user:
            return
        set_assign_cache_for_user(user["username"], result, excel_data)

    def _generate_excel_data(assignments: Optional[List[Dict[str, Any]]]) -> Optional[str]:
        if not assignments:
            return None
        try:
            excel_stream = build_excel(assignments, return_bytes=True)
            return base64.b64encode(excel_stream.getvalue()).decode("ascii")
        except Exception:
            return None

    def clear_assign_cache() -> None:
        user = current_user()
        if not user:
            return
        storage.delete_user_cache(user["username"])

    ANNOUNCEMENT_LIMIT = 50

    def _serialize_announcement(entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(entry, dict):
            return None
        title = str(entry.get("title") or "").strip()
        content = str(entry.get("content") or "").strip()
        if not title or not content:
            return None
        created_at = str(entry.get("created_at") or "").strip()
        created_label = str(entry.get("created_label") or "").strip()
        author = str(entry.get("author") or "").strip()
        ident = str(entry.get("id") or "").strip()
        if not ident:
            ident = secrets.token_hex(6)
        if not created_label and created_at:
            try:
                created_label = datetime.fromisoformat(created_at).astimezone(TAIPEI_TZ).strftime("%Y-%m-%d %H:%M")
            except Exception:
                created_label = created_at
        try:
            like_count = int(entry.get("like_count") or 0)
        except (TypeError, ValueError):
            like_count = 0
        try:
            dislike_count = int(entry.get("dislike_count") or 0)
        except (TypeError, ValueError):
            dislike_count = 0
        user_vote = str(entry.get("user_vote") or "").strip().lower() or None
        return {
            "id": ident,
            "title": title,
            "content": content,
            "created_at": created_at,
            "created_label": created_label,
            "author": author,
            "like_count": like_count,
            "dislike_count": dislike_count,
            "user_vote": user_vote,
        }

    def load_announcements(username: Optional[str] = None) -> List[Dict[str, Any]]:
        if username is None and has_request_context():
            user = current_user()
            if user:
                username = user.get("username")
        items: List[Dict[str, Any]] = []
        for raw in storage.list_announcements_with_votes(ANNOUNCEMENT_LIMIT, username=username):
            parsed = _serialize_announcement(raw)
            if parsed:
                items.append(parsed)
        return items

    def add_announcement(title: str, content: str, author: Optional[str]) -> None:
        title = title.strip()
        content = content.strip()
        if not title or not content:
            return
        now = datetime.now(TAIPEI_TZ)
        entry = {
            "id": secrets.token_hex(6),
            "title": title,
            "content": content,
            "author": author or "",
            "created_at": now.isoformat(),
            "created_label": now.strftime("%Y-%m-%d %H:%M"),
        }
        storage.insert_announcement(entry, ANNOUNCEMENT_LIMIT)

    def delete_announcement_entry(announcement_id: str) -> bool:
        announcement_id = (announcement_id or "").strip()
        if not announcement_id:
            return False
        return storage.delete_announcement(announcement_id)

    def set_announcement_vote(announcement_id: str, username: str, vote_type: Optional[str]) -> Optional[Dict[str, Any]]:
        updated = storage.set_announcement_vote(announcement_id, username, vote_type)
        if not updated:
            return None
        return _serialize_announcement(updated)

    FEEDBACK_LIMIT = 200
    VALID_FEEDBACK_STATUS = {"open", "resolved"}

    def add_feedback_entry(message: str, email: Optional[str], username: Optional[str]) -> int:
        message = (message or "").strip()
        email = (email or "").strip()
        username = (username or "").strip()
        if not message:
            return 0
        now = datetime.now(TAIPEI_TZ)
        entry = {
            "username": username or None,
            "email": email or None,
            "message": message,
            "status": "open",
            "created_at": now.isoformat(),
        }
        return storage.add_feedback(entry)

    def list_feedback_entries() -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for raw in storage.list_feedback(FEEDBACK_LIMIT):
            parsed = {
                "id": raw.get("id"),
                "username": (raw.get("username") or "-"),
                "email": (raw.get("email") or "-"),
                "message": raw.get("message") or "",
                "status": raw.get("status") or "open",
                "created_at": raw.get("created_at") or "",
            }
            ts_raw = parsed["created_at"]
            if ts_raw:
                try:
                    dt = datetime.fromisoformat(ts_raw)
                    if not dt.tzinfo:
                        dt = dt.replace(tzinfo=TAIPEI_TZ)
                    parsed["created_label"] = dt.astimezone(TAIPEI_TZ).strftime("%Y-%m-%d %H:%M")
                except Exception:
                    parsed["created_label"] = ts_raw
            else:
                parsed["created_label"] = "-"
            items.append(parsed)
        return items

    def update_feedback_status_entry(feedback_id: int, status: str) -> bool:
        if status not in VALID_FEEDBACK_STATUS:
            return False
        try:
            fid = int(feedback_id)
        except (TypeError, ValueError):
            return False
        return storage.update_feedback_status(fid, status)

    def _google_ready() -> bool:
        return bool(google_client_id and google_client_secret and google_redirect_uri)

    def _assignment_uid(item: Dict[str, Any]) -> str:
        return f"{item.get('course_id')}|{item.get('title')}|{item.get('url')}"

    def _select_assignments_from_result(result: Optional[Dict[str, Any]], selected_uids: List[str]) -> List[Dict[str, Any]]:
        if not isinstance(result, dict) or not selected_uids:
            return []
        selected = set(selected_uids)
        return [
            item
            for item in result.get("all_assignments", [])
            if _assignment_uid(item) in selected
        ]

    def _google_redirect_uri() -> str:
        return google_redirect_uri or url_for("google_callback", _external=True)

    def _google_state_signer() -> URLSafeTimedSerializer:
        return URLSafeTimedSerializer(app.secret_key, salt="google-calendar")

    def _build_google_state() -> str:
        token = secrets.token_urlsafe(16)
        return _google_state_signer().dumps({"nonce": token})

    def _verify_google_state(value: str) -> bool:
        try:
            _google_state_signer().loads(value, max_age=300)
            return True
        except SignatureExpired:
            flash("Google 授權逾時，請再試一次。", "error")
        except BadSignature:
            flash("Google 授權驗證失敗，請重新操作。", "error")
        return False

    def load_google_tokens(username: str) -> Optional[Dict[str, Any]]:
        return storage.load_google_tokens(username)

    def save_google_tokens(username: str, payload: Dict[str, Any]) -> None:
        storage.save_google_tokens(username, dict(payload))

    def clear_google_tokens(username: str) -> None:
        storage.clear_google_tokens(username)

    def _client_ip() -> Optional[str]:
        if not has_request_context():
            return None
        forwarded = request.headers.get("X-Forwarded-For", "")
        if forwarded:
            for part in forwarded.split(","):
                ip = part.strip()
                if ip:
                    return ip
        return request.remote_addr

    def record_ui_event(action: str, status: str = "success", meta: Optional[Dict[str, Any]] = None) -> None:
        if not action:
            return
        details = dict(meta or {})
        user = current_user() if has_request_context() else None
        if user:
            details.setdefault("username", user["username"])
            details.setdefault("is_guest", user.get("is_guest"))
            details.setdefault("is_admin", user.get("is_admin"))
        traffic_tracker.record_visit(_client_ip(), action=action, status=status, metadata=details)

    def usage_stats() -> Dict[str, int]:
        return traffic_tracker.snapshot()

    def current_stats_version() -> int:
        return traffic_tracker.version()

    def _ensure_google_access_token(username: str, tokens: Dict[str, Any]) -> Dict[str, Any]:
        if not _google_ready():
            raise RuntimeError("尚未設定 Google OAuth。")
        expires_at = tokens.get("expires_at", 0)
        if time.time() < expires_at - 60:
            return tokens
        refresh_token = tokens.get("refresh_token")
        if not refresh_token:
            raise RuntimeError("Google access token 已過期，請重新授權。")
        refreshed = refresh_google_token(
            refresh_token,
            client_id=google_client_id,
            client_secret=google_client_secret,
        )
        tokens["access_token"] = refreshed.get("access_token")
        tokens["expires_at"] = compute_expiry(refreshed.get("expires_in", 3600))
        save_google_tokens(username, tokens)
        return tokens

    def _escape_ics_text(value: Optional[str]) -> str:
        text = (value or "").replace("\\", "\\\\")
        text = text.replace("\n", "\\n").replace(",", "\\,").replace(";", "\\;")
        return text

    def _build_calendar(assignments: List[Dict[str, Any]]) -> Optional[str]:
        if not assignments:
            return None
        dtstamp = datetime.now(TAIPEI_TZ).strftime("%Y%m%dT%H%M%S")
        lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//NYCU E3//EN",
            "CALSCALE:GREGORIAN",
            "X-WR-TIMEZONE:Asia/Taipei",
        ]
        for idx, entry in enumerate(assignments):
            due_ts = entry.get("due_ts")
            if not due_ts:
                continue
            due_dt = datetime.fromtimestamp(due_ts, tz=TAIPEI_TZ)
            end_dt = due_dt + timedelta(hours=1)
            dt_value = due_dt.strftime("%Y%m%dT%H%M%S")
            dt_end_value = end_dt.strftime("%Y%m%dT%H%M%S")
            summary = entry.get("title", "").strip()
            description = entry.get("url") or ""
            lines += [
                "BEGIN:VEVENT",
                f"UID=e3-{entry.get('course_id', 'unknown')}-{idx}@e3",
                f"DTSTAMP={dtstamp}",
                f"DTSTART;TZID=Asia/Taipei:{dt_value}",
                f"DTEND;TZID=Asia/Taipei:{dt_end_value}",
                f"SUMMARY:{_escape_ics_text(summary)}",
                f"DESCRIPTION:{_escape_ics_text(description)}",
                "END:VEVENT",
            ]
        lines.append("END:VCALENDAR")
        return "\r\n".join(lines)

    def _build_dashboard_context(user: Dict[str, Any]) -> Dict[str, Any]:
        admin_view_options: List[Dict[str, Any]] = []
        viewed_username = user["username"]
        if user.get("is_admin"):
            admin_view_options = list_admin_view_options()
            requested_view_username = (_request_view_username() or "").strip()
            if requested_view_username and requested_view_username != user["username"]:
                valid_usernames = {item["username"] for item in admin_view_options}
                if requested_view_username in valid_usernames:
                    viewed_username = requested_view_username
                else:
                    flash("找不到指定帳號的資料快取，已切回目前登入帳號。", "warning")
        is_admin_view = is_admin_viewing_other_user(actor=user, viewed_username=viewed_username)
        if user["username"] not in {item["username"] for item in admin_view_options}:
            self_cache = load_cache_from_disk(user["username"]) or {}
            admin_view_options.insert(
                0,
                {
                    "username": user["username"],
                    "is_admin": bool(user.get("is_admin")),
                    "fetched_ts": self_cache.get("ts"),
                    "fetched_label": datetime.fromtimestamp(
                        int(self_cache.get("ts")), TAIPEI_TZ
                    ).strftime("%Y-%m-%d %H:%M")
                    if self_cache.get("ts")
                    else "尚未更新",
                    "assignment_count": len((self_cache.get("result") or {}).get("all_assignments", [])),
                    "course_count": len((self_cache.get("result") or {}).get("courses", [])),
                },
            )
        cache = get_assign_cache(viewed_username)
        result = cache.get("result") if cache else None
        excel_data = cache.get("excel_data") if cache else None
        guest_mode = bool(user.get("is_guest"))
        if result and not excel_data:
            excel_data = _generate_excel_data(result.get("all_assignments"))
            if excel_data:
                set_assign_cache_for_user(viewed_username, result, excel_data)
        if not result and not guest_mode and not is_admin_view:
            flash("正在載入資料，請稍候...", "info")
        google_linked = bool(not is_admin_view and load_google_tokens(user["username"]))
        stats = usage_stats()
        stats_version_value = current_stats_version()
        announcements_list = load_announcements(None if is_admin_view else user["username"])
        cache_ts_val = cache.get("ts") if cache else None
        now_ts = int(datetime.now(TAIPEI_TZ).timestamp())
        _annotate_new_assignments(
            result,
            username=viewed_username,
            readonly=is_admin_view,
            now_ts=now_ts,
        )
        last_updated_label = None
        if cache_ts_val:
            try:
                last_updated_label = datetime.fromtimestamp(int(cache_ts_val), TAIPEI_TZ).strftime("%Y-%m-%d %H:%M")
            except Exception:
                last_updated_label = None
        return {
            "result": result,
            "excel_data": excel_data,
            "user": user,
            "google_ready": _google_ready(),
            "google_linked": google_linked,
            "guest_mode": guest_mode,
            "stats": stats,
            "stats_version": stats_version_value,
            "now_ts": now_ts,
            "preferences": get_user_preferences(viewed_username),
            "cache_ts": cache_ts_val,
            "last_updated_ts": cache_ts_val,
            "last_updated_label": last_updated_label,
            "announcements": announcements_list,
            "announcement_version": announcements_list[0]["id"] if announcements_list else None,
            "viewed_username": viewed_username,
            "is_admin_view": is_admin_view,
            "admin_view_options": admin_view_options,
        }

    def _study_plan_week_rows(videos: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
        start_day = datetime.strptime(STUDY_PLAN_START, "%Y-%m-%d").date()
        today = _study_plan_business_date()
        replan_settings = storage.get_study_plan_replan_settings()
        videos_by_subject: Dict[str, List[Dict[str, Any]]] = {}
        for video in videos:
            subject = str(video.get("subject") or "")
            videos_by_subject.setdefault(subject, []).append(video)
        for subject_videos in videos_by_subject.values():
            subject_videos.sort(key=lambda item: int(item.get("sequence") or 0))

        watched_by_subject = {
            subject: sum(
                min(
                    _study_plan_nonnegative_number(item.get("watched_seconds")),
                    _study_plan_nonnegative_number(item.get("duration_seconds")),
                )
                for item in videos_by_subject.get(subject, [])
            )
            for subject in STUDY_PLAN_SUBJECTS
        }
        video_ranges: Dict[str, List[Tuple[float, float, Dict[str, Any]]]] = {}
        for subject, subject_videos in videos_by_subject.items():
            cursor_seconds = 0.0
            ranges: List[Tuple[float, float, Dict[str, Any]]] = []
            for item in subject_videos:
                duration_seconds = _study_plan_nonnegative_number(item.get("duration_seconds"))
                ranges.append((cursor_seconds, cursor_seconds + duration_seconds, item))
                cursor_seconds += duration_seconds
            video_ranges[subject] = ranges

        short_subject = {
            "線性代數": "線代",
            "離散數學": "離散",
            "資料結構": "DS",
            "作業系統": "OS",
            "計算機組織": "計組",
            "演算法": "演算法",
        }
        week_rows: List[Dict[str, Any]] = []
        planned_before = {subject: 0.0 for subject in STUDY_PLAN_SUBJECTS}
        replanned_before = {subject: 0.0 for subject in STUDY_PLAN_SUBJECTS}
        for definition in _study_plan_schedule_definitions(videos, replan_settings):
            week_start = definition["start"]
            week_end = definition["end"]
            subject_targets = dict(definition["subject_targets"])
            subjects = list(definition["subjects"])
            credit_baselines = dict(definition.get("credit_baselines") or {})
            subject_credits: Dict[str, float] = {}
            weekly_videos: Dict[int, Dict[str, Any]] = {}
            for subject, target_seconds in subject_targets.items():
                if subject in credit_baselines:
                    prior_seconds = credit_baselines.get(subject, 0.0) + replanned_before.get(subject, 0.0)
                else:
                    prior_seconds = planned_before.get(subject, 0.0)
                credited_seconds = min(
                    max(watched_by_subject.get(subject, 0.0) - prior_seconds, 0.0),
                    target_seconds,
                )
                subject_credits[subject] = credited_seconds
                range_end = prior_seconds + target_seconds
                for video_start, video_end, video in video_ranges.get(subject, []):
                    if video_end > prior_seconds and video_start < range_end:
                        weekly_videos[int(video.get("id") or id(video))] = video

            remaining_credit = dict(subject_credits)
            daily_recommendations: List[Dict[str, Any]] = []
            for index, daily_target in enumerate(definition["daily_targets"]):
                allocations = dict(daily_target["allocations"])
                target_seconds = sum(allocations.values())
                credited_seconds = 0.0
                daily_subject_progress: List[Dict[str, Any]] = []
                for subject, subject_target in allocations.items():
                    amount = min(subject_target, remaining_credit.get(subject, 0.0))
                    credited_seconds += amount
                    remaining_credit[subject] = max(0.0, remaining_credit.get(subject, 0.0) - amount)
                    daily_subject_progress.append(
                        {
                            "name": subject,
                            "short": short_subject.get(subject, subject),
                            "target_seconds": subject_target,
                            "target_hours": round(subject_target / 3600, 2),
                            "credited_seconds": amount,
                            "credited_hours": round(amount / 3600, 2),
                            "completion": round(
                                _study_plan_completion_percent(subject_target, amount),
                                1,
                            ),
                        }
                    )
                has_target = target_seconds > 0
                if has_target:
                    completion = _study_plan_completion_percent(target_seconds, credited_seconds)
                else:
                    completion = 0.0
                current_day = daily_target["date"]
                if not has_target:
                    if current_day == today:
                        state = "active"
                        state_label = "彈性日"
                    else:
                        state = "upcoming"
                        state_label = "未排程"
                elif completion >= 100:
                    state = "early" if today < current_day else "complete"
                    state_label = "提早完成" if state == "early" else "完成"
                elif completion > 0:
                    state = "early" if today < current_day else "partial"
                    state_label = "超前" if state == "early" else "部分"
                elif current_day == today:
                    state = "active"
                    state_label = "進行中"
                elif today > current_day:
                    state = "behind" if target_seconds > 0 else "complete"
                    state_label = "待補" if state == "behind" else "完成"
                else:
                    state = "upcoming"
                    state_label = "未開始"
                focus = daily_target.get("focus") or "＋".join(
                    f"{short_subject.get(subject, subject)} {round(seconds / 3600, 2):g}h"
                    for subject, seconds in allocations.items()
                )
                daily_recommendations.append(
                    {
                        "label": STUDY_PLAN_DAILY_LABELS[index],
                        "date": current_day.isoformat(),
                        "short_date": current_day.strftime("%m/%d"),
                        "focus": focus or "彈性整理",
                        "allocations": allocations,
                        "subject_progress": daily_subject_progress,
                        "has_target": has_target,
                        "target_seconds": target_seconds,
                        "credited_seconds": credited_seconds,
                        "hours": round(target_seconds / 3600, 2),
                        "credited_hours": round(credited_seconds / 3600, 2),
                        "completion": round(completion, 1),
                        "state": state,
                        "state_label": state_label,
                    }
                )

            target_seconds = sum(subject_targets.values())
            watched_seconds = sum(subject_credits.values())
            week_is_complete = _study_plan_total_is_complete(target_seconds, watched_seconds)
            completion = _study_plan_completion_percent(
                target_seconds,
                watched_seconds,
                complete_override=week_is_complete,
            )
            if week_is_complete:
                state = "early" if today < week_end else "complete"
                state_label = "提早完成" if state == "early" else "已達標"
            elif week_start <= today <= week_end:
                state = "active"
                state_label = "進行中"
            elif today > week_end:
                state = "behind"
                state_label = "待補"
            else:
                state = "upcoming"
                state_label = "未開始"
            subject_mix = [
                {
                    "name": subject,
                    "short": short_subject.get(subject, subject),
                    "target_seconds": subject_targets[subject],
                    "target_hours": round(subject_targets[subject] / 3600, 1),
                    "watched_seconds": subject_credits.get(subject, 0.0),
                    "watched_hours": round(subject_credits.get(subject, 0.0) / 3600, 1),
                    "completion": round(
                        _study_plan_completion_percent(
                            subject_targets[subject],
                            subject_credits.get(subject, 0.0),
                        ),
                        1,
                    ),
                }
                for subject in subjects
            ]
            week_rows.append(
                {
                    "number": definition["number"],
                    "subject": "＋".join(subjects) if subjects else "彈性整理",
                    "subjects": subjects,
                    "subject_mix": subject_mix,
                    "start": week_start.isoformat(),
                    "end": week_end.isoformat(),
                    "target_minutes": target_seconds / 60,
                    "target_seconds": target_seconds,
                    "video_hours": round(target_seconds / 3600, 1),
                    "suggested_weekly_hours": round(target_seconds / 3600, 1),
                    "active_days": sum(1 for day in daily_recommendations if day["target_seconds"] > 0),
                    "daily_average_hours": round(
                        target_seconds
                        / 3600
                        / max(1, sum(1 for day in daily_recommendations if day["target_seconds"] > 0)),
                        1,
                    ),
                    "daily_recommendations": daily_recommendations,
                    "video_count": len(weekly_videos),
                    "completed_videos": sum(
                        1
                        for video in weekly_videos.values()
                        if _study_plan_video_is_complete(video.get("duration_seconds"), video.get("watched_seconds"))
                    ),
                    "watched_seconds": watched_seconds,
                    "watched_minutes": round(watched_seconds / 60, 1),
                    "watched_hours": round(watched_seconds / 3600, 2),
                    "remaining_hours": round(
                        max(0.0, target_seconds - watched_seconds) / 3600,
                        1,
                    ),
                    "completion": completion,
                    "state": state,
                    "state_label": state_label,
                    "is_replanned": bool(definition.get("is_replanned")),
                }
            )
            for subject, target_seconds in subject_targets.items():
                if subject in credit_baselines:
                    replanned_before[subject] = replanned_before.get(subject, 0.0) + target_seconds
                else:
                    planned_before[subject] = planned_before.get(subject, 0.0) + target_seconds

        active_week = next((row for row in week_rows if row["start"] <= today.isoformat() <= row["end"]), None)
        if active_week is None:
            active_week = week_rows[0] if today < start_day else week_rows[-1]
        summary = _study_plan_progress_summary(videos)
        return week_rows, active_week, summary

    def _build_study_home_context(
        videos: List[Dict[str, Any]],
        week_rows: List[Dict[str, Any]],
        current_week: Dict[str, Any],
        summary: Dict[str, Any],
    ) -> Dict[str, Any]:
        today = _study_plan_business_date()
        plan_start = datetime.strptime(STUDY_PLAN_START, "%Y-%m-%d").date()
        active_replan = storage.get_study_plan_replan_settings()
        effective_plan_end = str((active_replan or {}).get("end_date") or STUDY_PLAN_END)
        try:
            plan_end = datetime.strptime(effective_plan_end, "%Y-%m-%d").date()
        except ValueError:
            effective_plan_end = STUDY_PLAN_END
            plan_end = datetime.strptime(STUDY_PLAN_END, "%Y-%m-%d").date()
        total_plan_days = max(1, (plan_end - plan_start).days + 1)
        elapsed_days = min(max((today - plan_start).days + 1, 0), total_plan_days)
        elapsed_percent = min(100.0, max(0.0, elapsed_days / total_plan_days * 100))
        completion = float(summary.get("completion") or 0)
        total_target_minutes = float(summary.get("total_target") or 0)
        watched_minutes_total = float(summary.get("total_watched") or 0)
        target_minutes_by_today = 0.0
        today_target_minutes = 0.0
        today_iso = today.isoformat()
        for row in week_rows:
            row_start = str(row.get("start") or "")
            row_end = str(row.get("end") or "")
            if row_end and row_end < today_iso:
                target_minutes_by_today += float(row.get("target_minutes") or 0)
            elif row_start and row_start <= today_iso <= row_end:
                for day in row.get("daily_recommendations", []):
                    day_key = str(day.get("date") or "")
                    day_target_minutes = float(day.get("target_seconds") or 0) / 60
                    if day_key == today_iso:
                        today_target_minutes += day_target_minutes
                    if day_key <= today_iso:
                        target_minutes_by_today += day_target_minutes
        target_minutes_by_today = min(max(target_minutes_by_today, 0.0), total_target_minutes)
        scheduled_percent = min(100.0, (target_minutes_by_today / total_target_minutes * 100) if total_target_minutes else 0.0)
        pace_delta = completion - scheduled_percent
        pace_minutes = watched_minutes_total - target_minutes_by_today
        progress_race = _study_plan_progress_race(
            watched_minutes_total,
            target_minutes_by_today,
            total_target_minutes,
            today_target_minutes,
        )
        pace_state = str(progress_race["state"])
        pace_message = str(progress_race["headline_message"])
        if pace_state == "behind":
            pace_state = "behind"
            pace_label = "待補"
        elif pace_state == "early":
            pace_label = "超前進度"
        else:
            pace_label = "穩定推進"

        subject_rows: List[Dict[str, Any]] = []
        videos_by_subject: Dict[str, List[Dict[str, Any]]] = {subject: [] for subject in STUDY_PLAN_SUBJECTS}
        for video in videos:
            videos_by_subject.setdefault(str(video.get("subject") or ""), []).append(video)
        for subject in STUDY_PLAN_SUBJECTS:
            subject_videos = videos_by_subject.get(subject, [])
            subject_progress = _study_plan_progress_summary(subject_videos)
            target_seconds = float(subject_progress["total_target_seconds"])
            watched_seconds = float(subject_progress["total_watched_seconds"])
            completed_count = int(subject_progress["completed_videos"])
            subject_is_complete = _study_plan_subject_is_complete(target_seconds, watched_seconds)
            subject_weeks = [row for row in week_rows if subject in row.get("subjects", [row.get("subject")])]
            subject_state, subject_state_label = _study_plan_subject_status(
                subject_weeks,
                today,
                subject_is_complete=subject_is_complete,
            )
            subject_completion = min(
                100.0,
                (watched_seconds / target_seconds * 100) if target_seconds else 0.0,
            )
            subject_rows.append(
                {
                    "name": subject,
                    "completion": round(subject_completion, 1),
                    "target_hours": round(target_seconds / 3600, 1),
                    "watched_hours": round(watched_seconds / 3600, 1),
                    "completed_videos": completed_count,
                    "total_videos": len(subject_videos),
                    "state": subject_state,
                    "state_label": subject_state_label,
                }
            )
        weak_subjects = sorted(
            [item for item in subject_rows if item["completion"] < 100],
            key=lambda item: (item["completion"], -item["total_videos"]),
        )[:3]

        today_row = next(
            (row for row in current_week.get("daily_recommendations", []) if row.get("date") == today.isoformat()),
            None,
        )
        if today_row is None:
            today_row = next(
                (row for row in current_week.get("daily_recommendations", []) if row.get("state") in {"active", "upcoming", "behind"}),
                (current_week.get("daily_recommendations") or [{}])[0],
            )
        next_videos: List[Dict[str, Any]] = []
        overdue_subjects = [item["name"] for item in subject_rows if item["state"] == "behind"]
        current_subjects = list(current_week.get("subjects") or [str(current_week.get("subject") or "")])
        suggested_subjects = list(dict.fromkeys([*overdue_subjects, *current_subjects]))
        for current_subject in suggested_subjects:
            for video in videos_by_subject.get(current_subject, []):
                duration = float(video.get("duration_seconds") or 0)
                watched = min(float(video.get("watched_seconds") or 0), duration)
                if duration <= 0 or _study_plan_video_is_complete(duration, watched):
                    continue
                next_videos.append(
                    {
                        "id": int(video.get("id") or 0),
                        "subject": str(video.get("subject") or current_subject),
                        "sequence": int(video.get("sequence") or 0),
                        "title": str(video.get("title") or ""),
                        "remaining_minutes": round(max(0.0, duration - watched) / 60, 1),
                        "completion": round(min(100.0, watched / duration * 100), 1),
                    }
                )
                break

        last_updated_label = "尚未開始"
        latest_dt: Optional[datetime] = None
        for video in videos:
            updated = str(video.get("updated_at") or "").strip()
            if not updated:
                continue
            try:
                parsed = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                if latest_dt is None or parsed > latest_dt:
                    latest_dt = parsed
                    last_updated_label = updated
            except ValueError:
                last_updated_label = updated
        timeline_nodes = [
            {
                "number": row["number"],
                "subject": row["subject"],
                "completion": round(float(row["completion"]), 1),
                "state": row["state"],
                "state_label": row["state_label"],
            }
            for row in week_rows
        ]
        remaining_hours = max(0.0, (float(summary.get("total_target") or 0) - float(summary.get("total_watched") or 0)) / 60)
        total_hours = max(0.0, float(summary.get("total_target") or 0) / 60)
        watched_hours = max(0.0, float(summary.get("total_watched") or 0) / 60)
        visual_angle = round(completion / 100 * 360, 1)
        pace_hours = pace_minutes / 60
        daily_target_minutes = (float(summary.get("total_target") or 0) / total_plan_days) if total_plan_days else 0.0
        try:
            current_week_end = datetime.strptime(str(current_week.get("end") or ""), "%Y-%m-%d").date()
        except ValueError:
            current_week_end = today
        days_remaining_this_week = max(1, (current_week_end - today).days + 1)
        catchup_minutes_per_day = (
            math.ceil(abs(pace_minutes) / days_remaining_this_week) if pace_minutes < 0 else 0
        )
        catchup_hours, catchup_remainder_minutes = divmod(catchup_minutes_per_day, 60)
        if catchup_hours and catchup_remainder_minutes:
            catchup_time_label = f"{catchup_hours} 小時 {catchup_remainder_minutes} 分鐘"
        elif catchup_hours:
            catchup_time_label = f"{catchup_hours} 小時"
        else:
            catchup_time_label = f"{catchup_remainder_minutes} 分鐘"
        buffer_days = max(0.0, pace_minutes / daily_target_minutes) if pace_minutes > 0 and daily_target_minutes else 0.0
        pace_meter_position = min(96.0, max(4.0, 50.0 + pace_delta * 2.2))
        if pace_state == "behind":
            pace_action = f"若要這週追完，每天需多看 {catchup_time_label}。"
            pace_delta_stat_label = "本週每日加看"
            pace_delta_stat_value = catchup_time_label
        elif pace_state == "early":
            pace_action = f"已累積約 {buffer_days:.1f} 天緩衝，可休息或提前下一週。"
            pace_delta_stat_label = "時間緩衝"
            pace_delta_stat_value = str(progress_race["delta_label"])
        else:
            if progress_race["within_daily_allowance"]:
                pace_action = "目前不列為落後，完成今天安排的內容即可。"
                pace_delta_stat_label = "今日容許差距"
                pace_delta_stat_value = str(progress_race["today_target_label"])
            else:
                pace_action = "維持目前節奏即可貼近計畫進度。"
                pace_delta_stat_label = "進度差距"
                pace_delta_stat_value = str(progress_race["delta_label"])

        pace_insight = {
            "state": pace_state,
            "label": pace_label,
            "message": pace_message,
            "action": pace_action,
            "primary_value": progress_race["headline_value"],
            "primary_unit": progress_race["headline_unit"],
            "delta_hours": round(pace_hours, 1),
            "catchup_minutes_per_day": catchup_minutes_per_day,
            "days_remaining_this_week": days_remaining_this_week,
            "buffer_days": round(buffer_days, 1),
            "meter_position": round(pace_meter_position, 1),
            "target_today_hours": round(target_minutes_by_today / 60, 1),
            "watched_hours": round(watched_minutes_total / 60, 1),
            "delta_stat_label": pace_delta_stat_label,
            "delta_stat_value": pace_delta_stat_value,
        }

        metric_cards = [
            {
                "label": "整體完成率",
                "value": f"{completion:.1f}",
                "unit": "%",
                "icon": "progress",
                "state": pace_state,
            },
            {
                "label": "觀看時數",
                "value": f"{watched_hours:.1f}",
                "unit": f"/ {total_hours:.1f}h",
                "icon": "clock",
                "state": "green",
            },
            {
                "label": "完成影片",
                "value": str(int(summary.get("completed_videos") or 0)),
                "unit": f"/ {int(summary.get('total_videos') or 0)} 支",
                "icon": "check",
                "state": "purple",
            },
        ]

        chart_days = list(current_week.get("daily_recommendations") or [])
        week_start_date = str(current_week.get("start") or "")
        recent_days = [(today - timedelta(days=offset)).isoformat() for offset in range(6, -1, -1)]
        tracked_start_day = min([day for day in [STUDY_PLAN_START, week_start_date, recent_days[0]] if day])
        activity_events = storage.list_study_plan_activity_events(
            start_day=tracked_start_day,
            end_day=today.isoformat(),
        )
        activity_events_by_day: Dict[str, List[Dict[str, Any]]] = {}
        for event in activity_events:
            event_day = str(event.get("day") or "")
            if event_day:
                activity_events_by_day.setdefault(event_day, []).append(event)
        last_watched_event = next(
            (
                item
                for item in reversed(activity_events)
                if float(item.get("delta_seconds") or 0) > 0
            ),
            None,
        )
        continue_video: Optional[Dict[str, Any]] = None
        if last_watched_event:
            last_video_id = int(last_watched_event.get("video_id") or 0)
            last_video = next(
                (video for video in videos if int(video.get("id") or 0) == last_video_id),
                None,
            )
            if last_video:
                duration_seconds = max(0.0, float(last_video.get("duration_seconds") or 0))
                watched_seconds = min(
                    max(0.0, float(last_video.get("watched_seconds") or 0)),
                    duration_seconds,
                )
                continue_video = {
                    "id": last_video_id,
                    "subject": str(last_video.get("subject") or ""),
                    "sequence": int(last_video.get("sequence") or 0),
                    "title": str(last_video.get("title") or ""),
                    "remaining_minutes": round(max(0.0, duration_seconds - watched_seconds) / 60, 1),
                    "completion": round(
                        min(100.0, watched_seconds / duration_seconds * 100)
                        if duration_seconds
                        else 0.0,
                        1,
                    ),
                }
        if continue_video is None and next_videos:
            continue_video = dict(next_videos[0])
        # Each grouped event stores the first position of its learning day and the
        # final position saved that day. Keep corrections negative so a rewind never
        # leaves an earlier high-water mark in the weekly cumulative calculation.
        activity_seconds_by_day = {
            day: sum(float(item.get("delta_seconds") or 0) for item in events)
            for day, events in activity_events_by_day.items()
        }
        # Calendar intensity represents newly watched video progress. Corrections
        # that move a video's saved position backwards must not create negative
        # study time or cancel progress made on another video that day.
        calendar_seconds_by_day = {
            day: sum(max(0.0, float(item.get("delta_seconds") or 0)) for item in events)
            for day, events in activity_events_by_day.items()
        }
        calendar_days: List[Dict[str, Any]] = []
        for activity_day, events in sorted(activity_events_by_day.items()):
            calendar_activities: List[Dict[str, Any]] = []
            for item in events:
                delta_seconds = float(item.get("delta_seconds") or 0)
                if abs(delta_seconds) < 0.5:
                    continue
                duration_seconds = max(0.0, float(item.get("duration_seconds") or 0))
                watched_seconds = max(0.0, float(item.get("watched_seconds") or 0))
                calendar_activities.append(
                    {
                        "subject": str(item.get("subject") or ""),
                        "sequence": int(item.get("sequence") or 0),
                        "title": str(item.get("title") or ""),
                        "seconds": int(round(abs(delta_seconds))),
                        "is_correction": delta_seconds < 0,
                        "completion": round(
                            min(100.0, watched_seconds / duration_seconds * 100)
                            if duration_seconds
                            else 0.0,
                            1,
                        ),
                    }
                )
            calendar_seconds = calendar_seconds_by_day.get(activity_day, 0.0)
            if calendar_seconds > 0 or calendar_activities:
                calendar_days.append(
                    {
                        "date": activity_day,
                        "seconds": int(round(calendar_seconds)),
                        "activities": calendar_activities,
                    }
                )

        study_calendar = {
            "today": today.isoformat(),
            "initial_month": today.strftime("%Y-%m"),
            "first_month": plan_start.strftime("%Y-%m"),
            "days": calendar_days,
        }
        recorded_days = {day for day, seconds in activity_seconds_by_day.items() if seconds > 0}
        momentum_days = [{"date": day, "active": day in recorded_days, "label": day[5:]} for day in recent_days]
        active_recent_days = sum(1 for item in momentum_days if item["active"])
        momentum_score = min(100, int(round(active_recent_days / 7 * 100)))

        chart_rows: List[Dict[str, Any]] = []
        target_total = 0.0
        actual_total = 0.0
        target_cumulative = 0.0
        actual_cumulative = 0.0
        for day in chart_days:
            day_key = str(day.get("date") or "")
            is_future_day = bool(day_key and day_key > today.isoformat())
            target_hours = float(day.get("target_seconds") or 0) / 3600
            if is_future_day:
                actual_hours: Optional[float] = None
            else:
                actual_hours = activity_seconds_by_day.get(day_key, 0.0) / 3600
            target_total += target_hours
            target_cumulative += target_hours
            if actual_hours is not None:
                actual_total = max(0.0, actual_total + actual_hours)
                actual_cumulative = max(0.0, actual_cumulative + actual_hours)
            chart_rows.append(
                {
                    "label": str(day.get("label") or ""),
                    "date": day_key,
                    "short_date": str(day.get("short_date") or ""),
                    "state": str(day.get("state") or ""),
                    "state_label": str(day.get("state_label") or ""),
                    "target_hours": round(target_cumulative, 2),
                    "actual_hours": round(actual_cumulative, 2) if actual_hours is not None else None,
                    "actual_daily_hours": round(actual_hours, 2) if actual_hours is not None else None,
                    "is_future": is_future_day,
                }
            )
        chart_max_candidates = [1.0]
        chart_max_candidates.extend(float(row["target_hours"]) for row in chart_rows)
        chart_max_candidates.extend(float(row["actual_hours"]) for row in chart_rows if row["actual_hours"] is not None)
        chart_max_hours = max(chart_max_candidates)
        chart_max_hours = max(1.0, chart_max_hours * 1.08)

        def _chart_point(index: int, value: float) -> str:
            total_points = max(1, len(chart_rows) - 1)
            x = 48 + (288 * (index / total_points))
            y = 132 - (104 * min(max(value / chart_max_hours, 0.0), 1.0))
            return f"{round(x, 1)},{round(y, 1)}"

        for index, row in enumerate(chart_rows):
            target_y = 132 - (104 * min(max(float(row["target_hours"]) / chart_max_hours, 0.0), 1.0))
            row["target_point"] = _chart_point(index, float(row["target_hours"]))
            if row["actual_hours"] is not None:
                actual_y = 132 - (104 * min(max(float(row["actual_hours"]) / chart_max_hours, 0.0), 1.0))
                row["actual_point"] = _chart_point(index, float(row["actual_hours"]))
                row["actual_y"] = round(actual_y, 1)
                row["actual_label_y"] = round(max(16.0, actual_y - 9), 1)
            else:
                row["actual_point"] = ""
                row["actual_y"] = None
                row["actual_label_y"] = None
            row["x"] = round(48 + (288 * (index / max(1, len(chart_rows) - 1))), 1)
            row["target_y"] = round(target_y, 1)

        y_tick_values = [0.0, chart_max_hours / 2, chart_max_hours]
        y_ticks = []
        for tick_value in y_tick_values:
            tick_y = 132 - (104 * min(max(tick_value / chart_max_hours, 0.0), 1.0))
            y_ticks.append(
                {
                    "value": round(tick_value, 1),
                    "label": f"{tick_value:.1f}h",
                    "y": round(tick_y, 1),
                    "label_y": round(tick_y + 3, 1),
                }
            )

        week_chart = {
            "rows": chart_rows,
            "target_points": " ".join(str(row["target_point"]) for row in chart_rows),
            "actual_points": " ".join(str(row["actual_point"]) for row in chart_rows if row["actual_point"]),
            "y_ticks": y_ticks,
            "target_total": round(target_total, 1),
            "actual_total": round(actual_total, 1),
            "max_hours": round(chart_max_hours, 1),
        }
        today_chart_row = next((row for row in chart_rows if str(row.get("date") or "") == today.isoformat()), None)
        chart_today_hours = float(today_chart_row.get("actual_daily_hours") or 0.0) if today_chart_row else 0.0
        today_study_hours = chart_today_hours
        today_delta_seconds = today_study_hours * 3600
        today_study_minutes = int(round(today_study_hours * 60))
        total_target_seconds = max(0.0, float(summary.get("total_target") or 0) * 60)
        today_progress_delta = (today_delta_seconds / total_target_seconds * 100) if total_target_seconds else 0.0
        today_target_hours = max(0.0, today_target_minutes / 60)
        today_effective_hours = max(0.0, today_study_hours)
        today_remaining_hours = max(0.0, today_target_hours - today_effective_hours)
        today_target_completion = min(
            100.0,
            (today_effective_hours / today_target_hours * 100) if today_target_hours else 0.0,
        )
        if today_target_hours <= 0.001:
            today_task_state, today_task_label = "upcoming", "今日無排程"
        elif today_remaining_hours <= 0.01:
            today_task_state, today_task_label = "complete", "今日已達標"
        elif today_effective_hours > 0.001:
            today_task_state, today_task_label = "active", "進行中"
        else:
            today_task_state, today_task_label = "upcoming", "未開始"

        today_activity_events = activity_events_by_day.get(today.isoformat(), [])
        today_progress_days = _study_plan_today_progress_days(
            week_rows,
            videos,
            today_activity_events,
        )

        today_videos = []
        for item in activity_events_by_day.get(today.isoformat(), []):
            delta_seconds = float(item.get("delta_seconds") or 0)
            activity_minutes = round(abs(delta_seconds) / 60, 1)
            if activity_minutes <= 0:
                continue
            duration_seconds = max(0.0, float(item.get("duration_seconds") or 0))
            watched_seconds = max(0.0, float(item.get("watched_seconds") or 0))
            today_videos.append(
                {
                    "subject": str(item.get("subject") or ""),
                    "sequence": int(item.get("sequence") or 0),
                    "title": str(item.get("title") or ""),
                    "minutes": activity_minutes,
                    "is_correction": delta_seconds < 0,
                    "completion": round(min(100.0, watched_seconds / duration_seconds * 100) if duration_seconds else 0.0, 1),
                }
            )

        today_study = {
            "day": today.isoformat(),
            "hours": round(today_study_hours, 2),
            "minutes": today_study_minutes,
            "progress_delta": round(today_progress_delta, 2),
            "target_hours": round(today_target_hours, 2),
            "remaining_hours": round(today_remaining_hours, 2),
            "target_completion": round(today_target_completion, 1),
            "task_state": today_task_state,
            "task_label": today_task_label,
            "progress_days": today_progress_days,
            "videos": today_videos,
        }
        return {
            "plan_start": STUDY_PLAN_START,
            "plan_end": effective_plan_end,
            "plan_total_weeks": len(week_rows),
            "summary": summary,
            "total_hours": round(total_hours, 1),
            "remaining_hours": round(remaining_hours, 1),
            "elapsed_percent": round(elapsed_percent, 1),
            "pace_delta": round(pace_delta, 1),
            "pace_state": pace_state,
            "pace_label": pace_label,
            "pace_message": pace_message,
            "pace_insight": pace_insight,
            "progress_race": progress_race,
            "visual_angle": visual_angle,
            "metric_cards": metric_cards,
            "subject_rows": subject_rows,
            "weak_subjects": weak_subjects,
            "current_week": current_week,
            "today_row": today_row,
            "today_study": today_study,
            "next_videos": next_videos,
            "continue_video": continue_video,
            "momentum_days": momentum_days,
            "momentum_score": momentum_score,
            "momentum_angle": round(momentum_score * 3.6, 1),
            "active_recent_days": active_recent_days,
            "last_updated_label": last_updated_label,
            "timeline_nodes": timeline_nodes,
            "week_chart": week_chart,
            "study_calendar": study_calendar,
        }

    def _invalidate_study_progress_context() -> None:
        with study_progress_context_lock:
            study_progress_context_cache["expires_at"] = 0.0
            study_progress_context_cache["context"] = None

    def _load_study_progress_context() -> Dict[str, Any]:
        now = time.monotonic()
        with study_progress_context_lock:
            cached_context = study_progress_context_cache.get("context")
            if cached_context is not None and now < float(study_progress_context_cache.get("expires_at") or 0):
                return copy.deepcopy(cached_context)

        videos = storage.list_study_plan_videos_with_records()
        week_rows, current_week, summary = _study_plan_week_rows(videos)
        context = _build_study_home_context(videos, week_rows, current_week, summary)
        with study_progress_context_lock:
            study_progress_context_cache["context"] = context
            study_progress_context_cache["expires_at"] = time.monotonic() + 20.0
        return copy.deepcopy(context)

    _RECALL_EXCLUDED_CARD_MARKERS = (
        "待確認",
        "已修正",
        "需修正",
        "校正",
        "原筆記",
        "筆記中",
        "模糊",
        "無法辨識",
        "無法確認",
    )

    def _is_recall_concept_eligible(concept: Any) -> bool:
        if not isinstance(concept, dict):
            return False
        required_fields = (("concept", 120), ("explanation", 900))
        for field, max_length in required_fields:
            value = _repair_study_decoded_text(concept.get(field))
            if _study_text_quality_issue(value, max_length=max_length):
                return False
        card_text = "\n".join(
            str(concept.get(field) or "").strip()
            for field in (
                "concept",
                "recall_cue",
                "core_summary",
                "explanation",
                "simple_example",
                "example_problem",
                "example_method",
                "common_confusion",
                "memory_hint",
            )
        )
        return bool(card_text) and not any(marker in card_text for marker in _RECALL_EXCLUDED_CARD_MARKERS)

    def _build_recall_widget_context() -> Dict[str, Any]:
        today = _study_plan_business_date().isoformat()
        due_cards = storage.list_due_study_recall_cards(
            today=today,
            limit=18,
            concept_filter=_is_recall_concept_eligible,
        )
        cards: List[Dict[str, Any]] = []
        session_cache: Dict[int, Dict[str, Any]] = {}
        for due_card in due_cards:
            session_id = int(due_card["session_id"])
            recall_session = session_cache.get(session_id)
            if recall_session is None:
                recall_session = storage.get_study_recall_session(session_id) or {}
                session_cache[session_id] = recall_session
            concept_index = int(due_card["concept_index"])
            concepts = recall_session.get("key_concepts") or []
            if concept_index >= len(concepts) or not _is_recall_concept_eligible(concepts[concept_index]):
                continue
            concepts[concept_index]["topic"] = _normalize_study_concept_title(
                concepts[concept_index].get("topic"), recall_session.get("title") or "細分觀念"
            )
            concepts[concept_index]["concept"] = _normalize_study_concept_title(
                concepts[concept_index].get("concept"), concepts[concept_index].get("topic")
            )
            concepts[concept_index]["explanation"] = _normalize_study_math_markup(concepts[concept_index].get("explanation"))
            concepts[concept_index]["recall_cue"] = _normalize_study_math_markup(
                concepts[concept_index].get("recall_cue")
                or f"先回想「{concepts[concept_index].get('concept') or '這個觀念'}」的條件、核心關係與結論。"
            )
            concepts[concept_index]["core_summary"] = _normalize_study_math_markup(
                concepts[concept_index].get("core_summary")
            )
            concepts[concept_index]["card_type"] = (
                "example" if concepts[concept_index].get("card_type") == "example" else "concept"
            )
            concepts[concept_index]["example_problem"] = _normalize_study_math_markup(
                concepts[concept_index].get("example_problem")
            )
            concepts[concept_index]["example_method"] = _normalize_study_math_markup(
                concepts[concept_index].get("example_method")
            )
            concepts[concept_index]["simple_example"] = _normalize_study_math_markup(
                concepts[concept_index].get("simple_example")
            )
            concepts[concept_index]["reasoning_steps"] = [
                _normalize_study_math_markup(step)
                for step in (concepts[concept_index].get("reasoning_steps") or [])[:4]
                if str(step or "").strip()
            ]
            concepts[concept_index]["common_confusion"] = _normalize_study_math_markup(
                concepts[concept_index].get("common_confusion")
            )
            concepts[concept_index]["memory_hint"] = _normalize_study_math_markup(concepts[concept_index].get("memory_hint"))
            cards.append({**due_card, "concept_data": concepts[concept_index]})
        return {
            "due_count": len(cards),
            "cards": cards,
        }

    def _study_plan_minutes(value: Any) -> float:
        try:
            parsed = float(str(value or "0").strip())
        except (TypeError, ValueError):
            return 0.0
        if not math.isfinite(parsed):
            return 0.0
        return max(0.0, min(parsed, 1_440.0))

    _NOTE_IMAGE_MIME_TYPES = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".webp": "image/webp",
        ".gif": "image/gif",
    }

    def _extract_openai_text(payload: Dict[str, Any]) -> str:
        direct = payload.get("output_text")
        if isinstance(direct, str) and direct.strip():
            return direct.strip()
        for item in payload.get("output") or []:
            if not isinstance(item, dict):
                continue
            for content in item.get("content") or []:
                if isinstance(content, dict) and isinstance(content.get("text"), str):
                    return content["text"].strip()
        return ""

    _STUDY_LATEX_COMMANDS = frozenset(
        """
        acute aleph alpha angle approx arccos arcsin arctan arg array ast bar begin beta
        bmatrix boldsymbol boxed breve bullet cap cdot cdots check chi circ closure cong cos
        cosh cot coth csc cup ddagger ddot ddots degree delta det dfrac dim displaystyle div
        dot dots downarrow ell emptyset end epsilon equiv eta exists exp forall frac gamma gcd
        ge geq grad hat hbar hline hom hookrightarrow iff implies in infinity int iota ker lambda
        langle lbrace lceil ldots le left leftarrow leftrightarrow leq lfloor lim limits ln log
        longleftarrow longleftrightarrow longrightarrow mapsto mathbb mathbf mathcal mathit mathrm
        mathsf mathtt matrix max min mod mp nabla natural ne neg neq nexists ni norm not notin nu
        odot oint omega ominus operatorname oplus otimes overbrace overline partial phi pi pm pmatrix
        prod psi rangle rbrace rceil Re ref right rightarrow rfloor rho rm root scriptstyle sec setminus
        sigma sin sinh smallmatrix sqrt stackrel subset subseteq sum sup superset superseteq tan tanh tau
        text textbf textit textnormal theta tilde times to top triangle underbrace underline uparrow upsilon
        varepsilon varphi varpi varrho varsigma vartheta vdash vec vee vert vphantom wedge widehat widetilde
        xi zeta
        """.split()
    )

    def _repair_openai_latex_json_escapes(raw: str) -> str:
        """Protect LaTeX commands before JSON turns their prefixes into controls."""
        text = str(raw or "")
        repaired: List[str] = []
        in_string = False
        index = 0
        while index < len(text):
            char = text[index]
            if char == '"':
                in_string = not in_string
                repaired.append(char)
                index += 1
                continue
            if not in_string or char != "\\" or index + 1 >= len(text):
                repaired.append(char)
                index += 1
                continue
            following = text[index + 1]
            if following == "\\":
                repaired.append(text[index:index + 2])
                index += 2
                continue
            if following in {'"', "/"}:
                repaired.append(text[index:index + 2])
                index += 2
                continue
            if following == "u" and re.fullmatch(r"[0-9A-Fa-f]{4}", text[index + 2:index + 6]):
                repaired.append(text[index:index + 6])
                index += 6
                continue
            command_match = re.match(r"[A-Za-z]+", text[index + 1:])
            command = command_match.group(0) if command_match else ""
            is_latex_command = command in _STUDY_LATEX_COMMANDS
            is_latex_symbol = following in "()[]{}|,;!:%#&_"
            if is_latex_command or is_latex_symbol:
                repaired.append("\\\\")
                index += 1
                continue
            repaired.append(char)
            index += 1
        return "".join(repaired)

    def _repair_study_decoded_text(value: Any) -> str:
        text = str(value or "")
        corruption_markers = ("Ã", "Â", "â€", "ðŸ", "ï»¿", "锟斤拷")
        if any(marker in text for marker in corruption_markers) or any(0x80 <= ord(char) <= 0x9F for char in text):
            candidates = [text]
            for encoding in ("latin1", "cp1252"):
                try:
                    candidates.append(text.encode(encoding).decode("utf-8"))
                except (UnicodeEncodeError, UnicodeDecodeError):
                    continue

            def corruption_score(candidate: str) -> int:
                return (
                    sum(candidate.count(marker) for marker in corruption_markers) * 8
                    + candidate.count("\ufffd") * 12
                    + sum(1 for char in candidate if 0x80 <= ord(char) <= 0x9F) * 4
                )

            text = min(candidates, key=corruption_score)
        text = text.replace("\x08", "\\b").replace("\x0c", "\\f")
        text = re.sub(r"\t(?=[A-Za-z])", r"\\t", text)
        text = re.sub(r"\r(?=[A-Za-z])", r"\\r", text)
        text = re.sub(
            r"\n(?=(?:abla|e(?:q|xists)?|ot|u\b|atural|i\b|oindent|orm|ewline)(?:\b|[_^{}]))",
            r"\\n",
            text,
        )
        return text

    def _study_latex_markup_issue(value: Any) -> Optional[str]:
        text = str(value or "")
        delimiter_patterns = (
            (r"(?<!\\)\\\(", r"(?<!\\)\\\)"),
            (r"(?<!\\)\\\[", r"(?<!\\)\\\]"),
        )
        for opening, closing in delimiter_patterns:
            if len(re.findall(opening, text)) != len(re.findall(closing, text)):
                return "unbalanced_math_delimiter"
        math_spans = re.findall(
            r"(?<!\\)\\\[(.*?)(?<!\\)\\\]|(?<!\\)\\\((.*?)(?<!\\)\\\)",
            text,
            flags=re.DOTALL,
        )
        for display_body, inline_body in math_spans:
            body = display_body or inline_body
            if "\\(" in body or "\\)" in body or "\\[" in body or "\\]" in body:
                return "nested_math_delimiter"
            prose_free_body = re.sub(
                r"\\text\{(?:[^{}]|\{[^{}]*\})*\}",
                "",
                body,
            )
            if re.search(r"[\u3400-\u9fff]", prose_free_body):
                return "chinese_inside_math"
            brace_depth = 0
            for match in re.finditer(r"(?<!\\)[{}]", body):
                brace_depth += 1 if match.group(0) == "{" else -1
                if brace_depth < 0:
                    return "unbalanced_math_brace"
            if brace_depth:
                return "unbalanced_math_brace"
            environments = re.findall(r"\\begin\{([^{}]+)\}", body)
            closed_environments = re.findall(r"\\end\{([^{}]+)\}", body)
            if environments != closed_environments:
                return "unbalanced_math_environment"
            if re.search(r"\\(?:b|f|n|r|t)(?![A-Za-z])", body) or body.rstrip().endswith("\\"):
                return "broken_latex_command"
        return None

    def _openai_error_details(response: Any) -> Tuple[str, str, str]:
        if response is None:
            return "", "", ""
        try:
            payload = response.json()
        except (TypeError, ValueError, requests.RequestException):
            return "", "", ""
        error = payload.get("error") if isinstance(payload, dict) else None
        if not isinstance(error, dict):
            return "", "", ""
        return (
            str(error.get("code") or "").strip().lower(),
            str(error.get("type") or "").strip().lower(),
            str(error.get("message") or "").strip(),
        )

    def _is_openai_quota_error(code: str, error_type: str, message: str) -> bool:
        quota_codes = {
            "billing_hard_limit_reached",
            "billing_not_active",
            "insufficient_quota",
            "usage_limit_reached",
        }
        normalized_code = str(code or "").strip().lower()
        normalized_type = str(error_type or "").strip().lower()
        normalized_message = str(message or "").strip().lower()
        return (
            normalized_code in quota_codes
            or normalized_type in quota_codes
            or "current quota" in normalized_message
            or "billing" in normalized_message
            or "run out of credits" in normalized_message
        )

    def _call_openai_json(
        *,
        name: str,
        schema: Dict[str, Any],
        content: List[Dict[str, Any]],
        timeout: int = 120,
        reasoning_effort: Optional[str] = None,
        max_output_tokens: int = 12000,
        repair_simple_location_json: bool = False,
        json_retry_attempts: int = 1,
    ) -> Dict[str, Any]:
        _raise_if_study_upload_cancelled()

        def repair_strings(value: Any) -> Any:
            if isinstance(value, str):
                return _repair_study_decoded_text(value)
            if isinstance(value, list):
                return [repair_strings(item) for item in value]
            if isinstance(value, dict):
                return {key: repair_strings(item) for key, item in value.items()}
            return value

        request_body = {
            "model": openai_model,
            "store": False,
            "input": [{"role": "user", "content": content}],
            "text": {"format": {"type": "json_schema", "name": name, "strict": True, "schema": schema}},
            "max_output_tokens": max_output_tokens,
        }
        effective_reasoning_effort = normalize_openai_reasoning_effort(
            openai_model,
            reasoning_effort,
        )
        if effective_reasoning_effort:
            request_body["reasoning"] = {"effort": effective_reasoning_effort}
        response = None
        for attempt in range(6):
            _raise_if_study_upload_cancelled()
            try:
                response = requests.post(
                    "https://api.openai.com/v1/responses",
                    headers={"Authorization": f"Bearer {openai_api_key}", "Content-Type": "application/json"},
                    json=request_body,
                    timeout=timeout,
                )
                response.raise_for_status()
                _raise_if_study_upload_cancelled()
                break
            except requests.RequestException as exc:
                error_response = getattr(exc, "response", None)
                status_code = getattr(error_response, "status_code", None)
                error_code, error_type, error_message = _openai_error_details(error_response)
                quota_exhausted = status_code == 429 and _is_openai_quota_error(
                    error_code,
                    error_type,
                    error_message,
                )
                setattr(exc, "openai_error_code", error_code)
                setattr(exc, "openai_error_type", error_type)
                setattr(exc, "openai_error_message", error_message)
                retryable = (
                    status_code is None
                    or (status_code == 429 and not quota_exhausted)
                    or (status_code is not None and status_code >= 500)
                )
                retry_limit = 6 if status_code == 429 else 4
                if not retryable or attempt + 1 >= retry_limit:
                    raise
                retry_after = 0.0
                if error_response is not None:
                    try:
                        retry_after = float(error_response.headers.get("Retry-After") or 0)
                    except (TypeError, ValueError):
                        retry_after = 0.0
                retry_delay = min(
                    90.0,
                    max(retry_after, 5.0 * (2 ** attempt) if status_code == 429 else 2.0 * (attempt + 1)),
                )
                job_id = getattr(study_upload_context, "job_id", None)
                if job_id and status_code == 429:
                    _set_study_upload_job(
                        job_id,
                        message=(
                            f"AI 服務目前忙碌，{math.ceil(retry_delay)} 秒後自動重試"
                            f"（第 {attempt + 1}／{retry_limit - 1} 次）。"
                        ),
                    )
                app.logger.warning(
                    "Retrying OpenAI request %s after status=%s in %.1fs (attempt %s/%s)",
                    name,
                    status_code,
                    retry_delay,
                    attempt + 1,
                    retry_limit - 1,
                )
                _study_upload_retry_wait(retry_delay)
        if response is None:
            raise requests.RequestException("OpenAI request did not return a response")
        response_payload = response.json()
        output_text = _extract_openai_text(response_payload)
        if not output_text:
            incomplete = response_payload.get("incomplete_details") or {}
            raise ValueError(
                f"OpenAI returned no output text (status={response_payload.get('status')}, "
                f"reason={incomplete.get('reason')})"
            )
        protected_output_text = _repair_openai_latex_json_escapes(output_text)
        try:
            parsed = json.loads(protected_output_text)
        except json.JSONDecodeError as parse_error:
            incomplete = response_payload.get("incomplete_details") or {}
            app.logger.warning(
                "Invalid JSON for %s (status=%s, reason=%s, chars=%s, error=%s)",
                name,
                response_payload.get("status"),
                incomplete.get("reason"),
                len(output_text),
                parse_error,
            )
            if not repair_simple_location_json and json_retry_attempts > 0:
                retry_content = list(content) + [
                    {
                        "type": "input_text",
                        "text": (
                            "上一個回應的 JSON 字串未完整結束或格式損壞，因此未被接收。"
                            "請重新從頭輸出一份完整、合法且符合 schema 的 JSON；不要省略結尾，"
                            "不要輸出 Markdown 或任何 JSON 以外的文字。"
                        ),
                    }
                ]
                return _call_openai_json(
                    name=name,
                    schema=schema,
                    content=retry_content,
                    timeout=max(timeout, 360),
                    reasoning_effort=reasoning_effort,
                    max_output_tokens=min(24000, max_output_tokens + 4000),
                    repair_simple_location_json=repair_simple_location_json,
                    json_retry_attempts=json_retry_attempts - 1,
                )
            if not repair_simple_location_json:
                raise
            location_fields = (
                "location_id|found|left|top|right|bottom|start_x|start_y|end_x|end_y|confidence"
            )
            repaired_output = re.sub(
                rf"(true|false|-?\d+|\}}|\])\s*(?=\"(?:{location_fields})\"\s*:)",
                r"\1,",
                protected_output_text,
            )
            repaired_output = re.sub(r"}\s*{", "},{", repaired_output)
            repaired_output = re.sub(r",\s*([}\]])", r"\1", repaired_output)
            try:
                parsed = json.loads(repaired_output)
            except json.JSONDecodeError:
                if json_retry_attempts > 0:
                    return _call_openai_json(
                        name=name,
                        schema=schema,
                        content=content,
                        timeout=max(timeout, 360),
                        reasoning_effort=reasoning_effort,
                        max_output_tokens=min(24000, max_output_tokens + 4000),
                        repair_simple_location_json=repair_simple_location_json,
                        json_retry_attempts=json_retry_attempts - 1,
                    )
                app.logger.warning(
                    "Invalid simple location JSON for %s (status=%s, reason=%s, chars=%s)",
                    name,
                    response_payload.get("status"),
                    incomplete.get("reason"),
                    len(output_text),
                )
                raise
        if not isinstance(parsed, dict):
            raise ValueError("OpenAI did not return a JSON object")
        return repair_strings(parsed)

    def _study_text_quality_issue(value: Any, *, max_length: int) -> Optional[str]:
        text = str(value or "")
        if not text.strip():
            return "empty"
        if len(text) > max_length:
            return "too_long"
        if any(
            (ord(char) < 32 and char != "\n")
            or 0x7F <= ord(char) <= 0x9F
            or 0xE000 <= ord(char) <= 0xF8FF
            for char in text
        ):
            return "control_character"
        if "\ufffd" in text or any(marker in text for marker in ("Ã", "Â", "â€", "ðŸ", "锟斤拷", "ï»¿")):
            return "encoding_corruption"
        if re.search(r"\\(?:\(|\[)\s*(?:\.{3}|\\cdots|null|undefined|[?？])\s*\\(?:\)|\])", text, re.IGNORECASE):
            return "formula_placeholder"
        if re.search(r"\\\(\s*\\\)|\\\[\s*\\\]", text):
            return "empty_formula"
        latex_issue = _study_latex_markup_issue(text)
        if latex_issue:
            return latex_issue

        compact_lines = [line.strip() for line in text.splitlines() if line.strip()]
        if compact_lines:
            line_counts: Dict[str, int] = {}
            for line in compact_lines:
                line_counts[line] = line_counts.get(line, 0) + 1
            most_repeated = max(line_counts.values())
            if most_repeated >= 10 and most_repeated / len(compact_lines) >= 0.45:
                return "repeated_lines"

        inline_atoms = [match.strip() for match in re.findall(r"\\\((.{1,48}?)\\\)", text, flags=re.DOTALL)]
        if inline_atoms:
            atom_counts: Dict[str, int] = {}
            for atom in inline_atoms:
                atom_counts[atom] = atom_counts.get(atom, 0) + 1
            if max(atom_counts.values()) >= 12 and max(atom_counts.values()) / len(inline_atoms) >= 0.45:
                return "repeated_formula"
        return None

    def _study_relation_association_signature(
        association: Any,
        *,
        source_title: Any = "",
        target_title: Any = "",
    ) -> str:
        """Return a stable content key so templated relation copy is not repeated."""
        normalized = _normalize_study_math_markup(association).casefold()
        for title in (source_title, target_title):
            title_text = _normalize_study_math_markup(title).casefold().strip()
            if title_text:
                normalized = normalized.replace(title_text, "{card}")
        return re.sub(r"[\s，。；：、！？,.!?()（）\[\]{}]", "", normalized)

    def _study_relation_association_issue(
        association: Any,
        *,
        source_title: Any = "",
        target_title: Any = "",
    ) -> Optional[str]:
        issue = _study_text_quality_issue(association, max_length=240)
        if issue:
            return issue
        signature = _study_relation_association_signature(
            association,
            source_title=source_title,
            target_title=target_title,
        )
        generic_signatures = (
            "這兩張卡屬於同一份筆記中的直接相關觀念可一起對照複習",
            "這兩張卡適合一起複習",
            "兩張卡適合一起複習",
            "兩者相關可一起複習",
        )
        if not signature or any(generic in signature for generic in generic_signatures):
            return "generic_relation"
        return None

    def _study_has_invalid_negation_counterexample(value: Any) -> bool:
        canonical = str(value or "")

        def matrix_to_vector(match: re.Match[str]) -> str:
            body = match.group(1)
            body = re.sub(r"\\\\(?:\[[^\]]*\])?", ";", body)
            body = body.replace("&", ",")
            return "[" + body + "]"

        canonical = re.sub(
            r"\\begin\{(?:p|b|v|V|B)?matrix\}(.*?)\\end\{(?:p|b|v|V|B)?matrix\}",
            matrix_to_vector,
            canonical,
            flags=re.DOTALL,
        )
        canonical = canonical.replace("\\left", "").replace("\\right", "")
        canonical = canonical.replace("\\(", "").replace("\\)", "")
        pattern = re.compile(
            r"[A-Za-z][A-Za-z0-9_]*\(\s*\[([+\-\d\s,;]+)\]\s*\)\s*"
            r"(?:≠|!=|\\ne)\s*-\s*"
            r"[A-Za-z][A-Za-z0-9_]*\(\s*\[([+\-\d\s,;]+)\]\s*\)"
        )
        for match in pattern.finditer(canonical):
            try:
                left = [int(part) for part in re.split(r"[,;\s]+", match.group(1).strip()) if part]
                right = [int(part) for part in re.split(r"[,;\s]+", match.group(2).strip()) if part]
            except ValueError:
                continue
            if len(left) != len(right) or any(
                left_value != -right_value
                for left_value, right_value in zip(left, right)
            ):
                return True
        return False

    def _normalize_study_math_markup(value: Any) -> str:
        """Make model formula output renderable while leaving ordinary prose alone."""
        text = _repair_study_decoded_text(value).replace("\r\n", "\n").replace("\r", "\n").replace("\t", " ")
        text = re.sub(r"\\n(?=\s|\\[\[(])", "\n", text)
        # Older model output was sometimes JSON-escaped twice, leaving literal
        # ``\\(`` and ``\\beta`` in the browser. Collapse one extra escape only
        # when it precedes a KaTeX delimiter or command.
        structured_environments: List[str] = []
        text = re.sub(
            r"\\\\(?=(?:begin|end)\{(?:matrix|bmatrix|pmatrix|smallmatrix|vmatrix|Vmatrix|array|aligned|cases)\})",
            r"\\",
            text,
        )

        def protect_structured_environment(match: re.Match[str]) -> str:
            structured_environments.append(match.group(0))
            return f"E3LATEXENVPLACEHOLDER{len(structured_environments) - 1}END"

        structured_pattern = re.compile(
            r"\\begin\{(matrix|bmatrix|pmatrix|smallmatrix|vmatrix|Vmatrix|array|aligned|cases)\}"
            r".*?\\end\{\1\}",
            flags=re.DOTALL,
        )
        text = structured_pattern.sub(protect_structured_environment, text)
        for _ in range(3):
            repaired = re.sub(r"\\\\(?=[()A-Za-z])", r"\\", text)
            repaired = re.sub(
                r"\\\\(?=\[(?!\s*-?\d+(?:\.\d+)?(?:pt|em|ex|px|mm|cm|in)\s*\]))",
                r"\\",
                repaired,
            )
            repaired = re.sub(r"\\\\(?=\])", r"\\", repaired)
            if repaired == text:
                break
            text = repaired
        for environment_index, environment_text in enumerate(structured_environments):
            text = text.replace(
                f"E3LATEXENVPLACEHOLDER{environment_index}END",
                environment_text,
            )
        text = re.sub(r"\\begin\{(equation\*?|align\*?|gather\*?)\}", r"\\[", text)
        text = re.sub(r"\\end\{(equation\*?|align\*?|gather\*?)\}", r"\\]", text)
        text = re.sub(r"\$\$(.+?)\$\$", lambda match: "\\[" + match.group(1).strip() + "\\]", text, flags=re.DOTALL)
        text = re.sub(r"(?<!\$)\$(?!\$)([^$\n]+?)(?<!\$)\$(?!\$)", lambda match: "\\(" + match.group(1).strip() + "\\)", text)

        # Structured output can still contain duplicated delimiters or a bare formula
        # embedded in Chinese prose. Repair markup only; never rewrite note content.
        for _ in range(3):
            text = re.sub(r"(?<!\\)\\\[\s*(?<!\\)\\\[", r"\\[", text)
            text = re.sub(r"(?<!\\)\\\]\s*(?<!\\)\\\]", r"\\]", text)
            text = re.sub(r"(?<!\\)\\\(\s*(?<!\\)\\\(", r"\\(", text)
            text = re.sub(r"(?<!\\)\\\)\s*(?<!\\)\\\)", r"\\)", text)
        text = repair_math_delimiters(text)

        math_signal = re.compile(r"(?:=|≠|!=|⇔|→|↔|≤|≥|∈|∉|\\(?:ne|to|le|ge|in|notin|frac|sum|prod|int|begin\{(?:matrix|bmatrix|pmatrix|smallmatrix|vmatrix|Vmatrix|array|aligned|cases)\})|[A-Za-z]\s*\([^\n)]*\)|[A-Za-z]\s*[A-Za-z0-9]*[_^])")
        normalized_lines: List[str] = []
        in_display_math = False
        for line in text.split("\n"):
            stripped = line.strip()
            display_open_count = len(re.findall(r"(?<!\\)\\\[", stripped))
            display_close_count = len(re.findall(r"(?<!\\)\\\]", stripped))
            if (
                not stripped
                or "\\(" in stripped
                or "\\[" in stripped
                or in_display_math
            ):
                normalized_lines.append(line)
                in_display_math = max(
                    0,
                    int(in_display_math) + display_open_count - display_close_count,
                ) > 0
                continue
            if math_signal.search(stripped):
                cjk_count = len(re.findall(r"[\u3400-\u9fff]", stripped))
                if cjk_count == 0 and is_pure_math_expression(stripped):
                    normalized_lines.append(
                        wrap_bare_math_candidate(stripped, display_if_pure=True)
                    )
                    continue
                prefix_match = re.match(r"^(.*?[：:]\s*)(.+)$", stripped)
                if (
                    prefix_match
                    and len(re.findall(r"[\u3400-\u9fff]", prefix_match.group(2))) <= 2
                    and is_pure_math_expression(prefix_match.group(2).strip())
                ):
                    normalized_lines.append(
                        prefix_match.group(1)
                        + wrap_bare_math_candidate(prefix_match.group(2))
                    )
                    continue
            normalized_lines.append(line)
        normalized = "\n".join(normalized_lines).strip()

        protected_math = re.compile(
            r"((?<!\\)\\\[.*?(?<!\\)\\\]|(?<!\\)\\\(.*?(?<!\\)\\\))",
            re.DOTALL,
        )
        bare_candidate = re.compile(r"[A-Za-z0-9\\{}\[\]()`'_^=+\-*/<>|,:;. \t×≠⇔→↔≤≥∈∉]+")
        binary_math = re.compile(
            r"(?:\b[A-Za-z]\b|\d+|[)\]}])\s*[+\-*/]\s*(?:\b[A-Za-z]\b|\d+|[(\[{\\])"
        )
        standalone_symbol = re.compile(r"(?:[A-Za-z]|[A-Z]{2,4})(?:_\{?[^\s}]+\}?|\^\{?[^\s}]+\}?)?")

        def wrap_candidate(match: re.Match[str]) -> str:
            raw = match.group(0)
            body = raw.strip()
            if not body:
                return raw
            looks_like_math = bool(
                math_signal.search(body)
                or binary_math.search(body)
                or standalone_symbol.fullmatch(body)
            )
            if not looks_like_math:
                return raw
            return wrap_bare_math_candidate(raw)

        chunks = protected_math.split(normalized)
        def normalize_plain_chunk(chunk: str) -> str:
            environments: List[str] = []

            def protect_environment(match: re.Match[str]) -> str:
                environments.append(match.group(0))
                return f"E3LATEXBAREENVPH{len(environments) - 1}END"

            repaired = structured_pattern.sub(protect_environment, chunk)
            repaired = bare_candidate.sub(wrap_candidate, repaired)
            for environment_index, environment_text in enumerate(environments):
                repaired = repaired.replace(
                    f"E3LATEXBAREENVPH{environment_index}END",
                    f"\\({environment_text}\\)",
                )
            return repaired

        normalized = "".join(
            chunk if index % 2 else normalize_plain_chunk(chunk)
            for index, chunk in enumerate(chunks)
        )

        def keep_cjk_out_of_math(match: re.Match[str]) -> str:
            opener = match.group(1)
            body = match.group(2)
            closer = "\\)" if opener == "\\(" else "\\]"
            protected_text: List[str] = []

            def protect_text(command_match: re.Match[str]) -> str:
                protected_text.append(command_match.group(0))
                return f"E3LATEXTEXTPH{len(protected_text) - 1}END"

            repaired_body = re.sub(
                r"\\text\{(?:[^{}]|\{[^{}]*\})*\}",
                protect_text,
                body,
            )
            repaired_body = re.sub(
                r"[\u3400-\u9fff]+",
                lambda cjk: f"\\text{{{cjk.group(0)}}}",
                repaired_body,
            )
            for text_index, protected in enumerate(protected_text):
                repaired_body = repaired_body.replace(
                    f"E3LATEXTEXTPH{text_index}END",
                    protected,
                )
            return f"{opener}{repaired_body}{closer}"

        normalized = re.sub(
            r"(\\\(|\\\[)(.*?)(?:\\\)|\\\])",
            keep_cjk_out_of_math,
            normalized,
            flags=re.DOTALL,
        )
        return re.sub(r"\n{3,}", "\n\n", normalized).strip()

    def _strip_study_process_narration(value: Any) -> str:
        """Remove model workflow narration without rewriting study content."""
        text = _normalize_study_math_markup(value)
        leading_patterns = (
            r"^(?:保留(?:原始)?來源內容並(?:已)?修正為可直接使用的筆記)",
            r"^(?:(?:本|此|原始)?筆記(?:給出|註明|記載|紀載|整理|說明|指出))",
            r"^(?:(?:根據|依據|依照|參考)(?:原始)?(?:筆記|來源)(?:內容)?)",
            r"^(?:以下(?:是|為)(?:整理後|修正後)?(?:的)?(?:筆記|重點|內容))",
        )
        changed = True
        while changed:
            changed = False
            for pattern in leading_patterns:
                repaired = re.sub(pattern + r"[：:，,。\s]*", "", text, count=1)
                if repaired != text:
                    text = repaired.strip()
                    changed = True
        text = re.sub(
            r"[，,；;]\s*其(?:公式|矩陣|定義|內容)?(?:皆|均|分別)?如(?:原始)?"
            r"(?:來源|原文|筆記)(?:中)?(?:所)?(?:列|示|載|述)[。.]?",
            "。",
            text,
        )
        text = re.sub(
            r"(?:如|詳見)(?:原始)?(?:來源|原文|筆記)(?:中)?(?:所)?(?:列|示|載|述)",
            "",
            text,
        )
        text = re.sub(r"。{2,}", "。", text)
        return re.sub(r"\s+([，。；：])", r"\1", text).strip(" \t\n：:，,")

    def _normalize_study_concept_title(value: Any, fallback: Any = "") -> str:
        """Keep titles compact while preserving formulas required by the card."""

        def clean(candidate: Any) -> str:
            text = _repair_study_decoded_text(candidate).replace("\n", " ").strip()
            text = re.sub(
                r"\s*[（(][^（）()]{0,12}(?:例題|範例|例證|概念|定義|性質|方法)\s*[）)]?\s*$",
                "",
                text,
                flags=re.IGNORECASE,
            )
            text = re.sub(r"\s+", " ", text)
            text = text.strip(" \t:：,，;；/|·。-－—_")
            return _normalize_study_math_markup(text) if text else ""

        return clean(value) or clean(fallback) or "重點觀念"

    def _study_coordinate_guide_data_url(source: Image.Image) -> str:
        coordinate_guide = source.convert("RGBA")
        max_side = max(coordinate_guide.size)
        if max_side > 1800 or max_side < 500:
            scale = (1800 if max_side > 1800 else 500) / max_side
            coordinate_guide = coordinate_guide.resize(
                (
                    max(1, round(coordinate_guide.width * scale)),
                    max(1, round(coordinate_guide.height * scale)),
                ),
                Image.Resampling.LANCZOS,
            )
        pixel_width, pixel_height = coordinate_guide.size
        overlay = Image.new("RGBA", coordinate_guide.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        line_width = max(2, round(min(pixel_width, pixel_height) / 550))
        font_size = max(18, round(min(pixel_width, pixel_height) / 42))
        try:
            guide_font = ImageFont.truetype("DejaVuSans.ttf", font_size)
        except OSError:
            guide_font = ImageFont.load_default()
        for coordinate in range(0, 1001, 100):
            x = min(pixel_width - 1, round(pixel_width * coordinate / 1000))
            y = min(pixel_height - 1, round(pixel_height * coordinate / 1000))
            draw.line((x, 0, x, pixel_height), fill=(220, 30, 55, 115), width=line_width)
            draw.line((0, y, pixel_width, y), fill=(220, 30, 55, 115), width=line_width)
            if coordinate < 1000:
                if x + 2 < pixel_width:
                    draw.rectangle(
                        (x + 2, 0, min(pixel_width, x + font_size * 2.8), min(pixel_height, font_size * 1.25)),
                        fill=(255, 255, 255, 220),
                    )
                    draw.text((x + 4, 1), f"X{coordinate}", fill=(180, 0, 25, 255), font=guide_font)
                if y + 2 < pixel_height:
                    draw.rectangle(
                        (0, y + 2, min(pixel_width, font_size * 2.8), min(pixel_height, y + font_size * 1.3)),
                        fill=(255, 255, 255, 220),
                    )
                    draw.text((2, y + 3), f"Y{coordinate}", fill=(180, 0, 25, 255), font=guide_font)
        coordinate_guide = Image.alpha_composite(coordinate_guide, overlay).convert("RGB")
        guide_buffer = io.BytesIO()
        coordinate_guide.save(guide_buffer, format="JPEG", quality=88, optimize=True)
        return f"data:image/jpeg;base64,{base64.b64encode(guide_buffer.getvalue()).decode('ascii')}"

    def _study_image_data_url(source: Image.Image, *, max_side: int = 1800) -> str:
        encoded_image = source.convert("RGB")
        current_max_side = max(encoded_image.size)
        if current_max_side > max_side:
            scale = max_side / current_max_side
            encoded_image = encoded_image.resize(
                (
                    max(1, round(encoded_image.width * scale)),
                    max(1, round(encoded_image.height * scale)),
                ),
                Image.Resampling.LANCZOS,
            )
        image_buffer = io.BytesIO()
        encoded_image.save(image_buffer, format="JPEG", quality=90, optimize=True)
        return f"data:image/jpeg;base64,{base64.b64encode(image_buffer.getvalue()).decode('ascii')}"

    def _canonical_study_source_match_text(value: Any) -> str:
        return canonicalize_source_text(value)

    def _literal_study_source_evidence(value: Any) -> str:
        return literal_source_evidence(value)

    def _match_study_source_evidence_to_lines(
        evidence: str,
        lines: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], Dict[str, float]]:
        return match_source_evidence_to_lines(evidence, lines)

    def _build_study_source_ink_mask(source: Image.Image) -> Image.Image:
        working = source.convert("RGB")
        max_side = max(working.size)
        if max_side > 1800:
            scale = 1800 / max_side
            working = working.resize(
                (max(1, round(working.width * scale)), max(1, round(working.height * scale))),
                Image.Resampling.LANCZOS,
            )
        gray = ImageOps.grayscale(working)
        background = gray.filter(ImageFilter.GaussianBlur(radius=max(4.0, min(working.size) / 52)))
        local_contrast = ImageChops.difference(background, gray)
        mask = local_contrast.point(lambda value: 255 if value >= 14 else 0)
        return mask.filter(ImageFilter.MaxFilter(3))

    def _study_source_visual_line_bands(source: Image.Image) -> List[Tuple[int, int]]:
        """Segment physical ink rows; coordinates are normalized to the crop."""
        mask = _build_study_source_ink_mask(source)
        width, height = mask.size
        if width < 40 or height < 40:
            return []
        projection = list(mask.resize((1, height), Image.Resampling.BOX).getdata())
        smoothed = [
            sum(projection[max(0, index - 2) : min(height, index + 3)])
            / max(1, min(height, index + 3) - max(0, index - 2))
            for index in range(height)
        ]
        positive = sorted(value for value in smoothed if value > 0)
        background = positive[min(len(positive) - 1, round(len(positive) * 0.22))] if positive else 0
        threshold = max(3.0, min(12.0, background * 1.6))
        active_rows = [index for index, value in enumerate(smoothed) if value >= threshold]
        if not active_rows:
            return []
        maximum_gap = max(2, round(height * 0.004))
        raw_bands: List[Tuple[int, int]] = []
        band_start = active_rows[0]
        previous = active_rows[0]
        for current in active_rows[1:]:
            if current - previous > maximum_gap:
                raw_bands.append((band_start, previous + 1))
                band_start = current
            previous = current
        raw_bands.append((band_start, previous + 1))

        bands: List[Tuple[int, int]] = []
        for band_top, band_bottom in raw_bands:
            band_height = band_bottom - band_top
            if band_height < max(2, round(height * 0.004)):
                continue
            strip = mask.crop((0, band_top, width, band_bottom))
            column_projection = list(strip.resize((width, 1), Image.Resampling.BOX).getdata())
            active_columns = [index for index, value in enumerate(column_projection) if value >= 3]
            if not active_columns:
                continue
            ink_width_ratio = (active_columns[-1] - active_columns[0] + 1) / width
            normalized_height = band_height * 1000 / height
            if normalized_height <= 13 and ink_width_ratio >= 0.42:
                continue
            if normalized_height >= 115 and ink_width_ratio >= 0.62:
                continue
            bands.append(
                (
                    max(0, round((band_top - max(1, height * 0.002)) * 1000 / height)),
                    min(1000, round((band_bottom + max(1, height * 0.002)) * 1000 / height)),
                )
            )
        return bands

    def _study_source_band_sheet_data_urls(
        source: Image.Image,
        bands: List[Tuple[int, int]],
    ) -> List[str]:
        """Render physical ink rows with fixed IDs so OCR cannot swap their y positions."""
        if not bands:
            return []
        working = source.convert("RGB")
        content_width = min(1500, working.width)
        scale = content_width / max(1, working.width)
        label_width = 92
        try:
            label_font = ImageFont.truetype("DejaVuSans-Bold.ttf", 25)
        except OSError:
            label_font = ImageFont.load_default()
        sheet_urls: List[str] = []
        for group_start in range(0, len(bands), 8):
            rendered_rows: List[Image.Image] = []
            for band_index in range(group_start, min(len(bands), group_start + 8)):
                band_top, band_bottom = bands[band_index]
                pixel_top = max(0, math.floor(working.height * band_top / 1000))
                pixel_bottom = min(
                    working.height,
                    max(pixel_top + 1, math.ceil(working.height * band_bottom / 1000)),
                )
                vertical_padding = max(3, round((pixel_bottom - pixel_top) * 0.22))
                strip = working.crop(
                    (
                        0,
                        max(0, pixel_top - vertical_padding),
                        working.width,
                        min(working.height, pixel_bottom + vertical_padding),
                    )
                )
                if strip.width != content_width:
                    strip = strip.resize(
                        (content_width, max(1, round(strip.height * scale))),
                        Image.Resampling.LANCZOS,
                    )
                row_height = max(54, strip.height + 12)
                row = Image.new("RGB", (label_width + content_width, row_height), "white")
                row.paste(strip, (label_width, max(0, round((row_height - strip.height) / 2))))
                row_draw = ImageDraw.Draw(row)
                row_draw.rectangle(
                    (0, 0, row.width - 1, row.height - 1),
                    outline=(62, 92, 124),
                    width=2,
                )
                row_draw.text(
                    (12, max(4, round((row_height - 30) / 2))),
                    f"B{band_index + 1:02d}",
                    fill=(12, 72, 130),
                    font=label_font,
                )
                rendered_rows.append(row)
            sheet = Image.new(
                "RGB",
                (label_width + content_width, sum(row.height for row in rendered_rows)),
                "white",
            )
            offset_y = 0
            for row in rendered_rows:
                sheet.paste(row, (0, offset_y))
                offset_y += row.height
            sheet_urls.append(_study_image_data_url(sheet, max_side=2400))
        return sheet_urls

    def _align_study_source_lines_to_visual_bands(
        lines: List[Dict[str, Any]],
        bands: List[Tuple[int, int]],
    ) -> Dict[int, Tuple[int, int]]:
        """Monotonically align OCR line order to image-derived ink bands."""
        line_count = len(lines)
        band_count = len(bands)
        if not line_count or band_count < line_count or band_count > line_count * 3 + 8:
            return {}
        line_centers = [(line["top"] + line["bottom"]) / 2 for line in lines]
        band_centers = [(top + bottom) / 2 for top, bottom in bands]
        line_span = max(1.0, line_centers[-1] - line_centers[0])
        band_span = max(1.0, band_centers[-1] - band_centers[0])

        def assignment_cost(line_index: int, band_index: int) -> float:
            observed_cost = abs(line_centers[line_index] - band_centers[band_index])
            line_position = (
                (line_centers[line_index] - line_centers[0]) / line_span
                if line_count > 1
                else 0.5
            )
            band_position = (
                (band_centers[band_index] - band_centers[0]) / band_span
                if band_count > 1
                else 0.5
            )
            order_cost = abs(line_position - band_position) * 1000
            return observed_cost * 0.58 + order_cost * 0.42

        previous_costs = [float("inf")] * band_count
        backtrack: List[List[int]] = [[-1] * band_count for _ in range(line_count)]
        for band_index in range(0, band_count - line_count + 1):
            previous_costs[band_index] = assignment_cost(0, band_index)
        for line_index in range(1, line_count):
            current_costs = [float("inf")] * band_count
            best_previous_cost = float("inf")
            best_previous_index = -1
            minimum_band = line_index
            maximum_band = band_count - (line_count - line_index)
            for band_index in range(minimum_band, maximum_band + 1):
                candidate_previous_index = band_index - 1
                candidate_previous_cost = previous_costs[candidate_previous_index]
                if candidate_previous_cost < best_previous_cost:
                    best_previous_cost = candidate_previous_cost
                    best_previous_index = candidate_previous_index
                if best_previous_index >= 0:
                    current_costs[band_index] = best_previous_cost + assignment_cost(
                        line_index,
                        band_index,
                    )
                    backtrack[line_index][band_index] = best_previous_index
            previous_costs = current_costs
        final_band = min(range(band_count), key=lambda index: previous_costs[index])
        if not math.isfinite(previous_costs[final_band]):
            return {}
        assignments = [final_band]
        for line_index in range(line_count - 1, 0, -1):
            final_band = backtrack[line_index][final_band]
            if final_band < 0:
                return {}
            assignments.append(final_band)
        assignments.reverse()
        return {
            id(line): bands[band_index]
            for line, band_index in zip(lines, assignments)
        }

    def _study_source_page_content_top(source: Image.Image) -> int:
        """Find the first note row below a detected blue note-app toolbar."""
        working = source.convert("RGB")
        if working.width > 360:
            scale = 360 / working.width
            working = working.resize(
                (360, max(1, round(working.height * scale))),
                Image.Resampling.LANCZOS,
            )
        pixels = working.load()
        scan_bottom = max(1, round(working.height * 0.28))
        blue_rows: List[int] = []
        for y in range(scan_bottom):
            blue_pixels = 0
            for x in range(working.width):
                red, green, blue = pixels[x, y]
                if blue >= red + 24 and blue >= green + 12 and blue >= 70:
                    blue_pixels += 1
            if blue_pixels / working.width >= 0.18:
                blue_rows.append(y)
        if not blue_rows:
            return 0
        toolbar_bottom = max(blue_rows)
        toolbar_bottom_normalized = round(toolbar_bottom * 1000 / working.height)
        mask = _build_study_source_ink_mask(working)
        for band_top, band_bottom in _study_source_visual_line_bands(working):
            if band_top <= toolbar_bottom_normalized + 5:
                continue
            pixel_top = max(0, round(band_top * mask.height / 1000))
            pixel_bottom = min(mask.height, max(pixel_top + 1, round(band_bottom * mask.height / 1000)))
            strip = mask.crop((0, pixel_top, mask.width, pixel_bottom))
            column_projection = list(strip.resize((mask.width, 1), Image.Resampling.BOX).getdata())
            active_columns = [index for index, value in enumerate(column_projection) if value >= 3]
            if not active_columns:
                continue
            ink_width_ratio = (active_columns[-1] - active_columns[0] + 1) / mask.width
            if ink_width_ratio >= 0.22 and band_bottom - band_top >= 12:
                return max(toolbar_bottom_normalized, band_top - 8)
        return min(1000, toolbar_bottom_normalized + 35)

    def _refine_study_source_bbox_with_text_lines(
        mask: Image.Image,
        bbox: Dict[str, int],
        *,
        start_x: int,
        start_y: int,
        end_x: int,
        end_y: int,
        evidence_length: int,
    ) -> Tuple[Dict[str, int], bool]:
        width, height = mask.size
        if width < 40 or height < 40:
            return bbox, False

        def normalized_x(pixel: int) -> int:
            return max(0, min(1000, round(pixel * 1000 / width)))

        def normalized_y(pixel: int) -> int:
            return max(0, min(1000, round(pixel * 1000 / height)))

        def pixel_x(value: int) -> int:
            return max(0, min(width, round(width * value / 1000)))

        def pixel_y(value: int) -> int:
            return max(0, min(height, round(height * value / 1000)))

        def merged_ranges(indices: List[int], max_gap: int) -> List[Tuple[int, int]]:
            if not indices:
                return []
            ranges: List[Tuple[int, int]] = []
            range_start = indices[0]
            previous = indices[0]
            for current in indices[1:]:
                if current - previous > max_gap:
                    ranges.append((range_start, previous + 1))
                    range_start = current
                previous = current
            ranges.append((range_start, previous + 1))
            return ranges

        context_left = max(0, min(bbox["left"], start_x, end_x) - 70)
        context_right = min(1000, max(bbox["right"], start_x, end_x) + 70)
        context_top = max(0, min(bbox["top"], start_y, end_y) - 70)
        context_bottom = min(1000, max(bbox["bottom"], start_y, end_y) + 70)
        crop_left = pixel_x(context_left)
        crop_right = pixel_x(context_right)
        crop_top = pixel_y(context_top)
        crop_bottom = pixel_y(context_bottom)
        if crop_right - crop_left < 20 or crop_bottom - crop_top < 20:
            return bbox, False

        region = mask.crop((crop_left, crop_top, crop_right, crop_bottom))
        row_projection = list(region.resize((1, region.height), Image.Resampling.BOX).getdata())
        sorted_projection = sorted(row_projection)
        background_level = sorted_projection[min(len(sorted_projection) - 1, round(len(sorted_projection) * 0.55))]
        row_threshold = max(4, min(16, round(background_level * 1.45)))
        smoothed_rows = [
            sum(row_projection[max(0, index - 1) : min(len(row_projection), index + 2)])
            / max(1, min(len(row_projection), index + 2) - max(0, index - 1))
            for index in range(len(row_projection))
        ]
        active_rows = [index for index, value in enumerate(smoothed_rows) if value >= row_threshold]
        raw_bands = merged_ranges(active_rows, max(2, round(height * 0.0015)))
        maximum_rule_height = max(5, round(height * 0.0045))
        text_bands: List[Tuple[int, int]] = []
        rule_bands: List[Tuple[int, int]] = []
        for band_top, band_bottom in raw_bands:
            band_values = row_projection[band_top:band_bottom]
            band_height = band_bottom - band_top
            is_horizontal_rule = (
                band_height <= maximum_rule_height
                and band_values
                and max(band_values) >= 42
                and sum(band_values) / len(band_values) >= 18
            )
            if is_horizontal_rule:
                rule_bands.append((band_top, band_bottom))
            elif band_height >= 3:
                text_bands.append((band_top, band_bottom))
        if not text_bands:
            return bbox, False

        local_start_y = pixel_y(start_y) - crop_top
        local_end_y = pixel_y(end_y) - crop_top

        def band_distance(band: Tuple[int, int], anchor: int) -> int:
            if band[0] <= anchor <= band[1]:
                return 0
            return min(abs(anchor - band[0]), abs(anchor - band[1]))

        start_index = min(range(len(text_bands)), key=lambda index: band_distance(text_bands[index], local_start_y))
        end_index = min(range(len(text_bands)), key=lambda index: band_distance(text_bands[index], local_end_y))
        anchor_limit = max(24, round(height * 0.045))
        if (
            band_distance(text_bands[start_index], local_start_y) > anchor_limit
            or band_distance(text_bands[end_index], local_end_y) > anchor_limit
        ):
            return bbox, False
        if start_index > end_index:
            start_index, end_index = end_index, start_index

        selected_top = text_bands[start_index][0]
        selected_bottom = text_bands[end_index][1]
        selected_height = selected_bottom - selected_top
        if selected_height < 4:
            return bbox, False

        selected = region.crop((0, selected_top, region.width, selected_bottom))
        selected_draw = ImageDraw.Draw(selected)
        for rule_top, rule_bottom in rule_bands:
            if rule_bottom <= selected_top or rule_top >= selected_bottom:
                continue
            selected_draw.rectangle(
                (0, max(0, rule_top - selected_top), selected.width, min(selected.height, rule_bottom - selected_top)),
                fill=0,
            )
        column_projection = list(selected.resize((selected.width, 1), Image.Resampling.BOX).getdata())
        active_columns = [index for index, value in enumerate(column_projection) if value >= 2]
        if not active_columns:
            return bbox, False

        total_column_ink = sum(column_projection[index] for index in active_columns)

        def weighted_column(quantile: float) -> int:
            target = total_column_ink * quantile
            running = 0
            for index in active_columns:
                running += column_projection[index]
                if running >= target:
                    return index
            return active_columns[-1]

        ink_left = weighted_column(0.003)
        ink_right = weighted_column(0.997) + 1
        pad_x = max(4, round(width * 0.006))
        pad_y = max(4, round(height * 0.004))
        refined = {
            **bbox,
            "left": normalized_x(max(0, crop_left + ink_left - pad_x)),
            "top": normalized_y(max(0, crop_top + selected_top - pad_y)),
            "right": normalized_x(min(width, crop_left + ink_right + pad_x)),
            "bottom": normalized_y(min(height, crop_top + selected_bottom + pad_y)),
        }
        refined["left"] = max(0, min(refined["left"], start_x - 8, end_x - 8))
        refined["top"] = max(0, min(refined["top"], start_y - 8, end_y - 8))
        refined["right"] = min(1000, max(refined["right"], start_x + 8, end_x + 8))
        refined["bottom"] = min(1000, max(refined["bottom"], start_y + 8, end_y + 8))
        refined_width = refined["right"] - refined["left"]
        refined_height = refined["bottom"] - refined["top"]
        if evidence_length > 120 and refined_width < min(180, (bbox["right"] - bbox["left"]) * 0.35):
            return bbox, False
        if not (12 <= refined_width <= 960 and 8 <= refined_height <= 850):
            return bbox, False
        return refined, True

    def _snap_study_source_bbox_to_ink(
        image_bytes: bytes,
        bbox: Dict[str, int],
    ) -> Dict[str, int]:
        try:
            with Image.open(io.BytesIO(image_bytes)) as source_image:
                source = ImageOps.exif_transpose(source_image).convert("RGB")
            image_width, image_height = source.size
            margin_x = 24
            margin_y = 28
            crop_left = max(0, round(image_width * (bbox["left"] - margin_x) / 1000))
            crop_top = max(0, round(image_height * (bbox["top"] - margin_y) / 1000))
            crop_right = min(image_width, round(image_width * (bbox["right"] + margin_x) / 1000))
            crop_bottom = min(image_height, round(image_height * (bbox["bottom"] + margin_y) / 1000))
            if crop_right - crop_left < 24 or crop_bottom - crop_top < 16:
                return bbox
            crop = source.crop((crop_left, crop_top, crop_right, crop_bottom))
            max_side = max(crop.size)
            if max_side > 1200:
                scale = 1200 / max_side
                crop = crop.resize(
                    (max(1, round(crop.width * scale)), max(1, round(crop.height * scale))),
                    Image.Resampling.LANCZOS,
                )
            gray = ImageOps.grayscale(crop)
            blur_radius = max(3.0, min(crop.size) / 55)
            local_background = gray.filter(ImageFilter.GaussianBlur(radius=blur_radius))
            contrast = ImageChops.difference(local_background, gray)
            mask = contrast.point(lambda value: 255 if value >= 16 else 0)
            mask = mask.filter(ImageFilter.MaxFilter(3))

            local_left = round((bbox["left"] / 1000 * image_width - crop_left) * crop.width / max(1, crop_right - crop_left))
            local_top = round((bbox["top"] / 1000 * image_height - crop_top) * crop.height / max(1, crop_bottom - crop_top))
            local_right = round((bbox["right"] / 1000 * image_width - crop_left) * crop.width / max(1, crop_right - crop_left))
            local_bottom = round((bbox["bottom"] / 1000 * image_height - crop_top) * crop.height / max(1, crop_bottom - crop_top))
            row_projection = list(mask.resize((1, mask.height), Image.Resampling.BOX).getdata())
            row_threshold = 3
            row_padding = max(3, round(mask.height * 0.012))
            row_candidates = [
                index
                for index, value in enumerate(row_projection)
                if value >= row_threshold and local_top - row_padding <= index <= local_bottom + row_padding
            ]
            if not row_candidates:
                return bbox
            ink_top = max(0, min(row_candidates) - 3)
            ink_bottom = min(mask.height, max(row_candidates) + 4)
            line_mask = mask.crop((0, ink_top, mask.width, max(ink_top + 1, ink_bottom)))
            column_projection = list(line_mask.resize((line_mask.width, 1), Image.Resampling.BOX).getdata())
            column_padding = max(4, round(mask.width * 0.012))
            column_candidates = [
                index
                for index, value in enumerate(column_projection)
                if value >= 3 and local_left - column_padding <= index <= local_right + column_padding
            ]
            if not column_candidates:
                return bbox
            ink_left = max(0, min(column_candidates) - 4)
            ink_right = min(mask.width, max(column_candidates) + 5)

            def x_to_normalized(value: int) -> int:
                pixel = crop_left + value / max(1, crop.width) * (crop_right - crop_left)
                return round(pixel * 1000 / image_width)

            def y_to_normalized(value: int) -> int:
                pixel = crop_top + value / max(1, crop.height) * (crop_bottom - crop_top)
                return round(pixel * 1000 / image_height)

            snapped = {
                **bbox,
                "left": max(bbox["left"] - 20, min(bbox["left"] + 20, x_to_normalized(ink_left))),
                "top": max(bbox["top"] - 20, min(bbox["top"] + 20, y_to_normalized(ink_top))),
                "right": max(bbox["right"] - 20, min(bbox["right"] + 20, x_to_normalized(ink_right))),
                "bottom": max(bbox["bottom"] - 20, min(bbox["bottom"] + 20, y_to_normalized(ink_bottom))),
            }
            if snapped["right"] - snapped["left"] < 12 or snapped["bottom"] - snapped["top"] < 8:
                return bbox
            return snapped
        except (OSError, ValueError, TypeError):
            return bbox

    def _expand_study_source_bbox_through_edge_ink(
        image_bytes: bytes,
        bbox: Dict[str, int],
        *,
        end_x: int,
        end_y: int,
        evidence_length: int,
    ) -> Dict[str, int]:
        try:
            with Image.open(io.BytesIO(image_bytes)) as source_image:
                source = ImageOps.exif_transpose(source_image).convert("RGB")
            max_side = max(source.size)
            if max_side > 1400:
                scale = 1400 / max_side
                source = source.resize(
                    (max(1, round(source.width * scale)), max(1, round(source.height * scale))),
                    Image.Resampling.LANCZOS,
                )
            gray = ImageOps.grayscale(source)
            background = gray.filter(ImageFilter.GaussianBlur(radius=max(3.0, min(source.size) / 55)))
            contrast = ImageChops.difference(background, gray)
            mask = contrast.point(lambda value: 255 if value >= 16 else 0).filter(ImageFilter.MaxFilter(3))

            left = round(mask.width * bbox["left"] / 1000)
            top = round(mask.height * bbox["top"] / 1000)
            right = round(mask.width * bbox["right"] / 1000)
            bottom = round(mask.height * bbox["bottom"] / 1000)

            def extend_projection(
                projection: List[int],
                edge: int,
                limit: int,
                trigger_gap: int,
                stop_gap: int,
            ) -> int:
                active = [
                    index
                    for index, value in enumerate(projection)
                    if value >= 3 and edge - trigger_gap <= index <= limit
                ]
                if not active:
                    return edge
                first_after_edge = next((index for index in active if index >= edge), None)
                if first_after_edge is None or first_after_edge - edge > trigger_gap:
                    return edge
                extended = first_after_edge
                previous = first_after_edge
                for index in active:
                    if index < first_after_edge:
                        continue
                    if index - previous > stop_gap:
                        break
                    extended = index
                    previous = index
                return max(edge, extended + 5)

            expanded = dict(bbox)
            if (evidence_length > 180 or bbox["right"] - end_x <= 50) and right < mask.width:
                vertical_top = max(0, top - round(mask.height * 0.01))
                vertical_bottom = min(mask.height, bottom + round(mask.height * 0.01))
                right_projection = list(
                    mask.crop((0, vertical_top, mask.width, max(vertical_top + 1, vertical_bottom)))
                    .resize((mask.width, 1), Image.Resampling.BOX)
                    .getdata()
                )
                right_limit = min(
                    mask.width - 1,
                    right + round(mask.width * (0.35 if evidence_length > 160 else 0.30)),
                )
                extended_right = extend_projection(
                    right_projection,
                    right,
                    right_limit,
                    max(12, round(mask.width * 0.02)),
                    max(16, round(mask.width * (0.045 if evidence_length > 160 else 0.018))),
                )
                expanded["right"] = min(1000, round(extended_right * 1000 / mask.width))
            if bbox["bottom"] - end_y <= 50 and bottom < mask.height:
                horizontal_left = max(0, left - round(mask.width * 0.01))
                horizontal_right = min(mask.width, right + round(mask.width * 0.01))
                bottom_projection = list(
                    mask.crop((horizontal_left, 0, max(horizontal_left + 1, horizontal_right), mask.height))
                    .resize((1, mask.height), Image.Resampling.BOX)
                    .getdata()
                )
                bottom_limit = min(
                    mask.height - 1,
                    bottom + round(mask.height * (0.08 if evidence_length > 160 else 0.12)),
                )
                extended_bottom = extend_projection(
                    bottom_projection,
                    bottom,
                    bottom_limit,
                    max(12, round(mask.height * 0.012)),
                    max(10, round(mask.height * 0.009)),
                )
                expanded["bottom"] = min(1000, round(extended_bottom * 1000 / mask.height))
            if _validated_study_source_bbox(expanded) is None:
                return bbox
            return expanded
        except (OSError, ValueError, TypeError):
            return bbox

    def _localize_study_card_sources_legacy(
        images: List[Tuple[str, bytes, str]],
        key_concepts: List[Dict[str, Any]],
    ) -> Tuple[int, int]:
        requests_by_id: Dict[str, Dict[str, Any]] = {}
        requests_by_image: Dict[int, List[Dict[str, Any]]] = {}
        for concept_index, concept in enumerate(key_concepts):
            if not isinstance(concept, dict):
                continue
            for source_ref_index, source_ref in enumerate(concept.get("source_refs") or []):
                if not isinstance(source_ref, dict):
                    continue
                source_ref.pop("bbox", None)
                try:
                    image_index = int(source_ref.get("image_index") or 0)
                except (TypeError, ValueError):
                    continue
                evidence = _literal_study_source_evidence(source_ref.get("evidence"))
                if not (1 <= image_index <= len(images)) or not evidence:
                    continue
                location_id = f"c{concept_index}r{source_ref_index}"
                requests_by_id[location_id] = source_ref
                requests_by_image.setdefault(image_index, []).append(
                    {
                        "location_id": location_id,
                        "image_index": image_index,
                        "concept": str(concept.get("concept") or "")[:80],
                        "evidence": evidence[:240],
                    }
                )
        total = len(requests_by_id)
        if not total:
            return 0, 0
        located = 0
        seen: Set[str] = set()
        failed_pages = 0
        for image_index, localization_input in sorted(requests_by_image.items()):
            _raise_if_study_upload_cancelled()
            item_count = len(localization_input)
            location_schema = {
                "type": "object",
                "additionalProperties": False,
                "required": ["locations"],
                "properties": {
                    "locations": {
                        "type": "array",
                        "minItems": item_count,
                        "maxItems": item_count,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["location_id", "left", "top", "right", "bottom", "confidence"],
                            "properties": {
                                "location_id": {"type": "string", "maxLength": 12},
                                "left": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "top": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "right": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "bottom": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
                            },
                        },
                    },
                },
            }
            filename, image_bytes, mime_type = images[image_index - 1]
            with Image.open(io.BytesIO(image_bytes)) as source_image:
                clean_page = ImageOps.exif_transpose(source_image).convert("RGB")
            pixel_width, pixel_height = clean_page.size
            guide_data_url = _study_coordinate_guide_data_url(clean_page)
            prompt = (
                f"你是手寫筆記的精確視覺定位員。現在只提供 image_index={image_index} 這一張完整原圖，"
                f"原始 bitmap 尺寸為 {pixel_width}×{pixel_height} 像素，檔名為 {filename}。"
                "每個 location_id 都有卡片名稱與來源 evidence；請直接在眼前這張圖逐字比對，不得依段落順序或內容類型猜位置。"
                "先辨認支撐 evidence 的第一個可見字與最後一個可見字，再框出它們實際占用的最小連續區域；"
                "evidence 可能同時包含兩個定義、數條公式或題幹加推導；必須逐項確認 evidence 明確提到的每一部分都落在框內，"
                "不能找到第一個關鍵詞後就停止。決定 bottom 前，務必確認 evidence 最後一個定義或公式完整位於 bottom 上方。"
                "若是例題或推導，框必須涵蓋該卡使用的題幹與必要計算行，但排除相鄰且無關的題目或章節。"
                "座標必須相對於完整 bitmap（包含工具列、黑邊與頁面空白），左上為 (0,0)、右下為 (1000,1000)。"
                "left/top/right/bottom 使用整數，四周只保留約 8 至 15 個座標單位，不能用粗略的半頁或整段區帶。"
                "完整涵蓋來源的優先順序高於框得極小；需要涵蓋多個相鄰項目時可以適度擴張，但仍排除下一個無關段落。"
                "你會依序看到乾淨原圖與完全相同尺寸的紅色座標網格圖；用乾淨圖辨字，用網格上的 X0..X900、Y0..Y900 讀取位置。"
                "禁止使用模型內部縮圖的像素座標，輸出的數字必須直接對齊第二張圖的紅色網格標籤。"
                "若同一 evidence 跨相鄰數行，以一個矩形完整包住；若圖片無法唯一確認位置，confidence 必須低於 60。"
                "每個 location_id 恰好輸出一次且不得改名，只輸出 schema 指定 JSON。\n\n待定位來源：\n"
                + json.dumps(localization_input, ensure_ascii=False, separators=(",", ":"))
            )
            content: List[Dict[str, Any]] = [
                {"type": "input_text", "text": prompt},
                {
                    "type": "input_image",
                    "image_url": f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}",
                    "detail": "high",
                },
                {"type": "input_text", "text": "以下是同一張原圖的紅色 0–1000 座標網格輔助圖。"},
                {"type": "input_image", "image_url": guide_data_url, "detail": "high"},
            ]
            try:
                result = _call_openai_json(
                    name="study_recall_source_locations",
                    schema=location_schema,
                    content=content,
                    timeout=240,
                    reasoning_effort="medium",
                    max_output_tokens=8000,
                )
                locations = result.get("locations") if isinstance(result, dict) else None
                if not isinstance(locations, list):
                    raise ValueError("Missing source locations")
            except (requests.RequestException, ValueError, TypeError):
                failed_pages += 1
                app.logger.exception("Study-note source localization failed for image %s", image_index)
                continue
            coarse_by_id: Dict[str, Dict[str, int]] = {}
            for item in locations:
                if not isinstance(item, dict):
                    continue
                location_id = str(item.get("location_id") or "")
                source_ref = requests_by_id.get(location_id)
                if source_ref is None or location_id in coarse_by_id or location_id in seen:
                    continue
                try:
                    left = int(item.get("left"))
                    top = int(item.get("top"))
                    right = int(item.get("right"))
                    bottom = int(item.get("bottom"))
                    confidence = int(item.get("confidence"))
                except (TypeError, ValueError):
                    continue
                width = right - left
                height = bottom - top
                if confidence < 60 or not (0 <= left < right <= 1000 and 0 <= top < bottom <= 1000):
                    continue
                if width < 12 or height < 8 or width > 960 or height > 850:
                    continue
                coarse_by_id[location_id] = {
                    "left": left,
                    "top": top,
                    "right": right,
                    "bottom": bottom,
                    "confidence": confidence,
                }
            if not coarse_by_id:
                continue

            crop_specs: Dict[str, Dict[str, Any]] = {}
            refine_content: List[Dict[str, Any]] = []
            refinement_targets = {
                item["location_id"]: item
                for item in localization_input
                if item.get("location_id") in coarse_by_id
            }
            refine_prompt = (
                "你是手寫筆記來源框的第二階段精校員。每個 target 會依序提供同一個局部裁切的乾淨圖與紅色 "
                "0–1000 網格圖；座標只相對於該 target 的局部裁切，不是整張原圖。"
                "請逐字比對 concept 與 evidence，框住真正支撐卡片內容的第一個可見字到最後一個可見字。"
                "evidence 若明確包含定義、公式、條件或推導中的多個部分，矩形必須完整包含它們，但排除相鄰無關段落。"
                "先在乾淨圖確認文字，再用網格讀取 left/top/right/bottom；不得沿用或猜測第一階段座標。"
                "框的四周只留約 5 至 12 個局部座標單位，不能裁掉上下標、分數、矩陣、根號或公式末端。"
                "只有在裁切內可唯一辨識完整來源時 found=true；找不到、只有部分內容或有多個無法區分的位置時，"
                "found=false 且 confidence 低於 60。每個 location_id 必須恰好輸出一次且不得改名，只輸出 schema 指定 JSON。"
                "start_x/start_y 是起點錨點可見文字的中心，end_x/end_y 是終點錨點可見文字的中心；"
                "四者也使用局部 0–1000 座標，且 found=true 時兩個錨點都必須落在輸出矩形內。"
            )
            refine_content.append({"type": "input_text", "text": refine_prompt})
            for location_id, coarse in coarse_by_id.items():
                target = refinement_targets[location_id]
                target_evidence_length = len(str(target.get("evidence") or ""))
                coarse_width = coarse["right"] - coarse["left"]
                coarse_height = coarse["bottom"] - coarse["top"]
                padding_x = max(120, min(220, round(coarse_width / 3)))
                padding_y = max(140, min(240, coarse_height * 2))
                if target_evidence_length > 160:
                    padding_x = max(padding_x, 320)
                    padding_y = max(padding_y, 220)
                normalized_left = max(0, coarse["left"] - padding_x)
                normalized_top = max(0, coarse["top"] - padding_y)
                normalized_right = min(1000, coarse["right"] + padding_x)
                normalized_bottom = min(1000, coarse["bottom"] + padding_y)
                pixel_left = max(0, min(pixel_width - 1, math.floor(pixel_width * normalized_left / 1000)))
                pixel_top = max(0, min(pixel_height - 1, math.floor(pixel_height * normalized_top / 1000)))
                pixel_right = max(pixel_left + 1, min(pixel_width, math.ceil(pixel_width * normalized_right / 1000)))
                pixel_bottom = max(pixel_top + 1, min(pixel_height, math.ceil(pixel_height * normalized_bottom / 1000)))
                crop = clean_page.crop((pixel_left, pixel_top, pixel_right, pixel_bottom))
                crop_bounds = {
                    "left": round(pixel_left * 1000 / pixel_width),
                    "top": round(pixel_top * 1000 / pixel_height),
                    "right": round(pixel_right * 1000 / pixel_width),
                    "bottom": round(pixel_bottom * 1000 / pixel_height),
                }
                target_evidence = " ".join(str(target.get("evidence") or "").split())
                start_anchor = target_evidence[:28]
                end_anchor = target_evidence[-28:]
                target_label = (
                    f"TARGET {location_id}｜卡片：{target.get('concept') or ''}｜"
                    f"來源：{target_evidence}｜必須框入的起點錨點：{start_anchor}｜"
                    f"必須框入的終點錨點：{end_anchor}。輸出前逐字確認兩個錨點都在矩形內。"
                )
                clean_crop_url = _study_image_data_url(crop)
                guide_crop_url = _study_coordinate_guide_data_url(crop)
                crop_specs[location_id] = {
                    "bounds": crop_bounds,
                    "target_label": target_label,
                    "clean_url": clean_crop_url,
                    "guide_url": guide_crop_url,
                }
                refine_content.extend(
                    [
                        {"type": "input_text", "text": target_label},
                        {"type": "input_image", "image_url": clean_crop_url, "detail": "high"},
                        {
                            "type": "input_text",
                            "text": f"TARGET {location_id} 的同一裁切，以下為局部 0–1000 座標網格。",
                        },
                        {"type": "input_image", "image_url": guide_crop_url, "detail": "high"},
                    ]
                )

            refine_item_count = len(coarse_by_id)
            refinement_schema = {
                "type": "object",
                "additionalProperties": False,
                "required": ["locations"],
                "properties": {
                    "locations": {
                        "type": "array",
                        "minItems": refine_item_count,
                        "maxItems": refine_item_count,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "location_id",
                                "found",
                                "left",
                                "top",
                                "right",
                                "bottom",
                                "start_x",
                                "start_y",
                                "end_x",
                                "end_y",
                                "confidence",
                            ],
                            "properties": {
                                "location_id": {"type": "string", "maxLength": 12},
                                "found": {"type": "boolean"},
                                "left": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "top": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "right": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "bottom": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "start_x": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "start_y": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "end_x": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "end_y": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
                            },
                        },
                    }
                },
            }
            group_refinement_failed = False
            try:
                if refine_item_count > 1:
                    group_refinement_failed = True
                    refined_locations = []
                else:
                    refinement_result = _call_openai_json(
                        name="study_recall_source_locations_refined",
                        schema=refinement_schema,
                        content=refine_content,
                        timeout=240,
                        reasoning_effort="minimal",
                        max_output_tokens=4000,
                        repair_simple_location_json=True,
                    )
                    refined_locations = (
                        refinement_result.get("locations") if isinstance(refinement_result, dict) else None
                    )
                if not isinstance(refined_locations, list):
                    raise ValueError("Missing refined source locations")
            except ValueError as exc:
                app.logger.warning("Study-note grouped source refinement was incomplete for image %s: %s", image_index, exc)
                group_refinement_failed = True
                refined_locations = []
            except (requests.RequestException, TypeError):
                app.logger.exception("Study-note source refinement failed for image %s", image_index)
                group_refinement_failed = True
                refined_locations = []

            refined_by_id: Dict[str, Dict[str, Any]] = {}
            for item in refined_locations:
                if not isinstance(item, dict):
                    continue
                location_id = str(item.get("location_id") or "")
                if location_id not in coarse_by_id or location_id in refined_by_id:
                    continue
                refined_by_id[location_id] = item

            def refinement_is_usable(location_id: str, item: Any) -> bool:
                if not isinstance(item, dict) or item.get("found") is not True:
                    return False
                try:
                    local_left = int(item.get("left"))
                    local_top = int(item.get("top"))
                    local_right = int(item.get("right"))
                    local_bottom = int(item.get("bottom"))
                    start_x = int(item.get("start_x"))
                    start_y = int(item.get("start_y"))
                    end_x = int(item.get("end_x"))
                    end_y = int(item.get("end_y"))
                    confidence = int(item.get("confidence"))
                except (TypeError, ValueError):
                    return False
                if not all(0 <= value <= 1000 for value in (start_x, start_y, end_x, end_y)):
                    return False
                local_left = max(0, min(local_left, start_x - 12, end_x - 12))
                local_top = max(0, min(local_top, start_y - 12, end_y - 12))
                local_right = min(1000, max(local_right, start_x + 12, end_x + 12))
                local_bottom = min(1000, max(local_bottom, start_y + 12, end_y + 12))
                width = local_right - local_left
                height = local_bottom - local_top
                if not (
                    confidence >= 70
                    and 0 <= local_left < local_right <= 1000
                    and 0 <= local_top < local_bottom <= 1000
                    and 12 <= width <= 980
                    and 8 <= height <= 920
                ):
                    return False
                crop_bounds = crop_specs[location_id]["bounds"]
                crop_width = crop_bounds["right"] - crop_bounds["left"]
                crop_height = crop_bounds["bottom"] - crop_bounds["top"]
                refined_center_x = crop_bounds["left"] + (local_left + local_right) * crop_width / 2000
                refined_center_y = crop_bounds["top"] + (local_top + local_bottom) * crop_height / 2000
                coarse = coarse_by_id[location_id]
                coarse_center_x = (coarse["left"] + coarse["right"]) / 2
                coarse_center_y = (coarse["top"] + coarse["bottom"]) / 2
                max_center_shift_x = max(140, (coarse["right"] - coarse["left"]) * 0.45)
                max_center_shift_y = max(140, (coarse["bottom"] - coarse["top"]) * 0.45)
                refined_width = width * crop_width / 1000
                refined_height = height * crop_height / 1000
                coarse_width = coarse["right"] - coarse["left"]
                coarse_height = coarse["bottom"] - coarse["top"]
                evidence_length = len(str(refinement_targets[location_id].get("evidence") or ""))
                refined_area = refined_width * refined_height
                refined_aspect_ratio = refined_width / max(1, refined_height)
                if (
                    evidence_length > 160
                    and refined_width < coarse_width * 0.75
                    and refined_height < coarse_height * 0.55
                ):
                    return False
                if evidence_length > 180 and refined_area < 60_000 and refined_aspect_ratio < 8:
                    return False
                return (
                    abs(refined_center_x - coarse_center_x) <= max_center_shift_x
                    and abs(refined_center_y - coarse_center_y) <= max_center_shift_y
                )

            retry_ids = [
                location_id
                for location_id in coarse_by_id
                if not refinement_is_usable(location_id, refined_by_id.get(location_id))
            ]
            if not group_refinement_failed:
                retry_ids = retry_ids[:3]
            if retry_ids:
                single_refinement_schema = json.loads(json.dumps(refinement_schema))
                single_locations_schema = single_refinement_schema["properties"]["locations"]
                single_locations_schema["minItems"] = 1
                single_locations_schema["maxItems"] = 1
                for location_id in retry_ids:
                    _raise_if_study_upload_cancelled()
                    crop_spec = crop_specs[location_id]
                    prior_item: Optional[Dict[str, Any]] = None
                    for retry_attempt in range(2):
                        _raise_if_study_upload_cancelled()
                        second_attempt_note = ""
                        if retry_attempt and prior_item:
                            second_attempt_note = (
                                "上一個候選框未通過完整性或位置一致性檢查。請重新檢查 evidence 的最後一行，"
                                "必要時擴大框；不要重複上一組座標："
                                + json.dumps(prior_item, ensure_ascii=False, separators=(",", ":"))
                            )
                        retry_content = [
                            {
                                "type": "input_text",
                                "text": (
                                    refine_prompt
                                    + "現在只處理下列唯一 target。請先逐項核對 evidence 的起點、每條公式與終點；"
                                    "框內必須完整包含 evidence 的所有可見內容，但不能納入 evidence 結束後的下一個定義、例題或段落。"
                                    "請從乾淨裁切逐字找到來源，再用網格獨立讀取局部座標。"
                                    + second_attempt_note
                                ),
                            },
                            {"type": "input_text", "text": crop_spec["target_label"]},
                            {"type": "input_image", "image_url": crop_spec["clean_url"], "detail": "high"},
                            {
                                "type": "input_text",
                                "text": f"TARGET {location_id} 的同一裁切，以下為局部 0–1000 座標網格。",
                            },
                            {"type": "input_image", "image_url": crop_spec["guide_url"], "detail": "high"},
                        ]
                        try:
                            retry_result = _call_openai_json(
                                name="study_recall_source_location_retry",
                                schema=single_refinement_schema,
                                content=retry_content,
                                timeout=180,
                                reasoning_effort="low",
                                max_output_tokens=8000,
                                repair_simple_location_json=True,
                            )
                            retry_locations = retry_result.get("locations") if isinstance(retry_result, dict) else None
                            retry_item = retry_locations[0] if isinstance(retry_locations, list) and retry_locations else None
                            prior_item = retry_item if isinstance(retry_item, dict) else None
                            if (
                                isinstance(retry_item, dict)
                                and str(retry_item.get("location_id") or "") == location_id
                                and refinement_is_usable(location_id, retry_item)
                            ):
                                refined_by_id[location_id] = retry_item
                                break
                        except (requests.RequestException, ValueError, TypeError, IndexError):
                            app.logger.exception(
                                "Study-note individual source refinement failed for image %s target %s",
                                image_index,
                                location_id,
                            )
            for location_id, coarse in coarse_by_id.items():
                item = refined_by_id.get(location_id)
                if not refinement_is_usable(location_id, item):
                    continue
                try:
                    local_left = int(item.get("left"))
                    local_top = int(item.get("top"))
                    local_right = int(item.get("right"))
                    local_bottom = int(item.get("bottom"))
                    start_x = int(item.get("start_x"))
                    start_y = int(item.get("start_y"))
                    end_x = int(item.get("end_x"))
                    end_y = int(item.get("end_y"))
                    refined_confidence = int(item.get("confidence"))
                except (TypeError, ValueError):
                    continue
                local_left = max(0, min(local_left, start_x - 12, end_x - 12))
                local_top = max(0, min(local_top, start_y - 12, end_y - 12))
                local_right = min(1000, max(local_right, start_x + 12, end_x + 12))
                local_bottom = min(1000, max(local_bottom, start_y + 12, end_y + 12))
                local_width = local_right - local_left
                local_height = local_bottom - local_top
                if refined_confidence < 70:
                    continue
                if not (0 <= local_left < local_right <= 1000 and 0 <= local_top < local_bottom <= 1000):
                    continue
                if local_width < 12 or local_height < 8 or local_width > 980 or local_height > 920:
                    continue
                crop_bounds = crop_specs[location_id]["bounds"]
                crop_width = crop_bounds["right"] - crop_bounds["left"]
                crop_height = crop_bounds["bottom"] - crop_bounds["top"]
                candidate = {
                    "left": crop_bounds["left"] + round(local_left * crop_width / 1000),
                    "top": crop_bounds["top"] + round(local_top * crop_height / 1000),
                    "right": crop_bounds["left"] + round(local_right * crop_width / 1000),
                    "bottom": crop_bounds["top"] + round(local_bottom * crop_height / 1000),
                    "confidence": min(coarse["confidence"], refined_confidence),
                    "version": 2,
                }
                candidate = _snap_study_source_bbox_to_ink(image_bytes, candidate)
                end_x_full = crop_bounds["left"] + round(end_x * crop_width / 1000)
                end_y_full = crop_bounds["top"] + round(end_y * crop_height / 1000)
                candidate = _expand_study_source_bbox_through_edge_ink(
                    image_bytes,
                    candidate,
                    end_x=end_x_full,
                    end_y=end_y_full,
                    evidence_length=len(str(refinement_targets[location_id].get("evidence") or "")),
                )
                if _validated_study_source_bbox(candidate) is None:
                    continue
                requests_by_id[location_id]["bbox"] = candidate
                seen.add(location_id)
                located += 1

            def canonical_evidence(value: Any) -> str:
                return re.sub(r"\s+", "", str(value or "")).casefold()

            for location_id in coarse_by_id:
                if location_id not in seen:
                    continue
                target_evidence = canonical_evidence(refinement_targets[location_id].get("evidence"))
                target_bbox = _validated_study_source_bbox(requests_by_id[location_id].get("bbox"))
                if not target_evidence or not target_bbox:
                    continue
                containing_boxes = []
                for candidate_id in coarse_by_id:
                    if candidate_id == location_id or candidate_id not in seen:
                        continue
                    candidate_evidence = canonical_evidence(refinement_targets[candidate_id].get("evidence"))
                    candidate_bbox = _validated_study_source_bbox(requests_by_id[candidate_id].get("bbox"))
                    if (
                        not candidate_bbox
                        or len(candidate_evidence) <= len(target_evidence) + 5
                        or target_evidence not in candidate_evidence
                    ):
                        continue
                    overlap_width = max(
                        0,
                        min(target_bbox["right"], candidate_bbox["right"])
                        - max(target_bbox["left"], candidate_bbox["left"]),
                    )
                    overlap_height = max(
                        0,
                        min(target_bbox["bottom"], candidate_bbox["bottom"])
                        - max(target_bbox["top"], candidate_bbox["top"]),
                    )
                    target_area = (
                        (target_bbox["right"] - target_bbox["left"])
                        * (target_bbox["bottom"] - target_bbox["top"])
                    )
                    overlap_ratio = overlap_width * overlap_height / max(1, target_area)
                    if overlap_ratio >= 0.2:
                        continue
                    candidate_area = (
                        (candidate_bbox["right"] - candidate_bbox["left"])
                        * (candidate_bbox["bottom"] - candidate_bbox["top"])
                    )
                    containing_boxes.append((candidate_area, candidate_bbox))
                if containing_boxes:
                    _, containing_bbox = min(containing_boxes, key=lambda value: value[0])
                    requests_by_id[location_id]["bbox"] = {
                        **containing_bbox,
                        "confidence": max(60, containing_bbox["confidence"] - 5),
                        "version": 2,
                    }

            for location_id in coarse_by_id:
                if location_id in seen:
                    continue
                target_evidence = canonical_evidence(refinement_targets[location_id].get("evidence"))
                if not target_evidence:
                    continue
                containing_boxes = []
                for candidate_id in coarse_by_id:
                    if candidate_id == location_id or candidate_id not in seen:
                        continue
                    candidate_evidence = canonical_evidence(refinement_targets[candidate_id].get("evidence"))
                    candidate_bbox = _validated_study_source_bbox(requests_by_id[candidate_id].get("bbox"))
                    if (
                        not candidate_bbox
                        or len(candidate_evidence) <= len(target_evidence) + 5
                        or target_evidence not in candidate_evidence
                    ):
                        continue
                    area = (
                        (candidate_bbox["right"] - candidate_bbox["left"])
                        * (candidate_bbox["bottom"] - candidate_bbox["top"])
                    )
                    containing_boxes.append((area, candidate_bbox))
                if not containing_boxes:
                    continue
                _, containing_bbox = min(containing_boxes, key=lambda value: value[0])
                requests_by_id[location_id]["bbox"] = {
                    **containing_bbox,
                    "confidence": max(60, containing_bbox["confidence"] - 5),
                    "version": 2,
                }
                seen.add(location_id)
                located += 1
        if failed_pages == len(requests_by_image):
            raise ValueError("Source localization failed for every image")
        return located, total

    def _localize_study_card_sources_band_experiment(
        images: List[Tuple[str, bytes, str]],
        key_concepts: List[Dict[str, Any]],
    ) -> Tuple[int, int]:
        requests_by_id: Dict[str, Dict[str, Any]] = {}
        requests_by_image: Dict[int, List[Dict[str, Any]]] = {}
        for concept_index, concept in enumerate(key_concepts):
            if not isinstance(concept, dict):
                continue
            for source_ref_index, source_ref in enumerate(concept.get("source_refs") or []):
                if not isinstance(source_ref, dict):
                    continue
                source_ref.pop("bbox", None)
                try:
                    image_index = int(source_ref.get("image_index") or 0)
                except (TypeError, ValueError):
                    continue
                evidence = _literal_study_source_evidence(source_ref.get("evidence"))
                if not (1 <= image_index <= len(images)) or not evidence:
                    continue
                location_id = f"c{concept_index}r{source_ref_index}"
                requests_by_id[location_id] = source_ref
                requests_by_image.setdefault(image_index, []).append(
                    {
                        "location_id": location_id,
                        "concept": str(concept.get("concept") or "")[:80],
                        "evidence": evidence[:600],
                        "start_anchor_text": evidence[:36],
                        "end_anchor_text": evidence[-36:],
                    }
                )
        total = len(requests_by_id)
        if not total:
            return 0, 0

        located = 0
        failed_pages = 0
        for image_index, localization_input in sorted(requests_by_image.items()):
            _raise_if_study_upload_cancelled()
            item_count = len(localization_input)
            location_schema = {
                "type": "object",
                "additionalProperties": False,
                "required": ["locations"],
                "properties": {
                    "locations": {
                        "type": "array",
                        "minItems": item_count,
                        "maxItems": item_count,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "location_id",
                                "left",
                                "top",
                                "right",
                                "bottom",
                                "start_x",
                                "start_y",
                                "end_x",
                                "end_y",
                                "confidence",
                            ],
                            "properties": {
                                "location_id": {"type": "string", "maxLength": 12},
                                "left": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "top": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "right": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "bottom": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "start_x": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "start_y": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "end_x": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "end_y": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
                            },
                        },
                    }
                },
            }
            filename, image_bytes, _mime_type = images[image_index - 1]
            try:
                with Image.open(io.BytesIO(image_bytes)) as source_image:
                    clean_page = ImageOps.exif_transpose(source_image).convert("RGB")
                pixel_width, pixel_height = clean_page.size
                clean_data_url = _study_image_data_url(clean_page)
                guide_data_url = _study_coordinate_guide_data_url(clean_page)
                prompt = (
                    f"你是手寫筆記來源的精確視覺定位員。這是第 {image_index} 張、檔名 {filename}、"
                    f"canonical bitmap {pixel_width}×{pixel_height}。待定位項目都只來自這一張圖。"
                    "逐項比對 evidence，先找出 start_anchor_text 對應之第一段可見文字的中心，再找出 "
                    "end_anchor_text 對應之最後一段可見文字的中心；start_x/start_y 與 end_x/end_y 必須是這兩個"
                    "實際文字錨點的中心，不是矩形角落。接著以 left/top/right/bottom 框住從首錨點至尾錨點"
                    "所涵蓋的全部定義、條件、公式及必要推導。不得納入 evidence 結束後的下一個標題、例題或定義，"
                    "也不得因為先找到關鍵詞就漏掉 evidence 後半段。若 evidence 是較短內容，即使附近另有相關筆記也只框"
                    "該 evidence 本身。所有座標都相對於完整 canonical bitmap，左上 (0,0)、右下 (1000,1000)。"
                    "你會看到完全相同方向與長寬比的乾淨圖及紅色座標網格圖；乾淨圖用於逐字辨識，網格圖只用於讀座標。"
                    "矩形四周保留約 6 至 12 個座標單位，必須完整涵蓋上下標、矩陣、分數與公式末端。"
                    "每個 location_id 恰好輸出一次且不得改名；無法唯一辨識時 confidence 低於 60。只輸出 schema JSON。\n\n"
                    + json.dumps(localization_input, ensure_ascii=False, separators=(",", ":"))
                )
                source_content = [
                    {"type": "input_text", "text": prompt},
                    {"type": "input_image", "image_url": clean_data_url, "detail": "high"},
                    {"type": "input_text", "text": "同一張 canonical bitmap 的 0–1000 座標網格："},
                    {"type": "input_image", "image_url": guide_data_url, "detail": "high"},
                ]
                locations = None
                for source_attempt in range(2):
                    try:
                        result = _call_openai_json(
                            name="study_recall_source_locations_v3",
                            schema=location_schema,
                            content=source_content,
                            timeout=240,
                            reasoning_effort="minimal",
                            max_output_tokens=10000,
                            repair_simple_location_json=True,
                        )
                        locations = result.get("locations") if isinstance(result, dict) else None
                        if not isinstance(locations, list):
                            raise ValueError("Missing source locations")
                        break
                    except ValueError:
                        if source_attempt:
                            raise
                        app.logger.warning("Retrying incomplete source localization for image %s", image_index)
            except (OSError, requests.RequestException, ValueError, TypeError):
                failed_pages += 1
                app.logger.exception("Study-note source localization v3 failed for image %s", image_index)
                continue

            page_targets = {item["location_id"]: item for item in localization_input}
            coarse_by_id: Dict[str, Dict[str, int]] = {}
            for item in locations:
                if not isinstance(item, dict):
                    continue
                location_id = str(item.get("location_id") or "")
                if location_id not in page_targets or location_id in coarse_by_id:
                    continue
                try:
                    left = int(item.get("left"))
                    top = int(item.get("top"))
                    right = int(item.get("right"))
                    bottom = int(item.get("bottom"))
                    start_x = int(item.get("start_x"))
                    start_y = int(item.get("start_y"))
                    end_x = int(item.get("end_x"))
                    end_y = int(item.get("end_y"))
                    confidence = int(item.get("confidence"))
                except (TypeError, ValueError):
                    continue
                if confidence < 60 or not all(
                    0 <= value <= 1000
                    for value in (left, top, right, bottom, start_x, start_y, end_x, end_y)
                ):
                    continue
                coarse = {
                    "left": max(0, min(left, start_x - 12, end_x - 12)),
                    "top": max(0, min(top, start_y - 12, end_y - 12)),
                    "right": min(1000, max(right, start_x + 12, end_x + 12)),
                    "bottom": min(1000, max(bottom, start_y + 12, end_y + 12)),
                    "confidence": confidence,
                    "version": 3,
                }
                if _validated_study_source_bbox(coarse) is None:
                    continue
                coarse_by_id[location_id] = {
                    **coarse,
                    "start_x": start_x,
                    "start_y": start_y,
                    "end_x": end_x,
                    "end_y": end_y,
                }

            crop_specs: Dict[str, Dict[str, Any]] = {}
            refinement_prompt = (
                "你是手寫筆記的逐行轉錄員。第一張圖是完整局部裁切，只用來理解前後文；後續 BAND 圖由"
                "影像演算法將同一裁切中的實體書寫列切開並固定編號。你必須為每個 BAND 恰好輸出一筆，"
                "line_id 就是藍色 B 編號的數字，順序不可交換、遺漏或重複。text 只能轉錄該 BAND 圖內"
                "實際看得到的文字或公式，不得把相鄰 BAND 的內容移入、摘要、修正或依上下文補寫；若只有"
                "分隔線、零碎筆畫或沒有可辨識文字，text 輸出空字串且 confidence 為 0。left/right 相對於"
                "原始裁切的筆記寬度，不包含 BAND 圖左側藍色標籤欄，最左為 0、最右為 1000。只輸出 schema JSON。"
            )
            for location_id, coarse in coarse_by_id.items():
                coarse_width = coarse["right"] - coarse["left"]
                coarse_height = coarse["bottom"] - coarse["top"]
                padding_x = max(90, min(180, round(coarse_width * 0.28)))
                padding_y = max(90, min(190, round(coarse_height * 0.65)))
                target = page_targets[location_id]
                if len(str(target.get("evidence") or "")) > 160:
                    padding_x = max(padding_x, 150)
                    padding_y = max(padding_y, 150)
                crop_left = max(0, math.floor(pixel_width * max(0, coarse["left"] - padding_x) / 1000))
                crop_top = max(0, math.floor(pixel_height * max(0, coarse["top"] - padding_y) / 1000))
                crop_right = min(
                    pixel_width,
                    math.ceil(pixel_width * min(1000, coarse["right"] + padding_x) / 1000),
                )
                crop_bottom = min(
                    pixel_height,
                    math.ceil(pixel_height * min(1000, coarse["bottom"] + padding_y) / 1000),
                )
                if crop_right - crop_left < 20 or crop_bottom - crop_top < 20:
                    continue
                crop = clean_page.crop((crop_left, crop_top, crop_right, crop_bottom))
                visual_bands = _study_source_visual_line_bands(crop)
                if not visual_bands or len(visual_bands) > 60:
                    continue
                band_sheet_urls = _study_source_band_sheet_data_urls(crop, visual_bands)
                if not band_sheet_urls:
                    continue
                crop_specs[location_id] = {
                    "left": round(crop_left * 1000 / pixel_width),
                    "top": round(crop_top * 1000 / pixel_height),
                    "right": round(crop_right * 1000 / pixel_width),
                    "bottom": round(crop_bottom * 1000 / pixel_height),
                    "image": crop,
                    "clean_url": _study_image_data_url(crop),
                    "visual_bands": visual_bands,
                    "band_sheet_urls": band_sheet_urls,
                }

            refined_by_id: Dict[str, Dict[str, int]] = {}
            if crop_specs:
                refinement_schema = {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["lines"],
                    "properties": {
                        "lines": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 60,
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["line_id", "text", "left", "right", "confidence"],
                                "properties": {
                                    "line_id": {"type": "integer", "minimum": 1},
                                    "text": {"type": "string", "maxLength": 260},
                                    "left": {"type": "integer", "minimum": 0, "maximum": 1000},
                                    "right": {"type": "integer", "minimum": 0, "maximum": 1000},
                                    "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
                                },
                            },
                        }
                    },
                }
                parent_cancel_event = getattr(study_upload_context, "cancel_event", None)

                def refine_one_source(location_id: str) -> Tuple[str, Optional[Dict[str, int]]]:
                    if isinstance(parent_cancel_event, threading.Event):
                        study_upload_context.cancel_event = parent_cancel_event
                    crop_spec = crop_specs[location_id]
                    try:
                        visual_bands = crop_spec["visual_bands"]
                        refinement_content: List[Dict[str, Any]] = [
                            {"type": "input_text", "text": refinement_prompt},
                            {
                                "type": "input_text",
                                "text": (
                                    f"TARGET {location_id} 共有 {len(visual_bands)} 個 BAND。"
                                    "先看完整裁切理解內容，再逐張轉錄 BAND 圖。"
                                ),
                            },
                            {"type": "input_image", "image_url": crop_spec["clean_url"], "detail": "high"},
                        ]
                        for sheet_index, sheet_url in enumerate(crop_spec["band_sheet_urls"], start=1):
                            first_band = (sheet_index - 1) * 8 + 1
                            last_band = min(len(visual_bands), first_band + 7)
                            refinement_content.extend(
                                [
                                    {
                                        "type": "input_text",
                                        "text": f"BAND B{first_band:02d} 至 B{last_band:02d}：",
                                    },
                                    {"type": "input_image", "image_url": sheet_url, "detail": "high"},
                                ]
                            )
                        refinement_result = _call_openai_json(
                            name="study_recall_source_bands_v7",
                            schema=refinement_schema,
                            content=refinement_content,
                            timeout=180,
                            reasoning_effort="minimal",
                            max_output_tokens=12000,
                        )
                        raw_lines = refinement_result.get("lines") if isinstance(refinement_result, dict) else None
                        if not isinstance(raw_lines, list):
                            raise ValueError("Missing source text lines")
                        valid_lines: List[Dict[str, Any]] = []
                        seen_band_ids: Set[int] = set()
                        for line in raw_lines:
                            if not isinstance(line, dict):
                                continue
                            try:
                                line_id = int(line.get("line_id"))
                                if not (1 <= line_id <= len(visual_bands)) or line_id in seen_band_ids:
                                    continue
                                band_top, band_bottom = visual_bands[line_id - 1]
                                normalized_line = {
                                    "line_id": line_id,
                                    "text": str(line.get("text") or "").strip(),
                                    "left": int(line.get("left")),
                                    "top": band_top,
                                    "right": int(line.get("right")),
                                    "bottom": band_bottom,
                                    "confidence": int(line.get("confidence")),
                                }
                            except (TypeError, ValueError):
                                continue
                            seen_band_ids.add(line_id)
                            if (
                                normalized_line["text"]
                                and normalized_line["confidence"] >= 45
                                and 0 <= normalized_line["left"] < normalized_line["right"] <= 1000
                                and 0 <= normalized_line["top"] < normalized_line["bottom"] <= 1000
                            ):
                                valid_lines.append(normalized_line)
                        if seen_band_ids != set(range(1, len(visual_bands) + 1)):
                            raise ValueError("Incomplete or duplicate source bands")
                        valid_lines.sort(key=lambda line: line["line_id"])
                        selected_lines, match_metrics = _match_study_source_evidence_to_lines(
                            str(page_targets[location_id].get("evidence") or ""),
                            valid_lines,
                        )
                        if not selected_lines or not source_line_match_is_verified(
                            str(page_targets[location_id].get("evidence") or ""),
                            match_metrics,
                        ):
                            return location_id, None
                        selected_ids = {id(line) for line in selected_lines}
                        selected_indices = [
                            index for index, line in enumerate(valid_lines) if id(line) in selected_ids
                        ]
                        first_index = min(selected_indices)
                        last_index = max(selected_indices)
                        selected_left = min(line["left"] for line in selected_lines)
                        selected_top = min(line["top"] for line in selected_lines)
                        selected_right = max(line["right"] for line in selected_lines)
                        selected_bottom = max(line["bottom"] for line in selected_lines)
                        visual_alignment = {
                            id(line): (line["top"], line["bottom"])
                            for line in valid_lines
                        }
                        using_visual_alignment = True
                        typical_line_height = median(
                            line["bottom"] - line["top"] for line in selected_lines
                        )
                        vertical_padding = max(18, min(52, round(typical_line_height * 0.58)))
                        horizontal_padding = max(18, min(38, round(typical_line_height * 0.34)))

                        def overlaps_selected_width(line: Dict[str, Any]) -> bool:
                            overlap = max(
                                0,
                                min(selected_right, line["right"]) - max(selected_left, line["left"]),
                            )
                            narrower_width = max(
                                1,
                                min(selected_right - selected_left, line["right"] - line["left"]),
                            )
                            return overlap / narrower_width >= 0.18

                        local_top = max(0, selected_top - vertical_padding)
                        local_bottom = min(1000, selected_bottom + vertical_padding)
                        if first_index > 0:
                            previous_line = valid_lines[first_index - 1]
                            previous_bottom = (
                                visual_alignment[id(previous_line)][1]
                                if using_visual_alignment and id(previous_line) in visual_alignment
                                else previous_line["bottom"]
                            )
                            if (
                                overlaps_selected_width(previous_line)
                                and previous_bottom <= selected_top
                            ):
                                local_top = max(
                                    local_top,
                                    round((previous_bottom + selected_top) / 2),
                                )
                        if last_index + 1 < len(valid_lines):
                            next_line = valid_lines[last_index + 1]
                            next_top = (
                                visual_alignment[id(next_line)][0]
                                if using_visual_alignment and id(next_line) in visual_alignment
                                else next_line["top"]
                            )
                            if (
                                overlaps_selected_width(next_line)
                                and next_top >= selected_bottom
                            ):
                                local_bottom = min(
                                    local_bottom,
                                    round((selected_bottom + next_top) / 2),
                                )
                        local_left = max(0, selected_left - horizontal_padding)
                        local_right = min(1000, selected_right + horizontal_padding)
                        first_line = selected_lines[0]
                        last_line = selected_lines[-1]
                        local_start_x = round((first_line["left"] + first_line["right"]) / 2)
                        local_start_y_candidate = (
                            round(sum(visual_alignment[id(first_line)]) / 2)
                            if using_visual_alignment
                            else round((first_line["top"] + first_line["bottom"]) / 2)
                        )
                        local_start_y = max(selected_top, min(selected_bottom, local_start_y_candidate))
                        local_end_x = round((last_line["left"] + last_line["right"]) / 2)
                        local_end_y_candidate = (
                            round(sum(visual_alignment[id(last_line)]) / 2)
                            if using_visual_alignment
                            else round((last_line["top"] + last_line["bottom"]) / 2)
                        )
                        local_end_y = max(selected_top, min(selected_bottom, local_end_y_candidate))
                        verification_confidence = round(
                            (
                                float(match_metrics.get("score") or 0.0) * 0.30
                                + float(match_metrics.get("coverage") or 0.0) * 0.42
                                + float(match_metrics.get("boundary_coverage") or 0.0) * 0.28
                            )
                            * 100
                        )
                        refined_confidence = min(
                            coarse_by_id[location_id]["confidence"],
                            round(sum(line["confidence"] for line in selected_lines) / len(selected_lines)),
                            verification_confidence,
                        )
                        if refined_confidence < 60:
                            return location_id, None
                        crop_width = crop_spec["right"] - crop_spec["left"]
                        crop_height = crop_spec["bottom"] - crop_spec["top"]

                        def full_x(value: int) -> int:
                            return crop_spec["left"] + round(value * crop_width / 1000)

                        def full_y(value: int) -> int:
                            return crop_spec["top"] + round(value * crop_height / 1000)

                        refined_left = full_x(local_left)
                        refined_top = full_y(local_top)
                        refined_right = full_x(local_right)
                        refined_bottom = full_y(local_bottom)
                        return location_id, {
                            "left": max(0, refined_left),
                            "top": max(0, refined_top),
                            "right": min(1000, refined_right),
                            "bottom": min(1000, refined_bottom),
                            "start_x": full_x(local_start_x),
                            "start_y": full_y(local_start_y),
                            "end_x": full_x(local_end_x),
                            "end_y": full_y(local_end_y),
                            "confidence": refined_confidence,
                            "version": SOURCE_BBOX_VERSION,
                            "text_verified": True,
                            "match_score": round(float(match_metrics.get("score") or 0.0), 4),
                            "match_coverage": round(float(match_metrics.get("coverage") or 0.0), 4),
                            "boundary_coverage": round(
                                float(match_metrics.get("boundary_coverage") or 0.0),
                                4,
                            ),
                            "evidence_length": len(
                                _canonical_study_source_match_text(
                                    page_targets[location_id].get("evidence")
                                )
                            ),
                        }
                    except _StudyUploadCancelled:
                        raise
                    except (requests.RequestException, ValueError, TypeError, IndexError):
                        app.logger.exception(
                            "Study-note source line matching v5 failed for image %s target %s",
                            image_index,
                            location_id,
                        )
                        return location_id, None
                    finally:
                        if hasattr(study_upload_context, "cancel_event"):
                            del study_upload_context.cancel_event

                executor = ThreadPoolExecutor(
                    max_workers=min(4, len(crop_specs)),
                    thread_name_prefix="study-source-locator",
                )
                try:
                    futures = [executor.submit(refine_one_source, location_id) for location_id in crop_specs]
                    for future in as_completed(futures):
                        _raise_if_study_upload_cancelled()
                        location_id, refined = future.result()
                        if refined is not None:
                            refined_by_id[location_id] = refined
                except _StudyUploadCancelled:
                    for future in futures:
                        future.cancel()
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise
                else:
                    executor.shutdown(wait=True)

            seen_page: Set[str] = set()
            for location_id in coarse_by_id:
                model_location = refined_by_id.get(location_id)
                if model_location is None:
                    continue
                start_x = int(model_location["start_x"])
                start_y = int(model_location["start_y"])
                end_x = int(model_location["end_x"])
                end_y = int(model_location["end_y"])
                confidence = int(model_location["confidence"])
                location_version = int(model_location.get("version") or 3)
                candidate_seed = {
                    "left": max(0, min(int(model_location["left"]), start_x - 12, end_x - 12)),
                    "top": max(0, min(int(model_location["top"]), start_y - 12, end_y - 12)),
                    "right": min(1000, max(int(model_location["right"]), start_x + 12, end_x + 12)),
                    "bottom": min(1000, max(int(model_location["bottom"]), start_y + 12, end_y + 12)),
                    "confidence": confidence,
                    "version": location_version,
                    "text_verified": bool(model_location.get("text_verified")),
                    "match_score": model_location.get("match_score"),
                    "match_coverage": model_location.get("match_coverage"),
                    "boundary_coverage": model_location.get("boundary_coverage"),
                    "evidence_length": model_location.get("evidence_length"),
                }
                if _validated_study_source_bbox(candidate_seed) is None:
                    continue
                candidate = candidate_seed
                if _validated_study_source_bbox(candidate) is None:
                    continue
                requests_by_id[location_id]["bbox"] = candidate
                seen_page.add(location_id)
                located += 1

        if failed_pages == len(requests_by_image):
            raise ValueError("Source localization failed for every image")
        return located, total

    def _localize_study_card_sources_model_consensus_legacy(
        images: List[Tuple[str, bytes, str]],
        key_concepts: List[Dict[str, Any]],
    ) -> Tuple[int, int]:
        targets_by_image: Dict[int, List[Dict[str, Any]]] = {}
        for concept_index, concept in enumerate(key_concepts):
            if not isinstance(concept, dict):
                continue
            for source_ref_index, source_ref in enumerate(concept.get("source_refs") or []):
                if not isinstance(source_ref, dict):
                    continue
                source_ref.pop("bbox", None)
                try:
                    image_index = int(source_ref.get("image_index") or 0)
                except (TypeError, ValueError):
                    continue
                evidence = _literal_study_source_evidence(source_ref.get("evidence"))
                if not (1 <= image_index <= len(images)) or not evidence:
                    continue
                targets_by_image.setdefault(image_index, []).append(
                    {
                        "location_id": f"c{concept_index}r{source_ref_index}",
                        "concept": str(concept.get("concept") or "")[:100],
                        "card_context": " ".join(
                            str(concept.get(field) or "").strip()
                            for field in (
                                "core_summary",
                                "explanation",
                                "simple_example",
                                "example_problem",
                                "example_method",
                                "memory_hint",
                            )
                            if str(concept.get(field) or "").strip()
                        )[:900],
                        "evidence": evidence[:700],
                        "source_ref": source_ref,
                    }
                )
        total = sum(len(targets) for targets in targets_by_image.values())
        if not total:
            return 0, 0
        debug_localization = _env_flag_truthy(os.getenv("E3_SOURCE_LOCALIZATION_DEBUG"))

        def record_debug(target: Dict[str, Any], stage: str, **details: Any) -> None:
            if debug_localization:
                target["source_ref"]["_localization_debug"] = {
                    "stage": stage,
                    **json_safe(details),
                }

        location_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "found",
                "visible_excerpt",
                "left",
                "top",
                "right",
                "bottom",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "confidence",
            ],
            "properties": {
                "found": {"type": "boolean"},
                "visible_excerpt": {"type": "string", "maxLength": 900},
                "left": {"type": "integer", "minimum": 0, "maximum": 1000},
                "top": {"type": "integer", "minimum": 0, "maximum": 1000},
                "right": {"type": "integer", "minimum": 0, "maximum": 1000},
                "bottom": {"type": "integer", "minimum": 0, "maximum": 1000},
                "start_x": {"type": "integer", "minimum": 0, "maximum": 1000},
                "start_y": {"type": "integer", "minimum": 0, "maximum": 1000},
                "end_x": {"type": "integer", "minimum": 0, "maximum": 1000},
                "end_y": {"type": "integer", "minimum": 0, "maximum": 1000},
                "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
            },
        }
        crop_transcription_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["visible_text"],
            "properties": {
                "visible_text": {"type": "string", "maxLength": 2500},
            },
        }

        def agreement_between(
            first: Dict[str, Any],
            second: Dict[str, Any],
        ) -> Optional[Tuple[float, Dict[str, Any]]]:
            intersection_width = max(
                0,
                min(first["right"], second["right"]) - max(first["left"], second["left"]),
            )
            intersection_height = max(
                0,
                min(first["bottom"], second["bottom"]) - max(first["top"], second["top"]),
            )
            first_width = first["right"] - first["left"]
            first_height = first["bottom"] - first["top"]
            second_width = second["right"] - second["left"]
            second_height = second["bottom"] - second["top"]
            intersection_area = intersection_width * intersection_height
            smaller_area = min(first_width * first_height, second_width * second_height)
            smaller_overlap = intersection_area / max(1, smaller_area)
            horizontal_overlap = intersection_width / max(1, min(first_width, second_width))
            vertical_overlap = intersection_height / max(1, min(first_height, second_height))
            first_center = (
                (first["left"] + first["right"]) / 2,
                (first["top"] + first["bottom"]) / 2,
            )
            second_center = (
                (second["left"] + second["right"]) / 2,
                (second["top"] + second["bottom"]) / 2,
            )
            center_distance = math.hypot(
                (first_center[0] - second_center[0]) / max(80, first_width, second_width),
                (first_center[1] - second_center[1]) / max(55, first_height, second_height),
            )
            center_score = max(0.0, 1.0 - center_distance)
            anchor_y_tolerance = max(38, round(max(first_height, second_height) * 0.28))
            anchor_x_tolerance = max(70, round(max(first_width, second_width) * 0.38))
            anchor_differences = (
                abs(first["start_x"] - second["start_x"]),
                abs(first["start_y"] - second["start_y"]),
                abs(first["end_x"] - second["end_x"]),
                abs(first["end_y"] - second["end_y"]),
            )
            if (
                anchor_differences[0] > anchor_x_tolerance
                or anchor_differences[2] > anchor_x_tolerance
                or anchor_differences[1] > anchor_y_tolerance
                or anchor_differences[3] > anchor_y_tolerance
            ):
                return None
            anchor_score = max(
                0.0,
                1.0
                - (
                    anchor_differences[1] + anchor_differences[3]
                )
                / max(1, anchor_y_tolerance * 2),
            )
            agreement = (
                smaller_overlap * 0.38
                + vertical_overlap * 0.20
                + horizontal_overlap * 0.13
                + center_score * 0.11
                + anchor_score * 0.18
            )
            if (
                smaller_overlap < 0.52
                or vertical_overlap < 0.62
                or horizontal_overlap < 0.42
                or agreement < 0.64
            ):
                return None
            first_area = first_width * first_height
            second_area = second_width * second_height
            tighter = first if first_area <= second_area else second
            return agreement, tighter

        located = 0
        failed_pages = 0
        parent_cancel_event = getattr(study_upload_context, "cancel_event", None)
        for image_index, page_targets in sorted(targets_by_image.items()):
            _raise_if_study_upload_cancelled()
            filename, image_bytes, _mime_type = images[image_index - 1]
            try:
                with Image.open(io.BytesIO(image_bytes)) as opened:
                    clean_page = ImageOps.exif_transpose(opened).convert("RGB")
                clean_data_url = _study_image_data_url(clean_page)
                guide_data_url = _study_coordinate_guide_data_url(clean_page)
                page_width, page_height = clean_page.size
            except (OSError, ValueError, TypeError):
                failed_pages += 1
                app.logger.exception("Unable to prepare source image %s", image_index)
                continue

            def locate_one_target(
                target: Dict[str, Any],
            ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], bool]:
                if isinstance(parent_cancel_event, threading.Event):
                    study_upload_context.cancel_event = parent_cancel_event
                successful_response = False
                candidates: List[Dict[str, Any]] = []
                try:
                    for pass_index in range(2):
                        _raise_if_study_upload_cancelled()
                        prompt = (
                            f"你是手寫筆記的單一來源定位員。這次只定位一個項目，不得搜尋或輸出其他重點。"
                            f"圖片是第 {image_index} 張 {filename}，canonical bitmap 為 {page_width}×{page_height}。"
                            f"卡片標題：{target['concept']}。待找原文 evidence：{target['evidence']}。"
                            "先逐字確認 evidence 的開頭、公式關係與結尾都真的出現在同一個連續區塊；只看到相同關鍵詞、"
                            "相鄰例題或語意相關內容都不算。start_x/start_y 是 evidence 第一個可見字元或公式的中心，"
                            "end_x/end_y 是最後一個可見字元或公式的中心，兩者都不是矩形角落。visible_excerpt 必須逐字轉錄矩形內實際看見、且與 evidence"
                            "對應的完整文字，不可直接複製提示中的 evidence，不可摘要或補字。矩形只框該連續原文及其"
                            "必要公式，不含上一個標題、下一題、相鄰定義或大片空白。所有座標相對完整 canonical bitmap，"
                            "左上 (0,0)、右下 (1000,1000)，四周只留 5 至 10 單位。若無法同時確認首尾文字或位置不唯一，"
                            "found=false，所有矩形與首尾錨點座標填 0、visible_excerpt 留空、confidence 低於 60。"
                            f"這是第 {pass_index + 1} 次獨立定位，不得假設另一輪的答案。只輸出 schema JSON。"
                        )
                        if pass_index == 0:
                            content = [
                                {"type": "input_text", "text": prompt},
                                {"type": "input_image", "image_url": clean_data_url, "detail": "high"},
                                {"type": "input_text", "text": "同一張圖片的座標網格，只用來讀座標："},
                                {"type": "input_image", "image_url": guide_data_url, "detail": "high"},
                            ]
                        else:
                            content = [
                                {"type": "input_text", "text": prompt},
                                {"type": "input_text", "text": "先用網格確認區域，再回乾淨圖逐字核對："},
                                {"type": "input_image", "image_url": guide_data_url, "detail": "high"},
                                {"type": "input_image", "image_url": clean_data_url, "detail": "high"},
                            ]
                        try:
                            result = _call_openai_json(
                                name=f"study_recall_source_consensus_v12_{pass_index + 1}",
                                schema=location_schema,
                                content=content,
                                timeout=240,
                                reasoning_effort="low",
                                max_output_tokens=1800,
                            )
                            successful_response = True
                        except (requests.RequestException, ValueError, TypeError):
                            app.logger.exception(
                                "Independent source locator failed for image %s target %s pass %s",
                                image_index,
                                target["location_id"],
                                pass_index + 1,
                            )
                            continue
                        if not bool(result.get("found")):
                            continue
                        try:
                            candidate = {
                                "left": int(result.get("left")),
                                "top": int(result.get("top")),
                                "right": int(result.get("right")),
                                "bottom": int(result.get("bottom")),
                                "start_x": int(result.get("start_x")),
                                "start_y": int(result.get("start_y")),
                                "end_x": int(result.get("end_x")),
                                "end_y": int(result.get("end_y")),
                                "confidence": int(result.get("confidence")),
                            }
                        except (TypeError, ValueError):
                            continue
                        if candidate["confidence"] < 68 or _validated_study_source_bbox(
                            {**candidate, "version": 1}
                        ) is None:
                            continue
                        if not all(
                            0 <= candidate[key] <= 1000
                            for key in ("start_x", "start_y", "end_x", "end_y")
                        ):
                            continue
                        visible_excerpt = str(result.get("visible_excerpt") or "").strip()
                        selected, metrics = _match_study_source_evidence_to_lines(
                            target["evidence"],
                            [{"text": visible_excerpt}],
                        )
                        if not selected or not source_line_match_is_verified(target["evidence"], metrics):
                            continue
                        candidate["match_metrics"] = metrics
                        candidates.append(candidate)
                    if len(candidates) != 2:
                        return target, None, successful_response
                    consensus = agreement_between(candidates[0], candidates[1])
                    if consensus is None:
                        return target, None, successful_response
                    agreement, tighter = consensus
                    metrics = {
                        key: min(
                            float(candidates[0]["match_metrics"].get(key) or 0.0),
                            float(candidates[1]["match_metrics"].get(key) or 0.0),
                        )
                        for key in ("score", "coverage", "boundary_coverage")
                    }
                    confidence = min(
                        int(candidates[0]["confidence"]),
                        int(candidates[1]["confidence"]),
                        round(agreement * 100),
                        round(
                            (
                                metrics["score"] * 0.32
                                + metrics["coverage"] * 0.42
                                + metrics["boundary_coverage"] * 0.26
                            )
                            * 100
                        ),
                    )
                    if confidence < 64:
                        return target, None, successful_response
                    evidence_length = len(_canonical_study_source_match_text(target["evidence"]))
                    expected_lines = estimated_source_line_count(target["evidence"])
                    start_anchor_y = round(median(candidate["start_y"] for candidate in candidates))
                    end_anchor_y = round(median(candidate["end_y"] for candidate in candidates))
                    anchor_margin = min(46, 30 + max(0, expected_lines - 1) * 2)
                    candidate_seed = {
                        "left": tighter["left"],
                        "top": max(
                            tighter["top"],
                            min(start_anchor_y, end_anchor_y) - anchor_margin,
                        ),
                        "right": tighter["right"],
                        "bottom": min(
                            tighter["bottom"],
                            max(start_anchor_y, end_anchor_y) + anchor_margin,
                        ),
                        "confidence": confidence,
                        "version": SOURCE_BBOX_VERSION,
                    }
                    if (
                        _validated_study_source_bbox(
                            {**candidate_seed, "version": 1}
                        )
                        is None
                        or not source_bbox_span_is_plausible(
                            target["evidence"], candidate_seed
                        )
                    ):
                        return target, None, successful_response
                    crop_left = max(
                        0, math.floor(candidate_seed["left"] * page_width / 1000)
                    )
                    crop_top = max(
                        0, math.floor(candidate_seed["top"] * page_height / 1000)
                    )
                    crop_right = min(
                        page_width,
                        math.ceil(candidate_seed["right"] * page_width / 1000),
                    )
                    crop_bottom = min(
                        page_height,
                        math.ceil(candidate_seed["bottom"] * page_height / 1000),
                    )
                    if crop_right <= crop_left or crop_bottom <= crop_top:
                        return target, None, successful_response
                    crop_image = clean_page.crop(
                        (crop_left, crop_top, crop_right, crop_bottom)
                    )
                    try:
                        crop_result = _call_openai_json(
                            name="study_recall_source_crop_transcription_v12",
                            schema=crop_transcription_schema,
                            content=[
                                {
                                    "type": "input_text",
                                    "text": (
                                        "你只會看到一張從手寫筆記裁下的小圖，且不知道系統正在尋找什麼。"
                                        "請按由上到下、由左到右的順序，逐字轉錄裁切範圍內真正可見的所有文字、"
                                        "數字與公式。不得猜測裁切外內容，不得依學科常識補句，不得摘要、改寫或"
                                        "修正；被邊界切斷而無法辨識的字元以〔截斷〕表示。只輸出 schema JSON。"
                                    ),
                                },
                                {
                                    "type": "input_image",
                                    "image_url": _study_image_data_url(crop_image),
                                    "detail": "high",
                                },
                            ],
                            timeout=180,
                            reasoning_effort="low",
                            max_output_tokens=2200,
                        )
                    except (requests.RequestException, ValueError, TypeError):
                        app.logger.exception(
                            "Blind crop verification failed for image %s target %s",
                            image_index,
                            target["location_id"],
                        )
                        return target, None, successful_response
                    crop_visible_text = str(crop_result.get("visible_text") or "").strip()
                    crop_selected, crop_metrics = _match_study_source_evidence_to_lines(
                        target["evidence"],
                        [{"text": crop_visible_text}],
                    )
                    if not crop_selected or not source_line_match_is_verified(
                        target["evidence"], crop_metrics
                    ):
                        return target, None, successful_response
                    metrics = {
                        key: min(float(metrics.get(key) or 0.0), float(crop_metrics.get(key) or 0.0))
                        for key in ("score", "coverage", "boundary_coverage")
                    }
                    candidate = {
                        **candidate_seed,
                        "confidence": confidence,
                        "version": SOURCE_BBOX_VERSION,
                        "text_verified": True,
                        "match_score": round(metrics["score"], 4),
                        "match_coverage": round(metrics["coverage"], 4),
                        "boundary_coverage": round(metrics["boundary_coverage"], 4),
                        "evidence_length": evidence_length,
                        "coordinate_agreement": round(agreement, 4),
                        "expected_lines": expected_lines,
                        "span_verified": True,
                        "crop_verified": True,
                        "crop_match_score": round(float(crop_metrics.get("score") or 0.0), 4),
                        "crop_match_coverage": round(float(crop_metrics.get("coverage") or 0.0), 4),
                        "crop_boundary_coverage": round(
                            float(crop_metrics.get("boundary_coverage") or 0.0), 4
                        ),
                        "crop_match_precision": round(
                            float(crop_metrics.get("precision") or 0.0), 4
                        ),
                    }
                    if _validated_study_source_bbox(candidate) is None:
                        return target, None, successful_response
                    return target, candidate, successful_response
                finally:
                    if hasattr(study_upload_context, "cancel_event"):
                        del study_upload_context.cancel_event

            executor = ThreadPoolExecutor(
                max_workers=min(4, len(page_targets)),
                thread_name_prefix="study-source-consensus",
            )
            page_had_response = False
            try:
                futures = [executor.submit(locate_one_target, target) for target in page_targets]
                for future in as_completed(futures):
                    _raise_if_study_upload_cancelled()
                    target, candidate, successful_response = future.result()
                    page_had_response = page_had_response or successful_response
                    if candidate is None:
                        continue
                    target["source_ref"]["bbox"] = candidate
                    located += 1
            except _StudyUploadCancelled:
                for future in futures:
                    future.cancel()
                executor.shutdown(wait=False, cancel_futures=True)
                raise
            else:
                executor.shutdown(wait=True)
            if not page_had_response:
                failed_pages += 1

        if failed_pages == len(targets_by_image):
            raise ValueError("Source localization failed for every image")
        return located, total

    def _study_source_index_pages(source_pages: Any) -> List[Dict[str, Any]]:
        """Build page text from OCR that was independently cropped from each image."""
        indexed_pages: List[Dict[str, Any]] = []
        for page in source_pages or []:
            if not isinstance(page, dict):
                continue
            try:
                image_index = int(page.get("image_index") or 0)
            except (TypeError, ValueError):
                continue
            localization_index = page.get("localization_index")
            if (
                image_index <= 0
                or not isinstance(localization_index, dict)
                or int(localization_index.get("version") or 0)
                != SOURCE_PAGE_INDEX_VERSION
                or str(localization_index.get("kind") or "") != "sections"
                or not isinstance(localization_index.get("lines"), list)
            ):
                continue
            section_texts = [
                str(line.get("text") or "").strip()
                for line in localization_index["lines"]
                if isinstance(line, dict) and str(line.get("text") or "").strip()
            ]
            if section_texts:
                indexed_pages.append(
                    {
                        "image_index": image_index,
                        "transcription": "\n\n".join(section_texts),
                    }
                )
        return indexed_pages

    def _resolve_study_source_page(
        evidence: Any,
        source_pages: Any,
        *,
        preferred_image_index: Any = None,
        context: Any = "",
    ) -> Optional[Dict[str, Any]]:
        indexed_pages = _study_source_index_pages(source_pages)
        if indexed_pages:
            indexed_resolution = resolve_source_evidence_page(
                evidence,
                indexed_pages,
                preferred_image_index=preferred_image_index,
                context=context,
            )
            if indexed_resolution:
                return {**indexed_resolution, "page_match_source": "section_ocr"}
        transcription_resolution = resolve_source_evidence_page(
            evidence,
            source_pages or [],
            preferred_image_index=preferred_image_index,
            context=context,
        )
        if transcription_resolution:
            return {
                **transcription_resolution,
                "page_match_source": "page_transcription",
            }
        return None

    def _localize_study_card_sources(
        images: List[Tuple[str, bytes, str]],
        key_concepts: List[Dict[str, Any]],
        source_pages: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[int, int]:
        """Locate sources from image-derived geometry and target-free OCR text."""
        targets_by_image: Dict[int, List[Dict[str, Any]]] = {}
        for concept_index, concept in enumerate(key_concepts):
            if not isinstance(concept, dict):
                continue
            card_context = " ".join(
                str(concept.get(field) or "").strip()
                for field in (
                    "concept",
                    "topic",
                    "core_summary",
                    "explanation",
                    "example_problem",
                    "example_method",
                    "simple_example",
                )
                if str(concept.get(field) or "").strip()
            )[:1800]
            for source_ref_index, source_ref in enumerate(concept.get("source_refs") or []):
                if not isinstance(source_ref, dict):
                    continue
                source_ref.pop("bbox", None)
                try:
                    image_index = int(source_ref.get("image_index") or 0)
                except (TypeError, ValueError):
                    continue
                evidence = _literal_study_source_evidence(source_ref.get("evidence"))
                if not evidence:
                    continue
                page_resolution = (
                    _resolve_study_source_page(
                        evidence,
                        source_pages or [],
                        preferred_image_index=image_index,
                        context=card_context,
                    )
                    if source_pages
                    else {
                        "image_index": image_index,
                        "page_verified": 1 <= image_index <= len(images),
                        "match_kind": "legacy_assigned",
                        "match_margin": 0.0,
                    }
                )
                if not page_resolution:
                    continue
                image_index = int(page_resolution.get("image_index") or 0)
                if not (1 <= image_index <= len(images)):
                    continue
                source_ref["image_index"] = image_index
                targets_by_image.setdefault(image_index, []).append(
                    {
                        "location_id": f"c{concept_index}r{source_ref_index}",
                        "concept": str(concept.get("concept") or "")[:100],
                        "card_context": card_context,
                        "evidence": evidence[:700],
                        "source_ref": source_ref,
                        "page_resolution": page_resolution,
                    }
                )
        total = sum(len(targets) for targets in targets_by_image.values())
        if not total:
            return 0, 0
        debug_localization = _env_flag_truthy(os.getenv("E3_SOURCE_LOCALIZATION_DEBUG"))

        def record_debug(target: Dict[str, Any], stage: str, **details: Any) -> None:
            if debug_localization:
                target["source_ref"]["_localization_debug"] = {
                    "stage": stage,
                    **json_safe(details),
                }

        pages_by_index: Dict[int, Dict[str, Any]] = {}
        for page in source_pages or []:
            if not isinstance(page, dict):
                continue
            try:
                image_index = int(page.get("image_index") or 0)
            except (TypeError, ValueError):
                continue
            if image_index > 0:
                pages_by_index[image_index] = page

        crop_batch_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["crops"],
            "properties": {
                "crops": {
                    "type": "array",
                    "maxItems": 8,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["crop_id", "visible_text", "confidence"],
                        "properties": {
                            "crop_id": {"type": "string", "maxLength": 40},
                            "visible_text": {"type": "string", "maxLength": 3200},
                            "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
                        },
                    },
                }
            },
        }
        section_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["separator_ids"],
            "properties": {
                "separator_ids": {
                    "type": "array",
                    "maxItems": 30,
                    "items": {"type": "string", "maxLength": 8},
                }
            },
        }
        reanchor_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["matches"],
            "properties": {
                "matches": {
                    "type": "array",
                    "maxItems": 8,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": [
                            "location_id",
                            "section_id",
                            "anchor",
                            "confidence",
                        ],
                        "properties": {
                            "location_id": {"type": "string", "maxLength": 40},
                            "section_id": {"type": "integer", "minimum": 0, "maximum": 80},
                            "anchor": {"type": "string", "maxLength": 420},
                            "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
                        },
                    },
                }
            },
        }

        def fallback_page_sections(page: Image.Image) -> List[Dict[str, Any]]:
            bounds = estimate_source_page_content_bounds(page)
            return [
                {
                    "line_id": 1,
                    **bounds,
                    "geometry_confidence": 0.62,
                    "text": "",
                }
            ]

        def separator_marker_image(
            page: Image.Image,
            candidates: List[Dict[str, Any]],
        ) -> Image.Image:
            page = page.convert("RGB")
            margin_width = max(150, round(page.width * 0.14))
            marked = Image.new("RGB", (page.width + margin_width, page.height), "white")
            marked.paste(page, (0, 0))
            draw = ImageDraw.Draw(marked)
            ruler_left = page.width + max(8, round(margin_width * 0.08))
            draw.line(
                (ruler_left, 0, ruler_left, page.height),
                fill=(125, 125, 125),
                width=max(1, round(page.width * 0.0015)),
            )
            try:
                font = ImageFont.load_default(size=max(12, round(page.width * 0.018)))
            except TypeError:
                font = ImageFont.load_default()
            for candidate in candidates:
                pixel_y = round(int(candidate["y"]) * page.height / 1000)
                draw.line(
                    (ruler_left, pixel_y, ruler_left + round(margin_width * 0.32), pixel_y),
                    fill=(220, 20, 105),
                    width=max(2, round(page.width * 0.0025)),
                )
                draw.text(
                    (ruler_left + round(margin_width * 0.38), max(0, pixel_y - 10)),
                    str(candidate["separator_id"]),
                    fill=(180, 0, 80),
                    font=font,
                )
            return marked

        def segment_page_sections(
            page: Image.Image,
            *,
            image_index: int,
            filename: str,
        ) -> Tuple[List[Dict[str, Any]], bool]:
            content_bounds = estimate_source_page_content_bounds(page)
            separator_candidates = detect_source_horizontal_separator_candidates(page)
            if not separator_candidates:
                return fallback_page_sections(page), True
            marked_page = separator_marker_image(page, separator_candidates)
            prompt = (
                "你是筆記區塊分隔線分類員，完全不知道之後要搜尋的卡片內容。原圖右側新增白色標尺欄，"
                "洋紅色短刻度與 S 編號只用來指出像素演算法找到的候選高度，不是筆記內容。請選出真正把上下兩個"
                "筆記主題、題目或觀念區塊分開的候選線編號。橫跨主要筆記寬度的手繪虛線或實線可選；"
                "大型矩形內容框的下緣若確實把框內內容與下方內容分開，也可選。短底線、公式分數線、矩陣線、"
                "刪除線、照片或工具列邊框、文字筆畫都不可選。沒有真正區塊分隔線時輸出空陣列。"
                "只可輸出圖上存在的 S 編號，不可自行估算座標。"
                f"這是第 {image_index} 張 {filename}。只輸出 schema JSON。"
            )
            try:
                result = _call_openai_json(
                    name="study_source_page_sections_v15",
                    schema=section_schema,
                    content=[
                        {"type": "input_text", "text": prompt},
                        {
                            "type": "input_image",
                            "image_url": _study_image_data_url(marked_page),
                            "detail": "high",
                        },
                    ],
                    timeout=240,
                    reasoning_effort="low",
                    max_output_tokens=3200,
                )
                had_response = True
            except (requests.RequestException, ValueError, TypeError):
                app.logger.exception("Study-note page section segmentation failed for image %s", image_index)
                # Continue with high-confidence image-derived separator rules.
                # This keeps section geometry available during API rate limits.
                result = {"separator_ids": []}
                had_response = False
            content_left = max(20, int(content_bounds["left"]))
            content_top = max(0, int(content_bounds["top"]))
            content_right = min(980, int(content_bounds["right"]))
            content_bottom = min(1000, int(content_bounds["bottom"]))
            if content_right - content_left < 80 or content_bottom - content_top < 25:
                return fallback_page_sections(page), had_response
            candidates_by_id = {
                str(candidate["separator_id"]): candidate
                for candidate in separator_candidates
            }
            selected_ids = {
                str(separator_id).strip().upper()
                for separator_id in result.get("separator_ids") or []
            }
            selected_ids.update(
                str(candidate["separator_id"])
                for candidate in separator_candidates
                if float(candidate.get("full_span") or 0.0) >= 0.45
                and (
                    float(candidate.get("full_coverage") or 0.0)
                    / max(1, int(candidate.get("run_count") or 0))
                ) >= 0.018
                and float(candidate.get("full_longest_run") or 0.0) <= 0.065
                and int(candidate.get("run_count") or 0) >= 8
                and int(candidate.get("thickness") or 999) <= 18
                and float(candidate.get("context_density") or 1.0) <= 0.13
                and float(candidate.get("separator_likelihood") or 0.0) >= 0.50
            )
            selected_ids.update(
                str(candidate["separator_id"])
                for candidate in separator_candidates
                if float(candidate.get("full_span") or 0.0) >= 0.40
                and (
                    float(candidate.get("full_coverage") or 0.0)
                    / max(1, int(candidate.get("run_count") or 0))
                ) >= 0.018
                and float(candidate.get("full_longest_run") or 0.0) <= 0.06
                and int(candidate.get("run_count") or 0) >= 8
                and int(candidate.get("thickness") or 999) <= 12
                and float(candidate.get("context_density") or 1.0) <= 0.05
                and float(candidate.get("separator_likelihood") or 0.0) >= 0.42
            )
            selected_ids.update(
                str(candidate["separator_id"])
                for candidate in separator_candidates
                if float(candidate.get("full_span") or 0.0) >= 0.70
                and float(candidate.get("full_coverage") or 0.0) >= 0.18
                and float(candidate.get("full_longest_run") or 0.0) <= 0.09
                and int(candidate.get("run_count") or 0) >= 8
                and int(candidate.get("thickness") or 999) <= 18
                and float(candidate.get("separator_likelihood") or 0.0) >= 0.42
            )
            selected_ids.update(
                str(candidate["separator_id"])
                for candidate in separator_candidates
                if 0.32 <= float(candidate.get("full_span") or 0.0) < 0.43
                and float(candidate.get("full_coverage") or 0.0) >= 0.15
                and (
                    float(candidate.get("full_coverage") or 0.0)
                    / max(1, int(candidate.get("run_count") or 0))
                ) >= 0.019
                and float(candidate.get("full_longest_run") or 0.0) <= 0.04
                and int(candidate.get("run_count") or 0) >= 8
                and int(candidate.get("thickness") or 999) <= 6
                and float(candidate.get("context_density") or 1.0) <= 0.04
                and float(candidate.get("separator_likelihood") or 0.0) >= 0.33
            )
            selected_ids.update(
                str(candidate["separator_id"])
                for candidate in separator_candidates
                if float(candidate.get("full_span") or 0.0) >= 0.80
                and float(candidate.get("full_coverage") or 0.0) >= 0.70
                and float(candidate.get("full_longest_run") or 0.0) >= 0.55
                and int(candidate.get("run_count") or 99) <= 4
                and float(candidate.get("separator_likelihood") or 0.0) >= 0.55
            )
            selected_candidates = sorted(
                (
                    candidate
                    for separator_id, candidate in candidates_by_id.items()
                    if separator_id in selected_ids
                    and content_top + 20 < int(candidate["y"]) < content_bottom - 20
                ),
                key=lambda candidate: int(candidate["y"]),
            )
            snapped_candidates: List[Dict[str, Any]] = []
            used_separator_ids: Set[str] = set()
            for selected_candidate in selected_candidates:
                selected_y = int(selected_candidate["y"])
                nearby = [
                    candidate
                    for candidate in separator_candidates
                    if str(candidate["separator_id"]) not in used_separator_ids
                    and abs(int(candidate["y"]) - selected_y) <= 110
                    and content_top + 20 < int(candidate["y"]) < content_bottom - 20
                ]
                best_nearby = max(
                    nearby or [selected_candidate],
                    key=lambda candidate: (
                        float(candidate.get("separator_likelihood") or 0.0),
                        -abs(int(candidate["y"]) - selected_y),
                    ),
                )
                selected_likelihood = float(
                    selected_candidate.get("separator_likelihood") or 0.0
                )
                snapped = (
                    best_nearby
                    if float(best_nearby.get("separator_likelihood") or 0.0)
                    >= selected_likelihood + 0.05
                    else selected_candidate
                )
                if (
                    float(snapped.get("context_density") or 0.0) > 0.075
                    and int(snapped.get("thickness") or 0) > 20
                    and float(snapped.get("full_longest_run") or 0.0) > 0.09
                    and not (
                        float(snapped.get("full_span") or 0.0) >= 0.80
                        and float(snapped.get("full_coverage") or 0.0) >= 0.70
                    )
                ):
                    continue
                if float(snapped.get("separator_likelihood") or 0.0) < 0.30:
                    continue
                if snapped_candidates and int(snapped["y"]) - int(snapped_candidates[-1]["y"]) < 24:
                    previous = snapped_candidates[-1]
                    if float(snapped.get("separator_likelihood") or 0.0) > float(
                        previous.get("separator_likelihood") or 0.0
                    ):
                        used_separator_ids.discard(str(previous["separator_id"]))
                        snapped_candidates[-1] = snapped
                        used_separator_ids.add(str(snapped["separator_id"]))
                    continue
                snapped_candidates.append(snapped)
                used_separator_ids.add(str(snapped["separator_id"]))
            boundaries = [
                (
                    int(candidate["y"]),
                    round(74 + min(0.22, float(candidate["score"])) * 100),
                )
                for candidate in snapped_candidates
            ]

            sections: List[Dict[str, Any]] = []
            section_top = content_top
            for y, confidence in [*boundaries, (content_bottom, 85)]:
                section_bottom = content_bottom if y == content_bottom else max(section_top + 25, y - 5)
                if section_bottom - section_top < 25:
                    section_top = min(content_bottom, y + 5)
                    continue
                sections.append(
                    {
                        "line_id": len(sections) + 1,
                        "left": content_left,
                        "top": section_top,
                        "right": content_right,
                        "bottom": section_bottom,
                        "geometry_confidence": round(confidence / 100, 4),
                        "text": "",
                    }
                )
                section_top = min(content_bottom, y + 5)
            return (sections or fallback_page_sections(page)), had_response

        def crop_from_bbox(
            page: Image.Image,
            bbox: Dict[str, Any],
            *,
            context_ratio: float,
        ) -> Optional[Image.Image]:
            try:
                left = int(bbox["left"])
                top = int(bbox["top"])
                right = int(bbox["right"])
                bottom = int(bbox["bottom"])
            except (KeyError, TypeError, ValueError):
                return None
            pixel_left = math.floor(left * page.width / 1000)
            pixel_top = math.floor(top * page.height / 1000)
            pixel_right = math.ceil(right * page.width / 1000)
            pixel_bottom = math.ceil(bottom * page.height / 1000)
            vertical_padding = max(5, round((pixel_bottom - pixel_top) * context_ratio))
            horizontal_padding = max(7, round(page.width * 0.007))
            pixel_left = max(0, pixel_left - horizontal_padding)
            pixel_right = min(page.width, pixel_right + horizontal_padding)
            pixel_top = max(0, pixel_top - vertical_padding)
            pixel_bottom = min(page.height, pixel_bottom + vertical_padding)
            if pixel_right - pixel_left < 12 or pixel_bottom - pixel_top < 8:
                return None
            crop = page.crop((pixel_left, pixel_top, pixel_right, pixel_bottom)).convert("RGB")
            if crop.height < 150:
                scale = min(3.2, 150 / max(1, crop.height))
                crop = crop.resize(
                    (max(1, round(crop.width * scale)), max(1, round(crop.height * scale))),
                    Image.Resampling.LANCZOS,
                )
            return crop

        def transcribe_crops(
            crop_specs: List[Tuple[str, Image.Image]],
            *,
            purpose: str,
        ) -> Tuple[Dict[str, Dict[str, Any]], bool]:
            transcribed: Dict[str, Dict[str, Any]] = {}
            had_response = False
            pending_specs = list(crop_specs)
            for attempt in range(2):
                if not pending_specs:
                    break
                for batch_start in range(0, len(pending_specs), 8):
                    _raise_if_study_upload_cancelled()
                    batch = pending_specs[batch_start : batch_start + 8]
                    content: List[Dict[str, Any]] = [
                        {
                            "type": "input_text",
                            "text": (
                                "你是手寫筆記裁切圖的逐字轉錄員。你不知道系統之後要找哪張卡，也不會看到任何待搜尋文字。"
                                "下方每個 crop_id 後緊接該裁切圖；請逐一按圖中自然閱讀順序轉錄真正可見的文字、數字與公式。"
                                "公式請盡量用 LaTeX，保留 =、不等號、箭頭、上下標、矩陣列與運算次序；不得依學科常識修正、"
                                "摘要或補入裁切外文字。無法辨識才使用〔不清楚〕，空白或純分隔線則 visible_text 留空。"
                                "每個輸入 crop_id 必須恰好輸出一次，且不得交換代碼。只輸出 schema JSON。"
                            ),
                        }
                    ]
                    for crop_id, crop in batch:
                        content.append({"type": "input_text", "text": f"crop_id={crop_id}"})
                        content.append(
                            {
                                "type": "input_image",
                                "image_url": _study_image_data_url(crop, max_side=2200),
                                "detail": "high",
                            }
                        )
                    try:
                        result = _call_openai_json(
                            name=f"study_source_section_ocr_{purpose}_v14_retry_{attempt}",
                            schema=crop_batch_schema,
                            content=content,
                            timeout=240,
                            reasoning_effort="low",
                            max_output_tokens=5200,
                        )
                        had_response = True
                    except (requests.RequestException, ValueError, TypeError):
                        app.logger.exception("Target-free source crop OCR failed for %s", purpose)
                        continue
                    expected_ids = {crop_id for crop_id, _crop in batch}
                    for item in result.get("crops") or []:
                        if not isinstance(item, dict):
                            continue
                        crop_id = str(item.get("crop_id") or "").strip()
                        if crop_id not in expected_ids or crop_id in transcribed:
                            continue
                        try:
                            confidence = max(0, min(100, int(item.get("confidence") or 0)))
                        except (TypeError, ValueError):
                            confidence = 0
                        candidate_transcription = {
                            "text": str(item.get("visible_text") or "").strip()[:3200],
                            "confidence": confidence,
                        }
                        if confidence >= 40 or attempt == 1:
                            transcribed[crop_id] = candidate_transcription
                pending_specs = [
                    spec for spec in pending_specs if spec[0] not in transcribed
                ]
            return transcribed, had_response

        def reanchor_targets_to_sections(
            targets: List[Dict[str, Any]],
            lines: List[Dict[str, Any]],
            *,
            image_index: int,
            _retry: bool = False,
        ) -> Dict[str, Dict[str, Any]]:
            """Repair legacy descriptive evidence using literal section OCR text."""
            if not targets or not lines:
                return {}
            lines_by_id = {
                int(line.get("line_id") or 0): line
                for line in lines
                if isinstance(line, dict)
                and int(line.get("line_id") or 0) > 0
                and str(line.get("text") or "").strip()
            }
            if not lines_by_id:
                return {}
            section_catalog = [
                {
                    "section_id": line_id,
                    "visible_text": str(line.get("text") or "")[:3200],
                }
                for line_id, line in sorted(lines_by_id.items())
            ]
            resolved: Dict[str, Dict[str, Any]] = {}
            for batch_start in range(0, len(targets), 8):
                _raise_if_study_upload_cancelled()
                batch = targets[batch_start : batch_start + 8]
                target_catalog = [
                    {
                        "location_id": str(target["location_id"]),
                        "concept": str(target.get("concept") or "")[:100],
                        "card_content": str(target.get("card_context") or "")[:900],
                        "legacy_evidence": str(target.get("evidence") or "")[:700],
                    }
                    for target in batch
                ]
                prompt = (
                    "你是舊筆記來源錨點修復員。section_catalog 是已按原圖方格逐字 OCR 的文字，"
                    "target_catalog 是卡片與舊來源描述。對每個 target，只能在某一個 section 的 visible_text "
                    "確實直接支持該卡片時配對；不確定就不要輸出該 target。section_id=0 表示不配對，但不要為它"
                    "編造 anchor。anchor 必須從所選 visible_text 逐字連續複製 12 至 220 個字元，保留公式、數字與"
                    "運算符，不可摘要、改寫、修正或拼接兩段。優先複製能唯一識別觀念或公式的最短完整片段。"
                    "同一 target 最多一筆；只輸出 schema JSON。\n"
                    + (
                        "這是第二次核對。上一輪未找到可靠錨點，請逐一重新檢查所有 section；仍不確定就省略。\n"
                        if _retry
                        else ""
                    )
                    + f"image_index={image_index}\n"
                    + "section_catalog="
                    + json.dumps(section_catalog, ensure_ascii=False, separators=(",", ":"))
                    + "\ntarget_catalog="
                    + json.dumps(target_catalog, ensure_ascii=False, separators=(",", ":"))
                )
                try:
                    result = _call_openai_json(
                        name=(
                            "study_source_legacy_reanchor_v1_retry"
                            if _retry
                            else "study_source_legacy_reanchor_v1"
                        ),
                        schema=reanchor_schema,
                        content=[{"type": "input_text", "text": prompt}],
                        timeout=180,
                        reasoning_effort="low",
                        max_output_tokens=3600,
                    )
                except (requests.RequestException, ValueError, TypeError):
                    app.logger.exception(
                        "Legacy source re-anchoring failed for image %s",
                        image_index,
                    )
                    continue
                targets_by_id = {
                    str(target["location_id"]): target for target in batch
                }
                for item in result.get("matches") or []:
                    if not isinstance(item, dict):
                        continue
                    location_id = str(item.get("location_id") or "").strip()
                    target = targets_by_id.get(location_id)
                    if target is None or location_id in resolved:
                        continue
                    try:
                        section_id = int(item.get("section_id") or 0)
                        model_confidence = int(item.get("confidence") or 0)
                    except (TypeError, ValueError):
                        continue
                    line = lines_by_id.get(section_id)
                    anchor = _literal_study_source_evidence(item.get("anchor"))
                    canonical_anchor = _canonical_study_source_match_text(anchor)
                    canonical_line = _canonical_study_source_match_text(
                        (line or {}).get("text")
                    )
                    if (
                        line is None
                        or model_confidence < 70
                        or len(canonical_anchor) < 8
                        or canonical_anchor not in canonical_line
                    ):
                        continue
                    anchor_lines, anchor_metrics = match_source_evidence_to_sections(
                        anchor,
                        lines,
                    )
                    if (
                        not anchor_lines
                        or int(anchor_lines[0].get("line_id") or 0) != section_id
                        or not source_section_match_is_verified(anchor, anchor_metrics)
                        or float(anchor_metrics.get("uniqueness") or 0.0) < 0.24
                    ):
                        continue
                    _original_lines, original_metrics = match_source_evidence_to_sections(
                        target["evidence"],
                        [line],
                    )
                    _concept_lines, concept_metrics = match_source_evidence_to_sections(
                        target.get("concept") or "",
                        [line],
                    )
                    _context_lines, context_metrics = match_source_evidence_to_sections(
                        target.get("card_context") or "",
                        [line],
                    )
                    semantic_score = max(
                        float(original_metrics.get("score") or 0.0),
                        float(concept_metrics.get("score") or 0.0),
                        float(context_metrics.get("score") or 0.0),
                    )
                    semantic_coverage = max(
                        float(original_metrics.get("coverage") or 0.0),
                        float(concept_metrics.get("coverage") or 0.0),
                        float(context_metrics.get("coverage") or 0.0),
                    )
                    semantic_formula = float(
                        original_metrics.get("formula_coverage") or 0.0
                    )
                    if (
                        semantic_score < 0.24
                        and semantic_coverage < 0.18
                        and semantic_formula < 0.40
                    ):
                        continue
                    resolved[location_id] = {
                        "line": line,
                        "anchor": anchor,
                        "metrics": anchor_metrics,
                        "model_confidence": model_confidence,
                        "semantic_score": semantic_score,
                    }
            if not _retry:
                still_unresolved = [
                    target
                    for target in targets
                    if str(target["location_id"]) not in resolved
                ]
                if still_unresolved:
                    resolved.update(
                        reanchor_targets_to_sections(
                            still_unresolved,
                            lines,
                            image_index=image_index,
                            _retry=True,
                        )
                    )
            return resolved

        located = 0
        successful_page_ocr = False
        used_cached_index = False
        used_transcription_fallback = False
        parent_cancel_event = getattr(study_upload_context, "cancel_event", None)
        for image_index, page_targets in sorted(targets_by_image.items()):
            _raise_if_study_upload_cancelled()
            filename, image_bytes, _mime_type = images[image_index - 1]
            try:
                with Image.open(io.BytesIO(image_bytes)) as opened:
                    clean_page = ImageOps.exif_transpose(opened).convert("RGB")
            except (OSError, ValueError, TypeError):
                app.logger.exception("Unable to prepare source image %s", image_index)
                continue
            page_digest = hashlib.sha256(image_bytes).hexdigest()
            source_page = pages_by_index.get(image_index)
            page_transcription = str((source_page or {}).get("transcription") or "")
            transcription_isolated = (
                str((source_page or {}).get("transcription_mode") or "")
                == "isolated_v1"
            )
            cached_index = source_page.get("localization_index") if source_page else None
            lines: List[Dict[str, Any]] = []
            if (
                isinstance(cached_index, dict)
                and int(cached_index.get("version") or 0) == SOURCE_PAGE_INDEX_VERSION
                and str(cached_index.get("kind") or "") == "sections"
                and str(cached_index.get("image_sha256") or "") == page_digest
                and isinstance(cached_index.get("lines"), list)
            ):
                for cached_line in cached_index["lines"]:
                    if not isinstance(cached_line, dict):
                        continue
                    try:
                        line = {
                            "line_id": int(cached_line.get("line_id") or len(lines) + 1),
                            "left": int(cached_line["left"]),
                            "top": int(cached_line["top"]),
                            "right": int(cached_line["right"]),
                            "bottom": int(cached_line["bottom"]),
                            "geometry_confidence": float(cached_line.get("geometry_confidence") or 0.0),
                            "ocr_confidence": int(cached_line.get("ocr_confidence") or 0),
                            "text": str(cached_line.get("text") or "")[:3200],
                            "transcription_fallback": bool(
                                cached_line.get("transcription_fallback")
                            ),
                            "transcription_isolated": bool(
                                cached_line.get("transcription_isolated")
                            ),
                        }
                    except (KeyError, TypeError, ValueError):
                        continue
                    if line["text"] and 0 <= line["left"] < line["right"] <= 1000 and 0 <= line["top"] < line["bottom"] <= 1000:
                        lines.append(line)
                used_cached_index = used_cached_index or bool(lines)

            if not lines:
                geometry, segmentation_had_response = segment_page_sections(
                    clean_page,
                    image_index=image_index,
                    filename=filename,
                )
                successful_page_ocr = successful_page_ocr or segmentation_had_response
                line_crops: List[Tuple[str, Image.Image]] = []
                for line in geometry:
                    crop = crop_from_bbox(clean_page, line, context_ratio=0.01)
                    if crop is not None:
                        line_crops.append((f"L{int(line['line_id']):03d}", crop))
                line_texts, had_response = transcribe_crops(
                    line_crops,
                    purpose=f"page_{image_index}_sections",
                )
                successful_page_ocr = successful_page_ocr or had_response
                for line in geometry:
                    crop_id = f"L{int(line['line_id']):03d}"
                    result = line_texts.get(crop_id) or {}
                    text = str(result.get("text") or "").strip()
                    if not text:
                        continue
                    lines.append(
                        {
                            **line,
                            "text": text[:3200],
                            "ocr_confidence": int(result.get("confidence") or 0),
                        }
                    )
                if (
                    transcription_isolated
                    and page_transcription
                    and len(lines) < len(geometry)
                ):
                    fallback_lines = assign_transcription_to_source_sections(
                        page_transcription,
                        geometry,
                    )
                    if fallback_lines:
                        lines = fallback_lines
                        used_transcription_fallback = True
                if source_page is not None:
                    source_page["localization_index"] = {
                        "version": SOURCE_PAGE_INDEX_VERSION,
                        "kind": "sections",
                        "bbox_version": SOURCE_BBOX_VERSION,
                        "image_sha256": page_digest,
                        "image_width": clean_page.width,
                        "image_height": clean_page.height,
                        "lines": lines,
                    }
            if not lines:
                continue

            candidates: List[Dict[str, Any]] = []
            unresolved_targets: List[Dict[str, Any]] = []

            def append_candidate(
                target: Dict[str, Any],
                selected_lines: List[Dict[str, Any]],
                metrics: Dict[str, Any],
                *,
                alignment_fallback: bool = False,
                verification_evidence: Optional[str] = None,
                reanchored: bool = False,
                anchor_model_confidence: int = 0,
            ) -> bool:
                candidate_bbox = source_bbox_from_lines(selected_lines)
                if candidate_bbox is None:
                    return False
                candidate_bbox = {
                    "left": max(20, int(candidate_bbox["left"])),
                    "top": max(0, int(candidate_bbox["top"])),
                    "right": min(980, int(candidate_bbox["right"])),
                    "bottom": min(1000, int(candidate_bbox["bottom"])),
                }
                if (
                    candidate_bbox["right"] - candidate_bbox["left"] < 80
                    or candidate_bbox["bottom"] - candidate_bbox["top"] < 25
                ):
                    return False
                crop = crop_from_bbox(clean_page, candidate_bbox, context_ratio=0.0)
                if crop is None:
                    return False
                candidates.append(
                    {
                        "target": target,
                        "selected_lines": selected_lines,
                        "metrics": metrics,
                        "bbox": candidate_bbox,
                        "crop": crop,
                        "alignment_fallback": alignment_fallback,
                        "verification_evidence": (
                            verification_evidence or target["evidence"]
                        ),
                        "reanchored": reanchored,
                        "anchor_model_confidence": anchor_model_confidence,
                        "transcription_fallback": bool(selected_lines)
                        and all(
                            bool(line.get("transcription_fallback"))
                            and bool(line.get("transcription_isolated"))
                            for line in selected_lines
                        ),
                    }
                )
                return True

            for target in page_targets:
                direct_lines, direct_metrics = match_source_evidence_to_sections(
                    target["evidence"],
                    lines,
                )
                direct_candidate = bool(direct_lines) and source_section_match_is_candidate(
                    target["evidence"], direct_metrics
                )
                alignment_lines, alignment_metrics = (
                    match_source_evidence_via_page_alignment(
                        target["evidence"],
                        page_transcription,
                        lines,
                    )
                    if page_transcription
                    else ([], {})
                )
                alignment_candidate = bool(
                    alignment_lines
                ) and source_page_alignment_match_is_candidate(
                    target["evidence"], alignment_metrics
                )
                selected_lines: List[Dict[str, Any]] = []
                metrics: Dict[str, Any] = {}
                alignment_fallback = False
                if direct_candidate:
                    selected_lines = direct_lines
                    metrics = direct_metrics
                    if (
                        alignment_candidate
                        and int(alignment_lines[0].get("line_id") or 0)
                        == int(direct_lines[0].get("line_id") or 0)
                    ):
                        metrics = {**direct_metrics, **alignment_metrics}
                elif alignment_candidate:
                    selected_lines = alignment_lines
                    metrics = alignment_metrics
                    alignment_fallback = True
                else:
                    unresolved_targets.append(
                        {
                            "target": target,
                            "direct_metrics": direct_metrics,
                            "alignment_metrics": alignment_metrics,
                        }
                    )
                    continue
                if not append_candidate(
                    target,
                    selected_lines,
                    metrics,
                    alignment_fallback=alignment_fallback,
                ):
                    record_debug(target, "candidate_geometry_rejected")

            if unresolved_targets:
                repairs = reanchor_targets_to_sections(
                    [item["target"] for item in unresolved_targets],
                    lines,
                    image_index=image_index,
                )
                for unresolved in unresolved_targets:
                    target = unresolved["target"]
                    repair = repairs.get(str(target["location_id"]))
                    if repair is None or not append_candidate(
                        target,
                        [repair["line"]],
                        repair["metrics"],
                        verification_evidence=str(repair["anchor"]),
                        reanchored=True,
                        anchor_model_confidence=int(
                            repair.get("model_confidence") or 0
                        ),
                    ):
                        record_debug(
                            target,
                            "candidate_match_rejected",
                            direct_metrics=unresolved["direct_metrics"],
                            alignment_metrics=unresolved["alignment_metrics"],
                            reanchor_attempted=True,
                        )

            verification_crops: Dict[Tuple[int, int, int, int], Tuple[str, Image.Image]] = {}
            for candidate in candidates:
                bbox = candidate["bbox"]
                bbox_key = (
                    int(bbox["left"]),
                    int(bbox["top"]),
                    int(bbox["right"]),
                    int(bbox["bottom"]),
                )
                verification_id = f"S{len(verification_crops) + 1:03d}"
                if bbox_key not in verification_crops:
                    verification_crops[bbox_key] = (verification_id, candidate["crop"])
                candidate["verification_id"] = verification_crops[bbox_key][0]
            verification_specs = list(verification_crops.values())
            crop_texts, crop_had_response = transcribe_crops(
                verification_specs,
                purpose=f"page_{image_index}_verify",
            )
            successful_page_ocr = successful_page_ocr or crop_had_response
            for candidate in candidates:
                target = candidate["target"]
                verification_evidence = str(
                    candidate.get("verification_evidence") or target["evidence"]
                )
                crop_result = crop_texts.get(str(candidate["verification_id"])) or {}
                crop_visible_text = str(crop_result.get("text") or "").strip()
                transcription_fallback_verified = False
                if not crop_visible_text:
                    fallback_metrics = candidate["metrics"]
                    transcription_fallback_verified = bool(
                        candidate.get("transcription_fallback")
                        and not candidate.get("alignment_fallback")
                        and target.get("page_resolution", {}).get("page_verified")
                        and source_section_match_is_verified(
                            verification_evidence,
                            fallback_metrics,
                        )
                        and float(fallback_metrics.get("uniqueness") or 0.0)
                        >= 0.20
                    )
                    if not transcription_fallback_verified:
                        record_debug(target, "verification_ocr_missing")
                        continue
                    crop_visible_text = "\n".join(
                        str(line.get("text") or "")
                        for line in candidate["selected_lines"]
                    )
                    crop_result = {"confidence": 72}
                crop_selected, crop_metrics = match_source_evidence_to_sections(
                    verification_evidence,
                    [{"text": crop_visible_text}],
                )
                crop_direct_verified = bool(
                    crop_selected
                ) and source_section_match_is_verified(
                    verification_evidence, crop_metrics
                )
                index_alignment_metrics = candidate["metrics"]
                has_index_alignment = (
                    float(index_alignment_metrics.get("alignment_score") or 0.0) > 0.0
                    and source_page_alignment_match_is_candidate(
                        verification_evidence, index_alignment_metrics
                    )
                )
                crop_alignment_metrics: Dict[str, Any] = {}
                crop_alignment_verified = False
                if has_index_alignment and page_transcription:
                    source_start = int(
                        index_alignment_metrics.get("alignment_source_start") or 0
                    )
                    source_end = int(
                        index_alignment_metrics.get("alignment_source_end") or 0
                    )
                    _crop_alignment_lines, crop_alignment_metrics = (
                        match_source_evidence_via_page_alignment(
                            verification_evidence,
                            page_transcription,
                            [{"line_id": 1, "text": crop_visible_text}],
                            expected_source_span=(source_start, source_end),
                        )
                    )
                    crop_alignment_verified = source_page_alignment_match_is_verified(
                        verification_evidence, crop_alignment_metrics
                    )
                verification_passed = (
                    crop_alignment_verified
                    if candidate.get("alignment_fallback")
                    else crop_direct_verified or crop_alignment_verified
                )
                if not verification_passed:
                    record_debug(
                        target,
                        "verification_match_rejected",
                        crop_text=crop_visible_text,
                        direct_metrics=crop_metrics,
                        alignment_metrics=crop_alignment_metrics,
                    )
                    continue
                index_metrics = candidate["metrics"]
                alignment_verified = bool(
                    crop_alignment_verified and has_index_alignment
                )
                formula_token_count = int(index_metrics.get("formula_token_count") or 0)
                formula_coverage = min(
                    float(index_metrics.get("formula_coverage") or 0.0),
                    float(crop_metrics.get("formula_coverage") or 0.0),
                )
                match_score = min(
                    float(index_metrics.get("score") or 0.0),
                    float(crop_metrics.get("score") or 0.0),
                )
                match_coverage = min(
                    float(index_metrics.get("coverage") or 0.0),
                    float(crop_metrics.get("coverage") or 0.0),
                )
                boundary_coverage = min(
                    float(index_metrics.get("boundary_coverage") or 0.0),
                    float(crop_metrics.get("boundary_coverage") or 0.0),
                )
                uniqueness = float(index_metrics.get("uniqueness") or 0.0)
                segmentation_stability = min(
                    float(line.get("geometry_confidence") or 0.0)
                    for line in candidate["selected_lines"]
                )
                if alignment_verified:
                    alignment_support = min(
                        max(
                            float(index_metrics.get("alignment_evidence_coverage") or 0.0),
                            float(index_metrics.get("alignment_context_coverage") or 0.0),
                        ),
                        max(
                            float(crop_alignment_metrics.get("alignment_evidence_coverage") or 0.0),
                            float(crop_alignment_metrics.get("alignment_context_coverage") or 0.0),
                        ),
                    )
                    confidence_score = (
                        min(
                            float(index_metrics.get("alignment_score") or 0.0),
                            float(crop_alignment_metrics.get("alignment_score") or 0.0),
                        )
                        * 0.34
                        + min(
                            float(index_metrics.get("alignment_interval_coverage") or 0.0),
                            float(crop_alignment_metrics.get("alignment_interval_coverage") or 0.0),
                        )
                        * 0.20
                        + min(
                            float(index_metrics.get("alignment_section_coverage") or 0.0),
                            float(crop_alignment_metrics.get("alignment_section_coverage") or 0.0),
                        )
                        * 0.12
                        + min(
                            float(index_metrics.get("alignment_page_coverage") or 0.0),
                            float(crop_alignment_metrics.get("alignment_page_coverage") or 0.0),
                        )
                        * 0.10
                        + alignment_support * 0.08
                        + uniqueness * 0.08
                        + segmentation_stability * 0.08
                    )
                else:
                    formula_component = formula_coverage if formula_token_count else match_coverage
                    confidence_score = (
                        match_coverage * 0.39
                        + formula_component * 0.20
                        + boundary_coverage * 0.17
                        + float(crop_metrics.get("precision") or 0.0) * 0.02
                        + uniqueness * 0.12
                        + segmentation_stability * 0.10
                    )
                crop_ocr_confidence = int(crop_result.get("confidence") or 0)
                if crop_ocr_confidence < 40:
                    record_debug(
                        target,
                        "verification_ocr_low_confidence",
                        confidence=crop_ocr_confidence,
                        crop_text=crop_visible_text,
                    )
                    continue
                confidence = round(
                    confidence_score * 90 + crop_ocr_confidence * 0.10
                )
                if alignment_verified:
                    # The two transcript alignments and same-span crop check are
                    # the hard acceptance gates. Keep the aggregate score as a
                    # display confidence without rejecting that verified result.
                    confidence = max(72, confidence)
                if transcription_fallback_verified:
                    confidence = max(72, confidence)
                if candidate.get("reanchored"):
                    confidence = max(78, confidence)
                minimum_confidence = (
                    72
                    if alignment_verified or transcription_fallback_verified
                    else 78
                    if candidate.get("reanchored")
                    else 82
                )
                if confidence < minimum_confidence:
                    record_debug(
                        target,
                        "confidence_rejected",
                        confidence=confidence,
                        crop_text=crop_visible_text,
                        index_metrics=index_metrics,
                        crop_metrics=crop_metrics,
                        crop_alignment_metrics=crop_alignment_metrics,
                    )
                    continue
                expected_lines = max(
                    1,
                    estimated_source_line_count(verification_evidence),
                    len(candidate["selected_lines"]),
                )
                bbox = {
                    **candidate["bbox"],
                    "confidence": confidence,
                    "version": SOURCE_BBOX_VERSION,
                    "text_verified": True,
                    "match_score": round(match_score, 4),
                    "match_coverage": round(match_coverage, 4),
                    "boundary_coverage": round(boundary_coverage, 4),
                    "evidence_length": len(
                        _canonical_study_source_match_text(verification_evidence)
                    ),
                    "expected_lines": expected_lines,
                    "span_verified": True,
                    "crop_verified": not transcription_fallback_verified,
                    "transcription_fallback_verified": transcription_fallback_verified,
                    "transcription_isolated": bool(
                        transcription_fallback_verified
                    ),
                    "crop_match_score": round(float(crop_metrics.get("score") or 0.0), 4),
                    "crop_match_coverage": round(float(crop_metrics.get("coverage") or 0.0), 4),
                    "crop_boundary_coverage": round(
                        float(crop_metrics.get("boundary_coverage") or 0.0), 4
                    ),
                    "crop_match_precision": round(float(crop_metrics.get("precision") or 0.0), 4),
                    "geometry_verified": True,
                    "page_verified": bool(
                        target.get("page_resolution", {}).get("page_verified")
                    ),
                    "source_image_index": image_index,
                    "page_match_kind": str(
                        target.get("page_resolution", {}).get("match_kind") or ""
                    ),
                    "page_match_margin": round(
                        float(
                            target.get("page_resolution", {}).get("match_margin")
                            or 0.0
                        ),
                        4,
                    ),
                    "formula_coverage": round(formula_coverage, 4),
                    "formula_token_count": formula_token_count,
                    "uniqueness": round(uniqueness, 4),
                    "segmentation_stability": round(segmentation_stability, 4),
                    "localization_method": (
                        "section_transcription_fallback"
                        if transcription_fallback_verified
                        else "section_ocr_reanchored"
                        if candidate.get("reanchored")
                        else "section_ocr_alignment"
                        if alignment_verified
                        else "section_ocr_rag"
                    ),
                    "anchor_verified": bool(candidate.get("reanchored")),
                    "localization_anchor": (
                        verification_evidence[:420]
                        if candidate.get("reanchored")
                        else ""
                    ),
                    "alignment_verified": alignment_verified,
                    "alignment_score": round(
                        float(index_metrics.get("alignment_score") or 0.0), 4
                    ),
                    "alignment_evidence_coverage": round(
                        float(index_metrics.get("alignment_evidence_coverage") or 0.0), 4
                    ),
                    "alignment_interval_coverage": round(
                        float(index_metrics.get("alignment_interval_coverage") or 0.0), 4
                    ),
                    "alignment_context_coverage": round(
                        float(index_metrics.get("alignment_context_coverage") or 0.0), 4
                    ),
                    "alignment_section_coverage": round(
                        float(index_metrics.get("alignment_section_coverage") or 0.0), 4
                    ),
                    "alignment_page_coverage": round(
                        float(index_metrics.get("alignment_page_coverage") or 0.0), 4
                    ),
                    "alignment_expected_span_agreement": round(
                        float(index_metrics.get("alignment_expected_span_agreement") or 0.0), 4
                    ),
                    "crop_alignment_score": round(
                        float(crop_alignment_metrics.get("alignment_score") or 0.0), 4
                    ),
                    "crop_alignment_evidence_coverage": round(
                        float(crop_alignment_metrics.get("alignment_evidence_coverage") or 0.0), 4
                    ),
                    "crop_alignment_interval_coverage": round(
                        float(crop_alignment_metrics.get("alignment_interval_coverage") or 0.0), 4
                    ),
                    "crop_alignment_context_coverage": round(
                        float(crop_alignment_metrics.get("alignment_context_coverage") or 0.0), 4
                    ),
                    "crop_alignment_section_coverage": round(
                        float(crop_alignment_metrics.get("alignment_section_coverage") or 0.0), 4
                    ),
                    "crop_alignment_page_coverage": round(
                        float(crop_alignment_metrics.get("alignment_page_coverage") or 0.0), 4
                    ),
                    "crop_alignment_expected_span_agreement": round(
                        float(crop_alignment_metrics.get("alignment_expected_span_agreement") or 0.0), 4
                    ),
                }
                validated = _validated_study_source_bbox(
                    bbox,
                    require_text_verified=True,
                    expected_image_index=image_index,
                )
                if validated is None:
                    record_debug(target, "bbox_validation_rejected", bbox=bbox)
                    continue
                target["source_ref"]["bbox"] = validated
                target["source_ref"].pop("_localization_debug", None)
                located += 1

        if (
            not successful_page_ocr
            and not used_cached_index
            and not used_transcription_fallback
        ):
            raise ValueError("Source page OCR failed for every image")
        if isinstance(parent_cancel_event, threading.Event):
            study_upload_context.cancel_event = parent_cancel_event
        return located, total

    app.extensions["study_source_localizer"] = _localize_study_card_sources

    def _validated_study_source_bbox(
        value: Any,
        *,
        require_text_verified: bool = False,
        expected_image_index: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        return validated_source_bbox(
            value,
            require_text_verified=require_text_verified,
            expected_image_index=expected_image_index,
        )

    def _study_source_coverage_items(source_pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        example_heading_pattern = re.compile(
            r"(?:^|(?<=[\n。；;]))\s*(?:"
            r"例題|範例|例子|算例|反例|練習題|練習|題目|問題|作業題|示範題|案例"
            r"|(?:worked\s+)?example|ex\.?|exercise|problem|question"
            r")\s*(?:[：:.)、-]|\d{0,3})",
            flags=re.IGNORECASE,
        )

        def is_example_block(value: str) -> bool:
            normalized = " ".join(value.split()).strip()
            if example_heading_pattern.search(normalized):
                return True
            # A question-shaped block is an example only when it also has a
            # concrete request or a solution marker. This avoids promoting
            # ordinary explanatory sentences containing "for example".
            has_request = bool(
                re.search(
                    r"(?:求出|求解|計算|判斷|證明|找出|解出|求其|是否|試證|solve|show\s+that|find|calculate|determine|prove)",
                    normalized,
                    flags=re.IGNORECASE,
                )
            )
            has_solution = bool(
                re.search(r"(?:解答|解：|解:|答案|solution|answer)", normalized, flags=re.IGNORECASE)
            )
            return has_request and (has_solution or bool(re.search(r"[?？=→≤≥]", normalized)))

        def split_blocks(value: str) -> List[str]:
            paragraphs = [part for part in re.split(r"\n\s*\n+", value) if part.strip()]
            result: List[str] = []
            for paragraph in paragraphs:
                # Notes frequently place several examples one after another
                # without a blank line. Split at explicit example headings,
                # while leaving numbered equations and normal prose intact.
                matches = list(example_heading_pattern.finditer(paragraph))
                if len(matches) <= 1:
                    result.append(paragraph)
                    continue
                starts = [match.start() for match in matches]
                if starts[0] > 0 and paragraph[: starts[0]].strip():
                    result.append(paragraph[: starts[0]])
                for index, start in enumerate(starts):
                    end = starts[index + 1] if index + 1 < len(starts) else len(paragraph)
                    result.append(paragraph[start:end])
            return result

        items: List[Dict[str, Any]] = []
        for page in source_pages:
            try:
                image_index = int(page.get("image_index") or 0)
            except (TypeError, ValueError):
                continue
            block_index = 0
            for block in split_blocks(str(page.get("transcription") or "")):
                compact = " ".join(block.split()).strip()
                clear_text = re.sub(r"〔[^〕]*〕", "", compact).strip(" -—_")
                if "〔無法推定〕" in compact:
                    continue
                example_block = is_example_block(clear_text)
                has_structure = bool(
                    re.search(
                        r"(?:=|≠|⇔|→|≤|≥|∈|∉|\\(?:frac|sum|prod|int|to|in|oplus)|"
                        r"\b(?:if|then|rank|det|Ex\.)\b|定義|條件|方法|性質|結論|證明|範例|例題)",
                        clear_text,
                        flags=re.IGNORECASE,
                    )
                )
                if len(clear_text) < 8 or (len(clear_text) < 20 and not example_block) or (
                    len(clear_text) < 34 and not has_structure and not example_block
                ):
                    continue
                block_index += 1
                items.append(
                    {
                        "id": f"p{image_index}b{block_index}",
                        "image_index": image_index,
                        "text": compact,
                        "priority": "required" if has_structure or example_block else "supporting",
                        "content_type": "example" if example_block else "concept",
                        "is_example": example_block,
                    }
                )
        return items

    def _study_source_page_coverage_plan(source_pages: List[Dict[str, Any]]) -> Dict[str, Any]:
        page_scores: Dict[int, int] = {}
        example_counts: Dict[int, int] = {}
        for page in source_pages:
            try:
                image_index = int(page.get("image_index") or 0)
            except (TypeError, ValueError):
                continue
            meaningful_blocks: List[str] = []
            page_items = [
                item
                for item in _study_source_coverage_items([page])
                if int(item.get("image_index") or 0) == image_index
            ]
            for item in page_items:
                block = str(item.get("text") or "")
                compact = " ".join(block.split()).strip()
                clear_text = re.sub(r"〔[^〕]*〕", "", compact).strip()
                if len(clear_text) >= 20:
                    meaningful_blocks.append(clear_text)
            if not meaningful_blocks:
                continue
            clear_length = sum(len(block) for block in meaningful_blocks)
            page_scores[image_index] = max(len(meaningful_blocks), math.ceil(clear_length / 220))
            example_counts[image_index] = sum(1 for item in page_items if item.get("is_example"))
        if not page_scores:
            return {"target_cards": 1, "page_quotas": {}}

        target_cards = max(
            len(page_scores),
            math.ceil(sum(page_scores.values()) / 2),
            sum(example_counts.values()),
        )
        page_quotas = {
            image_index: max(1, example_counts.get(image_index, 0))
            for image_index in page_scores
        }
        target_cards = max(target_cards, sum(page_quotas.values()))
        while sum(page_quotas.values()) < target_cards:
            image_index = max(
                page_scores,
                key=lambda index: (
                    page_scores[index] / (page_quotas[index] + 1),
                    page_scores[index],
                    -index,
                ),
            )
            page_quotas[image_index] += 1
        return {"target_cards": target_cards, "page_quotas": page_quotas}

    def _study_coverage_evidence_matches(evidence: Any, item_text: Any) -> bool:
        evidence_text = _canonical_study_source_match_text(evidence)
        coverage_text = _canonical_study_source_match_text(item_text)
        if not evidence_text or not coverage_text:
            return False
        if evidence_text in coverage_text or coverage_text in evidence_text:
            return True
        match = SequenceMatcher(None, evidence_text, coverage_text, autojunk=False).find_longest_match()
        return match.size >= 18 and match.size / max(1, min(len(evidence_text), len(coverage_text))) >= 0.72

    def _enrich_study_card_coverage_ids(payload: Any, source_pages: List[Dict[str, Any]]) -> None:
        if not isinstance(payload, dict) or not isinstance(payload.get("key_concepts"), list):
            return
        coverage_items = _study_source_coverage_items(source_pages)
        for concept in payload["key_concepts"]:
            if not isinstance(concept, dict):
                continue
            matched_ids: List[str] = []
            for source_ref in concept.get("source_refs") or []:
                if not isinstance(source_ref, dict):
                    continue
                try:
                    image_index = int(source_ref.get("image_index") or 0)
                except (TypeError, ValueError):
                    continue
                evidence = source_ref.get("evidence") or ""
                for item in coverage_items:
                    if (
                        item["image_index"] == image_index
                        and item["id"] not in matched_ids
                        and _study_coverage_evidence_matches(evidence, item["text"])
                    ):
                        matched_ids.append(item["id"])
            concept["coverage_ids"] = matched_ids[:8]

    def _study_recall_coverage_gaps(payload: Any, source_pages: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(payload, dict) or not isinstance(payload.get("key_concepts"), list):
            return {"page_quotas": {"payload": "invalid"}, "coverage_items": ["all"], "example_items": ["all"]}
        page_texts = {
            int(page.get("image_index") or 0): " ".join(str(page.get("transcription") or "").split())
            for page in source_pages
            if isinstance(page, dict)
        }
        cards_by_page: Dict[int, int] = {image_index: 0 for image_index in page_texts}
        for concept in payload["key_concepts"]:
            if not isinstance(concept, dict):
                continue
            valid_pages: Set[int] = set()
            for source_ref in concept.get("source_refs") or []:
                if not isinstance(source_ref, dict):
                    continue
                try:
                    image_index = int(source_ref.get("image_index") or 0)
                except (TypeError, ValueError):
                    continue
                evidence = " ".join(str(source_ref.get("evidence") or "").split()).strip()
                if evidence and evidence in page_texts.get(image_index, ""):
                    valid_pages.add(image_index)
            for image_index in valid_pages:
                cards_by_page[image_index] = cards_by_page.get(image_index, 0) + 1
        coverage_plan = _study_source_page_coverage_plan(source_pages)
        missing_page_quotas = {
            image_index: {
                "current": cards_by_page.get(image_index, 0),
                "required": quota,
            }
            for image_index, quota in coverage_plan["page_quotas"].items()
            if cards_by_page.get(image_index, 0) < quota
        }
        coverage_items = {item["id"]: item for item in _study_source_coverage_items(source_pages)}
        covered_ids: Set[str] = set()
        example_covered_ids: Set[str] = set()
        for concept in payload["key_concepts"]:
            if not isinstance(concept, dict):
                continue
            source_evidence = [
                (
                    int(source_ref.get("image_index") or 0),
                    " ".join(str(source_ref.get("evidence") or "").split()).strip(),
                )
                for source_ref in concept.get("source_refs") or []
                if isinstance(source_ref, dict)
            ]
            for coverage_id in concept.get("coverage_ids") or []:
                item = coverage_items.get(str(coverage_id))
                if not item:
                    continue
                if any(
                    image_index == item["image_index"]
                    and _study_coverage_evidence_matches(evidence, item["text"])
                    for image_index, evidence in source_evidence
                ):
                    covered_ids.add(str(coverage_id))
                    if item.get("is_example") and concept.get("card_type") == "example":
                        example_covered_ids.add(str(coverage_id))
            # Some model responses omit coverage_ids even when source_refs are
            # valid. Resolve example coverage directly from the evidence so a
            # missing example cannot pass as an ordinary concept card.
            if concept.get("card_type") == "example":
                for item_id, item in coverage_items.items():
                    if not item.get("is_example") or item_id in example_covered_ids:
                        continue
                    if any(
                        image_index == item["image_index"]
                        and _study_coverage_evidence_matches(evidence, item["text"])
                        for image_index, evidence in source_evidence
                    ):
                        example_covered_ids.add(item_id)
                        covered_ids.add(item_id)
        example_items = {
            item_id: item
            for item_id, item in coverage_items.items()
            if item.get("is_example")
        }
        return {
            "page_quotas": missing_page_quotas,
            "coverage_items": sorted(set(coverage_items) - covered_ids),
            "example_items": [
                {
                    "id": item_id,
                    "image_index": item["image_index"],
                    "text": str(item["text"])[:600],
                }
                for item_id, item in sorted(example_items.items())
                if item_id not in example_covered_ids
            ],
        }

    def _study_recall_page_coverage_met(payload: Any, source_pages: List[Dict[str, Any]]) -> bool:
        gaps = _study_recall_coverage_gaps(payload, source_pages)
        return not gaps["page_quotas"] and not gaps["coverage_items"] and not gaps.get("example_items")

    def _study_recall_coverage_metrics(
        payload: Any,
        source_pages: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        gaps = _study_recall_coverage_gaps(payload, source_pages)
        coverage_items = _study_source_coverage_items(source_pages)
        missing_ids = set(gaps["coverage_items"])
        required_items = [item for item in coverage_items if item.get("priority") == "required"]
        supporting_items = [item for item in coverage_items if item.get("priority") != "required"]
        example_items = [item for item in coverage_items if item.get("is_example")]
        required_covered = sum(item["id"] not in missing_ids for item in required_items)
        supporting_covered = sum(item["id"] not in missing_ids for item in supporting_items)
        example_missing = len(gaps.get("example_items") or [])
        page_plan = _study_source_page_coverage_plan(source_pages)
        planned_pages = set(page_plan["page_quotas"])
        missing_pages = {
            int(image_index)
            for image_index, values in gaps["page_quotas"].items()
            if isinstance(values, dict) and int(values.get("current") or 0) <= 0
        }
        represented_pages = len(planned_pages - missing_pages)
        required_ratio = required_covered / len(required_items) if required_items else 1.0
        supporting_ratio = supporting_covered / len(supporting_items) if supporting_items else 1.0
        page_ratio = represented_pages / len(planned_pages) if planned_pages else 1.0
        overall_ratio = (
            (len(coverage_items) - len(missing_ids)) / len(coverage_items)
            if coverage_items
            else 1.0
        )
        quality_score = required_ratio * 0.65 + page_ratio * 0.25 + supporting_ratio * 0.10
        return {
            "quality_score": round(quality_score, 4),
            "example_ratio": round(
                ((len(example_items) - example_missing) / len(example_items))
                if example_items else 1.0,
                4,
            ),
            "required_ratio": round(required_ratio, 4),
            "supporting_ratio": round(supporting_ratio, 4),
            "page_ratio": round(page_ratio, 4),
            "overall_ratio": round(overall_ratio, 4),
            "required_total": len(required_items),
            "required_missing": len(required_items) - required_covered,
            "example_total": len(example_items),
            "example_missing": example_missing,
            "supporting_total": len(supporting_items),
            "supporting_missing": len(supporting_items) - supporting_covered,
            "planned_pages": len(planned_pages),
            "missing_pages": sorted(missing_pages),
            "gaps": gaps,
        }

    def _study_recall_coverage_needs_repair(
        payload: Any,
        source_pages: List[Dict[str, Any]],
    ) -> bool:
        metrics = _study_recall_coverage_metrics(payload, source_pages)
        return bool(
            metrics["example_ratio"] < 1.0
            or metrics["required_ratio"] < 0.90
            or metrics["page_ratio"] < 0.80
            or metrics["quality_score"] < 0.82
        )

    def _validate_recall_output(payload: Any, source_pages: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        def normalize_math_markup(value: str) -> str:
            return _normalize_study_math_markup(value)

        def strip_process_narration(value: str) -> str:
            return _strip_study_process_narration(value)

        def has_transpose_dimension_conflict(value: str) -> bool:
            if not ("A^T" in value or "A^{T}" in value or "轉置" in value):
                return False
            match = re.search(
                r"M_\{([^{}\s]+)[×x]([^{}\s]+)\}\s*(?:→|\\to)\s*M_\{([^{}\s]+)[×x]([^{}\s]+)\}",
                value,
            )
            if not match:
                return False
            source_rows, source_columns, target_rows, target_columns = match.groups()
            return target_rows != source_columns or target_columns != source_rows

        def mapping_signatures(value: str) -> Set[str]:
            canonical = str(value or "")
            canonical = re.sub(r"\\mathbb\{([A-Za-z])\}", r"\1", canonical)
            canonical = canonical.replace("\\mathbb R", "R").replace("ℝ", "R").replace("ℂ", "C")
            canonical = canonical.replace("\\times", "×").replace("\\to", "→").replace("->", "→")
            canonical = re.sub(r"(?<=[A-Za-z0-9])\s*x\s*(?=[A-Za-z0-9])", "×", canonical)
            canonical = re.sub(r"[{}\s_]", "", canonical)
            space = r"(?:[A-Z][A-Za-z0-9]*(?:\^[A-Za-z0-9]+(?:×[A-Za-z0-9]+)?)?(?:×[A-Za-z0-9]+)?)"
            return set(re.findall(rf"{space}→{space}", canonical))

        def has_mapping_signature_conflict(value: str, source_refs: List[Dict[str, Any]]) -> bool:
            card_signatures = mapping_signatures(value)
            if not card_signatures:
                return False
            source_signatures = mapping_signatures(" ".join(ref["evidence"] for ref in source_refs))
            if not source_signatures:
                return False
            return not card_signatures.issubset(source_signatures)

        def has_invalid_negation_counterexample(value: str) -> bool:
            return _study_has_invalid_negation_counterexample(value)

        def has_matrix_product_dimension_conflict(value: str) -> bool:
            canonical = str(value or "")
            canonical = canonical.replace("\\times", "×").replace("\\(", "").replace("\\)", "")
            canonical = canonical.replace("\\[", "").replace("\\]", "")
            dimensions: Dict[str, Tuple[str, str]] = {}
            for name, rows, columns in re.findall(
                r"\b([A-Z])_?\{?([A-Za-z0-9]+)×([A-Za-z0-9]+)\}?",
                canonical,
            ):
                dimensions[name] = (rows, columns)
            for name, rows, columns in re.findall(
                r"\b([A-Z])\b\s*(?:為|is|:)\s*\{?([A-Za-z0-9]+)×([A-Za-z0-9]+)\}?",
                canonical,
                flags=re.IGNORECASE,
            ):
                dimensions[name.upper()] = (rows, columns)
            for left, right, rows, columns in re.findall(
                r"\(?([A-Z])([A-Z])\)?_?\{?([A-Za-z0-9]+)×([A-Za-z0-9]+)\}?",
                canonical,
            ):
                left_dimensions = dimensions.get(left)
                right_dimensions = dimensions.get(right)
                if not left_dimensions or not right_dimensions:
                    continue
                if left_dimensions[1] != right_dimensions[0]:
                    return True
                if (rows, columns) != (left_dimensions[0], right_dimensions[1]):
                    return True
            return False

        if not isinstance(payload, dict):
            return None
        summary = strip_process_narration(str(payload.get("summary") or ""))
        detected_topic = str(payload.get("detected_topic") or "").strip()
        detected_topic = re.sub(r"[（(][^）)]*(?:修正|校正|審核)[^）)]*[）)]", "", detected_topic).strip()
        raw_concepts = payload.get("key_concepts")
        if (
            not summary
            or _study_text_quality_issue(summary, max_length=1200)
            or not detected_topic
            or _study_text_quality_issue(detected_topic, max_length=120)
            or not isinstance(raw_concepts, list)
        ):
            return None
        page_transcriptions = {
            int(page.get("image_index") or 0): " ".join(str(page.get("transcription") or "").split())
            for page in source_pages
            if isinstance(page, dict)
        }
        valid_coverage_ids = {item["id"] for item in _study_source_coverage_items(source_pages)}
        prepared_concepts: List[Dict[str, Any]] = []
        correction_records: List[Dict[str, Any]] = []
        rejected_corrupted_content = False
        for item in raw_concepts:
            if not isinstance(item, dict):
                continue
            concept = str(item.get("concept") or "").strip()
            concept = re.sub(r"[（(][^）)]*(?:修正|校正|審核)[^）)]*[）)]", "", concept).strip()
            recall_cue = normalize_math_markup(strip_process_narration(str(item.get("recall_cue") or "").strip()))
            core_summary = normalize_math_markup(strip_process_narration(str(item.get("core_summary") or "").strip()))
            explanation = normalize_math_markup(strip_process_narration(str(item.get("explanation") or "").strip()))
            card_type = "example" if item.get("card_type") == "example" else "concept"
            example_problem = normalize_math_markup(strip_process_narration(str(item.get("example_problem") or "").strip()))
            example_method = normalize_math_markup(strip_process_narration(str(item.get("example_method") or "").strip()))
            simple_example = normalize_math_markup(strip_process_narration(str(item.get("simple_example") or "").strip()))
            memory_hint = normalize_math_markup(strip_process_narration(str(item.get("memory_hint") or "").strip()))
            common_confusion = normalize_math_markup(strip_process_narration(str(item.get("common_confusion") or "").strip()))
            reasoning_steps = [
                normalize_math_markup(strip_process_narration(str(step or "").strip()))
                for step in (item.get("reasoning_steps") or [])[:4]
                if str(step or "").strip()
            ] if isinstance(item.get("reasoning_steps"), list) else []
            topic = _normalize_study_concept_title(item.get("topic"), detected_topic)
            concept = _normalize_study_concept_title(concept, topic or detected_topic)
            quality_issues = (
                _study_text_quality_issue(concept, max_length=120),
                _study_text_quality_issue(recall_cue, max_length=180),
                _study_text_quality_issue(core_summary, max_length=320),
                _study_text_quality_issue(explanation, max_length=900),
                _study_text_quality_issue(example_problem, max_length=420) if example_problem else None,
                _study_text_quality_issue(example_method, max_length=340) if example_method else None,
                _study_text_quality_issue(simple_example, max_length=420) if simple_example else None,
                _study_text_quality_issue(memory_hint, max_length=240) if memory_hint else None,
                _study_text_quality_issue(common_confusion, max_length=240) if common_confusion else None,
                *(_study_text_quality_issue(step, max_length=220) for step in reasoning_steps),
                _study_text_quality_issue(topic, max_length=80),
            )
            source_bound_card_text = " ".join(
                [
                    core_summary,
                    explanation,
                    example_problem,
                    example_method,
                    common_confusion,
                    *reasoning_steps,
                ]
            )
            if any(quality_issues):
                rejected_corrupted_content = True
                continue
            if card_type == "example" and example_problem and not example_method:
                # Keep a clearly detected source example even when the model
                # did not find a worked solution. Never invent a method just
                # to satisfy the card schema.
                example_method = "來源未提供完整解法"
                source_bound_card_text = " ".join(
                    [core_summary, explanation, example_problem, example_method, *reasoning_steps]
                )
            if card_type == "example" and not example_problem:
                continue
            if card_type == "concept" and not simple_example:
                continue
            if card_type != "example":
                example_problem = ""
                example_method = ""
            else:
                simple_example = ""
            related_concepts = item.get("related_concepts")
            search_keywords = [
                " ".join(str(value or "").split()).strip()[:40]
                for value in (item.get("search_keywords") or [])[:8]
                if str(value or "").strip()
            ] if isinstance(item.get("search_keywords"), list) else []
            source_refs: List[Dict[str, Any]] = []
            for source_ref in item.get("source_refs") or []:
                if not isinstance(source_ref, dict):
                    continue
                try:
                    image_index = int(source_ref.get("image_index") or 0)
                except (TypeError, ValueError):
                    continue
                evidence = _literal_study_source_evidence(source_ref.get("evidence"))
                page_resolution = _resolve_study_source_page(
                    evidence,
                    source_pages,
                    preferred_image_index=image_index,
                    context=" ".join(
                        (
                            concept,
                            topic,
                            recall_cue,
                            core_summary,
                            source_bound_card_text,
                        )
                    ),
                )
                if not page_resolution:
                    continue
                image_index = int(page_resolution["image_index"])
                source_refs.append({"image_index": image_index, "evidence": evidence[:240]})
            correction = item.get("correction") if isinstance(item.get("correction"), dict) else {}
            correction_applied = bool(correction.get("applied"))
            correction_original = " ".join(str(correction.get("original") or "").split()).strip()
            correction_corrected = " ".join(str(correction.get("corrected") or "").split()).strip()
            correction_reason = " ".join(str(correction.get("reason") or "").split()).strip()
            coverage_ids = [
                str(value).strip()
                for value in (item.get("coverage_ids") or [])
                if str(value).strip() in valid_coverage_ids
            ] if isinstance(item.get("coverage_ids"), list) else []
            keyword_corpus = _canonical_study_source_match_text(
                " ".join(
                    [
                        concept,
                        topic,
                        recall_cue,
                        source_bound_card_text,
                        *(source_ref["evidence"] for source_ref in source_refs),
                    ]
                )
            )
            search_keywords = [
                keyword
                for keyword in search_keywords
                if len(_canonical_study_source_match_text(keyword)) >= 2
                and _canonical_study_source_match_text(keyword) in keyword_corpus
            ]
            if (
                concept
                and explanation
                and source_refs
                and not has_transpose_dimension_conflict(source_bound_card_text)
                and not has_mapping_signature_conflict(source_bound_card_text, source_refs)
                and not has_invalid_negation_counterexample(source_bound_card_text)
                and not has_matrix_product_dimension_conflict(source_bound_card_text)
                and _is_recall_concept_eligible(
                {"concept": concept, "explanation": explanation, "memory_hint": memory_hint}
                )
            ):
                prepared = {
                    "concept": concept[:80],
                    "recall_cue": recall_cue,
                    "core_summary": core_summary,
                    "explanation": explanation,
                    "card_type": card_type,
                    "example_problem": example_problem,
                    "example_method": example_method,
                    "simple_example": simple_example,
                    "reasoning_steps": reasoning_steps,
                    "common_confusion": common_confusion,
                    "memory_hint": memory_hint,
                    "topic": topic[:48] or detected_topic[:48],
                    "note_topic": detected_topic[:80],
                    "source_refs": source_refs[:4],
                    "coverage_ids": list(dict.fromkeys(coverage_ids))[:8],
                    "related_concepts": [
                        str(value).strip()[:80]
                        for value in related_concepts[:2]
                        if str(value).strip()
                    ] if isinstance(related_concepts, list) else [],
                    "search_keywords": list(dict.fromkeys(search_keywords))[:8],
                }
                prepared_concepts.append(prepared)
                if correction_applied and correction_original and correction_corrected and correction_reason:
                    correction_records.append(
                        {
                            "concept": prepared["concept"],
                            "original": correction_original[:240],
                            "corrected": correction_corrected[:240],
                            "reason": correction_reason[:300],
                            "image_index": source_refs[0]["image_index"],
                        }
                    )
        if rejected_corrupted_content:
            app.logger.warning(
                "Discarded one or more corrupted study cards before final validation; kept=%s",
                len(prepared_concepts),
            )
        if not prepared_concepts:
            return None
        prepared_payload = {"key_concepts": prepared_concepts}
        if not _study_recall_page_coverage_met(prepared_payload, source_pages):
            app.logger.warning(
                "Final study-card coverage has non-blocking gaps after filtering: %s",
                _study_recall_coverage_metrics(prepared_payload, source_pages),
            )
        title_lookup = {item["concept"].casefold(): item["concept"] for item in prepared_concepts}
        concepts_by_title = {item["concept"]: item for item in prepared_concepts}
        for item in prepared_concepts:
            normalized_related: List[str] = []
            for related in item["related_concepts"]:
                matched = title_lookup.get(related.casefold())
                if matched and matched != item["concept"] and matched not in normalized_related:
                    normalized_related.append(matched)
            item["related_concepts"] = normalized_related[:2]
        for item in prepared_concepts:
            for related in list(item["related_concepts"]):
                target = concepts_by_title.get(related)
                if target is not None and item["concept"] not in target["related_concepts"]:
                    target["related_concepts"] = (target["related_concepts"] + [item["concept"]])[:2]
        return {
            "detected_topic": detected_topic[:80],
            "summary": summary,
            "key_concepts": prepared_concepts,
            "correction_records": correction_records,
        }

    def _analyze_study_note_image_batch(
        images: List[Tuple[str, bytes, str]],
        *,
        subject: str,
        allow_corrections: bool,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        _raise_if_study_upload_cancelled()
        if not openai_api_key:
            return None, "尚未設定 OPENAI_API_KEY，無法分析筆記。"

        def review_zoom_crops(image_bytes: bytes) -> List[Dict[str, Any]]:
            try:
                with Image.open(io.BytesIO(image_bytes)) as opened:
                    source = ImageOps.exif_transpose(opened).convert("RGB")
                    width, height = source.size
                    if width < 320 or height < 320:
                        return []
                    if height >= width:
                        boxes = [
                            (0, 0, width, max(1, round(height * 0.5))),
                            (0, min(height - 1, round(height * 0.25)), width, max(1, round(height * 0.75))),
                            (0, min(height - 1, round(height * 0.5)), width, height),
                        ]
                    else:
                        boxes = [
                            (0, 0, max(1, round(width * 0.5)), height),
                            (min(width - 1, round(width * 0.25)), 0, max(1, round(width * 0.75)), height),
                            (min(width - 1, round(width * 0.5)), 0, width, height),
                        ]
                    crops: List[Dict[str, Any]] = []
                    for crop_index, box in enumerate(boxes, start=1):
                        original_crop = source.crop(box)
                        max_dimension = max(original_crop.size)
                        scale = min(2.4, max(1.6, 2800 / max(1, max_dimension)))
                        target_size = (
                            max(1, round(original_crop.width * scale)),
                            max(1, round(original_crop.height * scale)),
                        )

                        color_crop = original_crop.resize(target_size, Image.Resampling.LANCZOS)
                        color_crop = color_crop.filter(
                            ImageFilter.UnsharpMask(radius=1.1, percent=170, threshold=2)
                        )
                        color_output = io.BytesIO()
                        color_crop.save(color_output, format="JPEG", quality=94, optimize=True)
                        crops.append(
                            {
                                "label": f"重疊區塊 {crop_index} 的彩色銳化放大版",
                                "bytes": color_output.getvalue(),
                            }
                        )

                        contrast_crop = ImageOps.grayscale(original_crop)
                        contrast_crop = ImageOps.autocontrast(contrast_crop, cutoff=0.5)
                        contrast_crop = contrast_crop.resize(target_size, Image.Resampling.LANCZOS)
                        contrast_crop = contrast_crop.filter(
                            ImageFilter.UnsharpMask(radius=1.0, percent=190, threshold=1)
                        ).convert("RGB")
                        contrast_output = io.BytesIO()
                        contrast_crop.save(contrast_output, format="JPEG", quality=94, optimize=True)
                        crops.append(
                            {
                                "label": f"重疊區塊 {crop_index} 的灰階高對比放大版",
                                "bytes": contrast_output.getvalue(),
                            }
                        )
                    return crops
            except (OSError, ValueError):
                return []

        def transcription_has_example_signals(value: Any) -> bool:
            text = " ".join(str(value or "").split())
            return bool(
                re.search(
                    r"(?:例題|範例|算例|反例|練習題|題目|問題|解答|"
                    r"\b(?:worked\s+example|example|ex\.|exercise|problem|question|solution)\b|"
                    r"(?:求出|求解|計算|判斷|證明|找出|解出|求其|是否|試證).{0,80}(?:[?？=→≤≥]|答案|解：|解:))",
                    text,
                    flags=re.IGNORECASE,
                )
            )
        transcription_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["detected_topic", "pages"],
            "properties": {
                "detected_topic": {"type": "string", "maxLength": 80},
                "pages": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": len(images),
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["image_index", "transcription", "uncertain_fragments"],
                        "properties": {
                            "image_index": {"type": "integer", "minimum": 1, "maximum": len(images)},
                            "transcription": {"type": "string", "maxLength": 8000},
                            "uncertain_fragments": {
                                "type": "array",
                                "maxItems": 20,
                                "items": {"type": "string", "maxLength": 240},
                            },
                        },
                    },
                },
            },
        }
        def single_page_transcription_schema(image_index: int) -> Dict[str, Any]:
            schema = json.loads(json.dumps(transcription_schema))
            schema["properties"]["pages"]["minItems"] = 1
            schema["properties"]["pages"]["maxItems"] = 1
            schema["properties"]["pages"]["items"]["properties"]["image_index"] = {
                "type": "integer",
                "enum": [image_index],
            }
            return schema

        def parsed_single_page(result: Any, image_index: int) -> Dict[str, Any]:
            pages = result.get("pages") if isinstance(result, dict) else None
            if not isinstance(pages, list) or len(pages) != 1 or not isinstance(pages[0], dict):
                raise ValueError(f"Incomplete transcription for image {image_index}")
            page = pages[0]
            returned_index = int(page.get("image_index") or 0)
            page_text = str(page.get("transcription") or "").strip()
            if returned_index != image_index or not page_text:
                raise ValueError(f"Invalid transcription for image {image_index}")
            return {
                "image_index": image_index,
                "transcription": page_text[:8000],
                "transcription_mode": "isolated_v1",
                "uncertain_fragments": [
                    " ".join(str(value).split())[:240]
                    for value in (page.get("uncertain_fragments") or [])[:20]
                    if str(value).strip()
                ],
            }

        def run_isolated_page_jobs(
            page_indices: List[int],
            worker: Callable[[int, Tuple[str, bytes, str]], Dict[str, Any]],
            on_completed: Optional[Callable[[int, int, int], None]] = None,
        ) -> Dict[int, Dict[str, Any]]:
            """Run page-isolated model calls concurrently without sharing images."""
            if not page_indices:
                return {}
            parent_cancel_event = getattr(study_upload_context, "cancel_event", None)

            def wrapped(image_index: int) -> Tuple[int, Dict[str, Any]]:
                if isinstance(parent_cancel_event, threading.Event):
                    study_upload_context.cancel_event = parent_cancel_event
                try:
                    _raise_if_study_upload_cancelled()
                    return image_index, worker(image_index, images[image_index - 1])
                finally:
                    if hasattr(study_upload_context, "cancel_event"):
                        del study_upload_context.cancel_event

            results: Dict[int, Dict[str, Any]] = {}
            executor = ThreadPoolExecutor(
                max_workers=min(3, len(page_indices)),
                thread_name_prefix="study-page-ocr",
            )
            futures = {
                executor.submit(wrapped, image_index): image_index
                for image_index in page_indices
            }
            try:
                completed = 0
                for future in as_completed(futures):
                    _raise_if_study_upload_cancelled()
                    image_index, result = future.result()
                    results[image_index] = result
                    completed += 1
                    if on_completed:
                        on_completed(image_index, completed, len(page_indices))
            except BaseException:
                for future in futures:
                    future.cancel()
                executor.shutdown(wait=False, cancel_futures=True)
                raise
            else:
                executor.shutdown(wait=True)
            return results

        transcription_instruction = (
                    f"你是跨學科筆記的忠實轉錄員。這次網站選定科目是「{subject}」，網站允許的科目只有：{ '、'.join(STUDY_PLAN_SUBJECTS) }。"
                    "逐張轉錄看得清楚的標題、敘述、定義、步驟、例子、表格文字、專有名詞與符號，不要整理、解釋、修正知識內容或補充原圖沒有的觀念；只允許依下述規則做最小的字元級上下文補全。例題辨識要逐區塊執行：檢查『例題、範例、算例、反例、練習題、題目、問題、案例、解答』等中文標籤，以及 Example、Ex.、Exercise、Problem、Question、Solution 等英文標籤；也檢查有編號的小題、問號或明確的求解／計算／證明／判斷要求，和題目後接解答或計算過程的版面。每一個獨立題設都要原樣轉錄，不能只記錄最後答案，也不能把相鄰不同題目的數值或解法合併。"
                    "transcription 只能依自然閱讀順序寫入圖片上實際存在的字元；不得用括號或句子描述頁面位置、顏色、圖示、照片、版面或內容大意，例如『頁中列出』『右側原稿示意』『下方有例子』都不屬於轉錄。圖形若沒有可辨識文字就不要替它寫說明。"
                    "凡是原圖中的數學式、物理量關係、化學方程式、統計式、程式碼片段或其他具有結構的符號表達，請使用可渲染的 LaTeX：行內使用 \\( ... \\)，獨立式使用 \\[ ... \\]；保留等號、條件、上下標、矩陣、反應箭頭與原本順序。普通文字、專有名詞與非公式內容不要硬改成 LaTeX。"
                    "若局部字元、數字或符號無法直接辨認，先利用同頁前後文、同份筆記重複出現的記號、公式成對結構、表格欄列與相鄰推導判斷。只有候選內容可被這些局部證據唯一決定時才補入 transcription，不可用課本常識延伸整句或補入新觀念。"
                    "每個補全都要在 uncertain_fragments 記錄『已補全｜推定：...｜依據：...｜信心：高／中』；若仍有兩種以上合理結果，才在原位置寫〔無法推定〕，並記錄『未補全｜上下文：...』。補全後的 transcription 直接放可讀文字，不要插入待確認說明。"
                    "所有原文中的具體名稱、數值、單位、變數、符號、版本、日期、條件與例外都必須保留；不得擅自泛化、特例化、翻譯成不同概念或套用其他科目的知識。detected_topic 只能描述這份「{subject}」筆記實際出現的主題，不得建立第七個科目。"
                    "你這次只會看到一張原圖；不得想像、延續或抄入其他頁的內容。detected_topic 只依目前這張圖實際內容命名。"
        )
        card_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["detected_topic", "summary", "key_concepts"],
            "properties": {
                "detected_topic": {"type": "string", "maxLength": 80},
                "summary": {"type": "string", "maxLength": 900},
                "key_concepts": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["concept", "card_type", "recall_cue", "core_summary", "explanation", "simple_example", "example_problem", "example_method", "reasoning_steps", "common_confusion", "memory_hint", "topic", "related_concepts", "search_keywords", "source_refs", "coverage_ids", "correction"],
                        "properties": {
                            "concept": {"type": "string", "maxLength": 80},
                            "card_type": {"type": "string", "enum": ["concept", "example"]},
                            "recall_cue": {"type": "string", "maxLength": 160},
                            "core_summary": {"type": "string", "maxLength": 280},
                            "explanation": {"type": "string", "maxLength": 620},
                            "simple_example": {"type": "string", "maxLength": 360},
                            "example_problem": {"type": "string", "maxLength": 360},
                            "example_method": {"type": "string", "maxLength": 280},
                            "reasoning_steps": {
                                "type": "array",
                                "maxItems": 4,
                                "items": {"type": "string", "maxLength": 180},
                            },
                            "common_confusion": {"type": "string", "maxLength": 180},
                            "memory_hint": {"type": "string", "maxLength": 120},
                            "topic": {"type": "string", "maxLength": 48},
                            "related_concepts": {
                                "type": "array",
                                "maxItems": 2,
                                "items": {"type": "string", "maxLength": 80},
                            },
                            "search_keywords": {
                                "type": "array",
                                "maxItems": 8,
                                "items": {"type": "string", "maxLength": 40},
                            },
                            "source_refs": {
                                "type": "array",
                                "minItems": 1,
                                "maxItems": 4,
                                "items": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "required": ["image_index", "evidence"],
                                    "properties": {
                                        "image_index": {"type": "integer", "minimum": 1, "maximum": len(images)},
                                        "evidence": {"type": "string", "maxLength": 240},
                                    },
                                },
                            },
                            "coverage_ids": {
                                "type": "array",
                                "minItems": 1,
                                "maxItems": 8,
                                "items": {"type": "string", "maxLength": 16},
                            },
                            "correction": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["applied", "original", "corrected", "reason"],
                                "properties": {
                                    "applied": {"type": "boolean"},
                                    "original": {"type": "string", "maxLength": 240},
                                    "corrected": {"type": "string", "maxLength": 240},
                                    "reason": {"type": "string", "maxLength": 300},
                                },
                            },
                        },
                    },
                },
            },
        }
        try:
            if progress_callback:
                progress_callback(20, "正在逐頁忠實轉錄文字、符號與公式，不做延伸解釋。")
            def transcribe_initial_page(
                image_index: int,
                image: Tuple[str, bytes, str],
            ) -> Dict[str, Any]:
                _filename, image_bytes, mime_type = image
                transcription = _call_openai_json(
                    name=f"study_note_transcription_page_{image_index}",
                    schema=single_page_transcription_schema(image_index),
                    content=[
                        {
                            "type": "input_text",
                            "text": (
                                transcription_instruction
                                + f" 這是整批中的第 {image_index} 張；pages 必須只輸出 image_index={image_index}。"
                            ),
                        },
                        {
                            "type": "input_image",
                            "image_url": f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}",
                            "detail": "high",
                        },
                    ],
                    timeout=180,
                )
                return parsed_single_page(
                    transcription,
                    image_index,
                )

            def report_initial_page(
                _image_index: int,
                completed: int,
                total_pages: int,
            ) -> None:
                if progress_callback:
                    progress_callback(
                        20 + round(completed / max(1, total_pages) * 11),
                        f"已完成 {completed}/{total_pages} 張逐頁轉錄，頁面來源彼此隔離。",
                    )
            pages_by_index = run_isolated_page_jobs(
                list(range(1, len(images) + 1)),
                transcribe_initial_page,
                report_initial_page,
            )
            source_pages = [pages_by_index[index] for index in range(1, len(images) + 1) if index in pages_by_index]
            if len(source_pages) != len(images):
                raise ValueError("Incomplete transcription")
            initial_source_pages = json.loads(json.dumps(source_pages, ensure_ascii=False))

            symbol_audit_instruction = (
                "你是獨立的跨學科逐字元核對與上下文補全員。第一輪 transcription 只是待核草稿；請重新查看目前這一張完整原圖與其放大裁切，輸出修正過的完整單頁轉錄。"
                "逐一核對所有文字、專有名詞、數字、單位、符號、標點、大小寫、上下標、指數、分數、表格欄位、公式、方程式、反應式、程式碼與條件。字跡不清時，必須主動比較同頁前後句、同頁重複記號、公式左右結構、表格欄列和相鄰推導。"
                "例題與題組要執行額外數字符號稽核：先逐項列出題號、所有常數、係數、座標、矩陣元素、範圍端點、單位及答案數值，再逐一對照完整原圖、彩色銳化版與灰階高對比版。特別區分 0／6／8／9、1／7、3／5、正負號、小數點、逗號、分數線、括號、次方與上下標；兩個版本衝突時回到完整原圖與公式內部一致性判斷，不能只採信其中一張增強圖。"
                "輸出只能包含原圖實際書寫的字元，不得以『頁中／圖中／上方／下方／右側／原稿／黑板』等敘事描述圖片、位置或圖形；第一輪若有這類描述必須刪除，不能當作原文保留。"
                "只有上下文使缺字或符號只剩一個合理結果時才補上；可做最小字元級推理，但不得用外部課本知識補成更完整的定義、定理、結論或解法。不要因單一筆畫模糊就刪掉整段。"
                "原圖的具體值不得改成變數，變數不得改成具體值，專有名詞不得換成相近名詞，原圖未寫出的定義域、範圍、因果或結論不得補入。"
                "若草稿與原圖不一致，以原圖為準。每個採用的上下文補全都列入 uncertain_fragments，格式為『已補全｜推定：...｜依據：...｜信心：高／中』；若仍有兩種以上合理結果，才在 transcription 對應位置寫〔無法推定〕並記錄『未補全｜上下文：...』。"
                "不得寫入目前原圖以外的其他頁內容，即使草稿看起來像有接續內容也必須以目前原圖為準。"
            )
            def audit_symbol_page(
                image_index: int,
                image: Tuple[str, bytes, str],
            ) -> Dict[str, Any]:
                _filename, image_bytes, mime_type = image
                symbol_audit_content: List[Dict[str, Any]] = [
                    {
                        "type": "input_text",
                        "text": (
                            symbol_audit_instruction
                            + f" 目前是第 {image_index} 張，pages 只能輸出 image_index={image_index}。"
                            + "\n待核草稿="
                            + json.dumps(
                                pages_by_index[image_index],
                                ensure_ascii=False,
                                separators=(",", ":"),
                            )
                        ),
                    },
                    {
                        "type": "input_text",
                        "text": "以下是目前唯一允許轉錄的完整原圖；後面的圖都是同一頁重疊裁切，不是新頁面。",
                    },
                    {
                        "type": "input_image",
                        "image_url": f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}",
                        "detail": "high",
                    },
                ]
                for crop in review_zoom_crops(image_bytes):
                    symbol_audit_content.extend(
                        [
                            {
                                "type": "input_text",
                                "text": (
                                    f"目前原圖的{crop['label']}，只用來核對同一頁的小字、數字與符號。"
                                    "請逐字比較原圖與不同增強版本；增強造成的邊緣或雜點不得當成小數點、負號或筆畫。"
                                ),
                            },
                            {
                                "type": "input_image",
                                "image_url": f"data:image/jpeg;base64,{base64.b64encode(crop['bytes']).decode('ascii')}",
                                "detail": "high",
                            },
                        ]
                    )
                symbol_audit = _call_openai_json(
                    name=f"study_note_symbol_audit_page_{image_index}",
                    schema=single_page_transcription_schema(image_index),
                    content=symbol_audit_content,
                    timeout=210,
                )
                return parsed_single_page(
                    symbol_audit,
                    image_index,
                )

            def report_audited_page(
                _image_index: int,
                completed: int,
                total_pages: int,
            ) -> None:
                if progress_callback:
                    progress_callback(
                        32 + round(completed / max(1, total_pages) * 10),
                        f"已完成 {completed}/{total_pages} 張獨立符號核對。",
                    )
            audited_pages_by_index = run_isolated_page_jobs(
                list(range(1, len(images) + 1)),
                audit_symbol_page,
                report_audited_page,
            )
            source_pages = [
                audited_pages_by_index[index]
                for index in range(1, len(images) + 1)
                if index in audited_pages_by_index
            ]
            if len(source_pages) != len(images):
                raise ValueError("Incomplete symbol-audited transcription")

            reconciliation_instruction = (
                "你是單頁轉錄完整性仲裁員。initial_transcription 與 symbol_audit 是兩次獨立查看目前同一張圖片的結果，兩者都可能漏段或誤讀。"
                "請重新查看目前原圖，逐段比較兩稿的每一個標題、定義、公式、例題、證明、表格、程式碼與結論，輸出目前這一頁的完整最終轉錄。"
                "任何只出現在其中一稿的段落都必須回到圖片確認：圖片可見就完整保留，不可因另一稿漏掉而刪除；圖片不支持就不要保留，也不可把兩稿內容直接盲目拼接。"
                "最終 transcription 仍只能是圖片上實際可見字元；不得新增頁面位置、顏色、圖示、照片、版面或內容摘要等視覺敘事。若任一稿含『頁中列出』『右側原稿』『下方示意』等描述，除非這些字真的寫在圖上，否則一律移除。"
                "逐頁由上到下輸出，不可省略側欄、右半部、頁尾、小字例題或接續公式。例題的題設、操作與結論均須轉錄；請逐一盤點中文例題／範例／算例／反例／練習題／題目／問題／解答標籤、英文 Example／Ex.／Exercise／Problem／Question／Solution 標籤、編號小題，以及含明確求解要求的題組；不同題設必須分開保留，不能只留下答案或把多題合併。原圖本身的錯誤也照原樣轉錄，留給後續內容校正。"
                "仍無法唯一辨識的字元依前後文做最小補全並記錄信心；無法唯一推定才標〔無法推定〕。不得用課本知識補入圖片沒有的定義或解法。"
                "數學與結構化符號使用可渲染的 LaTeX。你看不到也不得輸出其他頁內容；pages 只能有目前這一頁。"
            )
            initial_pages_by_index = {
                int(page.get("image_index") or 0): page
                for page in initial_source_pages
                if isinstance(page, dict)
            }
            reconciliation_required_indices: Set[int] = set()
            for audited_page in source_pages:
                image_index = int(audited_page.get("image_index") or 0)
                initial_page = initial_pages_by_index.get(image_index) or {}
                initial_text = _canonical_study_source_match_text(
                    initial_page.get("transcription") or ""
                )
                audited_text = _canonical_study_source_match_text(
                    audited_page.get("transcription") or ""
                )
                shorter_length = min(len(initial_text), len(audited_text))
                longer_length = max(len(initial_text), len(audited_text), 1)
                similarity = (
                    SequenceMatcher(None, initial_text, audited_text, autojunk=False).ratio()
                    if initial_text and audited_text
                    else 0.0
                )
                uncertainty_text = " ".join(
                    str(value or "")
                    for value in (
                        *(initial_page.get("uncertain_fragments") or []),
                        *(audited_page.get("uncertain_fragments") or []),
                    )
                )
                has_example_signals = transcription_has_example_signals(
                    " ".join(
                        (
                            str(initial_page.get("transcription") or ""),
                            str(audited_page.get("transcription") or ""),
                        )
                    )
                )
                if (
                    has_example_signals
                    or similarity < 0.985
                    or shorter_length / longer_length < 0.97
                    or "未補全" in uncertainty_text
                    or "〔無法推定〕" in str(initial_page.get("transcription") or "")
                    or "〔無法推定〕" in str(audited_page.get("transcription") or "")
                ):
                    reconciliation_required_indices.add(image_index)
            if progress_callback:
                progress_callback(
                    43,
                    "正在比對兩輪轉錄差異，補回任何被單次辨識遺漏的段落。"
                    if reconciliation_required_indices
                    else "兩輪轉錄高度一致，已略過不必要的第三次辨識。",
                )
            reconciled_pages_by_index: Dict[int, Dict[str, Any]] = {
                image_index: audited_pages_by_index[image_index]
                for image_index in range(1, len(images) + 1)
                if image_index not in reconciliation_required_indices
            }

            def reconcile_page(
                image_index: int,
                image: Tuple[str, bytes, str],
            ) -> Dict[str, Any]:
                _filename, image_bytes, mime_type = image
                reconciliation_content: List[Dict[str, Any]] = [
                    {
                        "type": "input_text",
                        "text": (
                            reconciliation_instruction
                            + f" 目前是第 {image_index} 張，pages 只能輸出 image_index={image_index}。"
                            + "\ninitial_transcription="
                            + json.dumps(
                                initial_pages_by_index[image_index],
                                ensure_ascii=False,
                                separators=(",", ":"),
                            )
                            + "\nsymbol_audit="
                            + json.dumps(
                                audited_pages_by_index[image_index],
                                ensure_ascii=False,
                                separators=(",", ":"),
                            )
                        ),
                    },
                    {
                        "type": "input_image",
                        "image_url": f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}",
                        "detail": "high",
                    },
                ]
                if transcription_has_example_signals(
                    " ".join(
                        (
                            str(initial_pages_by_index[image_index].get("transcription") or ""),
                            str(audited_pages_by_index[image_index].get("transcription") or ""),
                        )
                    )
                ):
                    reconciliation_content[0]["text"] += (
                        "\n這一頁含有疑似例題。請建立數字符號核對清單，逐一比對題號、所有數值、"
                        "小數點、正負號、分子分母、括號、上下標、矩陣元素與運算符；兩稿即使一致，"
                        "仍必須以原圖和放大版重新確認，不能沿用共同誤讀。只把核對後結果寫入 transcription。"
                    )
                    for crop in review_zoom_crops(image_bytes):
                        reconciliation_content.extend(
                            [
                                {
                                    "type": "input_text",
                                    "text": (
                                        f"例題數字符號專用：{crop['label']}。"
                                        "同時參照完整原圖判斷，不可把影像增強雜點當成符號。"
                                    ),
                                },
                                {
                                    "type": "input_image",
                                    "image_url": f"data:image/jpeg;base64,{base64.b64encode(crop['bytes']).decode('ascii')}",
                                    "detail": "high",
                                },
                            ]
                        )
                reconciled = _call_openai_json(
                    name=f"study_note_transcription_reconciled_page_{image_index}",
                    schema=single_page_transcription_schema(image_index),
                    content=reconciliation_content,
                    timeout=240,
                    max_output_tokens=9000,
                )
                return parsed_single_page(
                    reconciled,
                    image_index,
                )
            reconciled_pages_by_index.update(
                run_isolated_page_jobs(
                    sorted(reconciliation_required_indices),
                    reconcile_page,
                )
            )
            source_pages = [
                reconciled_pages_by_index[index]
                for index in range(1, len(images) + 1)
                if index in reconciled_pages_by_index
            ]
            if len(source_pages) != len(images):
                raise ValueError("Incomplete reconciled transcription")
            for page in source_pages:
                transcription_text = str(page.get("transcription") or "")
                transcription_text = re.sub(
                    r"\$\$(.+?)\$\$",
                    lambda match: "\\[" + match.group(1).strip() + "\\]",
                    transcription_text,
                    flags=re.DOTALL,
                )
                transcription_text = re.sub(
                    r"(?<!\$)\$(?!\$)([^$\n]+?)(?<!\$)\$(?!\$)",
                    lambda match: "\\(" + match.group(1).strip() + "\\)",
                    transcription_text,
                )
                page["transcription"] = transcription_text

            coverage_plan = _study_source_page_coverage_plan(source_pages)
            audit_card_floor = int(coverage_plan["target_cards"])
            page_card_quotas = {
                str(image_index): quota
                for image_index, quota in coverage_plan["page_quotas"].items()
            }
            coverage_checklist = [
                {
                    "id": item["id"],
                    "image_index": item["image_index"],
                    "text": str(item["text"])[:500],
                    "content_type": item.get("content_type") or "concept",
                    "is_example": bool(item.get("is_example")),
                }
                for item in _study_source_coverage_items(source_pages)
            ]
            correction_rule = (
                "只可修正可由來源已有的標準定義、公式或直接計算毫無歧義地確認的錯誤；遇到這類錯誤時必須保留該觀念、輸出最小必要修正後的正確卡片並填寫 correction，不得以刪卡取代校正。不得為了修正補入來源未使用的新觀念或解法。"
                if allow_corrections
                else "不得修正任何內容；所有 correction.applied 必須是 false，其餘欄位為空字串。"
            )
            organizer_prompt = (
                "你是跨學科的忠實筆記編輯，只能根據下方逐頁轉錄稿整理重點卡。除了 concept 卡 simple_example 可依後述規則做最小代入示範外，不得加入轉錄稿沒有的定義、背景知識、例子、用途、推導、因果關係或專有名詞解釋；"
                "不得把僅被提到的術語另做定義卡。可以改寫語序與合併同一觀念的重複句，但每一句資訊都必須能在轉錄稿找到。"
                "每張卡只處理一個原筆記正在記錄的觀念，concept 使用不超過 18 個中文字的短標題。recall_cue 是揭示內容前的回想線索，以 2 至 4 個來源已有的關鍵詞呈現，可使用『條件 → 關係 → 結論』等短結構，但不能直接洩露完整答案、不能使用問號，也不能寫成考題。core_summary 用 1 至 2 個短句或一個完整公式直接寫出這張卡最需要記住的結論。explanation 再補足成立條件、觀念脈絡與來源確實寫出的最短推導；通常 2 至 4 個短句即可。"
                "reasoning_steps 只在來源確實包含推導、程序或解題步驟時填入 2 至 4 個可重用短步驟，純定義卡輸出空陣列。common_confusion 只有來源明確比較兩個觀念、指出易錯處，或允許校正且有直接可驗證錯誤時才填寫，否則輸出空字串；不可自行猜測學生會錯在哪裡。"
                "若來源是例題、範例或算例，card_type 必須為 example；一般觀念卡為 concept。example 卡的 example_problem 必須寫成一個可直接作答的具體題目示例，完整保留必要的數值、向量、矩陣、函數、條件與明確要求，不可只寫『判斷某性質』卻省略實際題設；example_method 只用 1 至 2 個短句寫來源實際採用、可重複使用的核心判斷或策略，不得編號或逐步列舉；reasoning_steps 再列必要操作。題目、方法與步驟不可混寫或重複。example 卡的 simple_example 必須是空字串。"
                "每張 concept 卡都要填 simple_example：優先整理來源中原有的最短例子；若來源沒有例子，只能把該卡來源已寫出的定義或公式代入最簡單的數值、符號或情境，做 1 至 2 句的最小示範。不得引入新定理、新術語、新條件、不同題型或來源外的解法，也不能只是重複定義。concept 卡的 example_problem 與 example_method 必須是空字串。"
                "例題卡可刪除不影響作答的冗長背景，但不得刪掉具體題設、必要數值、給定式或要求，也不可只給最終答案。不得把偶然數值寫進 core_summary。只有用來否定性質的最小反例，才可保留證明失敗所必需的數值。"
                "例題方法可以把來源中反覆使用或清楚展示的操作抽象成一般步驟，但每個步驟都必須能由該例題的 source_refs 支持；不可補入來源沒用到的定理、捷徑或新解法。若來源只有題目和答案、沒有可辨識過程，就不要臆造解法。"
                "刪除教學口吻、重要性說明、驗證過程、重複結論、公式的文字重述、同義改寫、延伸提醒與『換句話說』『這表示』『可以看出』『值得注意』等填充語。"
                "所有文字欄位與 summary 都必須直接陳述知識，禁止使用『筆記給出』『筆記註明』『筆記記載／紀載』『筆記指出』『筆記中提到』『根據筆記可知』『筆記的重點是』或任何同義前綴。不要考題，也不要提到原文、來源、OCR、核對或修正過程。輸出前逐欄自檢；只要出現這類來源敘事，就重新改寫成直接知識陳述後才輸出。"
                "影像中清楚可辨的公式、方程式、反應式、統計式、程式碼或其他關鍵結構都要保留；卡片數量不設上限。由你依複習目標判斷拆分或合併，但不可漏掉來源中的核心內容。coverage_checklist 中每一個 is_example=true 的獨立例題、範例、反例、練習題、題目或英文 Example/Exercise/Problem 都必須各自出現在一張 card_type=example 卡中；不可只把題目塞進 concept 卡的 explanation 或 simple_example，也不可把兩個有不同題設的例題合成一張。題目若只有題設沒有解法，仍要建立例題卡，example_method 誠實填寫『來源未提供完整解法』，不得自行補解。"
                f"先按頁建立完整內容清單，再依實際資訊量分配卡片。建議總卡數至少約 {audit_card_floor} 張、各頁資訊量參考值為 {json.dumps(page_card_quotas, ensure_ascii=False)}，但不是固定張數。優先保留每個清楚公式、定義、方法、例題策略與結論；同一觀念的成對定義、同方法例題或同一定理下的緊密內容可由你合併，一張卡若包含不同複習目標則應拆開，不得為湊張數重複寫卡。"
                f"coverage_checklist={json.dumps(coverage_checklist, ensure_ascii=False, separators=(',', ':'))}。priority=required 的區塊應優先進入卡片；supporting 區塊可在不破壞單一卡片主題的前提下併入 explanation。is_example=true 的區塊是不可合併遺漏的逐題清單，coverage_ids 與 source_refs 必須真實對應，不可虛報。"
                "所有公式與結構化符號一律使用 \\( ... \\) 或 \\[ ... \\] 包住的 LaTeX；長公式用 aligned 合理換行。矩陣與向量必須使用可渲染的 matrix/bmatrix/pmatrix 環境，欄之間用 &，列之間用 \\\\，不可把多個分量直接黏在同一格。不要使用裸露的 $...$、$$...$$ 或只寫線性純文字公式。普通文字保持自然繁體中文。memory_hint 只有原文存在明確記憶線索時才填寫，否則輸出空字串。"
                "每張卡必須提供 1 至 4 個 source_refs。緊密相關的連續內容可由同一卡引用多段，讓卡片在不混雜不同觀念的前提下保留更多資訊。evidence 必須逐字複製對應頁 transcription 中一段連續、且能直接在原圖看到的文字，連空白與 LaTeX 都不要改，作為可程式比對的來源。evidence 禁止改寫、摘要或描述頁面位置與圖形；不得輸出『頁中列出』『右側原稿示意』『下方有例子』等非原文字句。若卡片跨兩段，分成兩個 source_refs，不可自行寫一段銜接敘述。"
                "source_pages 的 uncertain_fragments 若標為『已補全』且信心為高或中，代表該缺字已由第二輪模型依局部上下文獨立核對，可將 transcription 中補全後的連續文字正常整理成卡片；標為『未補全』或仍含〔無法推定〕的片段不得作為關鍵事實。不要在卡片正文提到補全過程。"
                "整理時盡量沿用轉錄稿原本的名詞、短語、變數、條件排列與公式，不要為了流暢改成課本式同義說法。只有字元辨識不清、前後自相矛盾、公式結構不可能成立或可由來源直接驗算出錯時，才做最小必要補全或校正。每張卡的 search_keywords 保留 3 至 8 個最可能被使用者回想起來搜尋的原文詞、專有名詞、縮寫、變數組合或公式名稱；只能取自該卡內容、source_refs 或已完成的高信心校正，不得加入來源外同義詞。"
                "topic 必須是科目底下精確的細分觀念，例如『線性映射判定』『像與反像』『直和與基底』『矩陣可逆性』；禁止直接使用線性代數、離散數學、資料結構、演算法、作業系統、計算機組織等科目名稱，也不要使用『其他』『綜合重點』『課堂筆記』等空泛名稱。"
                "依內容自然分成 3 至 8 個細分主題；同一 topic 的卡片應共享明確觀念脈絡，彼此不同的定義、方法或章節必須拆開。topic 只能表達一個觀念群組，不得為湊數而用斜線、頓號或『與』串接像／反像、直和、線性判定等無直接從屬關係的分類。related_concepts 最多 2 個，只連結本批卡片中明確有推導、比較或前置關係者。summary 最多 5 個完整短句，只列最後保留的核心結論，不重複推導與例子，不可新增資訊，也不可在句中截斷。"
                + correction_rule
                + "\n\n逐頁忠實轉錄稿：\n"
                + json.dumps(source_pages, ensure_ascii=False, separators=(",", ":"))
            )
            if progress_callback:
                progress_callback(52, "符號核對完成，正在只依原文整理重點卡。")
            draft = _call_openai_json(
                name="study_recall_grounded_draft",
                schema=card_schema,
                content=[{"type": "input_text", "text": organizer_prompt}],
                timeout=300,
                max_output_tokens=20000,
            )
            verifier_prompt = (
                "你是跨學科筆記忠實度審核員。請重新查看隨附原始圖片，逐句核對 draft、source_pages 與圖片，輸出修訂後的完整 JSON。"
                "圖片是最終依據：先修正 OCR 對係數、正負號、上下標、矩陣分量或 LaTeX 的誤讀，再確認卡片與原圖一致。"
                "逐一核對名稱、數值、單位、符號、範圍、條件、例外、表格欄位與步驟；一般知識卡的具體內容必須原樣保留，不可改成更一般或更特殊的形式。例題方法卡可以省略非必要題目數值並抽出來源已展示的解題流程，但不得改變方法成立的條件或增加來源沒有的步驟。"
                "對來源中明確寫出的公式、方程式、反應式、統計式、程式碼或推導做相應的內部一致性檢查；若結果與來源不相容，依原圖修正，原圖本身確實錯誤且允許校正時才建立 correction。不要把數學驗證規則套用到沒有公式的科目。"
                "刪除任何無法由來源直接支持的句子、卡片、口訣或關聯；不得自行補上更完整的課本知識。"
                "每個 source_refs.evidence 必須仍是對應 transcription 中逐字連續出現、而且能直接在原圖看到的片段，錯頁或不完全相同就修正引用。evidence 不得以括號描述頁面位置、圖示、照片、顏色、原稿或內容大意；遇到這類非原文字句要改成真正可見的連續文字。已由上下文唯一補全且列有高／中信心紀錄的文字可正常保留；只有仍含〔無法推定〕或沒有可靠來源的卡片才刪除。"
                "正文與 search_keywords 都應優先保留原筆記實際使用的詞彙、記號和條件順序；不要用外部同義詞取代。search_keywords 只保留能在卡片、source_refs 或高信心校正結果中找到的搜尋詞。"
                "保留清楚可辨的原筆記公式。correction 只有在錯誤毫無歧義且允許校正時才能保留，否則恢復原文或刪除該項。"
                "再次排除重複句、公式的文字重述、教學口吻與不影響複習的補充。所有卡片文字欄位與 summary 只要仍含『筆記給出／註明／記載／指出／提到』及其同義寫法，就視為審核不合格並改寫為直接知識陳述。recall_cue 不得洩露 core_summary，也不得寫成問題；reasoning_steps 與 common_confusion 沒有直接來源支持時必須留空。summary 只能用最多 5 個短句直接摘要核心結論。不要輸出審核說明。"
                "另外檢查所有例題卡：card_type 必須為 example，example_problem 必須包含一個具體、可直接作答的完整題目示例與明確要求，example_method 只留來源真正使用的可重用判斷，reasoning_steps 只留操作順序，simple_example 留空。三者不可互相重複，也不能創造新技巧。每張 concept 卡的 simple_example 都必須是簡短具體示範；若原文沒有例子，只允許對來源已有定義或公式做最小數值或符號代入，不得補充來源外知識。"
                "所有聲稱某性質不成立的例題都必須重新驗算前提與運算：測試輸入若不符合該性質的前提、計算錯誤，或實際上反而滿足該性質，就不能當成反例。來源清楚且允許校正時，必須依來源已有的映射、定義與直接計算修正成正確卡片並記入 correction；只有原圖與前後文仍無法唯一推定時才略過。"
                "先逐段清點 source_pages 與圖片中的標題、定義、公式、例題方法與結論；draft 漏掉但圖片或高／中信心的上下文補全仍足以確認核心觀念時，必須補回卡片。局部字跡不清不代表整段都要刪除；先利用重複記號、句法、公式結構與相鄰推導補全，只略過仍有多種合理結果且會影響正確性的字元。來源中清楚可辨但結論錯誤的觀念必須修正後補回，不得視為無來源。"
                f"各頁資訊量參考值為 {json.dumps(page_card_quotas, ensure_ascii=False)}。逐頁檢查 source_refs，優先讓有公式、定義、方法或例題策略的頁面得到代表卡；一般補充內容可併入相關卡片，不得用無關卡片虛報引用。"
                f"逐一核對 coverage_checklist={json.dumps(coverage_checklist, ensure_ascii=False, separators=(',', ':'))}；priority=required 的 id 優先保留，supporting id 可合併；coverage_ids 的來源 evidence 仍必須與該段重疊。"
                "逐張檢查 topic：不得等於任何科目名稱或空泛大分類，必須改成能描述該卡核心內容的單一細分觀念；明顯不同章節或方法不得共用同一 topic，禁止用斜線、頓號或『與』拼接互不從屬的分類。依全部內容整理成 3 至 8 個群組。"
                f"\nallow_corrections={str(allow_corrections).lower()}"
                "\nsource_pages=" + json.dumps(source_pages, ensure_ascii=False, separators=(",", ":"))
                + "\ndraft=" + json.dumps(draft, ensure_ascii=False, separators=(",", ":"))
            )
            if progress_callback:
                progress_callback(70, "正在逐句核對來源，移除無依據的延伸內容。")
            verifier_content: List[Dict[str, Any]] = [{"type": "input_text", "text": verifier_prompt}]
            for _filename, image_bytes, mime_type in images:
                verifier_content.append(
                    {
                        "type": "input_image",
                        "image_url": f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}",
                        "detail": "high",
                    }
                )
            verified = _call_openai_json(
                name="study_recall_grounded_verified",
                schema=card_schema,
                content=verifier_content,
                timeout=300,
                max_output_tokens=20000,
            )
            formula_audit_instruction = (
                "你是最後一道跨學科內容與忠實度稽核。根據原始圖片、source_pages 與 verified，輸出通過稽核的完整 JSON，不要輸出稽核過程。"
                "逐卡檢查內容是否能由原圖直接支持，並檢查名稱、數值、單位、條件、範圍、符號、步驟、表格與結論是否一致。原圖有公式、方程式、反應式、統計式或程式碼時，才對其做相應的結構與內部一致性檢查；沒有這些內容時不要自行補公式。"
                "所有推導、計算、分類、因果或比較都必須符合來源中明示的前提。來源本身清楚但內容有誤，且 allow_corrections=true 時，必須做最小必要修正、保留為正確卡片並記入 correction，不得以刪卡代替校正；只有結果完全沒有來源支持時才能刪除。"
                "重新對照圖片，OCR 與圖片不同時以圖片為準；這類 OCR、上下文補字、漏字、標點或 LaTeX 排版修復不算筆記內容校正，不得建立 correction。correction 只記錄原圖筆記本身可直接驗證的知識、公式、計算或結論錯誤。若某個文字、數字或符號無法直接辨認，先以同頁前後文、重複記號、公式結構與相鄰推導做最小補全；已被唯一推定且有高／中信心紀錄時保留。仍有多種合理結果時才刪除受影響的卡片，不可在卡片中寫模糊或待確認說明。"
                "除例題方法卡可從來源已展示的操作整理成可重用步驟外，禁止在具體內容與一般內容之間擅自轉換，禁止加入課本延伸、跨科聯想或來源沒有的教學解釋。方法卡只能描述 source_refs 實際出現的判斷與操作，不得替操作新增來源沒寫的理論名稱、資料結構、演算法、定理、空間分類、證明或另一套解法。"
                "同時刪除原筆記未明說的通則、額外定義、延伸例子與教學詮釋；唯一例外是 concept 卡 simple_example 可對來源既有定義或公式做最小數值／符號代入，不得因此產生新知識。source_refs.evidence 仍必須逐字存在於對應 transcription，並且是原圖可見字元，不得是頁面位置、圖示或內容大意的描述；"
                "優先保留來源原本的專有名詞、關鍵短語、變數與條件順序，只做最小必要的模糊字補全或可直接驗證校正。search_keywords 必須是可從卡片、source_refs 或高信心校正結果直接找到的原始搜尋詞，不得自行擴充同義詞。"
                "若修正結果是由來源中的明確內容直接得到，可引用包含該內容的原文。卡片正文只寫可複習的來源內容，絕對不可提到筆記、原稿、OCR、核對、稽核或修正過程；"
                "每張卡只輸出一個核心觀念所需的最短完整內容：recall_cue 提供不洩漏答案的關鍵詞，core_summary 放最需要記住的結論，explanation 放條件與脈絡，reasoning_steps 只放來源已有的必要推導或操作。不得輸出重複結論、驗證代回、同義重述、重要性說明與教學填充語。所有文字欄位與 summary 禁止出現『筆記給出』『筆記註明』『筆記記載／紀載』『根據筆記』『原文指出』等來源敘事；發現時必須先改寫，不能原樣輸出。"
                "例題卡的 card_type 必須為 example，並把具體且可直接作答的完整必要題設、可重用解法、操作順序分別寫入 example_problem、example_method、reasoning_steps，simple_example 留空；可刪冗長背景但不可省略數值、給定式、條件或要求。若來源沒有足夠過程可整理方法，仍保留該例題卡，example_method 填『來源未提供完整解法』，不可臆造解法。一般卡為 concept，example_problem 與 example_method 留空，simple_example 必須是來源例子或只對來源已有定義／公式所做的最小代入示範。"
                "對每個反例或性質判定例，逐項驗證輸入是否符合欲檢查性質的前提，並重新計算映射結果；來源清楚且 allow_corrections=true 時，錯誤反例、錯誤等號或錯誤結論必須校正並記錄，不能刪除該觀念。只有影像與前後文都不足以唯一判定正確內容時才略過。"
                f"稽核前先建立 source_pages 的內容清單，逐段比對 verified；依目前資訊量，本批建議至少約 {audit_card_floor} 張互不重複的候選卡片，但卡片數量不設上限，也不可為達成張數拆出空泛卡。圖片或高／中信心補全仍可確認核心定義、公式、方法或結論的段落若遭漏掉，應優先補回；同一張卡若混入可各自複習的獨立定義、方法或章節，才需要拆卡。局部不清先做最小上下文補全，不得刪除其餘可確認觀念；若清楚觀念本身寫錯且允許校正，補回修正後的正確卡片。"
                f"各頁資訊量參考值為 {json.dumps(page_card_quotas, ensure_ascii=False)}。逐頁計數 source_refs，優先補回缺頁的公式、定義、方法、例題策略與結論；一般補充段落可以併入同觀念卡。不可用與該頁無關的卡片虛報引用。"
                f"輸出前再核對 coverage_checklist={json.dumps(coverage_checklist, ensure_ascii=False, separators=(',', ':'))}；priority=required 的區塊優先進入卡片，supporting 區塊可合併；coverage_ids 與 evidence 必須實際對應。"
                "topic 必須是科目內的單一細分觀念，不得使用六科科目名稱、整份筆記標題或『綜合重點』等空泛文字，也不得用斜線、頓號或『與』把無直接從屬關係的分類硬併在一起；依最後保留卡片整理成 3 至 8 個群組。"
                "修正過程只放在 correction。summary 最多 5 個完整短句，只可摘要最後保留的卡片，禁止句中截斷或用空公式結尾。"
                f"\nallow_corrections={str(allow_corrections).lower()}"
            )
            if progress_callback:
                progress_callback(78, "正在驗證上下文補全與各科內容一致性，排除仍無法判定的片段。")
            verified_cards = [
                card
                for card in (verified.get("key_concepts") or [])
                if isinstance(card, dict)
            ]

            def audit_formula_card_batch(
                batch: List[Dict[str, Any]],
                *,
                batch_label: str,
            ) -> List[Dict[str, Any]]:
                _raise_if_study_upload_cancelled()
                if not batch:
                    return []
                relevant_indices = sorted(
                    {
                        int(source_ref.get("image_index") or 0)
                        for card in batch
                        for source_ref in (card.get("source_refs") or [])
                        if isinstance(source_ref, dict)
                        and 1 <= int(source_ref.get("image_index") or 0) <= len(images)
                    }
                )
                if not relevant_indices:
                    relevant_indices = list(range(1, len(images) + 1))
                relevant_pages = [
                    page
                    for page in source_pages
                    if int(page.get("image_index") or 0) in relevant_indices
                ]
                batch_payload = {
                    "detected_topic": str(verified.get("detected_topic") or "")[:80],
                    "summary": str(verified.get("summary") or "")[:900],
                    "key_concepts": batch,
                }
                batch_schema = json.loads(json.dumps(card_schema))
                batch_schema["properties"]["key_concepts"]["minItems"] = 0
                batch_schema["properties"]["key_concepts"]["maxItems"] = len(batch)
                batch_prompt = (
                    formula_audit_instruction
                    + "\n這是完整卡片集合中的獨立稽核批次。只能核對、最小修正或刪除 batch_cards 已有卡片；"
                    "不得新增其他卡片、不得補做其他頁內容、不得把兩張卡合成一張。輸出順序必須保持與 batch_cards 相同。"
                    f"\nbatch_label={batch_label}"
                    "\nsource_pages="
                    + json.dumps(relevant_pages, ensure_ascii=False, separators=(",", ":"))
                    + "\nbatch_cards="
                    + json.dumps(batch_payload, ensure_ascii=False, separators=(",", ":"))
                )
                batch_content: List[Dict[str, Any]] = [
                    {"type": "input_text", "text": batch_prompt}
                ]
                for image_index in relevant_indices:
                    _filename, image_bytes, mime_type = images[image_index - 1]
                    batch_content.extend(
                        [
                            {
                                "type": "input_text",
                                "text": f"此圖是 source_pages 的 image_index={image_index}。",
                            },
                            {
                                "type": "input_image",
                                "image_url": f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}",
                                "detail": "high",
                            },
                        ]
                    )
                try:
                    result = _call_openai_json(
                        name=f"study_recall_formula_audited_{batch_label}",
                        schema=batch_schema,
                        content=batch_content,
                        timeout=300,
                        reasoning_effort="medium",
                        max_output_tokens=max(4200, min(10000, len(batch) * 1800)),
                    )
                    output_cards = [
                        card
                        for card in (result.get("key_concepts") or [])
                        if isinstance(card, dict)
                    ]
                    if len(output_cards) <= len(batch):
                        return output_cards
                    raise ValueError("Formula audit returned extra cards")
                except (requests.RequestException, ValueError, TypeError) as exc:
                    if len(batch) > 1:
                        midpoint = max(1, len(batch) // 2)
                        app.logger.warning(
                            "Formula audit batch %s failed; splitting %s cards: %s",
                            batch_label,
                            len(batch),
                            exc,
                        )
                        return audit_formula_card_batch(
                            batch[:midpoint],
                            batch_label=f"{batch_label}a",
                        ) + audit_formula_card_batch(
                            batch[midpoint:],
                            batch_label=f"{batch_label}b",
                        )
                    app.logger.warning(
                        "Formula audit for one card failed; preserving verified card %s: %s",
                        str(batch[0].get("concept") or "")[:80],
                        exc,
                    )
                    return list(batch)

            audited_cards: List[Dict[str, Any]] = []
            formula_batches = [
                verified_cards[index : index + 6]
                for index in range(0, len(verified_cards), 6)
            ]
            for batch_index, batch in enumerate(formula_batches, start=1):
                audited_cards.extend(
                    audit_formula_card_batch(
                        batch,
                        batch_label=f"batch_{batch_index}",
                    )
                )
                if progress_callback:
                    progress_callback(
                        78 + round(batch_index / max(1, len(formula_batches)) * 2),
                        f"已完成第 {batch_index}/{len(formula_batches)} 批內容稽核。",
                    )
            audited = {
                "detected_topic": str(verified.get("detected_topic") or "")[:80],
                "summary": str(verified.get("summary") or "")[:900],
                "key_concepts": audited_cards or verified_cards,
            }
            _enrich_study_card_coverage_ids(audited, source_pages)
            initial_coverage_metrics = _study_recall_coverage_metrics(audited, source_pages)
            if _study_recall_coverage_needs_repair(audited, source_pages):
                coverage_gaps = _study_recall_coverage_gaps(audited, source_pages)
                if progress_callback:
                    progress_callback(
                        81,
                        "正在補強缺少的公式、定義、方法與例題策略，已完成內容會完整保留。",
                    )
                _raise_if_study_upload_cancelled()
                coverage_repair_schema = json.loads(json.dumps(card_schema))
                coverage_repair_prompt = (
                    "你是筆記資訊補強編輯。current_cards 已通過原圖內容審核；請完整保留其中正確、互不重複的卡片，只補強 coverage_gaps 中真正重要的遺漏內容並輸出完整 JSON。example_items 是逐題不可遺漏清單；其中每個 id 都必須新增或修正為一張獨立的 card_type=example 卡，不能以一般 concept 卡代替，也不能把不同 id 合併。"
                    "只能使用 source_pages 與原圖已有內容，不得補充課本知識。priority=required 的公式、定義、方法、例題策略與結論優先補回；supporting 一般敘述可併入最相關卡片，不必獨立成卡。"
                    "同一觀念的成對定義、連續推導或同方法例題可以合併成資訊完整的一張卡；不同章節或不同複習目標才拆卡。卡片數量不設上限，由你依來源資訊與複習目標決定。"
                    "source_refs.evidence 必須逐字連續存在於對應頁 transcription，並且能在原圖直接看到，不得用頁面位置、圖示或內容大意取代原文；coverage_ids 只能標記該卡實際整理的區塊，不可用無關卡片虛報。"
                    "例題必須使用 card_type=example，example_problem 要保留可直接作答的具體題設、數值／給定式、條件與要求，並和可重用解法 example_method、操作 reasoning_steps 分欄；simple_example 留空。若來源未提供解法，example_method 填『來源未提供完整解法』，不可臆造。一般卡使用 card_type=concept，兩個 example 欄位留空，simple_example 必須提供來源例子或只對來源既有定義／公式做最小代入的短示範。"
                    "卡片正文不得出現來源敘事、稽核說明、外部延伸或來源沒有的術語。錯誤只做可由原圖內容直接驗證的最小修正。不可因卡片數量而刪除重點。"
                    f"\nallow_corrections={str(allow_corrections).lower()}"
                    f"\npage_information_targets={json.dumps(coverage_plan['page_quotas'], ensure_ascii=False)}"
                    f"\ncurrent_coverage_metrics={json.dumps(initial_coverage_metrics, ensure_ascii=False, separators=(',', ':'))}"
                    f"\ncoverage_gaps={json.dumps(coverage_gaps, ensure_ascii=False, separators=(',', ':'))}"
                    f"\ncoverage_checklist={json.dumps(coverage_checklist, ensure_ascii=False, separators=(',', ':'))}"
                    "\nsource_pages=" + json.dumps(source_pages, ensure_ascii=False, separators=(",", ":"))
                    + "\ncurrent_cards=" + json.dumps(audited, ensure_ascii=False, separators=(",", ":"))
                )
                coverage_repair_content: List[Dict[str, Any]] = [
                    {"type": "input_text", "text": coverage_repair_prompt}
                ]
                for _filename, image_bytes, mime_type in images:
                    coverage_repair_content.append(
                        {
                            "type": "input_image",
                            "image_url": f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}",
                            "detail": "high",
                        }
                    )
                try:
                    repaired_audited = _call_openai_json(
                        name="study_recall_coverage_repair",
                        schema=coverage_repair_schema,
                        content=coverage_repair_content,
                        timeout=240,
                        reasoning_effort="medium",
                        max_output_tokens=16000,
                    )
                    _enrich_study_card_coverage_ids(repaired_audited, source_pages)
                    repaired_metrics = _study_recall_coverage_metrics(repaired_audited, source_pages)
                    initial_rank = (
                        initial_coverage_metrics["example_ratio"],
                        initial_coverage_metrics["required_ratio"],
                        initial_coverage_metrics["page_ratio"],
                        initial_coverage_metrics["quality_score"],
                        initial_coverage_metrics["overall_ratio"],
                    )
                    repaired_rank = (
                        repaired_metrics["example_ratio"],
                        repaired_metrics["required_ratio"],
                        repaired_metrics["page_ratio"],
                        repaired_metrics["quality_score"],
                        repaired_metrics["overall_ratio"],
                    )
                    if repaired_rank > initial_rank:
                        audited = repaired_audited
                    else:
                        app.logger.warning(
                            "Study-note coverage repair did not improve reliable coverage; preserving original cards: before=%s after=%s",
                            initial_coverage_metrics,
                            repaired_metrics,
                        )
                except (requests.RequestException, ValueError, TypeError) as exc:
                    app.logger.warning(
                        "Study-note coverage repair failed; preserving the already validated cards: %s",
                        exc,
                    )

            missing_example_items = list(
                _study_recall_coverage_gaps(audited, source_pages).get("example_items") or []
            )
            if missing_example_items:
                if progress_callback:
                    progress_callback(
                        82,
                        f"正在逐題補回 {len(missing_example_items)} 個遺漏例題，並重新核對數字符號。",
                    )
                existing_titles = [
                    str(card.get("concept") or "")[:80]
                    for card in audited.get("key_concepts") or []
                    if isinstance(card, dict) and str(card.get("concept") or "").strip()
                ]
                for missing_index, missing_item in enumerate(missing_example_items, start=1):
                    _raise_if_study_upload_cancelled()
                    target_id = str(missing_item.get("id") or "")
                    try:
                        target_image_index = int(missing_item.get("image_index") or 0)
                    except (TypeError, ValueError):
                        continue
                    if not target_id or not (1 <= target_image_index <= len(images)):
                        continue
                    relevant_page = next(
                        (
                            page
                            for page in source_pages
                            if int(page.get("image_index") or 0) == target_image_index
                        ),
                        None,
                    )
                    if not relevant_page:
                        continue
                    recovery_schema = json.loads(json.dumps(card_schema))
                    recovery_schema["properties"]["key_concepts"]["minItems"] = 1
                    recovery_schema["properties"]["key_concepts"]["maxItems"] = 1
                    recovery_prompt = (
                        "你是遺漏例題的逐題補卡員。只處理 missing_example，不得重寫、刪除或合併既有卡片。"
                        "輸出恰好一張 card_type=example 卡，coverage_ids 必須只包含 missing_example.id。"
                        "example_problem 要完整保留可直接作答所需的題號、所有數字、係數、向量、矩陣、函數、"
                        "條件與要求；example_method 與 reasoning_steps 只整理來源實際出現的解法。"
                        "若來源只有題目沒有解法，example_method 填『來源未提供完整解法』，不可自行補解。"
                        "請把完整原圖、彩色銳化放大版與灰階高對比放大版交叉比較，逐一確認 0/6/8/9、1/7、3/5、"
                        "正負號、小數點、分數線、括號、上下標、矩陣元素與運算符；增強圖有衝突時以完整原圖和公式"
                        "內部一致性為準。source_refs.evidence 必須逐字連續存在於 page.transcription。"
                        "其餘欄位遵守既有卡片 schema；不得增加來源外知識、另一種解法或不在圖片中的數值。"
                        f"\nallow_corrections={str(allow_corrections).lower()}"
                        f"\nmissing_example={json.dumps(missing_item, ensure_ascii=False, separators=(',', ':'))}"
                        f"\npage={json.dumps(relevant_page, ensure_ascii=False, separators=(',', ':'))}"
                        f"\nexisting_titles={json.dumps(existing_titles, ensure_ascii=False, separators=(',', ':'))}"
                    )
                    _filename, image_bytes, mime_type = images[target_image_index - 1]
                    recovery_content: List[Dict[str, Any]] = [
                        {"type": "input_text", "text": recovery_prompt},
                        {
                            "type": "input_image",
                            "image_url": f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}",
                            "detail": "high",
                        },
                    ]
                    for crop in review_zoom_crops(image_bytes):
                        recovery_content.extend(
                            [
                                {
                                    "type": "input_text",
                                    "text": f"例題數字符號核對用：{crop['label']}。",
                                },
                                {
                                    "type": "input_image",
                                    "image_url": f"data:image/jpeg;base64,{base64.b64encode(crop['bytes']).decode('ascii')}",
                                    "detail": "high",
                                },
                            ]
                        )
                    recovered_card: Optional[Dict[str, Any]] = None
                    for recovery_attempt in range(2):
                        try:
                            recovery_result = _call_openai_json(
                                name=f"study_recall_missing_example_{missing_index}_{recovery_attempt + 1}",
                                schema=recovery_schema,
                                content=recovery_content,
                                timeout=240,
                                reasoning_effort="medium",
                                max_output_tokens=5200,
                            )
                            _enrich_study_card_coverage_ids(recovery_result, source_pages)
                            candidate = next(
                                (
                                    card
                                    for card in recovery_result.get("key_concepts") or []
                                    if isinstance(card, dict)
                                    and card.get("card_type") == "example"
                                    and target_id in (card.get("coverage_ids") or [])
                                    and str(card.get("example_problem") or "").strip()
                                ),
                                None,
                            )
                            if candidate is not None:
                                recovered_card = candidate
                                break
                        except (requests.RequestException, ValueError, TypeError) as exc:
                            app.logger.warning(
                                "Missing example recovery %s attempt %s failed: %s",
                                target_id,
                                recovery_attempt + 1,
                                exc,
                            )
                    if recovered_card is not None:
                        audited.setdefault("key_concepts", []).append(recovered_card)
                        existing_titles.append(str(recovered_card.get("concept") or "")[:80])
                    else:
                        app.logger.warning(
                            "Missing example recovery remained incomplete for %s",
                            target_id,
                        )
                    if progress_callback:
                        progress_callback(
                            82 + round(missing_index / max(1, len(missing_example_items)) * 2),
                            f"已完成 {missing_index}/{len(missing_example_items)} 個遺漏例題的逐題核對。",
                        )
                _enrich_study_card_coverage_ids(audited, source_pages)

            if not _study_recall_page_coverage_met(audited, source_pages):
                app.logger.warning(
                    "Study-note coverage has non-blocking gaps; preserving reliable cards: %s",
                    _study_recall_coverage_metrics(audited, source_pages),
                )

            card_indices: List[int] = []
            example_indices: List[int] = []
            for index, card in enumerate(audited.get("key_concepts") or []):
                if not isinstance(card, dict):
                    continue
                card_indices.append(index)
                evidence_text = " ".join(
                    str(source_ref.get("evidence") or "")
                    for source_ref in card.get("source_refs") or []
                    if isinstance(source_ref, dict)
                )
                source_marks_example = bool(
                    re.search(
                        r"(?:\bEx(?:ample)?\s*\.|例題|範例|算例|反例|題目\s*[：:])",
                        evidence_text,
                        flags=re.IGNORECASE,
                    )
                )
                if card.get("card_type") == "example" or source_marks_example:
                    card["card_type"] = "example"
                    example_indices.append(index)

            if card_indices:
                example_schema = {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["cards"],
                    "properties": {
                        "cards": {
                            "type": "array",
                            "minItems": len(card_indices),
                            "maxItems": len(card_indices),
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": [
                                    "concept_index",
                                    "card_type",
                                    "simple_example",
                                    "example_problem",
                                    "example_method",
                                ],
                                "properties": {
                                    "concept_index": {"type": "integer", "minimum": 0, "maximum": max(card_indices)},
                                    "card_type": {"type": "string", "enum": ["concept", "example"]},
                                    "simple_example": {"type": "string", "maxLength": 360},
                                    "example_problem": {"type": "string", "maxLength": 360},
                                    "example_method": {"type": "string", "maxLength": 280},
                                },
                            },
                        }
                    },
                }
                example_catalog = [
                    {
                        "concept_index": index,
                        "concept": audited["key_concepts"][index].get("concept") or "",
                        "card_type": audited["key_concepts"][index].get("card_type") or "concept",
                        "core_summary": audited["key_concepts"][index].get("core_summary") or "",
                        "explanation": audited["key_concepts"][index].get("explanation") or "",
                        "current_simple_example": audited["key_concepts"][index].get("simple_example") or "",
                        "current_problem": audited["key_concepts"][index].get("example_problem") or "",
                        "current_method": audited["key_concepts"][index].get("example_method") or "",
                        "current_steps": audited["key_concepts"][index].get("reasoning_steps") or [],
                        "source_evidence": [
                            source_ref.get("evidence") or ""
                            for source_ref in audited["key_concepts"][index].get("source_refs") or []
                            if isinstance(source_ref, dict)
                        ],
                    }
                    for index in card_indices
                ]
                example_prompt = (
                    "你是重點卡例子格式編輯。只根據 source_evidence 與卡片既有內容，替每張卡整理一個清楚、短而具體的示例，不得加入來源外的知識。"
                    "card_type=example 的卡不可改成 concept。example_problem 必須是一個拿到後可以直接開始作答的具體題目示例：保留所有必要數值、向量、矩陣、函數、給定式、條件及明確要求；可以刪除不影響作答的背景敘述，但不可只留下抽象的『判斷是否成立』而沒有實際題設，也不可包含完整運算或最終答案。simple_example 必須留空。"
                    "example_method 用 1 至 2 個短句說明來源實際採用、可重用的判斷或策略，不得編號、不得寫『步驟一』，也不得直接重複 reasoning_steps；"
                    "既有 reasoning_steps 已在前一階段完成，不要在這次輸出重寫。"
                    "card_type=concept 的卡不可改成 example。simple_example 必須用 1 至 2 句給一個最小、具體且能看出觀念如何套用的例子；優先使用 source_evidence 原有例子。來源沒有例子時，只能把 source_evidence 已有的定義或公式代入最簡單的數值、符號或情境，例如替既有變數選小整數後展示公式結果；不得加入新定理、新名詞、新成立條件、新題型或不同解法，也不能只重複 core_summary。example_problem、example_method 與 reasoning_steps 維持原本內容，其中兩個 example 欄位必須留空。"
                    "來源只有最終結果而沒有方法時，不可發明新解法；可把來源明示的直接代入、列式、比較或計算寫成最小方法。"
                    "逐題重新檢查題設前提、函數或映射輸入、維度、正負號、上下標、代入、算術、等號與結論。若用反例否定性質，必須先確認測試值滿足該性質要求的關係；例如檢查齊次性 f(-u)=-f(u) 時，左側輸入必須真的是 -u。"
                    "內容與公式已由前一階段校正；本次只整理 simple_example、example_problem、example_method 三欄，不可重寫其他卡片內容。"
                    "不得加入來源沒有的定理、術語、公式、數值或另一套解法。數學式使用 KaTeX LaTeX，行內用 \\( ... \\)，獨立式用 \\[ ... \\]。"
                    "每個指定 concept_index 必須恰好輸出一次。example 卡的 example_problem 與 example_method 必須非空；concept 卡的 simple_example 必須非空。只輸出 schema JSON。\n\n"
                    f"allow_corrections={str(allow_corrections).lower()}\n"
                    + json.dumps(example_catalog, ensure_ascii=False, separators=(",", ":"))
                )
                prepared_examples: Dict[int, Dict[str, Any]] = {}
                for example_attempt in range(2):
                    try:
                        example_result = _call_openai_json(
                            name="study_recall_card_examples",
                            schema=example_schema,
                            content=[{"type": "input_text", "text": example_prompt}],
                            timeout=240,
                            max_output_tokens=12000,
                        )
                    except (requests.RequestException, ValueError, TypeError) as exc:
                        app.logger.warning(
                            "Study-note example presentation pass failed; preserving usable card content: %s",
                            exc,
                        )
                        if example_attempt == 0:
                            continue
                        break
                    for item in example_result.get("cards") or []:
                        if not isinstance(item, dict):
                            continue
                        concept_index = int(item.get("concept_index", -1))
                        if concept_index not in card_indices or concept_index in prepared_examples:
                            continue
                        expected_type = "example" if concept_index in example_indices else "concept"
                        if item.get("card_type") != expected_type:
                            continue
                        problem = str(item.get("example_problem") or "").strip()
                        method = str(item.get("example_method") or "").strip()
                        simple_example = str(item.get("simple_example") or "").strip()
                        if (
                            (expected_type == "example" and (not problem or not method or simple_example))
                            or (expected_type == "concept" and (not simple_example or problem or method))
                            or (_study_text_quality_issue(problem, max_length=420) if problem else None)
                            or (_study_text_quality_issue(method, max_length=340) if method else None)
                            or (_study_text_quality_issue(simple_example, max_length=420) if simple_example else None)
                        ):
                            continue
                        prepared_examples[concept_index] = {
                            "simple_example": simple_example if expected_type == "concept" else "",
                            "example_problem": problem,
                            "example_method": method,
                        }
                    if set(prepared_examples) == set(card_indices):
                        break
                    missing_examples = sorted(set(card_indices) - set(prepared_examples))
                    example_prompt += (
                        f"\n\n前一次缺少或格式不合格的 concept_index={missing_examples}。"
                        "下一次仍輸出完整 cards 陣列，並確保 example 卡的具體題目與方法、concept 卡的簡單例子都非空且公式格式完整。"
                    )
                if set(prepared_examples) != set(card_indices):
                    missing_examples = sorted(set(card_indices) - set(prepared_examples))
                    app.logger.warning(
                        "Study-note example presentation remained incomplete; preserving existing valid card sections: %s",
                        missing_examples,
                    )
                    for index in missing_examples:
                        current_card = audited["key_concepts"][index]
                        if index in example_indices and not (
                            str(current_card.get("example_problem") or "").strip()
                            and str(current_card.get("example_method") or "").strip()
                        ):
                            current_card["card_type"] = "concept"
                            current_card["example_problem"] = ""
                            current_card["example_method"] = ""
                for index, prepared_example in prepared_examples.items():
                    audited["key_concepts"][index].update(prepared_example)
            latex_schema = {
                "type": "object",
                "additionalProperties": False,
                "required": ["summary", "cards"],
                "properties": {
                    "summary": {"type": "string", "maxLength": 900},
                    "cards": {
                        "type": "array",
                        "minItems": len(audited.get("key_concepts") or []),
                        "maxItems": len(audited.get("key_concepts") or []),
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["concept_index", "concept", "topic", "card_type", "recall_cue", "core_summary", "explanation", "simple_example", "example_problem", "example_method", "reasoning_steps", "common_confusion", "memory_hint", "correction_applied", "correction_original", "correction_reason"],
                            "properties": {
                                "concept_index": {
                                    "type": "integer",
                                    "minimum": 0,
                                    "maximum": max(0, len(audited.get("key_concepts") or []) - 1),
                                },
                                "concept": {"type": "string", "maxLength": 80},
                                "topic": {"type": "string", "maxLength": 48},
                                "card_type": {"type": "string", "enum": ["concept", "example"]},
                                "recall_cue": {"type": "string", "maxLength": 160},
                                "core_summary": {"type": "string", "maxLength": 280},
                                "explanation": {"type": "string", "maxLength": 620},
                                "simple_example": {"type": "string", "maxLength": 360},
                                "example_problem": {"type": "string", "maxLength": 360},
                                "example_method": {"type": "string", "maxLength": 280},
                                "reasoning_steps": {
                                    "type": "array",
                                    "maxItems": 4,
                                    "items": {"type": "string", "maxLength": 180},
                                },
                                "common_confusion": {"type": "string", "maxLength": 180},
                                "memory_hint": {"type": "string", "maxLength": 120},
                                "correction_applied": {"type": "boolean"},
                                "correction_original": {"type": "string", "maxLength": 240},
                                "correction_reason": {"type": "string", "maxLength": 300},
                            },
                        },
                    },
                },
            }
            audited_cards = audited.get("key_concepts") if isinstance(audited, dict) else None
            if not isinstance(audited_cards, list) or not audited_cards:
                raise ValueError("Missing audited cards")
            latex_input = {
                "summary": audited.get("summary") or "",
                "cards": [
                    {
                        "concept_index": index,
                        "concept": item.get("concept") or "",
                        "topic": item.get("topic") or "",
                        "card_type": item.get("card_type") or "concept",
                        "recall_cue": item.get("recall_cue") or "",
                        "core_summary": item.get("core_summary") or "",
                        "explanation": item.get("explanation") or "",
                        "simple_example": item.get("simple_example") or "",
                        "example_problem": item.get("example_problem") or "",
                        "example_method": item.get("example_method") or "",
                        "reasoning_steps": item.get("reasoning_steps") or [],
                        "common_confusion": item.get("common_confusion") or "",
                        "memory_hint": item.get("memory_hint") or "",
                        "source_evidence": [
                            ref.get("evidence") or ""
                            for ref in item.get("source_refs") or []
                            if isinstance(ref, dict)
                        ],
                    }
                    for index, item in enumerate(audited_cards)
                    if isinstance(item, dict)
                ],
            }
            latex_prompt = (
                "你是最後的筆記呈現校對器。先根據每張卡的 source_evidence 判斷它是一般知識還是例題。一般知識卡必須刪除 source_evidence 不支持的補充、證明與術語，只保留來源內容並修正 LaTeX；除下述高信心錯誤校正及 simple_example 的最小代入示範外，不可改變知識內容。例題卡必須同時保留可直接作答的具體題目示例與可重用解題方法：example_problem 保留必要數值、給定式、條件和明確要求，只刪不影響作答的背景敘述及最終答案；example_method 與 reasoning_steps 保留來源實際展示的關鍵判斷與必要操作。"
                "例題方法不得加入 source_evidence 沒有使用的定理、術語或步驟，也不得替來源中的操作命名新的理論、演算法、資料結構、空間分類或證明方式。每張卡完成後逐一比對名詞：來源只有公式或操作時就直接保留公式或操作，不得補上課本分類名稱；例如來源只有 rank(A)=n 或 rank(A)=m，就不可額外稱為滿列秩、滿行秩或滿柱秩。其他科目也使用相同規則。只有最小反例卡可保留否定性質必需的數值。若任何卡片的前提、定義、計算、矩陣維度、等號或結論錯誤，但可由 source_evidence 中已有的定義、公式或直接計算明確判定，allow_corrections=true 時必須做最小必要修正、輸出正確卡片，不得刪除。"
                "修正時 correction_applied=true，correction_original 逐字放入來源中最小的錯誤片段，correction_reason 簡述可直接驗證的原因；未修正時 correction_applied=false 且兩個字串留空。OCR 誤讀、上下文補字、漏字、標點、用詞潤飾與 LaTeX 排版修復不算筆記內容錯誤，correction_applied 必須是 false。不得為修正補入來源沒有使用的新觀念、定理或另一套解法。已由前後文唯一補全且有高／中信心紀錄的文字必須正常保留；只有仍含〔無法推定〕的片段才維持略過結果。"
                "summary 可重新整理成最後保留卡片的核心總結，但不得列出例題的具體答案，也不得加入新知識。concept 改成不含公式、變數、題號或題目數值的簡短純中文名稱，忠實描述原卡核心；topic 可重新整理成科目內精確的細分觀念。recall_cue 只保留來源已有的 2 至 4 個提示關鍵詞，不可使用問號或直接揭露 core_summary。core_summary、explanation、example_problem、example_method、reasoning_steps、common_confusion 與 memory_hint 都只能修正排版，不能補入 source_evidence 沒有的知識；沒有直接來源支持的欄位必須留空。例題維持 card_type=example，將具體可作答的必要題設、可重用解法、操作步驟分欄，simple_example 留空；example_method 只能用 1 至 2 句寫策略，不得包含 1)、2)、『步驟』等逐項內容，也不得重複 reasoning_steps。一般卡維持 concept，example_problem 與 example_method 留空；simple_example 必須保留前一階段的簡短具體示例，並把其中公式修成 LaTeX。若來源沒有原例子，simple_example 只可對 source_evidence 已有定義或公式做最小數值／符號代入，不得加入新知識。"
                "每個數學、離散數學、演算法或計算機科學符號表達都必須使用可由 KaTeX 渲染的 LaTeX：行內一律用 \\( ... \\)，獨立式一律用 \\[ ... \\]。矩陣與向量的每個分量必須分格，欄用 &、列用 \\\\，禁止把兩個分量或兩列直接相連。"
                "包括變數與函數式、集合與邏輯式、上下標、向量、矩陣、映射、等式、不等式、複雜度、機率、求和、遞迴式及所有含運算符的式子。普通中文必須留在 LaTeX 定界符外；禁止輸出 \\(T為線性\\) 這類把中文直接放進數學模式的格式，應寫成 \\(T\\) 為線性。禁止在一組 \\( ... \\) 或 \\[ ... \\] 內再嵌套另一組定界符。"
                "禁止使用 $ 或 $$；禁止留下像 T(a,b)=...、R^2、x_i、rank(A) 這種沒有分隔符的裸露公式。"
                "topic 必須依全部卡片自然整理成 3 至 8 個單一觀念群組；不得等於任何六科科目名稱、整份筆記標題、其他、綜合重點或課堂筆記，也不得用斜線、頓號或『與』把無直接從屬關係的分類硬併在一起。相同知識脈絡共用 topic，明顯不同的定義、方法或章節分開。"
                "cards 的數量、順序與 concept_index 必須完全不變。不要在 explanation 或 summary 中提及修正過程，也不要重複 source_evidence。輸出文字禁止出現『筆記給出』『筆記註明』『筆記記載』『根據筆記』『保留來源內容』『如來源所列』或任何描述整理過程與引用來源的套話，直接陳述觀念。只輸出 schema 指定的 JSON。\n\n待整理內容：\n"
                f"allow_corrections={str(allow_corrections).lower()}。allow_corrections=false 時 correction_applied 必須全部為 false。\n"
                + json.dumps(latex_input, ensure_ascii=False, separators=(",", ":"))
            )
            if progress_callback:
                progress_callback(80, "內容審核完成，正在統一所有公式的 LaTeX 格式。")
            try:
                latex_result = _call_openai_json(
                    name="study_recall_latex_formatted",
                    schema=latex_schema,
                    content=[{"type": "input_text", "text": latex_prompt}],
                    timeout=300,
                    max_output_tokens=16000,
                )
            except (requests.RequestException, ValueError, TypeError) as exc:
                app.logger.warning(
                    "Study-note LaTeX presentation pass failed; using validated card content: %s",
                    exc,
                )
                latex_result = {"summary": audited.get("summary") or "", "cards": []}
            formatted_cards = {
                int(item.get("concept_index")): item
                for item in latex_result.get("cards") or []
                if isinstance(item, dict)
            }
            for index, card in enumerate(audited_cards):
                formatted_cards.setdefault(index, {"concept_index": index, **card})

            def safe_formatted_text(
                primary: Any,
                fallback: Any,
                *,
                max_length: int,
                allow_empty: bool = False,
                normalize_math: bool = True,
            ) -> str:
                for candidate in (primary, fallback):
                    raw = str(candidate or "").strip()
                    if not raw:
                        continue
                    raw_issue = _study_text_quality_issue(raw, max_length=max_length)
                    if raw_issue:
                        continue
                    if not normalize_math:
                        return raw
                    prepared = _normalize_study_math_markup(raw)
                    prepared_issue = _study_text_quality_issue(prepared, max_length=max_length)
                    if not prepared_issue:
                        return prepared
                    # The audited fallback has already passed the strict text and
                    # LaTeX validator. Normalization is a presentation enhancement;
                    # never let a non-idempotent edge case invalidate that content.
                    app.logger.warning(
                        "Study-card math normalization was rejected; preserving validated text "
                        "(raw_length=%s, prepared_length=%s, issue=%s)",
                        len(raw),
                        len(prepared),
                        prepared_issue,
                    )
                    return raw
                if allow_empty:
                    return ""
                raise ValueError("Corrupted study-card text")

            for index, card in enumerate(audited_cards):
                formatted = formatted_cards[index]
                card["topic"] = safe_formatted_text(
                    formatted.get("topic"), card.get("topic"), max_length=80, normalize_math=False
                )
                card["topic"] = _normalize_study_concept_title(
                    card["topic"], audited.get("detected_topic") or "細分觀念"
                )
                card["concept"] = _normalize_study_concept_title(
                    safe_formatted_text(
                        formatted.get("concept"), card.get("concept"), max_length=120, normalize_math=False
                    ),
                    card["topic"],
                )
                card["recall_cue"] = safe_formatted_text(
                    formatted.get("recall_cue"),
                    card.get("recall_cue"),
                    max_length=180,
                    allow_empty=True,
                )
                if not card["recall_cue"]:
                    card["recall_cue"] = f"先回想「{card['concept']}」的條件、核心關係與結論。"
                card["core_summary"] = safe_formatted_text(
                    formatted.get("core_summary"),
                    card.get("core_summary"),
                    max_length=320,
                    allow_empty=True,
                )
                if not card["core_summary"]:
                    card["core_summary"] = card["concept"]
                card["explanation"] = safe_formatted_text(
                    formatted.get("explanation"), card.get("explanation"), max_length=900
                )
                card["card_type"] = (
                    "example"
                    if formatted.get("card_type") == "example" or card.get("card_type") == "example"
                    else "concept"
                )
                card["example_problem"] = safe_formatted_text(
                    formatted.get("example_problem"),
                    card.get("example_problem"),
                    max_length=420,
                    allow_empty=True,
                )
                card["example_method"] = safe_formatted_text(
                    formatted.get("example_method"),
                    card.get("example_method"),
                    max_length=340,
                    allow_empty=True,
                )
                card["simple_example"] = safe_formatted_text(
                    formatted.get("simple_example"),
                    card.get("simple_example"),
                    max_length=420,
                    allow_empty=True,
                )
                if card["card_type"] != "example":
                    card["example_problem"] = ""
                    card["example_method"] = ""
                elif not card["example_problem"] or not card["example_method"]:
                    # Keep older/fallback model outputs usable while ensuring
                    # new strict-schema responses always produce both sections.
                    card["card_type"] = "concept"
                    card["example_problem"] = ""
                    card["example_method"] = ""
                else:
                    card["simple_example"] = ""
                formatted_steps = formatted.get("reasoning_steps")
                fallback_steps = card.get("reasoning_steps")
                primary_steps = formatted_steps if isinstance(formatted_steps, list) else []
                original_steps = fallback_steps if isinstance(fallback_steps, list) else []
                card["reasoning_steps"] = []
                for step_index in range(min(4, max(len(primary_steps), len(original_steps)))):
                    primary_step = primary_steps[step_index] if step_index < len(primary_steps) else ""
                    original_step = original_steps[step_index] if step_index < len(original_steps) else ""
                    prepared_step = safe_formatted_text(
                        primary_step,
                        original_step,
                        max_length=220,
                        allow_empty=True,
                    )
                    if prepared_step:
                        card["reasoning_steps"].append(prepared_step)
                card["common_confusion"] = safe_formatted_text(
                    formatted.get("common_confusion"),
                    card.get("common_confusion"),
                    max_length=240,
                    allow_empty=True,
                )
                card["memory_hint"] = safe_formatted_text(
                    formatted.get("memory_hint"),
                    card.get("memory_hint"),
                    max_length=240,
                    allow_empty=True,
                )
                if bool(formatted.get("correction_applied")):
                    card["correction"] = {
                        "applied": True,
                        "original": str(formatted.get("correction_original") or ""),
                        "corrected": card["explanation"],
                        "reason": str(formatted.get("correction_reason") or ""),
                    }
            audited["summary"] = safe_formatted_text(
                latex_result.get("summary"), audited.get("summary"), max_length=1200
            )

            card_count = len(audited_cards)
            desired_topic_count = min(8, max(3, round(math.sqrt(card_count)) + 1, card_count // 4))
            desired_topic_count = min(card_count, desired_topic_count)
            current_topics = {
                str(card.get("topic") or "").strip()
                for card in audited_cards
                if str(card.get("topic") or "").strip()
            }
            if card_count >= 3 and len(current_topics) != desired_topic_count:
                topic_schema = {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["assignments"],
                    "properties": {
                        "assignments": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["concept_index", "topic"],
                                "properties": {
                                    "concept_index": {
                                        "type": "integer",
                                        "minimum": 0,
                                        "maximum": max(0, card_count - 1),
                                    },
                                    "topic": {"type": "string", "maxLength": 36},
                                },
                            },
                        }
                    },
                }
                topic_catalog = [
                    {
                        "concept_index": index,
                        "concept": card.get("concept") or "",
                        "current_topic": card.get("topic") or "",
                        "core_summary": card.get("core_summary") or "",
                    }
                    for index, card in enumerate(audited_cards)
                ]
                topic_prompt = (
                    "你是筆記章節編輯。請只重新分組下列重點卡，不可改寫卡片內容。"
                    f"必須把 {card_count} 張卡完整分成恰好 {desired_topic_count} 個不重複的細分觀念主題；"
                    "同一概念脈絡、定義與應用、方法與例題應共用主題，不可讓每張卡各自成為一個主題。"
                    "主題名稱用 4 至 14 個繁體中文字直接描述共同觀念，不可含公式、變數、題號、破折號、未閉合括號，"
                    "不可等於線性代數、離散數學、資料結構、演算法、作業系統、計算機組織等科目名稱，也不可使用其他、綜合重點或課堂筆記。"
                    "每個 concept_index 必須恰好出現一次；相同群組的 topic 字串必須逐字完全相同。只輸出 schema 指定的 JSON。\n\n"
                    + json.dumps(topic_catalog, ensure_ascii=False, separators=(",", ":"))
                )
                grouped_topics: Optional[Dict[int, str]] = None
                for topic_attempt in range(1):
                    try:
                        topic_result = _call_openai_json(
                            name="study_recall_topic_groups",
                            schema=topic_schema,
                            content=[{"type": "input_text", "text": topic_prompt}],
                            timeout=180,
                            max_output_tokens=5000,
                        )
                    except (requests.RequestException, ValueError, TypeError) as exc:
                        app.logger.warning(
                            "Study-note topic regrouping failed; preserving existing detailed topics: %s",
                            exc,
                        )
                        break
                    assignments = topic_result.get("assignments") or []
                    candidate_topics: Dict[int, str] = {}
                    for assignment in assignments:
                        if not isinstance(assignment, dict):
                            continue
                        concept_index = int(assignment.get("concept_index", -1))
                        if concept_index in candidate_topics or not 0 <= concept_index < card_count:
                            candidate_topics = {}
                            break
                        topic = _normalize_study_concept_title(assignment.get("topic"), "")
                        if (
                            not topic
                            or topic in STUDY_PLAN_SUBJECTS
                            or _study_text_quality_issue(topic, max_length=36)
                            or any(character in topic for character in ("—", "–", "/", "／"))
                            or topic.count("（") != topic.count("）")
                            or topic.count("(") != topic.count(")")
                        ):
                            candidate_topics = {}
                            break
                        candidate_topics[concept_index] = topic
                    distinct_topics = set(candidate_topics.values())
                    if (
                        len(candidate_topics) == card_count
                        and len(distinct_topics) == desired_topic_count
                    ):
                        grouped_topics = candidate_topics
                        break
                    topic_prompt += (
                        f"\n\n前一次分組不合格。這次必須輸出 {card_count} 筆唯一 concept_index，"
                        f"且 topic 去重後必須恰好是 {desired_topic_count} 個。"
                    )
                if not grouped_topics:
                    app.logger.warning(
                        "Study-note topic regrouping was incomplete; preserving existing detailed topics"
                    )
                else:
                    for index, card in enumerate(audited_cards):
                        card["topic"] = grouped_topics[index]

            def card_integrity_text(card: Dict[str, Any]) -> str:
                return " ".join(
                    str(value or "")
                    for value in (
                        card.get("core_summary"),
                        card.get("explanation"),
                        card.get("simple_example"),
                        card.get("example_problem"),
                        card.get("example_method"),
                        *(card.get("reasoning_steps") or []),
                    )
                )

            invalid_claim_indices = [
                index
                for index, card in enumerate(audited_cards)
                if isinstance(card, dict)
                and _study_has_invalid_negation_counterexample(card_integrity_text(card))
            ]
            if invalid_claim_indices:
                integrity_schema = {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["cards"],
                    "properties": {
                        "cards": {
                            "type": "array",
                            "minItems": len(invalid_claim_indices),
                            "maxItems": len(invalid_claim_indices),
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": [
                                    "concept_index",
                                    "repairable",
                                    "core_summary",
                                    "explanation",
                                    "simple_example",
                                    "example_problem",
                                    "example_method",
                                    "reasoning_steps",
                                    "correction_original",
                                    "correction_reason",
                                ],
                                "properties": {
                                    "concept_index": {
                                        "type": "integer",
                                        "minimum": 0,
                                        "maximum": max(0, len(audited_cards) - 1),
                                    },
                                    "repairable": {"type": "boolean"},
                                    "core_summary": {"type": "string", "maxLength": 280},
                                    "explanation": {"type": "string", "maxLength": 620},
                                    "simple_example": {"type": "string", "maxLength": 360},
                                    "example_problem": {"type": "string", "maxLength": 360},
                                    "example_method": {"type": "string", "maxLength": 280},
                                    "reasoning_steps": {
                                        "type": "array",
                                        "maxItems": 4,
                                        "items": {"type": "string", "maxLength": 180},
                                    },
                                    "correction_original": {"type": "string", "maxLength": 240},
                                    "correction_reason": {"type": "string", "maxLength": 300},
                                },
                            },
                        }
                    },
                }
                integrity_catalog = [
                    {
                        "concept_index": index,
                        "concept": audited_cards[index].get("concept") or "",
                        "current_content": {
                            key: audited_cards[index].get(key) or ([] if key == "reasoning_steps" else "")
                            for key in (
                                "core_summary",
                                "explanation",
                                "simple_example",
                                "example_problem",
                                "example_method",
                                "reasoning_steps",
                            )
                        },
                        "source_evidence": [
                            source_ref.get("evidence") or ""
                            for source_ref in audited_cards[index].get("source_refs") or []
                            if isinstance(source_ref, dict)
                        ],
                    }
                    for index in invalid_claim_indices
                ]
                integrity_prompt = (
                    "你是錯誤反例修正員。程式已確認下列卡片使用 f(v) != -f(u) 否定齊次性，但 v 並不是 -u，因此該反例前提無效。"
                    "請根據 source_evidence 中的函數或映射定義重新計算，不得沿用原筆記錯誤結論，也不得刪除卡片。"
                    "source_evidence 有足夠的函數或映射定義可直接重算時 repairable=true 並完成修正；若來源完全沒有定義、條件或足以重算的公式，repairable=false 且其餘文字欄位與 correction 字串全部留空，不可猜測。"
                    "先以一般輸入直接驗證加法與齊次性；映射若實際為線性，就明確改成『為線性』並移除所有『不是線性／非線性』結論；若確實非線性，必須換成符合欲檢查性質前提且可由來源定義直接算出的有效反例。"
                    "同步修正 core_summary、explanation、simple_example、題目、方法與步驟，保留 card_type=example 所需的可讀分欄；例題卡的 simple_example 留空。"
                    "correction_original 放原來源中最小錯誤比較，correction_reason 說明輸入不符合測試關係及重新計算後的正確結論。"
                    "不得加入來源沒有的術語或另一套進階解法。公式使用 KaTeX LaTeX。每個 concept_index 恰好輸出一次，只輸出 schema JSON。\n\n"
                    + json.dumps(integrity_catalog, ensure_ascii=False, separators=(",", ":"))
                )
                corrected_claims: Optional[Dict[int, Dict[str, Any]]] = None
                for integrity_attempt in range(1):
                    try:
                        integrity_result = _call_openai_json(
                            name="study_recall_invalid_claim_repair",
                            schema=integrity_schema,
                            content=[{"type": "input_text", "text": integrity_prompt}],
                            timeout=240,
                            reasoning_effort="medium",
                            max_output_tokens=8000,
                        )
                    except (requests.RequestException, ValueError, TypeError) as exc:
                        app.logger.warning(
                            "Study-note invalid-claim repair failed; the final validator will discard only affected cards: %s",
                            exc,
                        )
                        break
                    candidate_claims: Dict[int, Dict[str, Any]] = {}
                    unrepairable_indices: Set[int] = set()
                    for item in integrity_result.get("cards") or []:
                        if not isinstance(item, dict):
                            continue
                        concept_index = int(item.get("concept_index", -1))
                        if concept_index not in invalid_claim_indices or concept_index in candidate_claims:
                            candidate_claims = {}
                            break
                        if not bool(item.get("repairable")):
                            unrepairable_indices.add(concept_index)
                            continue
                        prepared_claim = {
                            "core_summary": _normalize_study_math_markup(item.get("core_summary")),
                            "explanation": _normalize_study_math_markup(item.get("explanation")),
                            "simple_example": _normalize_study_math_markup(item.get("simple_example")),
                            "example_problem": _normalize_study_math_markup(item.get("example_problem")),
                            "example_method": _normalize_study_math_markup(item.get("example_method")),
                            "reasoning_steps": [
                                _normalize_study_math_markup(step)
                                for step in (item.get("reasoning_steps") or [])[:4]
                                if str(step or "").strip()
                            ],
                        }
                        correction_original = str(item.get("correction_original") or "").strip()
                        correction_reason = str(item.get("correction_reason") or "").strip()
                        if (
                            not all(prepared_claim[key] for key in ("core_summary", "explanation", "example_problem", "example_method"))
                            or not correction_original
                            or not correction_reason
                            or any(
                                _study_text_quality_issue(prepared_claim[key], max_length=900)
                                for key in ("core_summary", "explanation", "example_problem", "example_method")
                            )
                            or _study_has_invalid_negation_counterexample(card_integrity_text(prepared_claim))
                        ):
                            candidate_claims = {}
                            break
                        prepared_claim["correction"] = {
                            "applied": True,
                            "original": correction_original,
                            "corrected": prepared_claim["explanation"],
                            "reason": correction_reason,
                        }
                        candidate_claims[concept_index] = prepared_claim
                    if set(candidate_claims) | unrepairable_indices == set(invalid_claim_indices):
                        corrected_claims = candidate_claims
                        break
                    integrity_prompt += "\n\n前一次仍保留無效的負向量比較或欄位不完整。請重新計算後輸出完整正確卡片。"
                if corrected_claims is None:
                    app.logger.warning(
                        "Study-note invalid-claim repair remained incomplete; preserving unaffected cards"
                    )
                else:
                    for index, corrected_claim in corrected_claims.items():
                        audited_cards[index].update(corrected_claim)
        except json.JSONDecodeError:
            app.logger.exception("Faithful study-note JSON output remained incomplete after retry")
            return None, "AI 回傳格式不完整，系統已自動重試仍未成功；請稍後重新上傳，不需要更換圖片。"
        except requests.Timeout:
            app.logger.exception("Faithful study-note model request timed out after retry")
            return None, "AI 服務處理逾時，系統已自動重試仍未完成；請稍後重新上傳，不需要更換圖片。"
        except requests.HTTPError as exc:
            status_code = getattr(exc.response, "status_code", None)
            error_code = str(getattr(exc, "openai_error_code", "") or "")
            error_type = str(getattr(exc, "openai_error_type", "") or "")
            error_message = str(getattr(exc, "openai_error_message", "") or "")
            if not (error_code or error_type or error_message):
                error_code, error_type, error_message = _openai_error_details(exc.response)
            app.logger.exception("Faithful study-note model request failed with HTTP %s", status_code)
            if status_code == 429 and _is_openai_quota_error(error_code, error_type, error_message):
                return None, (
                    "OpenAI API 額度不足或已達每月使用上限。請管理員至 OpenAI 計費設定補充額度"
                    "或提高使用上限後再上傳；系統已停止無效重試。"
                )
            if status_code == 429:
                return None, "AI 服務目前使用量過高，系統已自動退避重試仍受限；請稍後再試，不是圖片內容有問題。"
            return None, f"AI 服務暫時無法處理（HTTP {status_code or '錯誤'}），請稍後再試。"
        except (requests.RequestException, ValueError, TypeError):
            app.logger.exception("Faithful study-note analysis failed")
            return None, "筆記忠實整理暫時失敗，請確認圖片清晰度、API 金鑰、模型設定與網路後重試。"
        validated = _validate_recall_output(audited, source_pages)
        if not validated:
            return None, "筆記內容不足以產生可靠的重點卡，請上傳更清晰或更多頁筆記。"
        _enrich_study_card_coverage_ids(validated, source_pages)
        remaining_example_items = (
            _study_recall_coverage_gaps(validated, source_pages).get("example_items") or []
        )
        if remaining_example_items:
            app.logger.warning(
                "Final example coverage validation failed; refusing an incomplete note: %s",
                [item.get("id") for item in remaining_example_items],
            )
            return None, (
                f"仍有 {len(remaining_example_items)} 個例題未能可靠建立卡片，"
                "系統已保留原筆記，請稍後重試。"
            )
        validated["source_transcription"] = source_pages
        validated["uncertain_fragments"] = [
            {"image_index": page["image_index"], "text": fragment}
            for page in source_pages
            for fragment in page.get("uncertain_fragments") or []
        ]
        if progress_callback:
            progress_callback(82, "重點卡已驗證，正在建立頁面文字索引並複核來源裁切。")
        _raise_if_study_upload_cancelled()
        try:
            _localize_study_card_sources(
                images,
                validated["key_concepts"],
                validated["source_transcription"],
            )
            retry_concepts: List[Dict[str, Any]] = []
            source_groups = 0
            located_groups = 0
            for concept in validated["key_concepts"]:
                if not isinstance(concept, dict):
                    continue
                refs_by_page: Dict[int, List[Dict[str, Any]]] = {}
                for source_ref in concept.get("source_refs") or []:
                    if not isinstance(source_ref, dict):
                        continue
                    try:
                        image_index = int(source_ref.get("image_index") or 0)
                    except (TypeError, ValueError):
                        continue
                    if (
                        1 <= image_index <= len(images)
                        and _literal_study_source_evidence(source_ref.get("evidence"))
                    ):
                        refs_by_page.setdefault(image_index, []).append(source_ref)
                missing_refs: List[Dict[str, Any]] = []
                for image_index, page_refs in refs_by_page.items():
                    source_groups += 1
                    if any(
                        _validated_study_source_bbox(
                            source_ref.get("bbox"),
                            require_text_verified=True,
                            expected_image_index=image_index,
                        )
                        is not None
                        for source_ref in page_refs
                    ):
                        located_groups += 1
                    else:
                        # One reliable source box per card and page is sufficient.
                        missing_refs.append(
                            max(
                                page_refs,
                                key=lambda source_ref: len(
                                    _literal_study_source_evidence(
                                        source_ref.get("evidence")
                                    )
                                ),
                            )
                        )
                if missing_refs:
                    retry_concept = {
                        field: copy.deepcopy(concept.get(field))
                        for field in (
                            "concept",
                            "topic",
                            "core_summary",
                            "explanation",
                            "example_problem",
                            "example_method",
                            "simple_example",
                        )
                    }
                    # Keep references shared so successful retry boxes are written
                    # directly back to the validated card set.
                    retry_concept["source_refs"] = missing_refs
                    retry_concepts.append(retry_concept)
            if (
                retry_concepts
                and source_groups
                and located_groups / source_groups < 0.90
            ):
                if progress_callback:
                    progress_callback(
                        88,
                        f"首次定位完成 {located_groups}/{source_groups} 個來源區塊，"
                        "正在只重試未定位區塊。",
                    )
                _raise_if_study_upload_cancelled()
                _localize_study_card_sources(
                    images,
                    retry_concepts,
                    validated["source_transcription"],
                )
        except (requests.RequestException, ValueError, TypeError):
            app.logger.exception("Study-note source localization failed")
        if progress_callback:
            progress_callback(100, "來源區塊定位完成，本批筆記已完成整理。")
        validated["organization_mode"] = "faithful"
        return validated, None

    def _consolidate_study_note_batch_cards(
        cards: List[Dict[str, Any]],
        *,
        subject: str,
    ) -> List[Dict[str, Any]]:
        if len(cards) < 2:
            return cards

        window_size = 100
        window_step = 80
        windows: List[List[int]] = []
        if len(cards) <= window_size:
            windows.append(list(range(len(cards))))
        else:
            for start in range(0, len(cards), window_step):
                window = list(range(start, min(len(cards), start + window_size)))
                if len(window) >= 2:
                    windows.append(window)
                if window and window[-1] == len(cards) - 1:
                    break

            topic_groups: Dict[str, List[int]] = {}
            for index, card in enumerate(cards):
                topic_key = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", str(card.get("topic") or "").lower())
                if topic_key:
                    topic_groups.setdefault(topic_key, []).append(index)
            for group in topic_groups.values():
                if len(group) < 2:
                    continue
                for start in range(0, len(group), window_step):
                    window = group[start : start + window_size]
                    if len(window) >= 2:
                        windows.append(window)

        merge_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["duplicate_groups"],
            "properties": {
                "duplicate_groups": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["concept_indexes", "keep_index"],
                        "properties": {
                            "concept_indexes": {
                                "type": "array",
                                "minItems": 2,
                                "items": {"type": "integer", "minimum": 0, "maximum": len(cards) - 1},
                            },
                            "keep_index": {"type": "integer", "minimum": 0, "maximum": len(cards) - 1},
                        },
                    },
                }
            },
        }
        proposed_groups: List[Tuple[List[int], int]] = []
        seen_windows: Set[Tuple[int, ...]] = set()
        for window in windows:
            window_key = tuple(window)
            if window_key in seen_windows:
                continue
            seen_windows.add(window_key)
            catalog = [
                {
                    "concept_index": index,
                    "topic": cards[index].get("topic") or "",
                    "concept": cards[index].get("concept") or "",
                    "card_type": cards[index].get("card_type") or "concept",
                    "core_summary": cards[index].get("core_summary") or "",
                    "explanation": cards[index].get("explanation") or "",
                    "source_pages": sorted(
                        {
                            int(source_ref.get("image_index") or 0)
                            for source_ref in cards[index].get("source_refs") or []
                            if isinstance(source_ref, dict)
                        }
                    ),
                }
                for index in window
            ]
            prompt = (
                f"你是「{subject}」筆記卡片的跨頁去重編輯。判斷下列卡片中哪些其實是同一個知識點、同一個公式或同一個複習目標，只有真正重複時才能合併。"
                "彼此相關、前後承接、同章節但可分別複習的卡片不是重複，必須保留。例題與一般觀念卡不得互相合併；題目條件或解法不同的例題也不得合併。"
                "每組 concept_indexes 放所有應合併的索引，keep_index 選內容最完整、最清楚且公式無缺漏的一張。沒有重要重複就輸出空陣列。不要因卡片很多而刪除任何不重複重點。只輸出 schema JSON。\n\n"
                + json.dumps(catalog, ensure_ascii=False, separators=(",", ":"))
            )
            try:
                merge_result = _call_openai_json(
                    name="study_note_cross_batch_merge",
                    schema=merge_schema,
                    content=[{"type": "input_text", "text": prompt}],
                    timeout=180,
                    reasoning_effort="medium",
                    max_output_tokens=5000,
                )
            except (requests.RequestException, ValueError, TypeError):
                app.logger.exception("Study-note cross-batch merge planning failed; preserving every card")
                continue
            allowed = set(window)
            for item in merge_result.get("duplicate_groups") or []:
                if not isinstance(item, dict):
                    continue
                indexes = sorted(
                    {
                        int(index)
                        for index in item.get("concept_indexes") or []
                        if isinstance(index, int) and index in allowed
                    }
                )
                keep_index = int(item.get("keep_index", -1))
                if len(indexes) >= 2 and keep_index in indexes:
                    proposed_groups.append((indexes, keep_index))

        removed: Set[int] = set()
        for indexes, requested_keep_index in proposed_groups:
            active_indexes = [index for index in indexes if index not in removed]
            if len(active_indexes) < 2:
                continue
            keep_index = requested_keep_index if requested_keep_index in active_indexes else max(
                active_indexes,
                key=lambda index: len(str(cards[index].get("explanation") or "")),
            )
            keep_card = cards[keep_index]
            for duplicate_index in active_indexes:
                if duplicate_index == keep_index:
                    continue
                duplicate = cards[duplicate_index]
                combined_refs: List[Dict[str, Any]] = []
                seen_refs: Set[Tuple[int, str]] = set()
                for source_ref in (keep_card.get("source_refs") or []) + (duplicate.get("source_refs") or []):
                    if not isinstance(source_ref, dict):
                        continue
                    key = (
                        int(source_ref.get("image_index") or 0),
                        " ".join(str(source_ref.get("evidence") or "").split()),
                    )
                    if key in seen_refs:
                        continue
                    seen_refs.add(key)
                    combined_refs.append(source_ref)
                keep_card["source_refs"] = combined_refs
                keep_card["coverage_ids"] = list(
                    dict.fromkeys((keep_card.get("coverage_ids") or []) + (duplicate.get("coverage_ids") or []))
                )
                if not bool((keep_card.get("correction") or {}).get("applied")) and bool(
                    (duplicate.get("correction") or {}).get("applied")
                ):
                    keep_card["correction"] = duplicate["correction"]
                removed.add(duplicate_index)
        return [card for index, card in enumerate(cards) if index not in removed]

    def _analyze_study_note_images(
        images: List[Tuple[str, Any, str]],
        *,
        subject: str,
        allow_corrections: bool,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        def materialize(selected_images: List[Tuple[str, Any, str]]) -> List[Tuple[str, bytes, str]]:
            prepared: List[Tuple[str, bytes, str]] = []
            for filename, source, mime_type in selected_images:
                if isinstance(source, Path):
                    image_bytes = source.read_bytes()
                elif isinstance(source, bytes):
                    image_bytes = source
                else:
                    raise ValueError("Unsupported study-note image source")
                if not image_bytes or len(image_bytes) > STUDY_NOTE_MAX_IMAGE_BYTES:
                    raise ValueError(f"筆記圖片 {filename} 大小不正確。")
                prepared.append((filename, image_bytes, mime_type))
            return prepared

        batch_count = math.ceil(len(images) / STUDY_NOTE_AI_BATCH_SIZE)
        analyses: List[Dict[str, Any]] = []
        for batch_index, batch_start in enumerate(range(0, len(images), STUDY_NOTE_AI_BATCH_SIZE)):
            _raise_if_study_upload_cancelled()
            batch_images = images[batch_start : batch_start + STUDY_NOTE_AI_BATCH_SIZE]

            def report_batch_progress(progress: int, message: str, *, current_batch: int = batch_index) -> None:
                if not progress_callback:
                    return
                combined_progress = _study_upload_time_weighted_progress(
                    progress,
                    batch_index=current_batch,
                    batch_count=batch_count,
                )
                progress_callback(
                    combined_progress,
                    f"第 {current_batch + 1}/{batch_count} 批：{message}",
                )

            analysis, error = _analyze_study_note_image_batch(
                materialize(batch_images),
                subject=subject,
                allow_corrections=allow_corrections,
                progress_callback=report_batch_progress,
            )
            if error or not analysis:
                return None, f"第 {batch_index + 1}/{batch_count} 批處理失敗：{error or '筆記分析失敗。'}"
            _offset_study_note_batch_analysis(analysis, batch_start)
            analyses.append(analysis)

        _raise_if_study_upload_cancelled()
        if batch_count == 1:
            if progress_callback:
                progress_callback(96, "所有頁面與來源定位均已完成，正在準備儲存。")
            return analyses[0], None
        if progress_callback:
            progress_callback(95, "所有批次已完成，正在由 AI 判斷跨批次卡片的合併與去重。")
        combined_cards = [
            card
            for analysis in analyses
            for card in analysis.get("key_concepts") or []
            if isinstance(card, dict)
        ]
        combined_cards = _consolidate_study_note_batch_cards(combined_cards, subject=subject)
        summaries: List[str] = []
        for analysis in analyses:
            summary = str(analysis.get("summary") or "").strip()
            if summary and summary not in summaries:
                summaries.append(summary)
        combined_summary = "\n".join(summaries)
        if len(combined_summary) > 900:
            combined_summary = combined_summary[:897].rstrip() + "..."
        topics = list(
            dict.fromkeys(
                str(analysis.get("detected_topic") or "").strip()
                for analysis in analyses
                if str(analysis.get("detected_topic") or "").strip()
            )
        )
        combined_topic = "、".join(topics)
        if len(combined_topic) > 80:
            combined_topic = topics[0][:80] if topics else subject
        combined_analysis = {
            "detected_topic": combined_topic or subject,
            "summary": combined_summary,
            "key_concepts": combined_cards,
            "source_transcription": [
                page for analysis in analyses for page in analysis.get("source_transcription") or []
            ],
            "uncertain_fragments": [
                fragment for analysis in analyses for fragment in analysis.get("uncertain_fragments") or []
            ],
            "correction_records": [
                record for analysis in analyses for record in analysis.get("correction_records") or []
            ],
            "organization_mode": "faithful",
        }
        if progress_callback:
            progress_callback(96, "跨批次卡片合併完成，正在準備儲存。")
        return combined_analysis, None

    def _rebuild_all_study_recall_relations() -> Optional[str]:
        _raise_if_study_upload_cancelled()
        sessions = storage.list_study_recall_sessions(limit=None)
        concepts_by_session: Dict[int, List[Dict[str, Any]]] = {}
        card_catalog: List[Dict[str, Any]] = []
        cards_by_id: Dict[str, Dict[str, Any]] = {}
        for recall_session in sessions:
            _raise_if_study_upload_cancelled()
            session_id = int(recall_session["id"])
            concepts = recall_session.get("key_concepts") or []
            concepts_by_session[session_id] = concepts
            for concept_index, concept in enumerate(concepts):
                if not isinstance(concept, dict) or not _is_recall_concept_eligible(concept):
                    continue
                card_id = f"s{session_id}:c{concept_index}"
                catalog_item = {
                    "id": card_id,
                    "note": str(concept.get("note_topic") or recall_session.get("title") or "")[:80],
                    "topic": str(concept.get("topic") or "")[:48],
                    "concept": str(concept.get("concept") or "")[:80],
                    "explanation": str(concept.get("explanation") or "")[:220],
                }
                card_catalog.append(catalog_item)
                cards_by_id[card_id] = {
                    "session_id": session_id,
                    "concept_index": concept_index,
                    "title": catalog_item["concept"],
                }

        if len(card_catalog) < 2:
            for concepts in concepts_by_session.values():
                for concept in concepts:
                    if isinstance(concept, dict):
                        concept["relations"] = []
            _raise_if_study_upload_cancelled()
            storage.replace_study_recall_concepts_bulk(concepts_by_session)
            return None

        schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["relations"],
            "properties": {
                "relations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["source_id", "target_id", "association"],
                        "properties": {
                            "source_id": {"type": "string"},
                            "target_id": {"type": "string"},
                            "association": {"type": "string", "maxLength": 160},
                        },
                    },
                }
            },
        }
        prompt = (
            "你是研究所考試的知識架構助教。以下是使用者目前所有重點卡。每次都必須忽略舊關聯，重新審視全部卡片。"
            "找出具有明確學理關係的卡片配對，包括前置知識、定義與推論、公式推導、互逆、比較、特例或實際應用。"
            "同份與不同份筆記都可連結，但不得只因同科目或關鍵字相似而連結。每張卡最多保留 2 個最有助於理解與記憶的強關聯；"
            "relations 必須依關聯的重要性由高到低輸出；沒有強關聯的卡片可不輸出。每組配對只輸出一次。association 請用精確、好記的繁體中文說明兩件事："
            "它們的觀念關聯在哪，以及複習時可以如何從一張聯想到另一張。說明必須從任一張卡閱讀都成立，形成雙向記憶橋接；"
            "請直接說明具體知識，不可使用『兩者相關』等空泛句子，也不要使用『前者／後者』等依賴輸出順序的代稱。"
            "每一組配對的 association 必須是該配對獨有的內容，不可重複使用同一句或只替換卡片名稱的套版句；若無法說出具體橋接關係，就不要輸出該配對。"
            "association 中若出現數學、統計、離散數學、演算法或計算機科學符號，必須使用可由 KaTeX 渲染的 LaTeX：行內一律用 \\( ... \\)，獨立公式用 \\[ ... \\]；不要輸出裸露的 Unicode 或純文字公式。"
            "每個 association 最多 120 個中文字，用 1 至 2 個完整短句寫完，最後必須以『。』『！』或『？』收尾；不可在公式、名詞或句子中途結束。"
            "source_id 與 target_id 只使用清單提供的 id，不得改寫；association 只能使用卡片標題或觀念名稱，絕對不可出現 s6:c6 這類內部 id。\n\n重點卡清單：\n"
            + json.dumps(card_catalog, ensure_ascii=False, separators=(",", ":"))
        )
        request_body = {
            "model": openai_model,
            "store": False,
            "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}],
            "reasoning": {
                "effort": normalize_openai_reasoning_effort(openai_model, "low")
            },
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "study_recall_relations",
                    "strict": True,
                    "schema": schema,
                }
            },
        }
        try:
            _raise_if_study_upload_cancelled()
            response = requests.post(
                "https://api.openai.com/v1/responses",
                headers={"Authorization": f"Bearer {openai_api_key}", "Content-Type": "application/json"},
                json=request_body,
                timeout=120,
            )
            response.raise_for_status()
            _raise_if_study_upload_cancelled()
            parsed = json.loads(_extract_openai_text(response.json()))
        except (requests.RequestException, ValueError, TypeError):
            return "AI 關聯分析暫時失敗，原有關聯已保留。"

        raw_relations = parsed.get("relations") if isinstance(parsed, dict) else None
        if not isinstance(raw_relations, list):
            return "AI 未回傳有效的關聯資料，原有關聯已保留。"

        for concepts in concepts_by_session.values():
            for concept in concepts:
                if isinstance(concept, dict):
                    concept["relations"] = []
        relation_counts = {card_id: 0 for card_id in cards_by_id}
        card_titles_by_id = {
            card_id.casefold(): str(card.get("title") or "").strip()
            for card_id, card in cards_by_id.items()
        }
        seen_pairs = set()
        seen_association_signatures = set()
        for relation in raw_relations:
            _raise_if_study_upload_cancelled()
            if not isinstance(relation, dict):
                continue
            source_id = str(relation.get("source_id") or "").strip()
            target_id = str(relation.get("target_id") or "").strip()
            association = _normalize_study_math_markup(
                " ".join(str(relation.get("association") or "").split())
            )
            association = re.sub(
                r"\bs\d+\s*:\s*c\d+\b",
                lambda match: card_titles_by_id.get(re.sub(r"\s+", "", match.group(0)).casefold(), ""),
                association,
                flags=re.IGNORECASE,
            )
            association = re.sub(r"\s+([，。；：、！？])", r"\1", association).strip(" \t:：,，;；-")
            pair = tuple(sorted((source_id, target_id)))
            source = cards_by_id.get(source_id)
            target = cards_by_id.get(target_id)
            association_issue = _study_relation_association_issue(
                association,
                source_title=(source or {}).get("title"),
                target_title=(target or {}).get("title"),
            )
            association_signature = _study_relation_association_signature(
                association,
                source_title=(source or {}).get("title"),
                target_title=(target or {}).get("title"),
            )
            if (
                source_id not in cards_by_id
                or target_id not in cards_by_id
                or source_id == target_id
                or pair in seen_pairs
                or not association
                or len(association) > 180
                or not association.endswith(("。", "！", "？"))
                or association_issue
                or association_signature in seen_association_signatures
                or relation_counts[source_id] >= 2
                or relation_counts[target_id] >= 2
            ):
                continue
            seen_pairs.add(pair)
            seen_association_signatures.add(association_signature)
            relation_counts[source_id] += 1
            relation_counts[target_id] += 1
            source = cards_by_id[source_id]
            target = cards_by_id[target_id]
            source_concept = concepts_by_session[source["session_id"]][source["concept_index"]]
            target_concept = concepts_by_session[target["session_id"]][target["concept_index"]]
            source_concept["relations"].append(
                {
                    "session_id": target["session_id"],
                    "concept_index": target["concept_index"],
                    "title": target["title"],
                    "association": association,
                }
            )
            target_concept["relations"].append(
                {
                    "session_id": source["session_id"],
                    "concept_index": source["concept_index"],
                    "title": source["title"],
                    "association": association,
                }
            )
        _raise_if_study_upload_cancelled()
        storage.replace_study_recall_concepts_bulk(concepts_by_session)
        return None

    def fetch_assignments_for(user: Dict[str, str]) -> Tuple[Dict[str, Any], Optional[str]]:
        opts = CollectOptions(
            base_url=base_url,
            scope=default_scope,
            course_id=None,
            include_completed=True,
            all_courses=False,
            all_courses_all_terms=False,
            username=None,
            password=None,
            moodle_session=user.get("moodle_session"),
            insecure=False,
            timeout=default_timeout,
            debug=False,
        )
        result = collect_assignments(opts)
        excel_data = _generate_excel_data(result.get("all_assignments"))
        return result, excel_data

    @app.before_request
    def enforce_canonical_host():
        if not canonical_host:
            return
        forwarded_host = request.headers.get("X-Forwarded-Host")
        host = (forwarded_host or request.host).split(",", 1)[0].strip()
        normalized_host = host.lower().rstrip(".")
        desired_host = canonical_host.lower().strip().rstrip("./")
        if "://" in desired_host:
            desired_host = (urlsplit(desired_host).netloc or desired_host).rstrip(".")
        proto = request.headers.get("X-Forwarded-Proto", request.scheme)
        needs_host_redirect = normalized_host != desired_host
        needs_proto_redirect = proto != "https"
        if needs_host_redirect or needs_proto_redirect:
            parts = urlsplit(request.url)
            new_url = urlunsplit(
                (
                    "https",
                    desired_host,
                    parts.path,
                    parts.query,
                    parts.fragment,
                )
            )
            return redirect(new_url, code=301)

    @app.after_request
    def add_no_store_headers(resp):
        cache_control = resp.headers.get("Cache-Control", "")
        content_disposition = resp.headers.get("Content-Disposition", "")
        if "attachment" in content_disposition.lower() or cache_control.startswith("public"):
            return resp
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if current_user():
            return redirect(url_for("index"))
        if request.method == "POST":
            login_type = request.form.get("login_type", "password")
            if login_type == "session":
                raw_session = request.form.get("moodle_session", "").strip()
                if not raw_session:
                    flash("請貼上有效的 MoodleSession 值。", "error")
                else:
                    digest = hashlib.sha1(raw_session.encode("utf-8")).hexdigest()[:10]
                    session_label = f"Session-{digest}"
                    existing_cache = load_cache_from_disk(session_label)
                    try:
                        result = None
                        excel_data = None
                        if not existing_cache:
                            result, excel_data = fetch_assignments_for(
                                {"username": session_label, "moodle_session": raw_session}
                            )
                        _start_web_session(
                            session_label,
                            moodle_session=raw_session,
                            is_guest=False,
                            is_admin=bool(admin_user_id and session_label == admin_user_id),
                            permanent=True,
                        )
                        record_ui_event("login_success", meta={"username": session_label})
                        if existing_cache:
                            flash("已載入先前的課程資料，系統將在背景自動更新最新內容。", "info")
                        else:
                            try:
                                set_assign_cache(result, excel_data)
                                flash("已成功透過 E3 Session 取得最新資訊。", "success")
                            except Exception:
                                flash("Session 登入成功，但暫存資料寫入失敗。", "warning")
                        response = redirect(url_for("index"))
                        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
                        response.headers["Pragma"] = "no-cache"
                        response.headers["Expires"] = "0"
                        return response
                    except Exception as exc:
                        flash(f"Session 驗證失敗：{exc}，請確認 MoodleSession 是否正確。", "error")
            else:
                raw_username = request.form.get("username", "").strip()
                raw_password = request.form.get("password", "")
                if not raw_username or not raw_password:
                    flash("請輸入帳號與密碼。", "error")
                else:
                    try:
                        sess = requests.Session()
                        login_with_password(sess, base_url, raw_username, raw_password, timeout=default_timeout)
                        cookie_val = sess.cookies.get("MoodleSession")
                        if not cookie_val:
                            raise RuntimeError("登入成功但未取得 MoodleSession。")
                        _start_web_session(
                            raw_username,
                            moodle_session=cookie_val,
                            is_guest=False,
                            is_admin=bool(admin_user_id and raw_username == admin_user_id),
                            permanent=True,
                        )
                        record_ui_event("login_success", meta={"username": raw_username})
                        existing_cache = load_cache_from_disk(raw_username)
                        if existing_cache:
                            flash("已載入先前的課程資料，系統將在背景自動更新最新內容。", "info")
                        else:
                            try:
                                result, excel_data = fetch_assignments_for(
                                    {"username": raw_username, "moodle_session": cookie_val}
                                )
                                set_assign_cache(result, excel_data)
                                flash("已成功獲取最新資訊。", "success")
                            except Exception as exc:
                                flash(f"登入成功但獲取資料失敗：{exc}，程式將在背景重試。", "warning")
                        response = redirect(url_for("index"))
                        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
                        response.headers["Pragma"] = "no-cache"
                        response.headers["Expires"] = "0"
                        return response
                    except Exception as exc:
                        flash(f"{exc}", "error")
        announcements_list = load_announcements()
        return render_template_string(
            LOGIN_TEMPLATE,
            stats=usage_stats(),
            stats_version=current_stats_version(),
            announcements=announcements_list,
            announcement_version=announcements_list[0]["id"] if announcements_list else None,
            support_email=support_email,
            app_home_url=app_home_url,
        )

    @app.route("/healthz", methods=["GET"])
    def health_check():
        return {"status": "ok"}, 200

    @app.route("/traffic/stats", methods=["GET"])
    def traffic_stats():
        stats = usage_stats()
        payload = {
            "version": current_stats_version(),
            "online": stats["online"],
            "total": stats["total"],
        }
        return payload, 200, {"Cache-Control": "no-store, max-age=0"}

    @app.route("/privacy", methods=["GET"])
    def privacy_policy():
        return render_template_string(
            PRIVACY_TEMPLATE,
            app_home_url=app_home_url,
            support_email=support_email,
            google_scope=GOOGLE_CALENDAR_SCOPE,
            legal_entity_name=legal_entity_name,
            effective_date=legal_effective_date,
        )

    @app.route("/terms", methods=["GET"])
    def terms_of_service():
        return render_template_string(
            TERMS_TEMPLATE,
            app_home_url=app_home_url,
            support_email=support_email,
            google_scope=GOOGLE_CALENDAR_SCOPE,
            legal_entity_name=legal_entity_name,
        )

    @app.post("/ui-event")
    @login_required
    def ui_event():
        payload = request.get_json(silent=True) or {}
        action = str(payload.get("action") or "").strip()
        status = str(payload.get("status") or "info").strip() or "info"
        meta = payload.get("meta")
        if not isinstance(meta, dict):
            meta = None
        if not action:
            return {"ok": False, "error": "action required"}, 400
        record_ui_event(action, status, meta)
        return {"ok": True}

    @app.get("/session/status")
    @login_required
    def session_status():
        user = current_user()
        return {"ok": True, "username": user["username"] if user else None}

    @app.get("/api/cache")
    @login_required
    def api_cache():
        user = current_user()
        viewed_username = get_viewed_username(actor=user)
        cache = get_assign_cache(viewed_username) or {}
        preferences = get_user_preferences(viewed_username)
        include_cache = str(request.args.get("include_cache") or "").lower() in {"1", "true", "yes"}
        refresh_state = _refresh_job_state(viewed_username)
        payload = {
            "ok": True,
            "ts": cache.get("ts"),
            "has_result": bool(cache.get("result")) if cache else False,
            "preferences": preferences,
            "viewed_username": viewed_username,
            "readonly_view": is_admin_viewing_other_user(actor=user, viewed_username=viewed_username),
            "refresh_status": refresh_state.get("status") if refresh_state else None,
            "refresh_error": refresh_state.get("error") if refresh_state else None,
            "refresh_in_progress": bool(refresh_state and refresh_state.get("status") == "running"),
            "refresh_started_at": refresh_state.get("started_at") if refresh_state else None,
            "refresh_finished_at": refresh_state.get("finished_at") if refresh_state else None,
        }
        if include_cache:
            payload["cache"] = cache
        return payload

    @app.post("/preferences")
    @login_required
    def save_preferences():
        user = current_user()
        if is_admin_viewing_other_user(actor=user):
            return {"ok": False, "error": "readonly_view"}, 403
        payload = request.get_json(silent=True) or {}
        updated = update_user_preferences(payload)
        return {"ok": True, "preferences": updated}

    @app.route("/guest-login", methods=["POST"])
    def guest_login():
        guest_name = f"訪客_{secrets.token_hex(3)}"
        _start_web_session(
            guest_name,
            moodle_session=None,
            is_guest=True,
            is_admin=False,
            permanent=False,
        )
        flash("已進入訪客模式：請使用匯出工具生成 JSON 後上傳即可瀏覽作業。", "info")
        record_ui_event("guest_login", meta={"username": guest_name})
        return redirect(url_for("index"))

    @app.route("/guest-tool", methods=["GET"])
    def guest_tool():
        tool_path = ROOT_DIR / "backend" / "tools" / "guest_export.exe"
        if not tool_path.exists():
            flash("找不到匯出工具。", "error")
            return redirect(url_for("login"))
        payload = send_file(
            tool_path,
            as_attachment=True,
            download_name="guest_export.exe",
            conditional=True,
            max_age=604800,
            etag=True,
        )
        stat = tool_path.stat()
        payload.headers["Cache-Control"] = "public, max-age=604800, immutable"
        payload.headers["Last-Modified"] = http_date(stat.st_mtime)
        payload.headers["Content-Length"] = str(stat.st_size)
        return payload

    @app.route("/guest-tool.py", methods=["GET"])
    def guest_tool_source():
        source_path = ROOT_DIR / "backend" / "tools" / "guest_export.py"
        if not source_path.exists():
            flash("找不到匯出工具原始碼。", "error")
            return redirect(url_for("login"))
        payload = send_file(
            source_path,
            as_attachment=True,
            download_name="guest_export.py",
            mimetype="text/x-python",
        )
        stat = source_path.stat()
        payload.headers["Cache-Control"] = "public, max-age=604800, immutable"
        payload.headers["Last-Modified"] = http_date(stat.st_mtime)
        payload.headers["Content-Length"] = str(stat.st_size)
        return payload

    @app.route("/guest/import", methods=["POST"])
    @login_required
    def guest_import():
        user = current_user()
        if not user or not user.get("is_guest"):
            flash("訪客匯入僅限訪客模式使用。", "error")
            record_ui_event("guest_import", "error", {"reason": "not_guest"})
            return redirect(url_for("index"))
        uploaded = request.files.get("guest_file")
        if not uploaded or not uploaded.filename:
            flash("請上傳由 guest_export 匯出工具產生的 JSON 檔。", "warning")
            record_ui_event("guest_import", "error", {"reason": "missing_file"})
            return redirect(url_for("index"))
        try:
            payload = json.load(uploaded.stream)
        except Exception as exc:
            flash(f"解析上傳檔案失敗：{exc}", "error")
            record_ui_event("guest_import", "error", {"reason": "parse_failed"})
            return redirect(url_for("index"))
        if payload.get("mode") != "guest_export_v1":
            flash("檔案格式不支援，請使用 guest_export 匯出工具產生的 JSON。", "warning")
            record_ui_event("guest_import", "error", {"reason": "unsupported_mode"})
            return redirect(url_for("index"))
        result = payload.get("result")
        excel_data = payload.get("excel_data")
        if not result:
            flash("檔案內容缺少作業資料。", "error")
            record_ui_event("guest_import", "error", {"reason": "missing_result"})
            return redirect(url_for("index"))
        if not excel_data:
            excel_data = _generate_excel_data((result or {}).get("all_assignments"))
        set_assign_cache(result, excel_data)
        flash("已匯入訪客資料（檔案）。", "success")
        record_ui_event(
            "guest_import",
            "success",
            {"items": len(result.get("all_assignments", [])), "has_excel": bool(excel_data)},
        )
        return redirect(url_for("index"))

    @app.post("/announcements/<announcement_id>/vote")
    @login_required
    def announcement_vote(announcement_id: str):
        user = current_user()
        if not user:
            return {"ok": False, "error": "not_logged_in"}, 401
        payload = request.get_json(silent=True) or {}
        requested_vote = str(payload.get("vote") or "").strip().lower()
        if requested_vote not in {"up", "down", "clear"}:
            return {"ok": False, "error": "invalid_vote"}, 400
        resolved_vote = None if requested_vote == "clear" else requested_vote
        updated = set_announcement_vote(announcement_id, user["username"], resolved_vote)
        if not updated:
            return {"ok": False, "error": "announcement_not_found"}, 404
        record_ui_event(
            "announcement_vote",
            "success",
            {"announcement_id": announcement_id, "vote": resolved_vote or "clear"},
        )
        return {"ok": True, "announcement": updated}
    @app.route("/google/authorize")
    @login_required
    def google_authorize():
        if not _google_ready():
            flash("尚未設定 Google OAuth，請先在伺服器端提供 Client ID/Secret。", "warning")
            record_ui_event("google_link", "error", {"stage": "authorize", "reason": "not_ready"})
            return redirect(url_for("index"))
        state = _build_google_state()
        session["google_auth_state"] = state
        record_ui_event("google_link", "start", {"stage": "authorize"})
        return redirect(
            build_google_authorize_url(
                google_client_id,
                _google_redirect_uri(),
                scope=GOOGLE_CALENDAR_SCOPE,
                state=state,
            )
        )

    @app.route("/google/callback")
    def google_callback():
        user = current_user()
        if not user:
            flash("請先登入 E3，再進行 Google 授權。", "warning")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": "not_logged_in"})
            return redirect(url_for("login"))
        if not _google_ready():
            flash("尚未設定 Google OAuth。", "warning")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": "not_ready"})
            return redirect(url_for("index"))
        error = request.args.get("error")
        if error:
            flash(f"Google 授權失敗：{error}", "error")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": error})
            return redirect(url_for("index"))
        code = request.args.get("code")
        state = request.args.get("state")
        stored_state = session.get("google_auth_state")
        state_valid = bool(state) and _verify_google_state(state)
        if not code or not state_valid:
            flash("Google 授權資訊錯誤，請重新嘗試。", "error")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": "invalid_state"})
            return redirect(url_for("index"))
        if stored_state and state != stored_state:
            record_ui_event("google_link", "info", {"stage": "callback", "reason": "state_mismatch_but_signed"})
        session.pop("google_auth_state", None)
        try:
            token_resp = exchange_code_for_google_token(
                code,
                client_id=google_client_id,
                client_secret=google_client_secret,
                redirect_uri=_google_redirect_uri(),
            )
        except Exception as exc:  # pragma: no cover
            flash(f"換取 Google Token 失敗：{exc}", "error")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": "token_exchange"})
            return redirect(url_for("index"))
        existing = load_google_tokens(user["username"]) or {}
        refresh_token = token_resp.get("refresh_token") or existing.get("refresh_token")
        if not refresh_token:
            flash("Google 未提供 refresh token，請勾選同意並再次授權。", "error")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": "missing_refresh_token"})
            return redirect(url_for("index"))
        tokens = {
            "access_token": token_resp.get("access_token"),
            "refresh_token": refresh_token,
            "scope": token_resp.get("scope"),
            "token_type": token_resp.get("token_type"),
            "expires_at": compute_expiry(token_resp.get("expires_in", 3600)),
        }
        save_google_tokens(user["username"], tokens)
        flash("已成功連結 Google 日曆，可同步作業。", "success")
        record_ui_event("google_link", "success", {"stage": "callback"})
        return redirect(url_for("index"))

    @app.post("/google/unlink")
    @login_required
    def google_unlink():
        user = current_user()
        if user:
            clear_google_tokens(user["username"])
        flash("已解除 Google 日曆連結。", "info")
        record_ui_event("google_unlink", "success")
        return redirect(url_for("index"))

    @app.post("/google/sync")
    @login_required
    def google_sync():
        if not _google_ready():
            flash("尚未設定 Google OAuth，無法同步日曆。", "warning")
            record_ui_event("google_sync", "error", {"reason": "not_ready"})
            return redirect(url_for("index"))
        user = current_user()
        if not user:
            flash("請先登入後再同步。", "warning")
            record_ui_event("google_sync", "error", {"reason": "not_logged_in"})
            return redirect(url_for("login"))
        raw_selected = request.form.get("selected_uids", "")
        selected_uids: List[str] = []
        if raw_selected:
            try:
                selected_uids = json.loads(raw_selected)
            except Exception:
                selected_uids = []
        if not selected_uids:
            flash("請先選擇要導入的作業。", "warning")
            record_ui_event("google_sync", "error", {"reason": "no_selection"})
            return redirect(url_for("index"))
        tokens = load_google_tokens(user["username"])
        if not tokens:
            flash("尚未連結 Google 日曆。", "warning")
            record_ui_event("google_sync", "error", {"reason": "not_linked"})
            return redirect(url_for("index"))
        try:
            tokens = _ensure_google_access_token(user["username"], tokens)
        except Exception as exc:
            flash(f"無法更新 Google Token：{exc}", "error")
            record_ui_event("google_sync", "error", {"reason": "token_refresh"})
            return redirect(url_for("index"))
        record_ui_event("google_sync", "start", {"count": len(selected_uids)})
        try:
            cache = get_assign_cache() or {}
            result = cache.get("result") or {}
            excel_data = cache.get("excel_data")
            assignments = _select_assignments_from_result(result, selected_uids)
            if user.get("is_guest"):
                if not assignments:
                    raise RuntimeError("找不到訪客匯入的作業資料，請重新匯入後再試。")
            elif not assignments:
                result, excel_data = fetch_assignments_for({"username": user["username"], "moodle_session": session.get("moodle_session")})
                set_assign_cache(result, excel_data)
                assignments = _select_assignments_from_result(result, selected_uids)
            if not assignments:
                flash("找不到選擇的作業，請重新整理後再試。", "warning")
                record_ui_event("google_sync", "error", {"reason": "not_found"})
                return redirect(url_for("index"))
            synced = sync_assignments_to_google_calendar(
                assignments,
                access_token=tokens["access_token"],
                calendar_id=google_calendar_id,
            )
            flash(f"已將 {synced} 筆作業同步到 Google 日曆。", "success")
            record_ui_event("google_sync", "success", {"synced": synced})
        except GoogleUnauthorizedError:
            clear_google_tokens(user["username"])
            flash("Google 授權已失效，請重新連結後再嘗試。", "error")
            record_ui_event("google_sync", "error", {"reason": "unauthorized"})
        except Exception as exc:
            flash(f"同步 Google 日曆失敗：{exc}", "error")
            record_ui_event("google_sync", "error", {"reason": "exception"})
        return redirect(url_for("index"))

    @app.route("/logout")
    def logout():
        old_user = session.get("username")
        session_token = session.get("session_token")
        was_guest = bool(session.get("is_guest"))
        if old_user:
            if was_guest:
                storage.delete_user_cache(old_user)
            clear_google_tokens(old_user)
        if session_token:
            storage.clear_web_session(session_token)
        if old_user:
            record_ui_event("logout", meta={"username": old_user})
        session.clear()
        session.permanent = False
        session.modified = True
        flash("已登出。", "success")
        resp = redirect(url_for("index"))
        session_cookie_name = app.config.get("SESSION_COOKIE_NAME", "session")
        cookie_path = app.config.get("SESSION_COOKIE_PATH", "/")
        cookie_domain = app.config.get("SESSION_COOKIE_DOMAIN")
        resp.delete_cookie(session_cookie_name, path=cookie_path, domain=cookie_domain)
        host_only_domain = (request.host.split(":", 1)[0] or "").strip() or None
        if host_only_domain and host_only_domain != cookie_domain:
            resp.delete_cookie(session_cookie_name, path=cookie_path, domain=host_only_domain)
            if not host_only_domain.startswith("."):
                resp.delete_cookie(session_cookie_name, path=cookie_path, domain=f".{host_only_domain}")
        return resp

    @app.post("/api/assignments")
    @login_required
    def api_assignments():
        user = current_user()
        if is_admin_viewing_other_user(actor=user):
            return {"ok": False, "error": "readonly_view"}, 403
        if user and user.get("is_guest"):
            return {"ok": False, "error": "訪客模式不支援自動更新"}, 400
        username = user["username"]
        moodle_session_val = session.get("moodle_session")
        prev_cache = get_assign_cache() or {}
        prev_ts = prev_cache.get("ts") or 0
        if not _mark_refresh_job_started(username):
            return {
                "ok": True,
                "message": "背景更新已在進行中。",
                "background": True,
                "in_progress": True,
                "ts": prev_ts,
            }

        def _run_background():
            with app.app_context():
                try:
                    result, excel_data = fetch_assignments_for({"username": username, "moodle_session": moodle_session_val})
                    set_assign_cache_for_user(username, result, excel_data)
                    record_ui_event(
                        "refresh_assignments",
                        "success",
                        {"items": len(result.get("all_assignments", [])), "mode": "background"},
                    )
                except Exception as exc:  # pragma: no cover - background logging
                    record_ui_event("refresh_assignments", "error", {"reason": str(exc), "mode": "background"})
                    _mark_refresh_job_done(username, status="error", error=str(exc))
                else:
                    _mark_refresh_job_done(username, status="success")

        threading.Thread(target=_run_background, daemon=True).start()
        return {
            "ok": True,
            "message": "已啟動背景更新，稍後將自動刷新。",
            "background": True,
            "ts": prev_ts,
        }

    @app.route("/calendar.ics")
    @login_required
    def calendar_export():
        cache = get_assign_cache()
        assignments = cache.get("result", {}).get("all_assignments", []) if cache else []
        calendar = _build_calendar(assignments)
        if not calendar:
            flash("尚無可匯出的作業資料。", "info")
            record_ui_event("export_calendar", "error", {"reason": "no_assignments"})
            return redirect(url_for("index"))
        record_ui_event("export_calendar", "success", {"items": len(assignments)})
        return Response(
            calendar,
            mimetype="text/calendar",
            headers={"Content-Disposition": "attachment; filename=pending_assignments.ics"},
        )

    @app.get("/study-progress")
    def public_study_progress():
        user = current_user()
        context = _load_study_progress_context()
        return render_template_string(
            PUBLIC_STUDY_TEMPLATE,
            **context,
            share_url=request.url,
            is_admin=bool(user and user.get("is_admin")),
            admin_user=user,
        )

    @app.get("/public/study-progress")
    def public_study_progress_alias():
        return redirect(url_for("public_study_progress"), code=301)

    @app.get("/admin/study-home")
    @admin_required
    def admin_study_home():
        user = current_user()
        home_context = _load_study_progress_context()
        return render_template_string(
            STUDY_HOME_TEMPLATE,
            admin_user=user,
            recall_widget=_build_recall_widget_context(),
            **home_context,
        )

    @app.get("/admin/study-recall")
    @admin_required
    def admin_study_recall():
        def decorate_review_curve(concept: Dict[str, Any]) -> None:
            review = concept.get("review") or {}
            history = review.get("history") or []
            total_points = max(1, len(history))
            curve_points = []
            for index, entry in enumerate(history):
                x = 50 if total_points == 1 else 6 + index * 88 / (total_points - 1)
                y = 36 - max(0, min(int(entry.get("rating") or 1) - 1, 4)) * 7
                curve_points.append(f"{x:.1f},{y:.1f}")
            review["curve_points"] = " ".join(curve_points)
            review["history_label"] = " → ".join(str(entry.get("rating")) for entry in history) or "尚未自評"
            review["latest_curve_y"] = 36 - max(0, min(int(history[-1].get("rating") or 1) - 1, 4)) * 7 if history else 36
            fsrs_state = concept.get("fsrs_card") if isinstance(concept.get("fsrs_card"), dict) else {}
            try:
                stability_days = max(0.0, float(fsrs_state.get("stability")))
            except (TypeError, ValueError):
                stability_days = 0.0
            review["stability_label"] = (
                f"記憶穩定約 {stability_days:.1f} 天" if stability_days > 0 else ""
            )
            if len(history) >= 2:
                change = int(history[-1].get("rating") or 0) - int(history[-2].get("rating") or 0)
                review["trend_label"] = (
                    f"比上次 +{change}" if change > 0 else (f"比上次 {change}" if change < 0 else "與上次相同")
                )
            else:
                review["trend_label"] = ""
            concept["review"] = review

        def organize_session_concepts(session: Dict[str, Any], *, replace_concepts: bool) -> Dict[int, Dict[str, Any]]:
            raw_concepts = session.get("key_concepts") or []
            session["summary"] = _strip_study_process_narration(session.get("summary"))
            for page in session.get("source_transcription") or []:
                if isinstance(page, dict):
                    page["transcription"] = _normalize_study_math_markup(page.get("transcription"))
            image_urls = session.get("image_urls") or [
                url_for("admin_study_recall_image", session_id=session["id"], filename=filename)
                for filename in session.get("image_filenames") or []
            ]
            indexed_concepts = {
                index: concept
                for index, concept in enumerate(raw_concepts)
                if _is_recall_concept_eligible(concept)
            }
            for concept in indexed_concepts.values():
                concept["topic"] = _normalize_study_concept_title(
                    concept.get("topic"), session.get("title") or "細分觀念"
                )
                concept["concept"] = _normalize_study_concept_title(
                    concept.get("concept"), concept.get("topic") or session.get("title")
                )
            title_indexes = {
                str(concept.get("concept") or "").strip().casefold(): index
                for index, concept in indexed_concepts.items()
            }
            note_topic = next(
                (
                    str(concept.get("note_topic") or "").strip()
                    for concept in indexed_concepts.values()
                    if str(concept.get("note_topic") or "").strip()
                ),
                next(
                    (
                        str(concept.get("topic") or "").strip()
                        for concept in indexed_concepts.values()
                        if str(concept.get("topic") or "").strip()
                    ),
                    str(session.get("title") or "未分類筆記"),
                ),
            )
            topic_groups: Dict[str, List[Dict[str, Any]]] = {}
            for index, concept in indexed_concepts.items():
                concept["display_index"] = index
                concept["recall_cue"] = _normalize_study_math_markup(
                    concept.get("recall_cue")
                    or f"先回想「{concept.get('concept') or '這個觀念'}」的條件、核心關係與結論。"
                )
                concept["core_summary"] = _normalize_study_math_markup(concept.get("core_summary"))
                concept["explanation"] = _normalize_study_math_markup(concept.get("explanation"))
                concept["card_type"] = "example" if concept.get("card_type") == "example" else "concept"
                concept["example_problem"] = _normalize_study_math_markup(concept.get("example_problem"))
                concept["example_method"] = _normalize_study_math_markup(concept.get("example_method"))
                concept["simple_example"] = _normalize_study_math_markup(concept.get("simple_example"))
                concept["reasoning_steps"] = [
                    _normalize_study_math_markup(step)
                    for step in (concept.get("reasoning_steps") or [])[:4]
                    if str(step or "").strip()
                ]
                concept["common_confusion"] = _normalize_study_math_markup(concept.get("common_confusion"))
                concept["memory_hint"] = _normalize_study_math_markup(concept.get("memory_hint"))
                concept["topic"] = str(concept.get("topic") or note_topic).strip() or note_topic
                visible_source_refs = []
                for source_ref in concept.get("source_refs") or []:
                    if not isinstance(source_ref, dict):
                        continue
                    try:
                        image_index = int(source_ref.get("image_index") or 0)
                    except (TypeError, ValueError):
                        continue
                    evidence = " ".join(str(source_ref.get("evidence") or "").split()).strip()
                    if not (1 <= image_index <= len(image_urls)) or not evidence:
                        continue
                    locatable_evidence = _literal_study_source_evidence(evidence)
                    visible_source_refs.append(
                        {
                            "image_index": image_index,
                            "evidence": _normalize_study_math_markup(evidence),
                            "image_url": image_urls[image_index - 1],
                            "locatable": bool(locatable_evidence),
                            "bbox": _validated_study_source_bbox(
                                source_ref.get("bbox"),
                                require_text_verified=True,
                                expected_image_index=image_index,
                            ),
                        }
                    )
                concept["source_refs"] = collapse_source_refs_by_image(visible_source_refs)
                related_cards = []
                stored_relations = concept.get("relations")
                if isinstance(stored_relations, list):
                    for relation in stored_relations:
                        if not isinstance(relation, dict):
                            continue
                        try:
                            related_session_id = int(relation.get("session_id") or 0)
                            related_index = int(relation.get("concept_index"))
                        except (TypeError, ValueError):
                            continue
                        related_title = _normalize_study_math_markup(relation.get("title"))
                        association = _normalize_study_math_markup(
                            " ".join(str(relation.get("association") or "").split())
                        )
                        visible_card_titles = {
                            f"s{int(session['id'])}:c{index}".casefold(): str(concept.get("concept") or "").strip(),
                            f"s{related_session_id}:c{related_index}".casefold(): related_title,
                        }
                        association = re.sub(
                            r"\bs\d+\s*:\s*c\d+\b",
                            lambda match: visible_card_titles.get(
                                re.sub(r"\s+", "", match.group(0)).casefold(),
                                "",
                            ),
                            association,
                            flags=re.IGNORECASE,
                        )
                        association = re.sub(r"\s+([，。；：、！？])", r"\1", association).strip(" \t:：,，;；-")
                        if (
                            related_session_id <= 0
                            or related_index < 0
                            or not related_title
                            or not association
                            or _study_relation_association_issue(
                                association,
                                source_title=concept.get("concept"),
                                target_title=related_title,
                            )
                        ):
                            continue
                        related_cards.append(
                            {
                                "session_id": related_session_id,
                                "title": related_title,
                                "index": related_index,
                                "association": association,
                            }
                        )
                concept["related_cards"] = related_cards[:2]
                topic_groups.setdefault(concept["topic"], []).append(concept)
            session["note_topic"] = note_topic
            session["concept_groups"] = [
                {"topic": topic, "concepts": concepts}
                for topic, concepts in topic_groups.items()
            ]
            if replace_concepts:
                session["key_concepts"] = list(indexed_concepts.values())
            return indexed_concepts

        user = current_user()
        try:
            selected_id = int(request.args.get("session_id") or 0)
        except (TypeError, ValueError):
            selected_id = 0
        sessions = sorted(
            storage.list_study_recall_sessions(limit=36),
            key=lambda item: (str(item.get("created_at") or ""), int(item.get("id") or 0)),
        )
        session_groups_by_topic: Dict[str, List[Dict[str, Any]]] = {}
        for recall_session in sessions:
            concepts = recall_session.get("key_concepts") or []
            topic_label = next(
                (
                    str(concept.get("note_topic") or "").strip()
                    for concept in concepts
                    if isinstance(concept, dict) and str(concept.get("note_topic") or "").strip()
                ),
                next(
                    (
                        str(concept.get("topic") or "").strip()
                        for concept in concepts
                        if isinstance(concept, dict) and str(concept.get("topic") or "").strip()
                    ),
                    str(recall_session.get("subject") or recall_session.get("title") or "未分類筆記"),
                ),
            )
            recall_session["topic_label"] = topic_label
            session_groups_by_topic.setdefault(topic_label, []).append(recall_session)
        session_groups = [
            {"topic": topic, "sessions": grouped_sessions}
            for topic, grouped_sessions in session_groups_by_topic.items()
        ]
        selected_session = storage.get_study_recall_session(selected_id) if selected_id else None
        if selected_session is None and sessions:
            selected_session = storage.get_study_recall_session(int(sessions[-1]["id"]))
        if selected_session:
            selected_session["image_urls"] = [
                url_for("admin_study_recall_image", session_id=selected_session["id"], filename=filename)
                for filename in selected_session.get("image_filenames") or []
            ]
            for page in selected_session.get("source_transcription") or []:
                if not isinstance(page, dict):
                    continue
                try:
                    image_index = int(page.get("image_index") or 0)
                except (TypeError, ValueError):
                    continue
                page["image_url"] = (
                    selected_session["image_urls"][image_index - 1]
                    if 1 <= image_index <= len(selected_session["image_urls"])
                    else ""
                )
            for collection_name in ("uncertain_fragments", "correction_records"):
                for record in selected_session.get(collection_name) or []:
                    if not isinstance(record, dict):
                        continue
                    try:
                        image_index = int(record.get("image_index") or 0)
                    except (TypeError, ValueError):
                        continue
                    record["image_url"] = (
                        selected_session["image_urls"][image_index - 1]
                        if 1 <= image_index <= len(selected_session["image_urls"])
                        else ""
                    )
            organize_session_concepts(selected_session, replace_concepts=True)
            selected_session["source_location_total"] = sum(
                sum(
                    1
                    for source_ref in concept.get("source_refs") or []
                    if source_ref.get("locatable")
                )
                for concept in selected_session["key_concepts"]
            )
            selected_session["source_location_count"] = sum(
                1
                for concept in selected_session["key_concepts"]
                for source_ref in concept.get("source_refs") or []
                if source_ref.get("locatable") and source_ref.get("bbox")
            )
            selected_session["source_location_refined_count"] = sum(
                1
                for concept in selected_session["key_concepts"]
                for source_ref in concept.get("source_refs") or []
                if source_ref.get("locatable")
                and source_ref.get("bbox")
                and int(source_ref["bbox"].get("version") or 1) >= SOURCE_BBOX_VERSION
            )
            for concept in selected_session["key_concepts"]:
                decorate_review_curve(concept)
        today = _study_plan_business_date().isoformat()
        due_cards = storage.list_due_study_recall_cards(
            today=today,
            concept_filter=_is_recall_concept_eligible,
        )
        review_cards: List[Dict[str, Any]] = []
        review_sessions: Dict[int, Dict[str, Any]] = {}
        review_concept_indexes: Dict[int, Dict[int, Dict[str, Any]]] = {}
        for due_card in due_cards:
            session_id = int(due_card["session_id"])
            review_session = review_sessions.get(session_id)
            if review_session is None:
                review_session = storage.get_study_recall_session(session_id) or {}
                review_sessions[session_id] = review_session
            concept_index = int(due_card["concept_index"])
            concepts_by_index = review_concept_indexes.get(session_id)
            if concepts_by_index is None:
                concepts_by_index = organize_session_concepts(review_session, replace_concepts=False)
                review_concept_indexes[session_id] = concepts_by_index
            concept = concepts_by_index.get(concept_index)
            if concept is None:
                continue
            decorate_review_curve(concept)
            review_cards.append({**due_card, "concept_data": concept})
        review_schedule = storage.list_study_recall_schedule(
            start_date=today,
            concept_filter=_is_recall_concept_eligible,
        )
        return render_template_string(
            STUDY_RECALL_TEMPLATE,
            admin_user=user,
            subjects=STUDY_PLAN_SUBJECTS,
            today=today,
            sessions=sessions,
            session_groups=session_groups,
            due_cards=due_cards,
            review_cards=review_cards,
            review_schedule=review_schedule,
            selected_session=selected_session,
            openai_ready=bool(openai_api_key),
            nav_active="recall",
        )

    @app.get("/admin/study-recall/search")
    @admin_required
    def admin_study_recall_search():
        search_query = " ".join(str(request.args.get("q") or "").split()).strip()[:160]
        subject_filter = str(request.args.get("subject") or "").strip()
        content_type = str(request.args.get("type") or "all").strip().lower()
        sort_mode = str(request.args.get("sort") or "relevance").strip().lower()
        try:
            session_filter = max(0, int(request.args.get("session_id") or 0))
        except (TypeError, ValueError):
            return {"ok": False, "error": "筆記篩選條件無效。"}, 400
        if subject_filter and subject_filter not in STUDY_PLAN_SUBJECTS:
            return {"ok": False, "error": "搜尋科目無效。"}, 400
        if content_type not in {"all", "source", "formula", "example", "concept"}:
            return {"ok": False, "error": "內容類型篩選無效。"}, 400
        if sort_mode not in {"relevance", "recent"}:
            return {"ok": False, "error": "排序方式無效。"}, 400
        if not search_query:
            return {"ok": False, "error": "請輸入要查詢的內容。"}, 400
        started_at = time.perf_counter()
        raw_results = storage.search_study_recall_pages(
            query=search_query,
            subject=subject_filter or None,
            session_id=session_filter or None,
            content_type=content_type,
            sort=sort_mode,
            limit=16,
        )
        search_results: List[Dict[str, Any]] = []
        for result in raw_results:
            session_id = int(result["session_id"])
            image_index = int(result["image_index"])
            concept_index = result.get("concept_index")
            note_url = url_for("admin_study_recall", session_id=session_id)
            note_url = f"{note_url}#concept-{int(concept_index)}" if concept_index is not None else f"{note_url}#note-review"
            search_results.append(
                {
                    "session_id": session_id,
                    "study_date": result["study_date"],
                    "subject": result["subject"],
                    "title": result["title"],
                    "image_index": image_index,
                    "image_url": url_for(
                        "admin_study_recall_image",
                        session_id=session_id,
                        filename=result["image_filename"],
                    ),
                    "note_url": note_url,
                    "concept_title": _normalize_study_math_markup(result.get("concept_title")),
                    "topic": _normalize_study_math_markup(result.get("topic")),
                    "card_type": result.get("card_type") or "",
                    "has_formula": bool(result.get("has_formula")),
                    "excerpt": _normalize_study_math_markup(result.get("excerpt")),
                    "evidence": _normalize_study_math_markup(result.get("evidence")),
                    "match_reason": result.get("match_reason") or "相關內容",
                    "bbox": _validated_study_source_bbox(
                        result.get("bbox"),
                        expected_image_index=image_index,
                    ),
                }
            )
        record_ui_event(
            "study_recall_library_search",
            meta={
                "query_length": len(search_query),
                "subject": subject_filter or "all",
                "content_type": content_type,
                "session_id": session_filter,
                "result_count": len(search_results),
            },
        )
        return {
            "ok": True,
            "query": search_query,
            "result_count": len(search_results),
            "elapsed_ms": max(1, round((time.perf_counter() - started_at) * 1000)),
            "results": search_results,
        }

    @app.get("/admin/study-recall/<int:session_id>/image/<filename>")
    @admin_required
    def admin_study_recall_image(session_id: int, filename: str):
        recall_session = storage.get_study_recall_session(session_id)
        allowed_names = set((recall_session or {}).get("image_filenames") or [])
        if filename not in allowed_names or Path(filename).name != filename:
            return Response("Not Found", status=404, mimetype="text/plain")
        image_path = study_upload_root / str(session_id) / filename
        if not image_path.is_file():
            return Response("Not Found", status=404, mimetype="text/plain")
        return send_file(image_path, conditional=True, max_age=0)

    @app.post("/admin/study-recall/<int:session_id>/localize-sources")
    @admin_required
    def admin_study_recall_localize_sources(session_id: int):
        if not openai_api_key:
            return {"ok": False, "error": "來源定位尚未啟用，請先設定 OPENAI_API_KEY。"}, 503
        recall_session = storage.get_study_recall_session(session_id)
        if recall_session is None:
            return {"ok": False, "error": "找不到這份筆記。"}, 404
        concepts = recall_session.get("key_concepts") or []
        source_pages = recall_session.get("source_transcription") or []
        source_refs = [
            source_ref
            for concept in concepts
            if isinstance(concept, dict)
            for source_ref in concept.get("source_refs") or []
            if isinstance(source_ref, dict)
            and _literal_study_source_evidence(source_ref.get("evidence"))
        ]
        total_count = len(source_refs)
        if total_count == 0:
            return {"ok": False, "error": "這份筆記沒有可定位的來源片段。"}, 400

        images: List[Tuple[str, Any, str]] = []
        for filename in recall_session.get("image_filenames") or []:
            image_path = study_upload_root / str(session_id) / filename
            mime_type = _NOTE_IMAGE_MIME_TYPES.get(image_path.suffix.lower())
            if not mime_type or not image_path.is_file():
                return {"ok": False, "error": "找不到完整的原始筆記圖片，無法建立來源定位。"}, 404
            images.append((filename, image_path.read_bytes(), mime_type))
        if not images:
            return {"ok": False, "error": "找不到原始筆記圖片，無法建立來源定位。"}, 404

        user = current_user() or {}
        username = str(user.get("username") or "")
        now = time.time()
        with study_source_jobs_lock:
            expired_job_ids = [
                job_id
                for job_id, job in study_source_jobs.items()
                if float(job.get("updated_at") or 0) < now - STUDY_NOTE_STAGING_TTL_SECONDS
            ]
            for expired_job_id in expired_job_ids:
                study_source_jobs.pop(expired_job_id, None)
            active_job = next(
                (
                    (job_id, job)
                    for job_id, job in study_source_jobs.items()
                    if job.get("username") == username
                    and int(job.get("session_id") or 0) == session_id
                    and job.get("status") == "running"
                ),
                None,
            )
            if active_job is not None:
                active_job_id, active_job_data = active_job
                return {
                    "ok": True,
                    "background": True,
                    "job_id": active_job_id,
                    "status_url": url_for(
                        "admin_study_recall_localization_job",
                        job_id=active_job_id,
                    ),
                    "message": str(active_job_data.get("message") or "來源重新定位仍在進行。"),
                }, 202

            job_id = secrets.token_urlsafe(18)
            study_source_jobs[job_id] = {
                "username": username,
                "session_id": session_id,
                "status": "running",
                "progress": 12,
                "message": "已開始背景重新定位，可繼續使用其他頁面。",
                "created_at": now,
                "updated_at": now,
            }

        def _run_source_localization() -> None:
            try:
                _set_study_source_job(
                    job_id,
                    progress=24,
                    message="正在逐張比對重點卡與原始筆記。",
                )
                located_count, localized_total = _localize_study_card_sources(
                    images,
                    concepts,
                    source_pages,
                )
                _set_study_source_job(
                    job_id,
                    progress=92,
                    message="定位完成，正在安全寫回筆記。",
                )
                latest_session = storage.get_study_recall_session(session_id)
                if latest_session is None:
                    raise ValueError("這份筆記已不存在。")
                latest_concepts = latest_session.get("key_concepts") or []
                localized_sources: Dict[
                    Tuple[int, str], Tuple[int, Dict[str, Any]]
                ] = {}
                for concept_index, localized_concept in enumerate(concepts):
                    if not isinstance(localized_concept, dict):
                        continue
                    for localized_ref in localized_concept.get("source_refs") or []:
                        if not isinstance(localized_ref, dict):
                            continue
                        try:
                            image_index = int(localized_ref.get("image_index") or 0)
                        except (TypeError, ValueError):
                            continue
                        evidence_key = _canonical_study_source_match_text(
                            localized_ref.get("evidence")
                        )
                        bbox = _validated_study_source_bbox(
                            localized_ref.get("bbox"),
                            require_text_verified=True,
                            expected_image_index=image_index,
                        )
                        if image_index > 0 and evidence_key and bbox:
                            localized_sources[(concept_index, evidence_key)] = (
                                image_index,
                                bbox,
                            )
                for concept_index, latest_concept in enumerate(latest_concepts):
                    if not isinstance(latest_concept, dict):
                        continue
                    for latest_ref in latest_concept.get("source_refs") or []:
                        if not isinstance(latest_ref, dict):
                            continue
                        latest_ref.pop("bbox", None)
                        try:
                            image_index = int(latest_ref.get("image_index") or 0)
                        except (TypeError, ValueError):
                            continue
                        evidence_key = _canonical_study_source_match_text(
                            latest_ref.get("evidence")
                        )
                        localized_source = localized_sources.get(
                            (concept_index, evidence_key)
                        )
                        if localized_source:
                            resolved_image_index, bbox = localized_source
                            latest_ref["image_index"] = resolved_image_index
                            latest_ref["bbox"] = bbox
                localized_indexes = {
                    int(page.get("image_index") or 0): page.get("localization_index")
                    for page in source_pages
                    if isinstance(page, dict) and isinstance(page.get("localization_index"), dict)
                }
                latest_source_pages = latest_session.get("source_transcription") or []
                for latest_page in latest_source_pages:
                    if not isinstance(latest_page, dict):
                        continue
                    try:
                        latest_image_index = int(latest_page.get("image_index") or 0)
                    except (TypeError, ValueError):
                        continue
                    localization_index = localized_indexes.get(latest_image_index)
                    if localization_index:
                        latest_page["localization_index"] = localization_index
                storage.replace_study_recall_localization(
                    session_id,
                    key_concepts=latest_concepts,
                    source_transcription=latest_source_pages,
                )
                _set_study_source_job(
                    job_id,
                    status="success",
                    progress=100,
                    message=f"重新定位完成，已保留 {located_count} 個精確來源。",
                    located_count=located_count,
                    total_count=localized_total,
                )
                record_ui_event(
                    "study_recall_sources_relocalized",
                    meta={
                        "username": username,
                        "session_id": session_id,
                        "located_count": located_count,
                        "total_count": localized_total,
                    },
                )
            except Exception as exc:  # pragma: no cover - background integration path
                app.logger.exception("Study-note source localization backfill failed")
                message = str(exc).strip() or "AI 暫時無法完成來源定位，請稍後再試。"
                _set_study_source_job(
                    job_id,
                    status="error",
                    message=message[:240],
                )

        threading.Thread(target=_run_source_localization, daemon=True).start()
        return {
            "ok": True,
            "background": True,
            "job_id": job_id,
            "status_url": url_for(
                "admin_study_recall_localization_job",
                job_id=job_id,
            ),
            "message": "已開始背景重新定位，可繼續使用其他頁面。",
        }, 202

    @app.get("/admin/study-recall/localization-jobs/<job_id>")
    @admin_required
    def admin_study_recall_localization_job(job_id: str):
        user = current_user() or {}
        with study_source_jobs_lock:
            stored_job = study_source_jobs.get(job_id)
            job = dict(stored_job) if stored_job else None
        if not job or job.get("username") != user.get("username"):
            return {"ok": False, "error": "找不到這次重新定位工作。"}, 404
        payload = {
            "ok": True,
            "status": str(job.get("status") or "running"),
            "progress": int(job.get("progress") or 0),
            "message": str(job.get("message") or "正在重新定位來源。"),
        }
        if job.get("status") == "success":
            payload.update(
                located_count=int(job.get("located_count") or 0),
                total_count=int(job.get("total_count") or 0),
                reload_url=url_for(
                    "admin_study_recall",
                    session_id=int(job.get("session_id") or 0),
                ),
            )
        return payload

    @app.get("/admin/study-recall/<int:session_id>/cards/<int:concept_index>")
    @admin_required
    def admin_study_recall_card_detail(session_id: int, concept_index: int):
        recall_session = storage.get_study_recall_session(session_id)
        concepts = (recall_session or {}).get("key_concepts") or []
        if (
            concept_index < 0
            or concept_index >= len(concepts)
            or not isinstance(concepts[concept_index], dict)
            or not _is_recall_concept_eligible(concepts[concept_index])
        ):
            return {"ok": False, "error": "找不到這張關聯重點卡。"}, 404
        concept = concepts[concept_index]
        return {
            "ok": True,
            "card": {
                "title": _normalize_study_math_markup(concept.get("concept")),
                "topic": _normalize_study_math_markup(concept.get("topic")),
                "card_type": "example" if concept.get("card_type") == "example" else "concept",
                "core_summary": _normalize_study_math_markup(concept.get("core_summary")),
                "explanation": _normalize_study_math_markup(concept.get("explanation")),
                "simple_example": _normalize_study_math_markup(concept.get("simple_example")),
                "example_problem": _normalize_study_math_markup(concept.get("example_problem")),
                "example_method": _normalize_study_math_markup(concept.get("example_method")),
                "reasoning_steps": [
                    _normalize_study_math_markup(step)
                    for step in (concept.get("reasoning_steps") or [])[:6]
                    if str(step or "").strip()
                ],
                "common_confusion": _normalize_study_math_markup(concept.get("common_confusion")),
                "memory_hint": _normalize_study_math_markup(concept.get("memory_hint")),
            },
        }

    @app.post("/admin/study-recall/<int:session_id>/cards/<int:concept_index>/ask")
    @admin_required
    def admin_study_recall_ask_card(session_id: int, concept_index: int):
        if not openai_api_key:
            return {"ok": False, "error": "AI 問答尚未啟用，請先設定 OPENAI_API_KEY。"}, 503
        recall_session = storage.get_study_recall_session(session_id)
        concepts = (recall_session or {}).get("key_concepts") or []
        if concept_index < 0 or concept_index >= len(concepts) or not isinstance(concepts[concept_index], dict):
            return {"ok": False, "error": "找不到這張重點卡。"}, 404
        concept = concepts[concept_index]
        if not _is_recall_concept_eligible(concept):
            return {"ok": False, "error": "這張重點卡目前無法使用 AI 問答。"}, 400
        payload = request.get_json(silent=True) or {}
        question = " ".join(str(payload.get("question") or "").split()).strip()
        if not question:
            return {"ok": False, "error": "請輸入想詢問的內容。"}, 400
        if len(question) > 800:
            return {"ok": False, "error": "問題請控制在 800 字以內。"}, 400

        history: List[Dict[str, str]] = []
        raw_history = payload.get("history")
        if isinstance(raw_history, list):
            for entry in raw_history[-6:]:
                if not isinstance(entry, dict):
                    continue
                role = str(entry.get("role") or "").strip().lower()
                content = " ".join(str(entry.get("content") or "").split()).strip()[:1600]
                if role in {"user", "assistant"} and content:
                    history.append({"role": role, "content": content})

        relations = []
        for relation in (concept.get("relations") or [])[:2]:
            if not isinstance(relation, dict):
                continue
            title = str(relation.get("title") or "").strip()
            association = str(relation.get("association") or "").strip()
            if title and association:
                relations.append({"title": title[:80], "association": association[:240]})
        concept_topic = str(concept.get("topic") or "").strip()
        supporting_cards = []
        candidates = [
            (index, other)
            for index, other in enumerate(concepts)
            if index != concept_index and isinstance(other, dict) and _is_recall_concept_eligible(other)
        ]
        candidates.sort(
            key=lambda item: 0
            if concept_topic and str(item[1].get("topic") or "").strip() == concept_topic
            else 1
        )
        for _index, other in candidates[:6]:
            supporting_cards.append(
                {
                    "concept": str(other.get("concept") or "")[:80],
                    "core_summary": str(other.get("core_summary") or "")[:320],
                    "explanation": str(other.get("explanation") or "")[:600],
                    "simple_example": str(other.get("simple_example") or "")[:420],
                    "memory_hint": str(other.get("memory_hint") or "")[:120],
                }
            )
        card_context = {
            "note_title": str((recall_session or {}).get("title") or "")[:120],
            "note_summary": str((recall_session or {}).get("summary") or "")[:1200],
            "subject": str((recall_session or {}).get("subject") or "")[:48],
            "topic": concept_topic[:48],
            "concept": str(concept.get("concept") or "")[:80],
            "card_type": str(concept.get("card_type") or "concept")[:16],
            "core_summary": str(concept.get("core_summary") or "")[:320],
            "explanation": str(concept.get("explanation") or "")[:1200],
            "simple_example": str(concept.get("simple_example") or "")[:420],
            "example_problem": str(concept.get("example_problem") or "")[:420],
            "example_method": str(concept.get("example_method") or "")[:340],
            "memory_hint": str(concept.get("memory_hint") or "")[:160],
            "relations": relations,
            "supporting_cards": supporting_cards,
        }
        conversation = "\n".join(
            f"{'學生' if entry['role'] == 'user' else '助教'}：{entry['content']}"
            for entry in history
        )
        prompt = (
            "你是研究所考試筆記的即時 AI 助教。請以提供的重點卡為主要脈絡，回答學生目前不熟悉的觀念。"
            "重點卡、同主題卡與最近對話都可能含有錯誤，只能當作問題背景，不可當成事實依據；必須以可靠學科知識重新驗證。"
            "若卡片有可明確判定的錯誤，必須指出正確內容，不可沿用錯誤"
            "回答使用好理解的繁體中文，控制在 2 至 8 個短段落且完整收尾。數學表達式一律使用 LaTeX："
            "行內公式用 \\( ... \\)，獨立公式用 \\[ ... \\]。不要輸出 Markdown 標題、粗體標記或程式碼區塊。"
            "不得提及資料庫欄位、session_id、concept_index 或 s6:c6 這類內部代碼。\n\n"
            f"重點卡資料：\n{json.dumps(card_context, ensure_ascii=False)}\n\n"
            f"最近對話：\n{conversation or '尚無'}\n\n"
            f"學生這次的問題：{question}"
        )
        def request_answer(request_prompt: str) -> Tuple[str, bool]:
            response = requests.post(
                "https://api.openai.com/v1/responses",
                headers={"Authorization": f"Bearer {openai_api_key}", "Content-Type": "application/json"},
                json={
                    "model": openai_model,
                    "store": False,
                    "input": [{"role": "user", "content": [{"type": "input_text", "text": request_prompt}]}],
                    "reasoning": {
                        "effort": normalize_openai_reasoning_effort(openai_model, "low")
                    },
                    "max_output_tokens": 3200,
                },
                timeout=90,
            )
            response.raise_for_status()
            response_payload = response.json()
            return _extract_openai_text(response_payload).strip()[:8000], response_payload.get("status") == "incomplete"

        try:
            answer, incomplete = request_answer(prompt)
            if incomplete:
                answer, incomplete = request_answer(
                    prompt
                    + "\n\n上一次回答因長度限制而不完整。請重新從頭回答，不要接續殘句；保留必要公式與條件，"
                    "將完整答案壓縮在 1000 個繁體中文字內，並以完整句子收尾。"
                )
            answer = re.sub(r"\bs\d+\s*:\s*c\d+\b", "", answer, flags=re.IGNORECASE).strip()
        except requests.HTTPError as exc:
            error_code, error_type, error_message = _openai_error_details(exc.response)
            if _is_openai_quota_error(error_code, error_type, error_message):
                return {"ok": False, "error": "OpenAI API 額度不足，請管理員補充額度後再使用 AI 助教。"}, 503
            return {"ok": False, "error": "AI 助教暫時無法回答，請稍後再試。"}, 502
        except (requests.RequestException, ValueError, TypeError):
            return {"ok": False, "error": "AI 助教暫時無法回答，請稍後再試。"}, 502
        if incomplete:
            return {"ok": False, "error": "回答內容仍過長，請把問題縮小到一個觀念後再試。"}, 502
        if not answer:
            return {"ok": False, "error": "AI 助教沒有產生有效回答，請換個方式提問。"}, 502
        record_ui_event(
            "study_recall_card_question",
            meta={"session_id": session_id, "concept_index": concept_index, "history_count": len(history)},
        )
        return {"ok": True, "answer": answer}

    @app.post("/admin/study-recall/<int:session_id>/delete")
    @admin_required
    def admin_study_recall_delete(session_id: int):
        recall_session = storage.get_study_recall_session(session_id)
        if not recall_session:
            flash("找不到這份筆記紀錄。", "error")
            return redirect(url_for("admin_study_recall"))
        if not storage.delete_study_recall_session(session_id):
            flash("筆記刪除失敗，請再試一次。", "error")
            return redirect(url_for("admin_study_recall", session_id=session_id))
        upload_root = study_upload_root.resolve()
        image_directory = (upload_root / str(session_id)).resolve()
        if image_directory.parent == upload_root and image_directory.is_dir():
            try:
                shutil.rmtree(image_directory)
            except OSError:
                pass
        record_ui_event("study_recall_note_deleted", meta={"session_id": session_id, "subject": recall_session.get("subject")})
        flash("已刪除筆記、所屬重點卡、複習紀錄與原始圖片。", "success")
        return redirect(url_for("admin_study_recall"))

    @app.post("/admin/study-recall/<int:session_id>/rename")
    @admin_required
    def admin_study_recall_rename(session_id: int):
        recall_session = storage.get_study_recall_session(session_id)
        if not recall_session:
            flash("找不到這份筆記紀錄。", "error")
            return redirect(url_for("admin_study_recall"))
        title = " ".join(str(request.form.get("title") or "").split()).strip()
        if not title:
            flash("筆記名稱不能留空。", "error")
            return redirect(url_for("admin_study_recall", session_id=session_id))
        if len(title) > 120:
            flash("筆記名稱最多 120 個字。", "error")
            return redirect(url_for("admin_study_recall", session_id=session_id))
        if not storage.rename_study_recall_session(session_id, title):
            flash("筆記名稱修改失敗，請再試一次。", "error")
            return redirect(url_for("admin_study_recall", session_id=session_id))
        record_ui_event(
            "study_recall_note_renamed",
            meta={"session_id": session_id, "subject": recall_session.get("subject")},
        )
        flash("筆記名稱已更新。", "success")
        return redirect(url_for("admin_study_recall", session_id=session_id))

    @app.post("/admin/study-recall/upload-staging")
    @admin_required
    def admin_study_recall_upload_staging():
        user = current_user() or {}
        username = str(user.get("username") or "")
        if _active_study_upload_job(username):
            return _study_upload_error("已有一份筆記正在背景整理，請完成或取消後再上傳下一份。", 409)
        _cleanup_expired_study_upload_staging()
        try:
            image_index = int(request.form.get("image_index") or 0)
            total_images = int(request.form.get("total_images") or 0)
        except (TypeError, ValueError):
            return _study_upload_error("圖片上傳順序資料不正確。")
        if total_images < 1 or image_index < 1 or image_index > total_images:
            return _study_upload_error("圖片上傳順序資料不正確。")
        item = request.files.get("note_image")
        if not item or not item.filename:
            return _study_upload_error("找不到要上傳的筆記照片。")
        filename = secure_filename(item.filename) or "note-image"
        extension = Path(filename).suffix.lower()
        mime_type = _NOTE_IMAGE_MIME_TYPES.get(extension)
        if not mime_type:
            return _study_upload_error("筆記僅支援 JPG、PNG、WEBP 或 GIF 圖片。")
        image_bytes = item.stream.read(STUDY_NOTE_MAX_IMAGE_BYTES + 1)
        if not image_bytes or len(image_bytes) > STUDY_NOTE_MAX_IMAGE_BYTES:
            return _study_upload_error("每張筆記照片壓縮後必須小於 2MB。")

        upload_id = str(request.form.get("upload_id") or "").strip()
        manifest: Optional[Dict[str, Any]] = None
        directory: Optional[Path] = None
        if upload_id:
            manifest, directory = _read_study_upload_manifest(upload_id, username)
            if manifest is None or directory is None:
                return _study_upload_error("這次暫存上傳已失效，請重新選擇照片。", 404)
            if int(manifest.get("expected_count") or 0) != total_images:
                return _study_upload_error("圖片總數與這次暫存上傳不一致。")
        else:
            upload_id = secrets.token_urlsafe(24)
            directory = _study_upload_staging_directory(upload_id)
            if directory is None:
                return _study_upload_error("無法建立圖片暫存空間。", 500)
            _ensure_private_dir(directory)
            manifest = {
                "username": username,
                "expected_count": total_images,
                "files": {},
                "created_at": time.time(),
            }
        assert manifest is not None and directory is not None
        stored_name = f"{image_index:06d}{extension}"
        (directory / stored_name).write_bytes(image_bytes)
        files = manifest.get("files") if isinstance(manifest.get("files"), dict) else {}
        files[str(image_index)] = {
            "stored_name": stored_name,
            "original_name": filename,
            "mime_type": mime_type,
            "size": len(image_bytes),
        }
        manifest["files"] = files
        manifest["updated_at"] = time.time()
        _write_study_upload_manifest(directory, manifest)
        return {
            "ok": True,
            "upload_id": upload_id,
            "uploaded_count": len(files),
            "total_images": total_images,
        }

    @app.post("/admin/study-recall/upload-staging/<upload_id>/cancel")
    @admin_required
    def admin_study_recall_cancel_upload_staging(upload_id: str):
        user = current_user() or {}
        username = str(user.get("username") or "")
        removed = _remove_study_upload_staging(upload_id, username)
        return {"ok": True, "removed": removed}

    @app.post("/admin/study-recall/upload")
    @admin_required
    def admin_study_recall_upload():
        user = current_user() or {}
        username = str(user.get("username") or "")
        active_job_id = _active_study_upload_job(username)
        if active_job_id:
            if _is_study_upload_request():
                return {
                    "ok": True,
                    "background": True,
                    "job_id": active_job_id,
                    "message": "已有一份筆記正在背景整理。",
                }, 202
            flash("已有一份筆記正在背景整理，可先使用其他頁面。", "info")
            return redirect(url_for("admin_study_recall"))
        study_date = (request.form.get("study_date") or _study_plan_business_date().isoformat()).strip()
        try:
            date.fromisoformat(study_date)
        except ValueError:
            return _study_upload_error("請輸入有效的筆記日期。")
        subject = (request.form.get("subject") or "").strip()
        if subject not in STUDY_PLAN_SUBJECTS:
            return _study_upload_error("請選擇科目。")
        requested_title = (request.form.get("title") or "").strip()[:120]
        allow_corrections = str(request.form.get("allow_corrections") or "").strip().lower() in {"1", "true", "yes", "on"}
        skip_relation_rebuild = (
            request.headers.get("X-E3-Study-Reprocess") == "1"
            and request.headers.get("X-E3-Skip-Relation-Rebuild") == "1"
        )
        images: List[Tuple[str, bytes, str]] = []
        staging_directory: Optional[Path] = None
        staging_upload_id = str(request.form.get("upload_id") or "").strip()
        if staging_upload_id:
            manifest, staging_directory = _read_study_upload_manifest(staging_upload_id, username)
            if manifest is None or staging_directory is None:
                return _study_upload_error("這次暫存上傳已失效，請重新選擇照片。", 404)
            expected_count = int(manifest.get("expected_count") or 0)
            files = manifest.get("files") if isinstance(manifest.get("files"), dict) else {}
            if expected_count < 1 or len(files) != expected_count:
                return _study_upload_error(f"照片尚未傳完（{len(files)}/{expected_count} 張），請稍後再試。")
            for image_index in range(1, expected_count + 1):
                metadata = files.get(str(image_index))
                if not isinstance(metadata, dict):
                    return _study_upload_error(f"第 {image_index} 張照片尚未完成上傳。")
                stored_name = secure_filename(str(metadata.get("stored_name") or ""))
                image_path = (staging_directory / stored_name).resolve()
                if image_path.parent != staging_directory.resolve() or not image_path.is_file():
                    return _study_upload_error(f"第 {image_index} 張暫存照片已遺失，請重新上傳。")
                image_size = image_path.stat().st_size
                if image_size < 1 or image_size > STUDY_NOTE_MAX_IMAGE_BYTES:
                    return _study_upload_error(f"第 {image_index} 張照片大小不正確，請重新上傳。")
                images.append(
                    (
                        secure_filename(str(metadata.get("original_name") or "")) or f"note-{image_index}",
                        image_path,
                        str(metadata.get("mime_type") or "image/jpeg"),
                    )
                )
        else:
            incoming_files = [item for item in request.files.getlist("note_images") if item and item.filename]
            if not incoming_files:
                return _study_upload_error("請至少上傳 1 張筆記照片。")
            for item in incoming_files:
                filename = secure_filename(item.filename) or "note-image"
                extension = Path(filename).suffix.lower()
                mime_type = _NOTE_IMAGE_MIME_TYPES.get(extension)
                if not mime_type:
                    return _study_upload_error("筆記僅支援 JPG、PNG、WEBP 或 GIF 圖片。")
                image_bytes = item.stream.read(STUDY_NOTE_MAX_IMAGE_BYTES + 1)
                if not image_bytes or len(image_bytes) > STUDY_NOTE_MAX_IMAGE_BYTES:
                    return _study_upload_error("每張筆記照片壓縮後必須小於 2MB。")
                images.append((filename, image_bytes, mime_type))
        job_id = secrets.token_urlsafe(18)
        now = time.time()
        cancel_event = threading.Event()
        with study_upload_jobs_lock:
            study_upload_jobs[job_id] = {
                "username": username,
                "status": "running",
                "progress": 10,
                "message": "照片已接收，等待 AI 開始整理。",
                "cancel_event": cancel_event,
                "created_at": now,
                "updated_at": now,
            }

        def _run_study_upload() -> None:
            recall_id: Optional[int] = None
            destination: Optional[Path] = None
            study_upload_context.cancel_event = cancel_event
            study_upload_context.job_id = job_id

            def report_progress(progress: int, message: str) -> None:
                _raise_if_study_upload_cancelled()
                _set_study_upload_job(job_id, progress=progress, message=message)

            def cleanup_partial_upload() -> None:
                if recall_id is not None:
                    storage.delete_study_recall_session(recall_id)
                if destination is not None and destination.is_dir():
                    try:
                        shutil.rmtree(destination)
                    except OSError:
                        pass

            try:
                _raise_if_study_upload_cancelled()
                analysis, error = _analyze_study_note_images(
                    images,
                    subject=subject,
                    allow_corrections=allow_corrections,
                    progress_callback=report_progress,
                )
                if error or not analysis:
                    raise RuntimeError(error or "筆記分析失敗。")
                report_progress(98, "來源驗證與原圖定位完成，正在儲存原始圖片與卡片。")
                stored_names = [
                    f"{index + 1:02d}-{secrets.token_hex(5)}{Path(name).suffix.lower()}"
                    for index, (name, _bytes, _mime) in enumerate(images)
                ]
                title = requested_title or str(analysis.get("detected_topic") or "").strip()[:120] or f"{subject}筆記"
                _raise_if_study_upload_cancelled()
                recall_id = storage.create_study_recall_session(
                    study_date=study_date,
                    subject=subject,
                    title=title,
                    image_filenames=stored_names,
                    summary=analysis["summary"],
                    key_concepts=analysis["key_concepts"],
                    source_transcription=analysis.get("source_transcription") or [],
                    uncertain_fragments=analysis.get("uncertain_fragments") or [],
                    correction_records=analysis.get("correction_records") or [],
                    organization_mode=str(analysis.get("organization_mode") or "faithful"),
                )
                _raise_if_study_upload_cancelled()
                destination = _ensure_private_dir(study_upload_root / str(recall_id))
                for stored_name, (_original_name, image_source, _mime_type) in zip(stored_names, images):
                    _raise_if_study_upload_cancelled()
                    target = destination / stored_name
                    if isinstance(image_source, Path):
                        shutil.copyfile(image_source, target)
                    else:
                        target.write_bytes(image_source)
                if skip_relation_rebuild:
                    relation_error = None
                    final_message = "重點卡已完成；批次結束時會統一更新關聯與聯想。"
                else:
                    report_progress(99, "正在重新分析所有新舊重點卡的關聯與聯想。")
                    _raise_if_study_upload_cancelled()
                    with study_relation_rebuild_lock:
                        _raise_if_study_upload_cancelled()
                        relation_error = _rebuild_all_study_recall_relations()
                    _raise_if_study_upload_cancelled()
                    final_message = (
                        f"重點卡已完成；{relation_error}"
                        if relation_error
                        else "重點卡已完成，所有新舊卡片的關聯與聯想也已更新。"
                    )
                _set_study_upload_job(
                    job_id,
                    status="success",
                    progress=100,
                    message=final_message,
                    session_id=recall_id,
                )
                record_ui_event(
                    "study_recall_note_analyzed",
                    meta={"username": username, "session_id": recall_id, "subject": subject, "image_count": len(images)},
                )
            except _StudyUploadCancelled:
                cleanup_partial_upload()
                _set_study_upload_job(
                    job_id,
                    status="cancelled",
                    message="已取消這次筆記處理，可以立即上傳下一份筆記。",
                )
                record_ui_event("study_recall_note_analyzed", "cancelled", {"username": username})
            except Exception as exc:  # pragma: no cover - guarded by route-level integration tests
                cleanup_partial_upload()
                message = str(exc).strip() or "筆記背景處理失敗，請稍後重試。"
                _set_study_upload_job(job_id, status="error", message=message[:240])
                record_ui_event("study_recall_note_analyzed", "error", {"username": username, "reason": message[:160]})
            finally:
                if staging_directory is not None and staging_directory.is_dir():
                    try:
                        shutil.rmtree(staging_directory)
                    except OSError:
                        pass
                if hasattr(study_upload_context, "cancel_event"):
                    del study_upload_context.cancel_event
                if hasattr(study_upload_context, "job_id"):
                    del study_upload_context.job_id

        threading.Thread(target=_run_study_upload, daemon=True).start()
        if _is_study_upload_request():
            return {
                "ok": True,
                "background": True,
                "job_id": job_id,
                "message": "已開始背景整理，可自由前往其他頁面。",
            }, 202
        flash("已開始背景整理筆記，可自由前往其他頁面並從右下角查看進度。", "success")
        return redirect(url_for("admin_study_recall"))

    @app.get("/admin/study-recall/upload-jobs/<job_id>")
    @admin_required
    def admin_study_recall_upload_job(job_id: str):
        user = current_user() or {}
        with study_upload_jobs_lock:
            stored_job = study_upload_jobs.get(job_id)
            job = dict(stored_job) if stored_job else None
        if not job or job.get("username") != user.get("username"):
            return {"ok": False, "error": "找不到這次筆記處理工作。"}, 404
        payload = {
            "ok": True,
            "status": job.get("status") or "running",
            "progress": int(job.get("progress") or 0),
            "message": str(job.get("message") or "正在處理筆記。"),
        }
        if job.get("status") == "success" and job.get("session_id"):
            payload["session_id"] = int(job["session_id"])
            payload["redirect_url"] = url_for("admin_study_recall", session_id=int(job["session_id"]))
        return payload

    @app.post("/admin/study-recall/upload-jobs/<job_id>/cancel")
    @admin_required
    def admin_study_recall_cancel_upload_job(job_id: str):
        user = current_user() or {}
        with study_upload_jobs_lock:
            job = study_upload_jobs.get(job_id)
            if not job or job.get("username") != user.get("username"):
                return {"ok": False, "error": "找不到這次筆記處理工作。"}, 404
            status = str(job.get("status") or "running")
            if status == "success":
                return {"ok": False, "error": "筆記已完成，無法取消。", "status": status}, 409
            if status == "running":
                cancel_event = job.get("cancel_event")
                if isinstance(cancel_event, threading.Event):
                    cancel_event.set()
                job.update(
                    status="cancelled",
                    message="已取消這次筆記處理，可以立即上傳下一份筆記。",
                    updated_at=time.time(),
                )
            payload = {
                "ok": True,
                "status": "cancelled" if status in {"running", "cancelled"} else status,
                "progress": int(job.get("progress") or 0),
                "message": str(job.get("message") or "已取消這次筆記處理。"),
            }
        return payload

    @app.post("/admin/study-recall/<int:session_id>/rate-cards")
    @admin_required
    def admin_study_recall_rate_cards(session_id: int):
        is_async_rating = request.headers.get("X-E3-Recall-Rating") == "1"
        return_to = (request.form.get("return_to") or "").strip()
        if return_to not in {"admin_study_home", "admin_study_plan", "public_study_progress"}:
            return_to = ""

        def recall_redirect():
            return redirect(url_for(return_to) if return_to else url_for("admin_study_recall", session_id=session_id))

        def rating_error(message: str, status_code: int = 400):
            if is_async_rating:
                return {"ok": False, "error": message}, status_code
            flash(message, "error")
            return recall_redirect()

        recall_session = storage.get_study_recall_session(session_id)
        if not recall_session:
            return rating_error("找不到這份回想紀錄。", 404)
        ratings: Dict[int, int] = {}
        for index, _concept in enumerate(recall_session.get("key_concepts") or []):
            raw_rating = (request.form.get(f"rating_{index}") or "").strip()
            if not raw_rating:
                continue
            try:
                rating = int(raw_rating)
            except (TypeError, ValueError):
                rating = 0
            if rating not in {1, 2, 3, 4, 5}:
                return rating_error("印象分必須是 1 至 5 分。")
            ratings[index] = rating
        if not ratings:
            return rating_error("請至少為一張重點卡填寫印象分。")
        if storage.record_study_recall_card_ratings(
            session_id=session_id,
            ratings=ratings,
            review_date=_study_plan_business_date().isoformat(),
        ):
            next_session = storage.get_study_recall_session(session_id) or {}
            next_review_at = next_session.get("next_review_at") or "待安排"
            record_ui_event(
                "study_recall_cards_rated",
                meta={"session_id": session_id, "card_count": len(ratings), "next_review_at": next_review_at},
            )
            if is_async_rating:
                return {"ok": True, "remaining_due_count": _build_recall_widget_context()["due_count"]}
            flash(f"已記錄每張重點卡的印象分；最早的下一次複習是 {next_review_at}。", "success")
        elif is_async_rating:
            return {"ok": False, "error": "印象分暫時無法儲存，請再試一次。"}, 500
        else:
            flash("印象分暫時無法儲存，請再試一次。", "error")
        return recall_redirect()

    @app.route("/admin/study-settings", methods=["GET", "POST"])
    @admin_required
    def admin_study_settings():
        user = current_user()
        selected_subject = (request.args.get("subject") or request.form.get("subject") or "").strip()
        if selected_subject not in STUDY_PLAN_SUBJECTS:
            selected_subject = ""
        if request.method == "POST":
            try:
                video_id = int(request.form.get("video_id") or 0)
            except (TypeError, ValueError):
                video_id = 0
            parsed = _parse_youtube_url("") if request.form.get("clear") else _parse_youtube_url(request.form.get("youtube_url"))
            if not video_id:
                flash("找不到要設定的影片。", "error")
            elif parsed is None:
                flash("請輸入有效的 YouTube 影片連結，例如 https://www.youtube.com/watch?v=xxxxxxxxxxx。", "error")
            elif storage.update_study_plan_video_youtube(
                video_id=video_id,
                youtube_video_id=parsed["video_id"],
                youtube_playlist_id=parsed["playlist_id"],
                youtube_url=parsed["url"],
            ):
                record_ui_event(
                    "study_plan_video_youtube_updated",
                    meta={"video_id": video_id, "youtube_video_id": parsed["video_id"]},
                )
                flash("影片 YouTube 連結已更新。", "success")
            else:
                flash("找不到要設定的影片。", "error")
            return redirect(url_for("admin_study_settings", subject=selected_subject) if selected_subject else url_for("admin_study_settings"))

        videos = storage.list_study_plan_videos_with_records()
        if selected_subject:
            videos = [video for video in videos if video["subject"] == selected_subject]
        return render_template_string(
            STUDY_SETTINGS_TEMPLATE,
            admin_user=user,
            subjects=STUDY_PLAN_SUBJECTS,
            selected_subject=selected_subject,
            videos=videos,
        )

    @app.route("/admin/study-plan", methods=["GET", "POST"])
    @admin_required
    def admin_study_plan():
        user = current_user()
        if request.method == "POST":
            action = (request.form.get("action") or "save_video").strip()
            selected_subject = (request.form.get("subject") or "").strip()
            if selected_subject not in STUDY_PLAN_SUBJECTS:
                selected_subject = STUDY_PLAN_SUBJECTS[0]
            try:
                video_id = int(request.form.get("video_id") or 0)
            except (TypeError, ValueError):
                video_id = 0
            if action == "delete_video":
                if video_id and storage.delete_study_plan_video_record(video_id):
                    _invalidate_study_progress_context()
                    record_ui_event("study_plan_video_record_deleted", meta={"video_id": video_id})
            else:
                watched_minutes = _study_plan_minutes(request.form.get("watched_minutes"))
                notes = (request.form.get("notes") or "").strip()[:2000]
                if video_id and storage.upsert_study_plan_video_record(
                    video_id=video_id,
                    watched_seconds=watched_minutes * 60,
                    notes=notes,
                ):
                    _invalidate_study_progress_context()
                    record_ui_event(
                        "study_plan_video_record_saved",
                        meta={"video_id": video_id, "watched_minutes": watched_minutes},
                    )
            return redirect(url_for("admin_study_plan", subject=selected_subject))

        videos = storage.list_study_plan_videos_with_records()
        week_rows, _calendar_week, summary = _study_plan_week_rows(videos)
        current_week = _study_plan_progress_week(week_rows)
        current_subjects = current_week.get("subjects") or [current_week.get("subject")]
        default_subject = next(
            (subject for subject in current_subjects if subject in STUDY_PLAN_SUBJECTS),
            STUDY_PLAN_SUBJECTS[0],
        )
        selected_subject = (request.args.get("subject") or default_subject).strip()
        if selected_subject not in STUDY_PLAN_SUBJECTS:
            selected_subject = default_subject
        videos_by_subject: Dict[str, List[Dict[str, Any]]] = {subject: [] for subject in STUDY_PLAN_SUBJECTS}
        for video in videos:
            video["duration_minutes"] = round(float(video["duration_seconds"]) / 60, 1)
            video["watched_minutes"] = round(
                min(float(video["watched_seconds"]), float(video["duration_seconds"])) / 60,
                1,
            )
            video["completion"] = _study_plan_video_completion(video["duration_seconds"], video["watched_seconds"])
            videos_by_subject.setdefault(video["subject"], []).append(video)
        visible_videos = videos_by_subject.get(selected_subject, [])
        visible_video_ids = [int(video["id"]) for video in visible_videos]
        video_markers = storage.list_study_plan_video_markers(video_ids=visible_video_ids)
        replan_settings = storage.get_study_plan_replan_settings()
        replan_preview = _study_plan_replan_preview(replan_settings)
        next_week_start = _study_plan_week_start(_study_plan_business_date()) + timedelta(days=7)
        effective_plan_end = str((replan_settings or {}).get("end_date") or STUDY_PLAN_END)
        return render_template_string(
            STUDY_PLAN_TEMPLATE,
            admin_user=user,
            week_rows=week_rows,
            current_week=current_week,
            summary=summary,
            subjects=STUDY_PLAN_SUBJECTS,
            selected_subject=selected_subject,
            videos=visible_videos,
            video_count=len(visible_videos),
            plan_total_weeks=len(week_rows),
            plan_start=STUDY_PLAN_START,
            plan_end=effective_plan_end,
            recall_widget=_build_recall_widget_context(),
            video_markers=video_markers,
            replan_settings=replan_settings,
            replan_preview=replan_preview,
            replan_defaults={
                "start_date": next_week_start.isoformat(),
                "end_date": effective_plan_end,
                "weekday_hours": round(float((replan_settings or {}).get("weekday_minutes") or 180) / 60, 1),
                "weekend_hours": round(float((replan_settings or {}).get("weekend_minutes") or 120) / 60, 1),
            },
        )

    @app.post("/admin/study-plan/replan")
    @admin_required
    def admin_study_plan_replan():
        action = str(request.form.get("action") or "save").strip()
        selected_subject = str(request.form.get("subject") or "").strip()
        if selected_subject not in STUDY_PLAN_SUBJECTS:
            selected_subject = STUDY_PLAN_SUBJECTS[0]
        if action == "reset":
            if storage.delete_study_plan_replan_settings():
                _invalidate_study_progress_context()
                record_ui_event("study_plan_replan_reset")
                flash("已恢復原始讀書計畫。", "success")
            return redirect(url_for("admin_study_plan", subject=selected_subject))

        start_day = _study_plan_week_start(_study_plan_business_date()) + timedelta(days=7)
        try:
            end_day = datetime.strptime(str(request.form.get("end_date") or ""), "%Y-%m-%d").date()
            weekday_hours = float(request.form.get("weekday_hours") or 0)
            weekend_hours = float(request.form.get("weekend_hours") or 0)
        except (TypeError, ValueError):
            flash("請輸入有效的日期與每日可讀時數。", "error")
            return redirect(url_for("admin_study_plan", subject=selected_subject) + "#smart-replan")
        if end_day < start_day + timedelta(days=6):
            flash(f"目標日期至少需要晚於 {start_day.isoformat()} 一週。", "error")
            return redirect(url_for("admin_study_plan", subject=selected_subject) + "#smart-replan")
        if end_day > start_day + timedelta(days=366):
            flash("智慧計畫最長可安排一年。", "error")
            return redirect(url_for("admin_study_plan", subject=selected_subject) + "#smart-replan")
        if not (0.25 <= weekday_hours <= 12 and 0.25 <= weekend_hours <= 12):
            flash("平日與假日可讀時數需介於 0.25 至 12 小時。", "error")
            return redirect(url_for("admin_study_plan", subject=selected_subject) + "#smart-replan")

        videos = storage.list_study_plan_videos_with_records()
        baseline_by_subject: Dict[str, float] = {}
        subject_targets: Dict[str, float] = {}
        for subject in STUDY_PLAN_SUBJECTS:
            subject_videos = [video for video in videos if str(video.get("subject") or "") == subject]
            total_seconds = sum(_study_plan_nonnegative_number(video.get("duration_seconds")) for video in subject_videos)
            watched_seconds = sum(
                min(
                    _study_plan_nonnegative_number(video.get("watched_seconds")),
                    _study_plan_nonnegative_number(video.get("duration_seconds")),
                )
                for video in subject_videos
            )
            baseline_by_subject[subject] = watched_seconds
            remaining_seconds = max(0.0, total_seconds - watched_seconds)
            if remaining_seconds > 0.001:
                subject_targets[subject] = remaining_seconds
        if not subject_targets:
            flash("所有影片都已完成，目前不需要重新安排。", "success")
            return redirect(url_for("admin_study_plan", subject=selected_subject))
        storage.save_study_plan_replan_settings(
            start_date=start_day.isoformat(),
            end_date=end_day.isoformat(),
            weekday_minutes=weekday_hours * 60,
            weekend_minutes=weekend_hours * 60,
            baseline_by_subject=baseline_by_subject,
            subject_targets=subject_targets,
        )
        _invalidate_study_progress_context()
        record_ui_event(
            "study_plan_replanned",
            meta={
                "start_date": start_day.isoformat(),
                "end_date": end_day.isoformat(),
                "weekday_hours": weekday_hours,
                "weekend_hours": weekend_hours,
            },
        )
        flash("智慧重排已套用，既有觀看紀錄都已保留。", "success")
        return redirect(url_for("admin_study_plan", subject=selected_subject) + "#smart-replan")

    @app.post("/admin/study-plan/video-markers")
    @admin_required
    def admin_study_plan_video_markers():
        payload = request.get_json(silent=True) or {}
        try:
            video_id = int(payload.get("video_id") or 0)
            playback_seconds = float(payload.get("playback_seconds") or 0)
        except (TypeError, ValueError):
            return {"ok": False, "error": "invalid_payload"}, 400
        if video_id <= 0 or not math.isfinite(playback_seconds):
            return {"ok": False, "error": "invalid_payload"}, 400
        marker = storage.create_study_plan_video_marker(
            video_id=video_id,
            playback_seconds=playback_seconds,
            note=str(payload.get("note") or ""),
        )
        if not marker:
            return {"ok": False, "error": "video_not_found"}, 404
        record_ui_event(
            "study_plan_video_marker_created",
            meta={"video_id": video_id, "playback_seconds": round(marker["playback_seconds"], 1)},
        )
        return {"ok": True, "marker": marker}

    @app.patch("/admin/study-plan/video-markers/<int:marker_id>")
    @admin_required
    def admin_study_plan_video_marker_update(marker_id: int):
        payload = request.get_json(silent=True) or {}
        marker = storage.update_study_plan_video_marker(marker_id, note=str(payload.get("note") or ""))
        if not marker:
            return {"ok": False, "error": "marker_not_found"}, 404
        record_ui_event("study_plan_video_marker_updated", meta={"marker_id": marker_id})
        return {"ok": True, "marker": marker}

    @app.delete("/admin/study-plan/video-markers/<int:marker_id>")
    @admin_required
    def admin_study_plan_video_marker_delete(marker_id: int):
        if not storage.delete_study_plan_video_marker(marker_id):
            return {"ok": False, "error": "marker_not_found"}, 404
        record_ui_event("study_plan_video_marker_deleted", meta={"marker_id": marker_id})
        return {"ok": True}

    @app.post("/admin/study-plan/video-progress")
    @admin_required
    def admin_study_plan_video_progress():
        payload = request.get_json(silent=True) or {}
        try:
            video_id = int(payload.get("video_id") or 0)
            watched_seconds = float(payload.get("watched_seconds") or 0)
            expected_version = int(payload.get("expected_version"))
        except (TypeError, ValueError):
            return {"ok": False, "error": "invalid_payload"}, 400
        if video_id <= 0 or expected_version < 0:
            return {"ok": False, "error": "missing_video"}, 400
        if not math.isfinite(watched_seconds):
            return {"ok": False, "error": "invalid_progress"}, 400
        result = storage.update_study_plan_video_progress(
            video_id=video_id,
            watched_seconds=watched_seconds,
            expected_version=expected_version,
        )
        if not result:
            return {"ok": False, "error": "video_not_found"}, 404
        result["completion"] = _study_plan_video_completion(
            result.get("duration_seconds"),
            result.get("watched_seconds"),
        )
        if not result.get("stale"):
            _invalidate_study_progress_context()
        videos = storage.list_study_plan_videos_with_records()
        week_rows, _calendar_week, summary = _study_plan_week_rows(videos)
        current_week = _study_plan_progress_week(week_rows)
        if not result.get("stale"):
            record_ui_event(
                "study_plan_youtube_progress_saved",
                meta={"video_id": video_id, "watched_seconds": round(float(result["watched_seconds"]), 1)},
            )
        return {
            "ok": True,
            **result,
            "summary": {
                "total_watched_hours": round(float(summary["total_watched"]) / 60, 1),
                "completion": round(float(summary["completion"]), 1),
                "completed_videos": int(summary["completed_videos"]),
                "total_videos": int(summary["total_videos"]),
                "video_completion": round(float(summary["video_completion"]), 1),
            },
            "current_week": {
                "start": current_week["start"],
                "watched_minutes": round(float(current_week["watched_minutes"]), 1),
                "watched_hours": round(float(current_week["watched_seconds"]) / 3600, 2),
                "remaining_hours": round(float(current_week["remaining_hours"]), 1),
                "completion": round(float(current_week["completion"]), 1),
                "state": current_week["state"],
                "state_label": current_week["state_label"],
                "daily_recommendations": current_week["daily_recommendations"],
            },
        }

    @app.route("/admin/traffic", methods=["GET"])
    @login_required
    def admin_traffic():
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員瀏覽流量資訊。", "error")
            return redirect(url_for("index"))
        admin_view_options = list_admin_view_options()
        selected_view_username = user["username"]
        requested_view_username = (request.args.get("view_user") or "").strip()
        if requested_view_username:
            valid_usernames = {item["username"] for item in admin_view_options}
            if requested_view_username in valid_usernames:
                selected_view_username = requested_view_username

        def _fmt_ts(ts: Optional[float]) -> str:
            if not ts:
                return "-"
            try:
                dt = datetime.fromtimestamp(ts, tz=TAIPEI_TZ)
            except Exception:
                return "-"
            return dt.strftime("%Y-%m-%d %H:%M:%S")

        ACTION_LABELS = {
            "login_success": "登入成功",
            "logout": "登出",
            "guest_login": "訪客登入",
            "guest_import": "匯入訪客資料",
            "refresh_assignments": "更新作業資料",
            "ui-event": "操作事件",
        }

        def _action_description(action: str) -> str:
            action = action or "-"
            return ACTION_LABELS.get(action, action.replace("_", " "))

        user_breakdown = traffic_tracker.user_breakdown()
        ip_overview = traffic_tracker.ip_summary()
        guest_overview = traffic_tracker.guest_summary()
        formatted_users = [
            {
                "username": entry["username"],
                "count": entry["count"],
                "online": entry["online"],
                "last_seen": _fmt_ts(entry.get("last_seen")),
            }
            for entry in user_breakdown
        ]
        raw_events = traffic_tracker.recent_events(500)
        filtered_events = [
            ev
            for ev in raw_events
            if (ev.get("action") or "").lower() not in PASSIVE_TRAFFIC_ACTIONS
        ]
        formatted_events = []
        for ev in reversed(filtered_events[-200:]):
            meta = ev.get("meta") or {}
            role = "訪客" if meta.get("is_guest") else ("管理員" if meta.get("is_admin") else "一般使用者")
            detail_parts: List[str] = []
            for key in ("info", "course", "message", "target", "action_detail"):
                val = meta.get(key)
                if val:
                    detail_parts.append(f"{key}: {val}")
            extra = {
                key: value
                for key, value in meta.items()
                if key
                not in {"username", "is_guest", "is_admin", "info", "course", "message", "target", "action_detail"}
            }
            if extra:
                try:
                    detail_parts.append(json.dumps(extra, ensure_ascii=False))
                except Exception:
                    detail_parts.append(str(extra))
            formatted_events.append(
                {
                    "ts": _fmt_ts(ev.get("ts")),
                    "ip": ev.get("ip") or "-",
                    "action": ev.get("action") or "-",
                    "status": ev.get("status") or "info",
                    "username": meta.get("username") or "-",
                    "description": _action_description(ev.get("action") or "-"),
                    "details": "；".join(detail_parts),
                }
            )
        trend_window = request.args.get("trend", "hour")
        if trend_window not in {"hour", "day"}:
            trend_window = "hour"
        hourly_series = traffic_tracker.hourly_series()
        if trend_window == "day":
            daily_map: Dict[int, Set[str]] = {}
            for ts, members in traffic_tracker.hourly_buckets().items():
                day_dt = datetime.fromtimestamp(ts, tz=TAIPEI_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
                day_ts = int(day_dt.timestamp())
                day_set = daily_map.setdefault(day_ts, set())
                day_set.update(members)
            
            if daily_map:
                sorted_days_keys = sorted(daily_map.keys())
                min_day_ts = sorted_days_keys[0]
                max_day_ts = sorted_days_keys[-1]
                
                full_daily_series = []
                current_ts = min_day_ts
                while current_ts <= max_day_ts:
                    full_daily_series.append({
                        "ts": current_ts,
                        "count": len(daily_map.get(current_ts, []))
                    })
                    current_ts += 86400

                chart_labels = [datetime.fromtimestamp(item["ts"], tz=TAIPEI_TZ).strftime("%Y-%m-%d") for item in full_daily_series]
                chart_values = [item["count"] for item in full_daily_series]
            else:
                chart_labels = []
                chart_values = []
        else:
            if hourly_series:
                full_series = []
                series_dict = {item["ts"]: item["count"] for item in hourly_series}
                min_ts = hourly_series[0]["ts"]
                max_ts = hourly_series[-1]["ts"]
                
                current_ts = min_ts
                while current_ts <= max_ts:
                    full_series.append({
                        "ts": current_ts,
                        "count": series_dict.get(current_ts, 0)
                    })
                    current_ts += 3600
                
                hourly_series = full_series

            chart_labels = [datetime.fromtimestamp(item["ts"], tz=TAIPEI_TZ).strftime("%m-%d %H:00") for item in hourly_series]
            chart_values = [item["count"] for item in hourly_series]
        if not chart_labels or not chart_values:
            # fallback to on-the-fly aggregation of filtered events to avoid空白圖
            buckets: Dict[datetime, Set[str]] = {}
            for ev in filtered_events:
                ts = ev.get("ts")
                if not ts:
                    continue
                meta = ev.get("meta") or {}
                username = meta.get("username")
                if not username or meta.get("is_guest"):
                    continue
                try:
                    dt = datetime.fromtimestamp(float(ts), tz=TAIPEI_TZ)
                except Exception:
                    continue
                action = (ev.get("action") or "").lower()
                if action in PASSIVE_TRAFFIC_ACTIONS:
                    continue
                if trend_window == "day":
                    bucket = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                else:
                    bucket = dt.replace(minute=0, second=0, microsecond=0)
                bucket_set = buckets.setdefault(bucket, set())
                bucket_set.add(str(username))
            sorted_keys = sorted(buckets.keys())
            chart_labels = [
                key.strftime("%Y-%m-%d") if trend_window == "day" else key.strftime("%m-%d %H:00")
                for key in sorted_keys
            ]
            chart_values = [len(buckets[key]) for key in sorted_keys]
        action_counter: Counter = Counter()
        for ev in filtered_events:
            action_counter[ev.get("action") or "-"] += 1
        top_actions = [{"action": action, "count": count} for action, count in action_counter.most_common(5)]
        recent_unique_keys = set()
        for ev in raw_events:
            meta = ev.get("meta") or {}
            username = meta.get("username")
            if username and not meta.get("is_guest"):
                recent_unique_keys.add(username)
            elif not username and ev.get("ip"):
                recent_unique_keys.add(ev.get("ip"))
        summary = {
            "unique_users": len(formatted_users),
            "online_users": sum(1 for entry in formatted_users if entry["online"]),
            "recent_unique_users": len(recent_unique_keys),
            "last_event": _fmt_ts(filtered_events[-1].get("ts")) if filtered_events else "-",
            "last_action": (filtered_events[-1].get("action") or "-") if filtered_events else "-",
            "event_samples": len(filtered_events),
        }
        if formatted_users:
            summary["top_user"] = formatted_users[0]["username"]
            summary["top_user_count"] = formatted_users[0]["count"]
        else:
            summary["top_user"] = "-"
            summary["top_user_count"] = 0
        summary["ip_total_hits"] = ip_overview["total"]
        summary["unique_ips"] = ip_overview["unique"]
        summary["online_ips"] = ip_overview["online"]
        summary["guest_total"] = guest_overview.get("total", 0)
        summary["guest_online"] = guest_overview.get("online", 0)
        return render_template_string(
            TRAFFIC_TEMPLATE,
            stats=usage_stats(),
            stats_version=current_stats_version(),
            user_rows=formatted_users,
            events=formatted_events,
            generated_at=_fmt_ts(time.time()),
            admin_user=user,
            chart_labels=chart_labels,
            chart_values=chart_values,
            top_actions=top_actions,
            top_users=formatted_users[:5],
            summary=summary,
            trend_window=trend_window,
            trend_label="每小時" if trend_window == "hour" else "每天",
            ip_summary=ip_overview,
            admin_view_options=admin_view_options,
            selected_view_username=selected_view_username,
        )

    @app.route("/admin/traffic/reset", methods=["POST"])
    @login_required
    def admin_traffic_reset():
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員操作。", "error")
            return redirect(url_for("index"))
        traffic_tracker.reset()
        flash("已清除所有流量統計與累積訪問次數。", "success")
        record_ui_event("reset_traffic", "success")
        return redirect(url_for("admin_traffic"))

    @app.post("/admin/traffic/reset-user")
    @login_required
    def admin_traffic_reset_user():
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員操作。", "error")
            return redirect(url_for("index"))
        target = (request.form.get("username") or "").strip()
        if not target:
            flash("請提供要清除統計的帳號名稱。", "warning")
            return redirect(url_for("admin_traffic"))
        removed = traffic_tracker.remove_user_stats(target)
        deleted_events = storage.delete_traffic_events_for_user(target)
        if removed or deleted_events:
            flash(f"已清除 {target} 的統計與事件紀錄（移除 {deleted_events} 筆事件）。", "success")
            record_ui_event("reset_traffic_user", meta={"target": target, "events_removed": deleted_events})
        else:
            flash("找不到對應的統計資料，未執行變更。", "info")
        return redirect(url_for("admin_traffic"))

    @app.route("/admin/feedback", methods=["GET", "POST"])
    @login_required
    def admin_feedback():
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員操作。", "error")
            return redirect(url_for("index"))
        if request.method == "POST":
            feedback_id = request.form.get("id")
            status = request.form.get("status", "open")
            if update_feedback_status_entry(feedback_id, status):
                flash("狀態已更新。", "success")
            else:
                flash("更新失敗，請稍後再試。", "error")
            return redirect(url_for("admin_feedback"))
        feedback_items = list_feedback_entries()
        open_count = sum(1 for item in feedback_items if (item.get("status") or "open") == "open")
        return render_template_string(
            ADMIN_FEEDBACK_TEMPLATE,
            admin_user=user,
            feedback_entries=feedback_items,
            open_count=open_count,
        )

    @app.route("/admin/announcements", methods=["GET", "POST"])
    @login_required
    def admin_announcements():
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員操作。", "error")
            return redirect(url_for("index"))
        if request.method == "POST":
            title = request.form.get("title", "").strip()
            content = request.form.get("content", "").strip()
            if not title or not content:
                flash("請輸入公告標題與內容。", "error")
            else:
                add_announcement(title, content, user["username"])
                flash("公告已發布。", "success")
                return redirect(url_for("admin_announcements"))
        return render_template_string(
            ANNOUNCEMENTS_TEMPLATE,
            admin_user=user,
            announcements=load_announcements(),
        )

    @app.post("/admin/announcements/<announcement_id>/delete")
    @login_required
    def delete_announcement(announcement_id: str):
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員操作。", "error")
            return redirect(url_for("index"))
        if delete_announcement_entry(announcement_id):
            flash("公告已刪除。", "info")
        else:
            flash("找不到指定的公告。", "error")
        return redirect(url_for("admin_announcements"))

    @app.route("/feedback", methods=["GET", "POST"])
    def feedback():
        user = current_user()
        if request.method == "POST":
            message = request.form.get("message", "")
            email = request.form.get("email", "")
            name = request.form.get("name", "")
            username = user["username"] if user else name
            feedback_id = add_feedback_entry(message, email, username)
            if feedback_id:
                flash("已收到回報，感謝你的意見！", "success")
                record_ui_event("feedback_submitted", "success", {"feedback_id": feedback_id})
                return redirect(url_for("feedback"))
            flash("請輸入回報內容。", "error")
        return render_template_string(
            FEEDBACK_TEMPLATE,
            admin_user=user,
            support_email=support_email,
            stats=usage_stats(),
            stats_version=current_stats_version(),
        )

    @app.route("/", methods=["GET"])
    def index():
        user = current_user()
        if not user:
            return render_template_string(
                HOME_TEMPLATE,
                stats=usage_stats(),
                stats_version=current_stats_version(),
                google_scope=GOOGLE_CALENDAR_SCOPE,
                app_home_url=app_home_url,
                support_email=support_email,
            )
        context = _build_dashboard_context(user)
        return render_template_string(
            WEB_TEMPLATE,
            **context,
            app_home_url=app_home_url,
            support_email=support_email,
        )

    return app
