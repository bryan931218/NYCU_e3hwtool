from .routes.assignments import register_assignments_routes
from .routes.study_home import register_study_home_routes
from .routes.note_library import register_note_library_routes
from .routes.note_search import register_note_search_routes
from .routes.note_sources import register_note_sources_routes
from .routes.assistant import register_assistant_routes
from .routes.assistant_actions import register_assistant_actions_routes
from .routes.note_uploads import register_note_uploads_routes
from .routes.study_plan import register_study_plan_routes
from .routes.video import register_video_routes
from .routes.administration import register_administration_routes

from ..services.note_model_client import build_note_model_client
from ..services.note_text import build_note_text
from ..services.note_geometry import build_note_geometry
from ..services.note_localization_legacy import build_note_localization_legacy
from ..services.note_localization_bands import build_note_localization_bands
from ..services.note_localization_consensus import build_note_localization_consensus
from ..services.note_localization import build_note_localization
from ..services.note_validation import build_note_validation
from ..services.note_batch_analysis import build_note_batch_analysis
from ..shared.study_note_quality import is_study_note_process_metadata_card
from ..services.note_analysis import build_note_analysis
from ..services.note_relations import build_note_relations

from ..services.traffic import (
    PASSIVE_TRAFFIC_ACTIONS,
    TrafficTracker,
)
from ..services.study_upload_batches import (
    STUDY_NOTE_BATCH_CHECKPOINT_VERSION,
    STUDY_UPLOAD_ANALYSIS_START_PROGRESS,
    STUDY_UPLOAD_BATCHES_END_PROGRESS,
    _study_upload_time_weighted_progress,
    _study_upload_parallel_progress,
    _study_note_batch_signature,
    _load_study_note_batch_checkpoint,
    _save_study_note_batch_checkpoint,
    _offset_study_note_batch_analysis,
)
from ..services.study_progress import (
    STUDY_PLAN_WEEKEND_VIDEO_HOUR_CAP,
    STUDY_PLAN_DAILY_LABELS,
    STUDY_PLAN_COMPLETE_TOLERANCE_SECONDS,
    STUDY_PLAN_COMPLETE_RATIO,
    STUDY_PLAN_DAY_CUTOFF_HOUR,
    _study_plan_business_date,
    _study_plan_business_day_from_timestamp,
    _study_plan_nonnegative_number,
    _study_plan_video_completion,
    _study_plan_video_is_complete,
    _study_plan_default_video,
    _study_plan_credited_video_seconds,
    _study_plan_range_credited_seconds,
    _parse_youtube_url,
    _study_plan_total_is_complete,
    _study_plan_completion_percent,
    _study_plan_progress_summary,
    _study_plan_task_video_queue,
    _study_plan_interleave_video_queues,
    _study_plan_progress_week,
    _study_plan_subject_status,
    _study_plan_progress_race,
    _study_plan_pace_history,
    _study_plan_daily_recommendations,
    _study_plan_week_start,
)

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
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qs, urlsplit, urlunsplit

import requests
from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageFont, ImageOps
from flask import Flask, Response, flash, redirect, render_template_string, request, send_file, session, url_for, has_request_context
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.http import http_date
from werkzeug.utils import secure_filename

from ..services.collector import (
    CollectOptions,
    annotate_result_semesters,
    collect_assignments,
    current_semester_key,
    merge_current_semester_cache,
    normalize_semester_keys,
)
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
from ..services.youtube_playlists import (
    KNOWN_YOUTUBE_PLAYLISTS,
    YoutubePlaylistSyncBusyError,
    sync_known_youtube_playlists,
)
from ..services.youtube_frames import (
    YoutubeAudioError,
    YoutubeFrameError,
    fetch_youtube_audio_clip,
    fetch_youtube_cached_frame,
    fetch_youtube_storyboard_metadata,
)
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
    protect_markdown_code,
    repair_math_delimiters,
    restore_markdown_code,
    wrap_bare_math_candidate,
)
from ..shared.visual_notes import (
    merge_visual_regions,
    normalize_visual_regions,
    render_visual_region_svg,
    visual_region_crop_box,
)
from ..shared.study_note_composer import (
    StudyNoteToolAccumulator,
    StudyNoteToolError,
    build_study_note_tools,
    run_study_note_tool_conversation,
)
from ..shared.excel import build_excel
from ..shared.utils import json_safe


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
STUDY_MARKERS_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "admin_study_markers.html"
STUDY_MARKERS_TEMPLATE = STUDY_MARKERS_TEMPLATE_PATH.read_text(encoding="utf-8")
STUDY_SETTINGS_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "admin_study_settings.html"
STUDY_SETTINGS_TEMPLATE = STUDY_SETTINGS_TEMPLATE_PATH.read_text(encoding="utf-8")
STUDY_HOME_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "admin_study_home.html"
STUDY_HOME_TEMPLATE = STUDY_HOME_TEMPLATE_PATH.read_text(encoding="utf-8")
PUBLIC_STUDY_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "public_study_progress.html"
PUBLIC_STUDY_TEMPLATE = PUBLIC_STUDY_TEMPLATE_PATH.read_text(encoding="utf-8")
STUDY_RECALL_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "study_recall.html"
STUDY_RECALL_TEMPLATE = STUDY_RECALL_TEMPLATE_PATH.read_text(encoding="utf-8")
STUDY_RECALL_QUICK_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "study_recall_quick.html"
STUDY_RECALL_QUICK_TEMPLATE = STUDY_RECALL_QUICK_TEMPLATE_PATH.read_text(encoding="utf-8")
STUDY_UPLOAD_TRACKER_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "_study_upload_tracker.html"
STUDY_UPLOAD_TRACKER_TEMPLATE = STUDY_UPLOAD_TRACKER_TEMPLATE_PATH.read_text(encoding="utf-8")
GLOBAL_STUDY_ASSISTANT_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "_global_study_assistant.html"
GLOBAL_STUDY_ASSISTANT_TEMPLATE = GLOBAL_STUDY_ASSISTANT_TEMPLATE_PATH.read_text(encoding="utf-8")


def _attach_global_study_assistant(template: str) -> str:
    """Mount the authenticated assistant without duplicating page-specific markup."""
    if "data-e3-global-ai" in template or "</body>" not in template:
        return template
    return template.replace("</body>", f"{GLOBAL_STUDY_ASSISTANT_TEMPLATE}\n</body>", 1)


# Keep the assistant inside the study center. The assignment tracker and public pages
# stay focused on their own workflows.
STUDY_PLAN_TEMPLATE = _attach_global_study_assistant(STUDY_PLAN_TEMPLATE)
STUDY_MARKERS_TEMPLATE = _attach_global_study_assistant(STUDY_MARKERS_TEMPLATE)
STUDY_SETTINGS_TEMPLATE = _attach_global_study_assistant(STUDY_SETTINGS_TEMPLATE)
STUDY_HOME_TEMPLATE = _attach_global_study_assistant(STUDY_HOME_TEMPLATE)
STUDY_RECALL_QUICK_TEMPLATE = _attach_global_study_assistant(STUDY_RECALL_QUICK_TEMPLATE)

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
STUDY_NOTE_MAX_IMAGE_BYTES = 2 * 1024 * 1024
STUDY_NOTE_MAX_REQUEST_BYTES = 16 * 1024 * 1024
STUDY_NOTE_AI_BATCH_SIZE = 8
STUDY_NOTE_STAGING_TTL_SECONDS = 24 * 60 * 60


def _study_plan_schedule_definitions(
    videos: Iterable[Dict[str, Any]],
    replan_settings: Optional[Dict[str, Any]] = None,
    rest_days: Optional[Iterable[str]] = None,
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
            "is_rest_day": False,
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

    requested_rest_day_keys = {
        str(value)
        for value in (rest_days or [])
        if str(value) in days and first_day <= days[str(value)]["date"] <= last_scheduled_day
    }
    original_subjects_by_day = {
        key: {
            str(subject)
            for subject, seconds in dict(row.get("allocations") or {}).items()
            if _study_plan_nonnegative_number(seconds) > 0.001
        }
        for key, row in days.items()
    }

    def matching_future_rows(
        rest_day_key: str,
        rest_row: Dict[str, Any],
        blocked_days: set[str],
        subject: str,
    ) -> List[Dict[str, Any]]:
        return [
            row
            for key, row in sorted(days.items())
            if key > rest_day_key
            and key not in blocked_days
            and row["date"] <= last_scheduled_day
            and bool(row.get("replanned")) == bool(rest_row.get("replanned"))
            and subject in original_subjects_by_day.get(key, set())
        ]

    def can_redistribute_day(
        rest_day_key: str,
        rest_row: Dict[str, Any],
        blocked_days: set[str],
    ) -> bool:
        subjects = original_subjects_by_day.get(rest_day_key, set())
        return bool(subjects) and all(
            matching_future_rows(rest_day_key, rest_row, blocked_days, subject)
            for subject in subjects
        )

    # A rest day may only hand work to a later day in the same schedule segment.
    # Resolve from the end so consecutive requests keep the final usable day active.
    effective_rest_day_keys = set()
    for rest_day_key in sorted(requested_rest_day_keys, reverse=True):
        rest_row = days[rest_day_key]
        if can_redistribute_day(rest_day_key, rest_row, effective_rest_day_keys):
            effective_rest_day_keys.add(rest_day_key)

    for rest_day_key in sorted(effective_rest_day_keys):
        rest_row = days[rest_day_key]
        moved_allocations = dict(rest_row["allocations"])
        if not moved_allocations:
            rest_row["focus"] = "休息日"
            rest_row["is_rest_day"] = True
            rest_row["redistributed_seconds"] = 0.0
            rest_row["redistributed_day_count"] = 0
            continue
        touched_dates = set()
        for subject, seconds in moved_allocations.items():
            candidates = matching_future_rows(
                rest_day_key,
                rest_row,
                effective_rest_day_keys,
                str(subject),
            )
            addition = seconds / len(candidates)
            for candidate in candidates:
                candidate["allocations"][subject] = candidate["allocations"].get(subject, 0.0) + addition
                touched_dates.add(candidate["date"].isoformat())
        rest_row["allocations"] = {}
        rest_row["focus"] = "休息日"
        rest_row["is_rest_day"] = True
        rest_row["redistributed_seconds"] = sum(moved_allocations.values())
        rest_row["redistributed_day_count"] = len(touched_dates)

    for day_key, day_row in days.items():
        day_row["rest_day_requested"] = day_key in requested_rest_day_keys
        day_row["can_be_rest_day"] = bool(
            day_row["allocations"]
            and day_key not in effective_rest_day_keys
            and can_redistribute_day(day_key, day_row, effective_rest_day_keys)
        )

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


def _study_plan_replan_preview(
    settings: Optional[Dict[str, Any]],
    rest_days: Optional[Iterable[str]] = None,
) -> Optional[Dict[str, Any]]:
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
    rest_day_keys = {str(value) for value in (rest_days or [])}
    cursor_day = start_day
    while cursor_day <= end_day:
        if cursor_day.isoformat() in rest_day_keys and cursor_day < end_day:
            cursor_day += timedelta(days=1)
            continue
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
        watched_seconds = _study_plan_credited_video_seconds(
            duration_seconds,
            video.get("watched_seconds"),
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


def _env_flag_truthy(value: Optional[str]) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


_YOUTUBE_STORYBOARD_INDEX_PENDING: Set[str] = set()
_YOUTUBE_STORYBOARD_INDEX_LOCK = threading.Lock()


def _persist_youtube_storyboard_metadata(
    storage: PersistentStorage,
    payload: Any,
    logger: Any,
) -> bool:
    if not isinstance(payload, dict):
        return False
    try:
        saved = storage.upsert_youtube_storyboard_metadata(
            youtube_video_id=str(payload.get("youtube_video_id") or ""),
            duration_seconds=float(payload.get("duration_seconds") or 0),
            storyboard_spec=str(payload.get("storyboard_spec") or ""),
        )
    except Exception:
        logger.exception("YouTube storyboard metadata persistence failed")
        return False
    return bool(saved)


def _start_youtube_storyboard_index(
    storage: PersistentStorage,
    youtube_video_ids: Iterable[str],
    logger: Any,
) -> int:
    candidates = {
        str(video_id or "").strip()
        for video_id in youtube_video_ids
        if re.fullmatch(r"[A-Za-z0-9_-]{11}", str(video_id or "").strip())
    }
    missing = {
        video_id
        for video_id in candidates
        if storage.get_youtube_storyboard_metadata(video_id) is None
    }
    with _YOUTUBE_STORYBOARD_INDEX_LOCK:
        scheduled = sorted(missing - _YOUTUBE_STORYBOARD_INDEX_PENDING)
        _YOUTUBE_STORYBOARD_INDEX_PENDING.update(scheduled)
    if not scheduled:
        return 0

    def run() -> None:
        def index_one(video_id: str) -> Tuple[str, bool]:
            try:
                payload = fetch_youtube_storyboard_metadata(video_id)
                return video_id, _persist_youtube_storyboard_metadata(storage, payload, logger)
            except YoutubeFrameError as exc:
                logger.warning("YouTube storyboard indexing failed for %s: %s", video_id, exc)
                return video_id, False
            except Exception:
                logger.exception("Unexpected YouTube storyboard indexing failure for %s", video_id)
                return video_id, False

        try:
            worker_count = max(1, min(3, len(scheduled)))
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                results = list(executor.map(index_one, scheduled))
            succeeded = sum(1 for _, ok in results if ok)
            logger.info(
                "YouTube storyboard indexing completed: %s/%s",
                succeeded,
                len(scheduled),
            )
        finally:
            with _YOUTUBE_STORYBOARD_INDEX_LOCK:
                _YOUTUBE_STORYBOARD_INDEX_PENDING.difference_update(scheduled)

    threading.Thread(
        target=run,
        name="youtube-storyboard-index",
        daemon=True,
    ).start()
    return len(scheduled)


def create_app(*, default_base_url: Optional[str] = None, default_scope: str = "assignment", default_timeout: int = 30) -> Flask:
    env_defaults = load_env_defaults()
    visual_note_pipeline_enabled = _env_flag_truthy(
        os.getenv("E3_VISUAL_NOTE_PIPELINE", "1")
    )

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

    app = Flask(__name__, template_folder=str(FRONTEND_TEMPLATE_DIR))
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
    openai_transcription_model = (
        os.getenv("E3_OPENAI_TRANSCRIPTION_MODEL", "gpt-4o-mini-transcribe").strip()
        or "gpt-4o-mini-transcribe"
    )
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
        "semester_filter": [],
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
    study_upload_staging_lock = threading.Lock()
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

    def _find_study_upload_staging_for_job(
        job_id: str,
        username: str,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Path]]:
        try:
            directories = list(study_upload_staging_root.iterdir())
        except OSError:
            return None, None
        for directory in directories:
            if not directory.is_dir():
                continue
            try:
                manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
            except (OSError, ValueError, TypeError):
                continue
            if (
                isinstance(manifest, dict)
                and str(manifest.get("username") or "") == username
                and str(manifest.get("job_id") or "") == job_id
            ):
                return manifest, directory
        return None, None

    def _study_upload_job_can_resume(job_id: str, username: str) -> bool:
        manifest, directory = _find_study_upload_staging_for_job(job_id, username)
        if manifest is None or directory is None:
            return False
        expected_count = int(manifest.get("expected_count") or 0)
        files = manifest.get("files") if isinstance(manifest.get("files"), dict) else {}
        if expected_count < 1 or len(files) != expected_count:
            return False
        return all(
            isinstance(files.get(str(index)), dict)
            and (directory / secure_filename(str(files[str(index)].get("stored_name") or ""))).is_file()
            for index in range(1, expected_count + 1)
        )

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
        persisted_job: Optional[Dict[str, Any]] = None
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
            persisted_job = dict(job)
        if persisted_job is not None:
            try:
                storage.save_study_note_upload_job(
                    job_id=job_id,
                    username=str(persisted_job.get("username") or ""),
                    status=str(persisted_job.get("status") or "running"),
                    progress=int(persisted_job.get("progress") or 0),
                    message=str(persisted_job.get("message") or "正在處理筆記。"),
                    session_id=(
                        int(persisted_job["session_id"])
                        if persisted_job.get("session_id") is not None
                        else None
                    ),
                    created_at=float(persisted_job.get("created_at") or time.time()),
                    updated_at=float(persisted_job.get("updated_at") or time.time()),
                )
            except Exception:
                app.logger.exception("Unable to persist study-note upload job %s", job_id)

    def _reconcile_study_upload_job(job: Dict[str, Any]) -> Dict[str, Any]:
        """Turn a persisted running job without a live worker into an interruption."""

        snapshot = dict(job or {})
        if str(snapshot.get("status") or "") != "running":
            return snapshot
        job_id = str(snapshot.get("job_id") or "")
        username = str(snapshot.get("username") or "")
        if not job_id or not username:
            return snapshot

        with study_upload_jobs_lock:
            memory_job = study_upload_jobs.get(job_id)
            if memory_job is not None:
                if str(memory_job.get("status") or "") != "running":
                    return dict(memory_job)
                worker_thread = memory_job.get("worker_thread")
                if isinstance(worker_thread, threading.Thread) and (
                    worker_thread.is_alive() or worker_thread.ident is None
                ):
                    return dict(memory_job)
                # A job is briefly registered before its worker is attached.
                if worker_thread is None and time.time() - float(
                    memory_job.get("updated_at") or 0
                ) < 10:
                    return dict(memory_job)

        can_resume = _study_upload_job_can_resume(job_id, username)
        interrupted_at = time.time()
        interrupted_message = (
            "筆記上傳中斷，已保留原圖與完成進度，可以接續處理。"
            if can_resume
            else "筆記上傳中斷，暫存資料不完整，請重新選擇照片上傳。"
        )
        snapshot.update(
            status="interrupted",
            message=interrupted_message,
            updated_at=interrupted_at,
        )
        with study_upload_jobs_lock:
            memory_job = study_upload_jobs.get(job_id)
            if memory_job is not None and str(memory_job.get("status") or "") == "running":
                memory_job.update(snapshot)
        storage.save_study_note_upload_job(
            job_id=job_id,
            username=username,
            status="interrupted",
            progress=int(snapshot.get("progress") or 0),
            message=interrupted_message,
            session_id=(
                int(snapshot["session_id"])
                if snapshot.get("session_id") is not None
                else None
            ),
            created_at=float(snapshot.get("created_at") or interrupted_at),
            updated_at=interrupted_at,
        )
        return snapshot

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
            candidates = [
                dict(job)
                for job in study_upload_jobs.values()
                if job.get("username") == username and job.get("status") == "running"
            ]
        for job in candidates:
            reconciled = _reconcile_study_upload_job(job)
            if reconciled.get("status") == "running":
                return str(reconciled.get("job_id") or "") or None
        persisted_job = storage.get_current_study_note_upload_job(
            username,
            terminal_window_seconds=0,
        )
        if persisted_job and persisted_job.get("status") == "running":
            reconciled = _reconcile_study_upload_job(persisted_job)
            if reconciled.get("status") == "running":
                return str(reconciled.get("job_id") or "") or None
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
        semester_filter_provided = False
        semester_filter = raw.get("semester_filter")
        if "semester_filter" in raw:
            semester_filter_provided = True
        if semester_filter is None:
            semester_filter = raw.get("semesterFilter")
            if "semesterFilter" in raw:
                semester_filter_provided = True
        if semester_filter is None:
            semester_filter = raw.get("semesterFilters")
            if "semesterFilters" in raw:
                semester_filter_provided = True
        if semester_filter_provided:
            clean["semester_filter"] = normalize_semester_keys(semester_filter)
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
        preferences = get_user_preferences(viewed_username)
        if result:
            preferred_semesters = None
            if not result.get("selected_semesters"):
                preferred_semesters = preferences.get("semester_filter") or None
            annotate_result_semesters(result, selected_keys=preferred_semesters)
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
            "preferences": preferences,
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
        rest_days = storage.list_study_plan_rest_days()
        videos_by_subject: Dict[str, List[Dict[str, Any]]] = {}
        for video in videos:
            subject = str(video.get("subject") or "")
            videos_by_subject.setdefault(subject, []).append(video)
        for subject_videos in videos_by_subject.values():
            subject_videos.sort(key=lambda item: int(item.get("sequence") or 0))

        watched_by_subject = {
            subject: sum(
                _study_plan_credited_video_seconds(
                    item.get("duration_seconds"),
                    item.get("watched_seconds"),
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
        for definition in _study_plan_schedule_definitions(videos, replan_settings, rest_days):
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
                # A subject's study time is cumulative.  Progress made on a later
                # video must first satisfy the oldest unfinished target for that
                # same subject instead of being pinned to the video's original
                # schedule range.  Rest days have no allocation, so they are
                # deliberately skipped while the credit flows forward.
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
                is_rest_day = bool(daily_target.get("is_rest_day"))
                if is_rest_day:
                    state = "rest"
                    state_label = "休息日"
                elif not has_target:
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
                        "is_rest_day": is_rest_day,
                        "can_be_rest_day": bool(
                            daily_target.get("can_be_rest_day")
                            and has_target
                            and not is_rest_day
                            and not _study_plan_total_is_complete(target_seconds, credited_seconds)
                        ),
                        "redistributed_seconds": float(daily_target.get("redistributed_seconds") or 0),
                        "redistributed_day_count": int(daily_target.get("redistributed_day_count") or 0),
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
                    "progress_blocked": False,
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

    def _study_plan_today_task_videos(
        videos_by_subject: Dict[str, List[Dict[str, Any]]],
        week_rows: List[Dict[str, Any]],
        current_week: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        today = _study_plan_business_date()
        today_iso = today.isoformat()
        today_schedule = next(
            (
                day
                for week in week_rows
                for day in week.get("daily_recommendations", [])
                if str(day.get("date") or "") == today_iso
            ),
            None,
        )
        if today_schedule and today_schedule.get("is_rest_day"):
            return []
        overdue_subjects: List[str] = []
        for subject in STUDY_PLAN_SUBJECTS:
            subject_videos = videos_by_subject.get(subject, [])
            subject_progress = _study_plan_progress_summary(subject_videos)
            subject_weeks = [
                row
                for row in week_rows
                if subject in row.get("subjects", [row.get("subject")])
            ]
            subject_state, _subject_state_label = _study_plan_subject_status(
                subject_weeks,
                today,
                completion=float(subject_progress.get("completion") or 0),
                watched_seconds=float(subject_progress.get("total_watched_seconds") or 0),
            )
            if subject_state == "behind":
                overdue_subjects.append(subject)

        current_subjects = list(
            current_week.get("subjects")
            or [str(current_week.get("subject") or "")]
        )
        suggested_subjects = list(dict.fromkeys([*overdue_subjects, *current_subjects]))
        today_row = next(
            (
                row
                for row in current_week.get("daily_recommendations", [])
                if row.get("date") == today.isoformat()
            ),
            None,
        )
        if today_row is None:
            today_row = next(
                (
                    row
                    for row in current_week.get("daily_recommendations", [])
                    if row.get("state") in {"active", "upcoming", "behind"}
                ),
                (current_week.get("daily_recommendations") or [{}])[0],
            )
        today_allocations = {
            str(subject): max(0.0, float(seconds or 0))
            for subject, seconds in dict(today_row.get("allocations") or {}).items()
        }
        today_credited = {
            str(progress.get("name") or ""): max(
                0.0,
                float(progress.get("credited_seconds") or 0),
            )
            for progress in today_row.get("subject_progress", [])
        }
        subject_video_queues: List[List[Dict[str, Any]]] = []
        for current_subject in suggested_subjects:
            remaining_task_seconds = max(
                0.0,
                today_allocations.get(current_subject, 0.0)
                - today_credited.get(current_subject, 0.0),
            )
            task_queue = _study_plan_task_video_queue(
                videos_by_subject.get(current_subject, []),
                remaining_task_seconds,
            )
            subject_queue: List[Dict[str, Any]] = []
            for video in task_queue:
                duration = float(video.get("duration_seconds") or 0)
                watched = min(float(video.get("watched_seconds") or 0), duration)
                youtube_video_id = str(video.get("youtube_video_id") or "").strip()
                subject_queue.append(
                    {
                        "id": int(video.get("id") or 0),
                        "subject": str(video.get("subject") or current_subject),
                        "sequence": int(video.get("sequence") or 0),
                        "title": str(video.get("title") or ""),
                        "remaining_minutes": round(max(0.0, duration - watched) / 60, 1),
                        "completion": round(min(100.0, watched / duration * 100), 1),
                        "youtube_video_id": youtube_video_id,
                    }
                )
            if subject_queue:
                subject_video_queues.append(subject_queue)
        return _study_plan_interleave_video_queues(subject_video_queues)

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
            subject_completion = round(
                min(
                    100.0,
                    (watched_seconds / target_seconds * 100) if target_seconds else 0.0,
                ),
                1,
            )
            subject_weeks = [row for row in week_rows if subject in row.get("subjects", [row.get("subject")])]
            subject_state, subject_state_label = _study_plan_subject_status(
                subject_weeks,
                today,
                completion=subject_completion,
                watched_seconds=watched_seconds,
            )
            subject_rows.append(
                {
                    "name": subject,
                    "completion": subject_completion,
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
        next_videos = _study_plan_today_task_videos(
            videos_by_subject,
            week_rows,
            current_week,
        )

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

        study_time_today = storage.get_study_time_summary(day=today.isoformat())
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
            {
                "label": "今日學習時間",
                "value": f"{float(study_time_today['total_seconds']) / 3600:.1f}",
                "unit": "h",
                "icon": "clock",
                "state": "early",
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
        pace_history = _study_plan_pace_history(
            week_rows,
            storage.list_study_plan_daily_snapshots(
                start_day=STUDY_PLAN_START,
                end_day=today.isoformat(),
            ),
            today=today,
            current_watched_minutes=watched_minutes_total,
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
        last_video_id = int((last_watched_event or {}).get("video_id") or 0)
        last_subject = str((last_watched_event or {}).get("subject") or "").strip()
        continue_source = _study_plan_default_video(
            videos_by_subject.get(last_subject, []),
            last_watched_video_id=last_video_id,
            allow_completed_fallback=False,
        )
        if continue_source is None and next_videos:
            next_video_id = int(next_videos[0].get("id") or 0)
            continue_source = next(
                (video for video in videos if int(video.get("id") or 0) == next_video_id),
                None,
            )
            if continue_source and _study_plan_video_is_complete(
                continue_source.get("duration_seconds"),
                continue_source.get("watched_seconds"),
            ):
                continue_source = None
        if continue_source is None:
            continue_source = _study_plan_default_video(
                videos,
                allow_completed_fallback=False,
            )
        continue_video: Optional[Dict[str, Any]] = None
        if continue_source:
            duration_seconds = max(0.0, float(continue_source.get("duration_seconds") or 0))
            watched_seconds = min(
                max(0.0, float(continue_source.get("watched_seconds") or 0)),
                duration_seconds,
            )
            continue_video = {
                "id": int(continue_source.get("id") or 0),
                "subject": str(continue_source.get("subject") or ""),
                "sequence": int(continue_source.get("sequence") or 0),
                "title": str(continue_source.get("title") or ""),
                "remaining_minutes": round(max(0.0, duration_seconds - watched_seconds) / 60, 1),
                "completion": round(
                    min(100.0, watched_seconds / duration_seconds * 100)
                    if duration_seconds
                    else 0.0,
                    1,
                ),
            }
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
        study_time_days = storage.list_study_time_daily_totals(
            start_day=STUDY_PLAN_START,
            end_day=today.isoformat(),
        )
        learning_time_by_day = {
            str(item.get("date") or ""): item
            for item in study_time_days
            if str(item.get("date") or "")
        }
        calendar_days: List[Dict[str, Any]] = []
        calendar_day_keys = sorted(set(activity_events_by_day) | set(learning_time_by_day))
        for activity_day in calendar_day_keys:
            events = activity_events_by_day.get(activity_day, [])
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
            learning_time = learning_time_by_day.get(activity_day, {})
            learning_seconds = max(0.0, float(learning_time.get("total_seconds") or 0))
            if calendar_seconds > 0 or learning_seconds > 0 or calendar_activities:
                calendar_days.append(
                    {
                        "date": activity_day,
                        "seconds": int(round(calendar_seconds)),
                        "learning_seconds": int(round(learning_seconds)),
                        "learning_session_count": max(
                            0,
                            int(learning_time.get("session_count") or 0),
                        ),
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
        if today_row.get("is_rest_day"):
            today_task_state, today_task_label = "rest", "今日休息"
        elif today_target_hours <= 0.001:
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
            "is_rest_day": bool(today_row.get("is_rest_day")),
            "progress_days": today_progress_days,
            "videos": today_videos,
            "learning_seconds": round(float(study_time_today["total_seconds"])),
            "learning_hours": round(float(study_time_today["total_seconds"]) / 3600, 2),
            "video_learning_hours": round(float(study_time_today["video_seconds"]) / 3600, 2),
            "practice_learning_hours": round(float(study_time_today["practice_seconds"]) / 3600, 2),
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
            "pace_history": pace_history,
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
        if is_study_note_process_metadata_card(concept):
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

    def _interleave_recall_cards(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Keep the due-date priority while avoiding long same-subject runs."""

        pending = list(cards)
        interleaved: List[Dict[str, Any]] = []
        previous_subject = ""
        while pending:
            next_index = 0
            if previous_subject:
                next_index = next(
                    (
                        index
                        for index, item in enumerate(pending)
                        if str(item.get("subject") or item.get("session_title") or "")
                        != previous_subject
                    ),
                    0,
                )
            selected = pending.pop(next_index)
            interleaved.append(selected)
            previous_subject = str(
                selected.get("subject") or selected.get("session_title") or ""
            )
        return interleaved

    def _quick_review_quality_issues(item: Dict[str, Any]) -> List[str]:
        """Reject unclear or structurally unverifiable questions before display."""

        quick = item.get("quick_review") if isinstance(item.get("quick_review"), dict) else {}
        concept = item.get("concept_data") if isinstance(item.get("concept_data"), dict) else {}

        def compact(value: Any) -> str:
            return " ".join(str(value or "").split()).strip()

        def normalized(value: Any) -> str:
            return re.sub(r"[\s\W_]+", "", str(value or ""), flags=re.UNICODE).casefold()

        issues: List[str] = []
        prompt = compact(quick.get("prompt"))
        answer = compact(quick.get("answer_summary"))
        title = compact(concept.get("concept"))
        interaction = str(quick.get("interaction") or "written")
        if len(prompt) < 10:
            issues.append("prompt-too-short")
        if len(prompt) > 900:
            issues.append("prompt-too-long")
        if not title or title in {"這個觀念", "其他", "綜合重點"}:
            issues.append("unclear-concept-title")
        if len(answer) < 2:
            issues.append("answer-missing")
        if answer in {"如上", "如圖", "同上", "略", "來源未提供完整解法"}:
            issues.append("answer-not-verifiable")

        source_answers = {
            normalized(concept.get("core_summary")),
            normalized(concept.get("example_method")),
            normalized(concept.get("explanation")),
        }
        source_answers.discard("")
        if normalized(answer) not in source_answers:
            issues.append("answer-not-grounded")

        if interaction == "calculation":
            has_task_signal = bool(
                re.search(
                    r"[?？]|(?:求|計算|證明|判斷|找出|解出|求其|試證|determine|calculate|solve|prove)",
                    prompt,
                    flags=re.IGNORECASE,
                )
            )
            if not has_task_signal:
                issues.append("calculation-task-unclear")
            if re.search(r"(?:如|見|根據|依)(?:上|下|左|右)?(?:圖|表)|(?:上|下|左|右)(?:圖|表)|此圖|該圖|圖中|表中", prompt):
                issues.append("missing-visual-context")
            if not compact(quick.get("answer_method")) and not (quick.get("answer_steps") or []):
                issues.append("calculation-solution-missing")

        options = quick.get("exam_options") if isinstance(quick.get("exam_options"), list) else []
        if interaction in {"single_choice", "true_false"}:
            expected_count = 4 if interaction == "single_choice" else 2
            if len(options) != expected_count:
                issues.append("option-count-invalid")
            option_texts = [compact(option.get("text")) for option in options if isinstance(option, dict)]
            option_keys = [normalized(option_text) for option_text in option_texts]
            if any(len(option_text) < 2 or len(option_text) > 700 for option_text in option_texts):
                issues.append("option-text-invalid")
            if len(set(option_keys)) != len(option_keys):
                issues.append("duplicate-options")
            if sum(bool(option.get("is_correct")) for option in options if isinstance(option, dict)) != 1:
                issues.append("correct-option-not-unique")
            if interaction == "single_choice":
                correct_options = [
                    normalized(option.get("text"))
                    for option in options
                    if isinstance(option, dict) and option.get("is_correct")
                ]
                if correct_options != [normalized(answer)]:
                    issues.append("correct-option-answer-mismatch")
                for left_index, left_key in enumerate(option_keys):
                    for right_key in option_keys[left_index + 1 :]:
                        if min(len(left_key), len(right_key)) >= 8 and (
                            left_key in right_key or right_key in left_key
                        ):
                            issues.append("overlapping-options")
                            break
        return sorted(set(issues))

    def _fallback_quick_review_to_written(item: Dict[str, Any], issues: List[str]) -> Dict[str, Any]:
        """Replace a rejected generated format with one clear, source-grounded prompt."""

        prepared = copy.deepcopy(item)
        concept = prepared.get("concept_data") if isinstance(prepared.get("concept_data"), dict) else {}
        quick = prepared.get("quick_review") if isinstance(prepared.get("quick_review"), dict) else {}
        title = str(concept.get("concept") or "").strip()
        quick.update(
            {
                "interaction": "written",
                "exam_options": [],
                "is_objective": False,
                "question_type": "來源簡答題",
                "mission": "只回答筆記中能被核對的內容",
                "instruction": "請寫出核心結論，並補上一個成立條件或必要方法。",
                "prompt": f"依這份筆記，請完整寫出「{title}」的核心結論與成立條件。",
                "answer_title": "來源正解",
                "estimated_seconds": 75,
                "quality_status": "fallback",
                "quality_issues": issues,
            }
        )
        prepared["quick_review"] = quick
        return prepared

    def _build_quick_review_item(
        item: Dict[str, Any],
        question_pool: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Turn one grounded note card into a source-grounded exam drill."""

        prepared = copy.deepcopy(item)
        concept = prepared.get("concept_data") if isinstance(prepared.get("concept_data"), dict) else {}
        title = str(concept.get("concept") or "這個觀念").strip()
        content_kind = str(concept.get("content_kind") or "concept").strip().lower()
        card_type = "example" if concept.get("card_type") == "example" else "concept"
        example_problem = str(concept.get("example_problem") or "").strip()
        example_method = str(concept.get("example_method") or "").strip()
        core_summary = str(concept.get("core_summary") or "").strip()
        explanation = str(concept.get("explanation") or "").strip()
        simple_example = str(concept.get("simple_example") or "").strip()
        pitfall = str(concept.get("common_confusion") or "").strip()
        reasoning_steps = [
            str(step or "").strip()
            for step in (concept.get("reasoning_steps") or [])[:5]
            if str(step or "").strip()
        ]
        review = concept.get("review") if isinstance(concept.get("review"), dict) else {}
        try:
            last_rating = int(review.get("last_rating")) if review.get("last_rating") is not None else None
        except (TypeError, ValueError):
            last_rating = None

        estimated_seconds = 45
        answer_title = "答案骨架"
        if card_type == "example" and example_problem:
            question_type = "解題決策"
            mission = "先選方法，再走步驟"
            instruction = "先說你為什麼選這個方法，再口述必要步驟；不用算得很漂亮。"
            prompt = example_problem
            answer_title = "解題骨架"
            estimated_seconds = 75
        elif content_kind in {"procedure", "code"}:
            question_type = "順序重建"
            mission = "從起點一路推到結果"
            instruction = "依順序說出每個關鍵動作，特別留意不能互換的步驟。"
            prompt = f"不看筆記，你能完整重建「{title}」的流程嗎？"
            answer_title = "正確順序"
            estimated_seconds = 60
        elif content_kind == "comparison":
            question_type = "邊界辨析"
            mission = "說清楚差別，避免混用"
            instruction = "先給判斷準則，再指出最容易混淆的界線。"
            prompt = f"「{title}」應該怎麼判斷？哪些情況最容易誤用？"
            answer_title = "判斷準則"
        elif content_kind == "formula":
            question_type = "公式重建"
            mission = "公式、符號、條件都要到位"
            instruction = "先寫或說出關係式，再解釋每一部分與成立條件。"
            prompt = f"請從記憶中重建「{title}」的公式，並說明何時能用。"
            answer_title = "公式與條件"
            estimated_seconds = 60
        elif content_kind == "definition":
            question_type = "定義壓縮"
            mission = "用自己的話說準確"
            instruction = "不用逐字背，但必要條件、關係與結論不能漏。"
            prompt = f"請用一句話定義「{title}」，再補上它成立的必要條件。"
            answer_title = "定義骨架"
        elif len(reasoning_steps) >= 2:
            question_type = "順序重建"
            mission = "從起點一路推到結果"
            instruction = "依順序說出每個關鍵動作，特別留意不能互換的步驟。"
            prompt = f"不看筆記，你能完整重建「{title}」的流程嗎？"
            answer_title = "正確順序"
            estimated_seconds = 60
        elif last_rating is not None and last_rating <= 2:
            question_type = "弱點修復"
            mission = "把上次斷掉的記憶接回來"
            instruction = "先講核心結論，再主動補一個條件或易錯點。"
            prompt = f"上次這題卡住了：現在你能完整說明「{title}」嗎？"
            answer_title = "這次要接回的重點"
        elif pitfall:
            question_type = "防錯回想"
            mission = "記住結論，也記住不能怎麼用"
            instruction = "先回答核心結論，再說一個容易判斷錯的地方。"
            prompt = f"「{title}」的核心結論是什麼？使用時最需要防哪個錯？"
            answer_title = "核心與防錯線"
        elif content_kind == "fact":
            question_type = "精確回想"
            mission = "一句話答到關鍵"
            instruction = "用完整敘述回答，不要只說零散關鍵詞。"
            prompt = f"「{title}」最需要精確記住的結論是什麼？"
            answer_title = "精確答案"
        else:
            question_type = "30 秒教學"
            mission = "把觀念講到別人聽得懂"
            instruction = "先講核心結論，再補上一個條件、關係或用途。"
            prompt = f"如果只有 30 秒，你會怎麼教別人「{title}」？"
            answer_title = "最小完整答案"

        answer_summary = core_summary or (example_method if card_type == "example" else "")
        if not answer_summary:
            answer_summary = explanation
        answer_steps = reasoning_steps
        hint = str(concept.get("recall_cue") or concept.get("memory_hint") or "").strip()

        distractors: List[str] = []
        seen_distractors = {re.sub(r"\s+", "", answer_summary).casefold()}
        pool_items = question_pool if isinstance(question_pool, list) else []
        prioritized_pool = sorted(
            pool_items,
            key=lambda candidate: (
                str(candidate.get("subject") or "") != str(item.get("subject") or ""),
                str((candidate.get("concept_data") or {}).get("topic") or "")
                != str(concept.get("topic") or ""),
            ),
        )
        for candidate in prioritized_pool:
            if str(candidate.get("subject") or "") != str(item.get("subject") or ""):
                continue
            if (
                int(candidate.get("session_id") or 0) == int(item.get("session_id") or 0)
                and int(candidate.get("concept_index") or -1) == int(item.get("concept_index") or -1)
            ):
                continue
            candidate_concept = candidate.get("concept_data") if isinstance(candidate.get("concept_data"), dict) else {}
            candidate_text = str(
                candidate_concept.get("core_summary")
                or candidate_concept.get("example_method")
                or ""
            ).strip()
            normalized_candidate = re.sub(r"\s+", "", candidate_text).casefold()
            if not normalized_candidate or normalized_candidate in seen_distractors:
                continue
            if title and title.casefold() in candidate_text.casefold():
                continue
            seen_distractors.add(normalized_candidate)
            distractors.append(candidate_text)
            if len(distractors) >= 3:
                break

        interaction = "written"
        exam_options: List[Dict[str, Any]] = []
        seed = (
            int(item.get("session_id") or 0) * 37
            + int(item.get("concept_index") or 0) * 17
        )
        if card_type == "example" and example_problem:
            interaction = "calculation"
            question_type = "計算／解題題"
            mission = "依題意算出結果，並保留關鍵步驟"
            instruction = "請先在紙上完成作答；揭曉後核對答案、方法與必要步驟。"
            prompt = example_problem
            answer_title = "來源正解"
            estimated_seconds = 120
        elif len(distractors) >= 3 and seed % 3 != 0:
            interaction = "single_choice"
            question_type = "單選題"
            mission = "選出唯一正確的核心敘述"
            instruction = "四個選項只有一個符合這張筆記卡的原始內容。"
            prompt = f"依這份筆記，下列何者是「{title}」這張卡記錄的核心敘述？"
            option_texts = [answer_summary, *distractors[:3]]
            rotation = seed % len(option_texts)
            option_texts = option_texts[rotation:] + option_texts[:rotation]
            exam_options = [
                {"text": option_text, "is_correct": option_text == answer_summary}
                for option_text in option_texts
            ]
            answer_title = "正確答案"
            estimated_seconds = 50
        elif distractors:
            interaction = "true_false"
            question_type = "是非題"
            mission = "判斷敘述是否真的屬於這個觀念"
            instruction = "請依筆記中的定義與條件判斷，不要只憑關鍵字。"
            statement_is_correct = seed % 2 == 0
            statement = answer_summary if statement_is_correct else distractors[0]
            prompt = f"依這份筆記，判斷下列敘述是否為「{title}」的核心敘述：\n{statement}"
            exam_options = [
                {"text": "正確", "is_correct": statement_is_correct},
                {"text": "錯誤", "is_correct": not statement_is_correct},
            ]
            answer_title = "判斷依據"
            estimated_seconds = 35

        prepared["quick_review"] = {
            "question_type": question_type,
            "mission": mission,
            "instruction": instruction,
            "prompt": prompt,
            "hint": hint,
            "estimated_seconds": estimated_seconds,
            "answer_title": answer_title,
            "answer_summary": answer_summary,
            "answer_method": example_method if example_method != answer_summary else "",
            "answer_steps": answer_steps,
            "explanation": explanation if explanation != answer_summary else "",
            "example": simple_example,
            "pitfall": pitfall,
            "interaction": interaction,
            "exam_options": exam_options,
            "is_objective": bool(exam_options),
            "quality_status": "passed",
            "quality_issues": [],
        }
        quality_issues = _quick_review_quality_issues(prepared)
        if quality_issues:
            prepared = _fallback_quick_review_to_written(prepared, quality_issues)
            fallback_issues = _quick_review_quality_issues(prepared)
            prepared["quick_review"]["quality_valid"] = not fallback_issues
            prepared["quick_review"]["quality_issues"] = sorted(
                set(quality_issues + fallback_issues)
            )
        else:
            prepared["quick_review"]["quality_valid"] = True
        return prepared

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



    (
        _extract_openai_text,
        _repair_openai_latex_json_escapes,
        _repair_study_decoded_text,
        _study_latex_markup_issue,
        _openai_error_details,
        _is_openai_quota_error,
        _request_openai_response,
        _call_openai_json,
        _call_openai_study_note_tools,
    ) = build_note_model_client(
        _STUDY_LATEX_COMMANDS=_STUDY_LATEX_COMMANDS,
        _raise_if_study_upload_cancelled=_raise_if_study_upload_cancelled,
        _set_study_upload_job=_set_study_upload_job,
        _study_upload_retry_wait=_study_upload_retry_wait,
        app=app,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
        study_upload_context=study_upload_context,
        visual_note_pipeline_enabled=visual_note_pipeline_enabled,
    )

    (
        _study_text_quality_issue,
        _study_relation_association_signature,
        _study_relation_association_issue,
        _study_has_invalid_negation_counterexample,
        _normalize_study_math_markup,
        _normalize_study_library_answer_markup,
        _strip_study_process_narration,
        _normalize_study_concept_title,
    ) = build_note_text(
        _repair_study_decoded_text=_repair_study_decoded_text,
        _study_latex_markup_issue=_study_latex_markup_issue,
    )

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


    (
        _study_coordinate_guide_data_url,
        _study_image_data_url,
        _canonical_study_source_match_text,
        _literal_study_source_evidence,
        _match_study_source_evidence_to_lines,
        _build_study_source_ink_mask,
        _study_source_visual_line_bands,
        _study_source_band_sheet_data_urls,
        _align_study_source_lines_to_visual_bands,
        _study_source_page_content_top,
        _refine_study_source_bbox_with_text_lines,
        _snap_study_source_bbox_to_ink,
        _expand_study_source_bbox_through_edge_ink,
    ) = build_note_geometry(
        _validated_study_source_bbox=_validated_study_source_bbox,
    )

    (
        _localize_study_card_sources_legacy,
    ) = build_note_localization_legacy(
        _call_openai_json=_call_openai_json,
        _expand_study_source_bbox_through_edge_ink=_expand_study_source_bbox_through_edge_ink,
        _literal_study_source_evidence=_literal_study_source_evidence,
        _raise_if_study_upload_cancelled=_raise_if_study_upload_cancelled,
        _snap_study_source_bbox_to_ink=_snap_study_source_bbox_to_ink,
        _study_coordinate_guide_data_url=_study_coordinate_guide_data_url,
        _study_image_data_url=_study_image_data_url,
        _validated_study_source_bbox=_validated_study_source_bbox,
        app=app,
    )

    (
        _localize_study_card_sources_band_experiment,
    ) = build_note_localization_bands(
        _StudyUploadCancelled=_StudyUploadCancelled,
        _call_openai_json=_call_openai_json,
        _canonical_study_source_match_text=_canonical_study_source_match_text,
        _literal_study_source_evidence=_literal_study_source_evidence,
        _match_study_source_evidence_to_lines=_match_study_source_evidence_to_lines,
        _raise_if_study_upload_cancelled=_raise_if_study_upload_cancelled,
        _study_coordinate_guide_data_url=_study_coordinate_guide_data_url,
        _study_image_data_url=_study_image_data_url,
        _study_source_band_sheet_data_urls=_study_source_band_sheet_data_urls,
        _study_source_visual_line_bands=_study_source_visual_line_bands,
        _validated_study_source_bbox=_validated_study_source_bbox,
        app=app,
        study_upload_context=study_upload_context,
    )

    (
        _localize_study_card_sources_model_consensus_legacy,
    ) = build_note_localization_consensus(
        _StudyUploadCancelled=_StudyUploadCancelled,
        _call_openai_json=_call_openai_json,
        _canonical_study_source_match_text=_canonical_study_source_match_text,
        _env_flag_truthy=_env_flag_truthy,
        _literal_study_source_evidence=_literal_study_source_evidence,
        _match_study_source_evidence_to_lines=_match_study_source_evidence_to_lines,
        _raise_if_study_upload_cancelled=_raise_if_study_upload_cancelled,
        _study_coordinate_guide_data_url=_study_coordinate_guide_data_url,
        _study_image_data_url=_study_image_data_url,
        _validated_study_source_bbox=_validated_study_source_bbox,
        app=app,
        study_upload_context=study_upload_context,
    )

    (
        _study_source_index_pages,
        _resolve_study_source_page,
        _localize_study_card_sources,
    ) = build_note_localization(
        _call_openai_json=_call_openai_json,
        _canonical_study_source_match_text=_canonical_study_source_match_text,
        _env_flag_truthy=_env_flag_truthy,
        _literal_study_source_evidence=_literal_study_source_evidence,
        _raise_if_study_upload_cancelled=_raise_if_study_upload_cancelled,
        _study_image_data_url=_study_image_data_url,
        _validated_study_source_bbox=_validated_study_source_bbox,
        app=app,
        study_upload_context=study_upload_context,
    )

    app.extensions["study_source_localizer"] = _localize_study_card_sources


    (
        _study_source_coverage_items,
        _study_source_page_coverage_plan,
        _study_coverage_evidence_matches,
        _enrich_study_card_coverage_ids,
        _study_recall_coverage_gaps,
        _study_recall_page_coverage_met,
        _study_recall_coverage_metrics,
        _study_recall_coverage_needs_repair,
        _validate_recall_output,
    ) = build_note_validation(
        _canonical_study_source_match_text=_canonical_study_source_match_text,
        _is_recall_concept_eligible=_is_recall_concept_eligible,
        _literal_study_source_evidence=_literal_study_source_evidence,
        _normalize_study_concept_title=_normalize_study_concept_title,
        _normalize_study_math_markup=_normalize_study_math_markup,
        _resolve_study_source_page=_resolve_study_source_page,
        _strip_study_process_narration=_strip_study_process_narration,
        _study_has_invalid_negation_counterexample=_study_has_invalid_negation_counterexample,
        _study_text_quality_issue=_study_text_quality_issue,
        app=app,
    )

    (
        _analyze_study_note_image_batch,
    ) = build_note_batch_analysis(
        STUDY_PLAN_SUBJECTS=STUDY_PLAN_SUBJECTS,
        _call_openai_json=_call_openai_json,
        _call_openai_study_note_tools=_call_openai_study_note_tools,
        _canonical_study_source_match_text=_canonical_study_source_match_text,
        _enrich_study_card_coverage_ids=_enrich_study_card_coverage_ids,
        _is_openai_quota_error=_is_openai_quota_error,
        _literal_study_source_evidence=_literal_study_source_evidence,
        _localize_study_card_sources=_localize_study_card_sources,
        _normalize_study_concept_title=_normalize_study_concept_title,
        _normalize_study_math_markup=_normalize_study_math_markup,
        _openai_error_details=_openai_error_details,
        _raise_if_study_upload_cancelled=_raise_if_study_upload_cancelled,
        _study_has_invalid_negation_counterexample=_study_has_invalid_negation_counterexample,
        _study_recall_coverage_gaps=_study_recall_coverage_gaps,
        _study_recall_coverage_metrics=_study_recall_coverage_metrics,
        _study_recall_coverage_needs_repair=_study_recall_coverage_needs_repair,
        _study_recall_page_coverage_met=_study_recall_page_coverage_met,
        _study_source_coverage_items=_study_source_coverage_items,
        _study_source_page_coverage_plan=_study_source_page_coverage_plan,
        _study_text_quality_issue=_study_text_quality_issue,
        _validate_recall_output=_validate_recall_output,
        _validated_study_source_bbox=_validated_study_source_bbox,
        app=app,
        openai_api_key=openai_api_key,
        study_upload_context=study_upload_context,
        visual_note_pipeline_enabled=visual_note_pipeline_enabled,
    )

    (
        _consolidate_study_note_batch_cards,
        _analyze_study_note_images,
    ) = build_note_analysis(
        STUDY_NOTE_AI_BATCH_SIZE=STUDY_NOTE_AI_BATCH_SIZE,
        STUDY_NOTE_MAX_IMAGE_BYTES=STUDY_NOTE_MAX_IMAGE_BYTES,
        _analyze_study_note_image_batch=_analyze_study_note_image_batch,
        _call_openai_json=_call_openai_json,
        _raise_if_study_upload_cancelled=_raise_if_study_upload_cancelled,
        app=app,
        study_upload_context=study_upload_context,
    )

    (
        _rebuild_all_study_recall_relations,
    ) = build_note_relations(
        _extract_openai_text=_extract_openai_text,
        _is_recall_concept_eligible=_is_recall_concept_eligible,
        _normalize_study_math_markup=_normalize_study_math_markup,
        _raise_if_study_upload_cancelled=_raise_if_study_upload_cancelled,
        _study_relation_association_issue=_study_relation_association_issue,
        _study_relation_association_signature=_study_relation_association_signature,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
        storage=storage,
    )

    def fetch_assignments_for(
        user: Dict[str, str],
        *,
        semester_keys: Optional[Sequence[str]] = None,
        include_archived: bool = False,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        selected_semesters = normalize_semester_keys(semester_keys)
        if semester_keys is None:
            stored = _sanitize_preferences(storage.load_user_preferences(str(user.get("username") or "")))
            selected_semesters = normalize_semester_keys(stored.get("semester_filter"))
        if not selected_semesters:
            selected_semesters = [current_semester_key()]
        previous_cache = storage.load_user_cache(str(user.get("username") or "")) or {}
        previous_result = previous_cache.get("result")
        if not isinstance(previous_result, dict):
            previous_result = {}
        annotate_result_semesters(previous_result)
        current_key = current_semester_key()
        cached_semesters = normalize_semester_keys(
            [
                item.get("key")
                for item in (previous_result.get("available_semesters") or [])
                if isinstance(item, dict)
            ]
        )
        cached_archived = [key for key in cached_semesters if key != current_key]
        if include_archived:
            selected_semesters = normalize_semester_keys([*selected_semesters, *cached_semesters])
        opts = CollectOptions(
            base_url=base_url,
            scope=default_scope,
            course_id=None,
            include_completed=True,
            all_courses=False,
            # Once historical courses exist locally, refresh only the active
            # semester and merge the archived semesters from the durable cache.
            # This avoids repeatedly requesting removed E3 course endpoints.
            all_courses_all_terms=bool(include_archived and not cached_archived),
            semester_keys=None,
            username=None,
            password=None,
            moodle_session=user.get("moodle_session"),
            insecure=False,
            timeout=default_timeout,
            debug=False,
        )
        refreshed_result = collect_assignments(opts)
        result = merge_current_semester_cache(
            previous_result,
            refreshed_result,
            selected_keys=selected_semesters,
        )
        excel_data = _generate_excel_data(result.get("all_assignments"))
        return result, excel_data

    (
        enforce_canonical_host,
        add_no_store_headers,
        login,
        health_check,
        traffic_stats,
        privacy_policy,
        terms_of_service,
        ui_event,
        session_status,
        api_cache,
        save_preferences,
        guest_login,
        guest_tool,
        guest_tool_source,
        guest_import,
        announcement_vote,
        google_authorize,
        google_callback,
        google_unlink,
        google_sync,
        logout,
        api_assignments,
        calendar_export,
    ) = register_assignments_routes(
        LOGIN_TEMPLATE=LOGIN_TEMPLATE,
        PRIVACY_TEMPLATE=PRIVACY_TEMPLATE,
        ROOT_DIR=ROOT_DIR,
        TERMS_TEMPLATE=TERMS_TEMPLATE,
        _build_calendar=_build_calendar,
        _build_google_state=_build_google_state,
        _ensure_google_access_token=_ensure_google_access_token,
        _generate_excel_data=_generate_excel_data,
        _google_ready=_google_ready,
        _google_redirect_uri=_google_redirect_uri,
        _mark_refresh_job_done=_mark_refresh_job_done,
        _mark_refresh_job_started=_mark_refresh_job_started,
        _refresh_job_state=_refresh_job_state,
        _select_assignments_from_result=_select_assignments_from_result,
        _start_web_session=_start_web_session,
        _verify_google_state=_verify_google_state,
        admin_user_id=admin_user_id,
        app=app,
        app_home_url=app_home_url,
        base_url=base_url,
        canonical_host=canonical_host,
        clear_google_tokens=clear_google_tokens,
        current_stats_version=current_stats_version,
        current_user=current_user,
        default_timeout=default_timeout,
        fetch_assignments_for=fetch_assignments_for,
        get_assign_cache=get_assign_cache,
        get_user_preferences=get_user_preferences,
        get_viewed_username=get_viewed_username,
        google_calendar_id=google_calendar_id,
        google_client_id=google_client_id,
        google_client_secret=google_client_secret,
        is_admin_viewing_other_user=is_admin_viewing_other_user,
        legal_effective_date=legal_effective_date,
        legal_entity_name=legal_entity_name,
        load_announcements=load_announcements,
        load_cache_from_disk=load_cache_from_disk,
        load_google_tokens=load_google_tokens,
        login_required=login_required,
        record_ui_event=record_ui_event,
        save_google_tokens=save_google_tokens,
        set_announcement_vote=set_announcement_vote,
        set_assign_cache=set_assign_cache,
        set_assign_cache_for_user=set_assign_cache_for_user,
        storage=storage,
        support_email=support_email,
        update_user_preferences=update_user_preferences,
        usage_stats=usage_stats,
    )

    (
        public_study_progress,
        public_study_progress_alias,
        admin_study_home,
    ) = register_study_home_routes(
        PUBLIC_STUDY_TEMPLATE=PUBLIC_STUDY_TEMPLATE,
        STUDY_HOME_TEMPLATE=STUDY_HOME_TEMPLATE,
        STUDY_PLAN_SUBJECTS=STUDY_PLAN_SUBJECTS,
        _build_recall_widget_context=_build_recall_widget_context,
        _load_study_progress_context=_load_study_progress_context,
        _repair_legacy_study_assistant_time_moves=lambda *args, **kwargs: _repair_legacy_study_assistant_time_moves(*args, **kwargs),
        admin_required=admin_required,
        app=app,
        current_user=current_user,
    )

    (
        admin_study_recall,
        _study_glossary_clean_term,
        _study_glossary_catalog,
        _finalize_verified_glossary_entry,
        _glossary_target_score,
        _generate_verified_glossary,
        _start_glossary_refresh,
        admin_study_recall_glossary_verified,
        admin_study_recall_glossary_refresh,
        admin_study_recall_glossary,
    ) = register_note_library_routes(
        STUDY_PLAN_SUBJECTS=STUDY_PLAN_SUBJECTS,
        STUDY_RECALL_TEMPLATE=STUDY_RECALL_TEMPLATE,
        _call_openai_json=_call_openai_json,
        _is_recall_concept_eligible=_is_recall_concept_eligible,
        _literal_study_source_evidence=_literal_study_source_evidence,
        _normalize_study_concept_title=_normalize_study_concept_title,
        _normalize_study_math_markup=_normalize_study_math_markup,
        _strip_study_process_narration=_strip_study_process_narration,
        _study_plan_business_date=lambda *args, **kwargs: _study_plan_business_date(*args, **kwargs),
        _study_relation_association_issue=_study_relation_association_issue,
        _validated_study_source_bbox=_validated_study_source_bbox,
        admin_required=admin_required,
        app=app,
        current_user=current_user,
        openai_api_key=openai_api_key,
        storage=storage,
    )

    (
        admin_study_recall_quick_review,
        admin_study_recall_search,
        _serialize_study_recall_search_results,
        _public_study_recall_search_has_evidence,
        public_study_recall_search,
        admin_study_recall_library_ask,
    ) = register_note_search_routes(
        STUDY_PLAN_SUBJECTS=STUDY_PLAN_SUBJECTS,
        STUDY_RECALL_QUICK_TEMPLATE=STUDY_RECALL_QUICK_TEMPLATE,
        _build_quick_review_item=_build_quick_review_item,
        _build_recall_widget_context=_build_recall_widget_context,
        _extract_openai_text=_extract_openai_text,
        _interleave_recall_cards=_interleave_recall_cards,
        _is_openai_quota_error=_is_openai_quota_error,
        _normalize_study_library_answer_markup=_normalize_study_library_answer_markup,
        _normalize_study_math_markup=_normalize_study_math_markup,
        _openai_error_details=_openai_error_details,
        _request_openai_response=_request_openai_response,
        _study_plan_business_date=lambda *args, **kwargs: _study_plan_business_date(*args, **kwargs),
        _validated_study_source_bbox=_validated_study_source_bbox,
        admin_required=admin_required,
        app=app,
        current_user=current_user,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
        record_ui_event=record_ui_event,
        storage=storage,
    )

    (
        admin_study_recall_image,
        public_study_recall_image,
        _study_visual_region,
        admin_study_recall_visual_crop,
        admin_study_recall_visual_redraw,
        admin_study_recall_localize_sources,
        admin_study_recall_localization_job,
        admin_study_recall_card_detail,
        admin_study_recall_ask_card,
    ) = register_note_sources_routes(
        STUDY_NOTE_STAGING_TTL_SECONDS=STUDY_NOTE_STAGING_TTL_SECONDS,
        _NOTE_IMAGE_MIME_TYPES=_NOTE_IMAGE_MIME_TYPES,
        _canonical_study_source_match_text=_canonical_study_source_match_text,
        _extract_openai_text=_extract_openai_text,
        _is_openai_quota_error=_is_openai_quota_error,
        _is_recall_concept_eligible=_is_recall_concept_eligible,
        _literal_study_source_evidence=_literal_study_source_evidence,
        _localize_study_card_sources=_localize_study_card_sources,
        _normalize_study_math_markup=_normalize_study_math_markup,
        _openai_error_details=_openai_error_details,
        _set_study_source_job=_set_study_source_job,
        _validated_study_source_bbox=_validated_study_source_bbox,
        admin_required=admin_required,
        app=app,
        current_user=current_user,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
        record_ui_event=record_ui_event,
        storage=storage,
        study_source_jobs=study_source_jobs,
        study_source_jobs_lock=study_source_jobs_lock,
        study_upload_root=study_upload_root,
    )

    (
        _study_assistant_time_label,
        _study_assistant_subject_from_text,
        _study_assistant_subject_progress_answer,
        _sanitize_study_assistant_answer,
        _study_assistant_claims_execution,
        _study_assistant_explicit_video_progress_action,
        _study_assistant_explicit_time_move_action,
        _study_assistant_plan_metrics,
        _study_assistant_data_context,
        _build_study_assistant_proposal,
        admin_study_recall_ask_general,
    ) = register_assistant_routes(
        STUDY_PLAN_DAILY_VIDEO_SECONDS=STUDY_PLAN_DAILY_VIDEO_SECONDS,
        STUDY_PLAN_END=STUDY_PLAN_END,
        STUDY_PLAN_START=STUDY_PLAN_START,
        STUDY_PLAN_SUBJECTS=STUDY_PLAN_SUBJECTS,
        _call_openai_json=_call_openai_json,
        _extract_openai_text=_extract_openai_text,
        _is_openai_quota_error=_is_openai_quota_error,
        _normalize_study_math_markup=_normalize_study_math_markup,
        _openai_error_details=_openai_error_details,
        _repair_legacy_study_assistant_time_moves=lambda *args, **kwargs: _repair_legacy_study_assistant_time_moves(*args, **kwargs),
        _study_plan_business_date=lambda *args, **kwargs: _study_plan_business_date(*args, **kwargs),
        _study_plan_nonnegative_number=lambda *args, **kwargs: _study_plan_nonnegative_number(*args, **kwargs),
        _study_plan_schedule_definitions=lambda *args, **kwargs: _study_plan_schedule_definitions(*args, **kwargs),
        _study_plan_video_is_complete=lambda *args, **kwargs: _study_plan_video_is_complete(*args, **kwargs),
        _study_plan_week_start=lambda *args, **kwargs: _study_plan_week_start(*args, **kwargs),
        admin_required=admin_required,
        app=app,
        current_user=current_user,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
        record_ui_event=record_ui_event,
        storage=storage,
    )

    (
        _study_assistant_restore_action_status,
        _study_assistant_activity_seconds,
        _repair_legacy_study_assistant_time_moves,
        _study_assistant_numbers_match,
        _study_assistant_mappings_match,
        _study_assistant_verify_applied_action,
        _study_assistant_rollback_unverified_action,
        _study_assistant_verify_reverted_action,
        admin_study_assistant_action_apply,
        admin_study_assistant_action_undo,
    ) = register_assistant_actions_routes(
        _invalidate_study_progress_context=_invalidate_study_progress_context,
        _study_assistant_time_label=_study_assistant_time_label,
        admin_required=admin_required,
        app=app,
        current_user=current_user,
        record_ui_event=record_ui_event,
        storage=storage,
    )

    (
        admin_study_recall_delete,
        admin_study_recall_rename,
        admin_study_recall_upload_staging,
        admin_study_recall_cancel_upload_staging,
        _launch_study_note_upload_job,
        admin_study_recall_upload,
        admin_study_recall_upload_job,
        admin_study_recall_current_upload_job,
        admin_study_recall_resume_upload_job,
        admin_study_recall_cancel_upload_job,
        admin_study_recall_rate_cards,
    ) = register_note_uploads_routes(
        STUDY_NOTE_MAX_IMAGE_BYTES=STUDY_NOTE_MAX_IMAGE_BYTES,
        STUDY_PLAN_SUBJECTS=STUDY_PLAN_SUBJECTS,
        _NOTE_IMAGE_MIME_TYPES=_NOTE_IMAGE_MIME_TYPES,
        _StudyUploadCancelled=_StudyUploadCancelled,
        _active_study_upload_job=_active_study_upload_job,
        _analyze_study_note_images=_analyze_study_note_images,
        _build_recall_widget_context=_build_recall_widget_context,
        _cleanup_expired_study_upload_staging=_cleanup_expired_study_upload_staging,
        _ensure_private_dir=_ensure_private_dir,
        _find_study_upload_staging_for_job=_find_study_upload_staging_for_job,
        _is_study_upload_request=_is_study_upload_request,
        _raise_if_study_upload_cancelled=_raise_if_study_upload_cancelled,
        _read_study_upload_manifest=_read_study_upload_manifest,
        _rebuild_all_study_recall_relations=_rebuild_all_study_recall_relations,
        _remove_study_upload_staging=_remove_study_upload_staging,
        _reconcile_study_upload_job=_reconcile_study_upload_job,
        _set_study_upload_job=_set_study_upload_job,
        _study_plan_business_date=lambda *args, **kwargs: _study_plan_business_date(*args, **kwargs),
        _study_upload_error=_study_upload_error,
        _study_upload_job_can_resume=_study_upload_job_can_resume,
        _study_upload_staging_directory=_study_upload_staging_directory,
        _write_study_upload_manifest=_write_study_upload_manifest,
        admin_required=admin_required,
        app=app,
        current_user=current_user,
        record_ui_event=record_ui_event,
        storage=storage,
        study_relation_rebuild_lock=study_relation_rebuild_lock,
        study_upload_context=study_upload_context,
        study_upload_jobs=study_upload_jobs,
        study_upload_jobs_lock=study_upload_jobs_lock,
        study_upload_root=study_upload_root,
        study_upload_staging_lock=study_upload_staging_lock,
    )

    (
        admin_study_settings,
        admin_study_settings_youtube_sync,
        _study_plan_marker_library,
        admin_study_plan_markers,
        admin_study_plan,
        admin_study_plan_rest_day,
        admin_study_plan_replan,
    ) = register_study_plan_routes(
        STUDY_MARKERS_TEMPLATE=STUDY_MARKERS_TEMPLATE,
        STUDY_PLAN_END=STUDY_PLAN_END,
        STUDY_PLAN_START=STUDY_PLAN_START,
        STUDY_PLAN_SUBJECTS=STUDY_PLAN_SUBJECTS,
        STUDY_PLAN_TEMPLATE=STUDY_PLAN_TEMPLATE,
        STUDY_SETTINGS_TEMPLATE=STUDY_SETTINGS_TEMPLATE,
        _build_recall_widget_context=_build_recall_widget_context,
        _invalidate_study_progress_context=_invalidate_study_progress_context,
        _repair_legacy_study_assistant_time_moves=lambda *args, **kwargs: _repair_legacy_study_assistant_time_moves(*args, **kwargs),
        _start_youtube_storyboard_index=lambda *args, **kwargs: _start_youtube_storyboard_index(*args, **kwargs),
        _study_plan_business_date=lambda *args, **kwargs: _study_plan_business_date(*args, **kwargs),
        _study_plan_default_video=lambda *args, **kwargs: _study_plan_default_video(*args, **kwargs),
        _study_plan_minutes=_study_plan_minutes,
        _study_plan_nonnegative_number=lambda *args, **kwargs: _study_plan_nonnegative_number(*args, **kwargs),
        _study_plan_progress_week=lambda *args, **kwargs: _study_plan_progress_week(*args, **kwargs),
        _study_plan_replan_preview=lambda *args, **kwargs: _study_plan_replan_preview(*args, **kwargs),
        _study_plan_schedule_definitions=lambda *args, **kwargs: _study_plan_schedule_definitions(*args, **kwargs),
        _study_plan_today_task_videos=_study_plan_today_task_videos,
        _study_plan_total_is_complete=lambda *args, **kwargs: _study_plan_total_is_complete(*args, **kwargs),
        _study_plan_video_completion=lambda *args, **kwargs: _study_plan_video_completion(*args, **kwargs),
        _study_plan_week_rows=_study_plan_week_rows,
        _study_plan_week_start=lambda *args, **kwargs: _study_plan_week_start(*args, **kwargs),
        admin_required=admin_required,
        app=app,
        current_user=current_user,
        openai_api_key=openai_api_key,
        record_ui_event=record_ui_event,
        storage=storage,
    )

    (
        _transcribe_study_plan_marker_audio,
        _collect_study_plan_video_context,
        _generate_study_plan_marker_summary,
        admin_study_plan_video_markers,
        admin_study_plan_video_question,
        admin_study_plan_video_frame,
        admin_study_plan_video_marker_update,
        admin_study_plan_video_marker_summary,
        admin_study_plan_video_marker_delete,
        admin_study_plan_video_progress,
        admin_study_plan_study_time,
        admin_study_plan_study_time_delete,
    ) = register_video_routes(
        STUDY_PLAN_SUBJECTS=STUDY_PLAN_SUBJECTS,
        _extract_openai_text=_extract_openai_text,
        _invalidate_study_progress_context=_invalidate_study_progress_context,
        _is_openai_quota_error=_is_openai_quota_error,
        _openai_error_details=_openai_error_details,
        _persist_youtube_storyboard_metadata=lambda *args, **kwargs: _persist_youtube_storyboard_metadata(*args, **kwargs),
        _repair_study_decoded_text=_repair_study_decoded_text,
        _request_openai_response=_request_openai_response,
        _study_plan_business_date=lambda *args, **kwargs: _study_plan_business_date(*args, **kwargs),
        _study_plan_progress_week=lambda *args, **kwargs: _study_plan_progress_week(*args, **kwargs),
        _study_plan_today_task_videos=_study_plan_today_task_videos,
        _study_plan_video_completion=lambda *args, **kwargs: _study_plan_video_completion(*args, **kwargs),
        _study_plan_week_rows=_study_plan_week_rows,
        admin_required=admin_required,
        app=app,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
        openai_transcription_model=openai_transcription_model,
        record_ui_event=record_ui_event,
        storage=storage,
    )

    (
        admin_traffic,
        admin_traffic_reset,
        admin_traffic_reset_user,
        admin_feedback,
        admin_announcements,
        delete_announcement,
        feedback,
        index,
    ) = register_administration_routes(
        ADMIN_FEEDBACK_TEMPLATE=ADMIN_FEEDBACK_TEMPLATE,
        ANNOUNCEMENTS_TEMPLATE=ANNOUNCEMENTS_TEMPLATE,
        FEEDBACK_TEMPLATE=FEEDBACK_TEMPLATE,
        HOME_TEMPLATE=HOME_TEMPLATE,
        TRAFFIC_TEMPLATE=TRAFFIC_TEMPLATE,
        WEB_TEMPLATE=WEB_TEMPLATE,
        _build_dashboard_context=_build_dashboard_context,
        add_announcement=add_announcement,
        add_feedback_entry=add_feedback_entry,
        app=app,
        app_home_url=app_home_url,
        current_stats_version=current_stats_version,
        current_user=current_user,
        delete_announcement_entry=delete_announcement_entry,
        list_admin_view_options=list_admin_view_options,
        list_feedback_entries=list_feedback_entries,
        load_announcements=load_announcements,
        login_required=login_required,
        record_ui_event=record_ui_event,
        storage=storage,
        support_email=support_email,
        traffic_tracker=traffic_tracker,
        update_feedback_status_entry=update_feedback_status_entry,
        usage_stats=usage_stats,
    )

    # Keep the deterministic validator reachable for integration tests without
    # exposing a route or coupling tests to OpenAI/network behavior.
    app.extensions["study_source_localizer"] = _localize_study_card_sources
    app.extensions["study_note_output_validator"] = _validate_recall_output
    app.extensions["study_note_tool_composer"] = _call_openai_study_note_tools
    app.extensions["study_note_image_analyzer"] = _analyze_study_note_images
    app.extensions["study_note_coverage_builder"] = _study_source_coverage_items
    app.extensions["visual_note_pipeline_enabled"] = visual_note_pipeline_enabled

    return app
