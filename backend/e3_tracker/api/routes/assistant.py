"""Assistant routes and their feature helpers."""

import json
import math
import re
import secrets
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
import requests
from flask import request, url_for
from ...shared.config import normalize_openai_reasoning_effort


def register_assistant_routes(*,
    STUDY_PLAN_DAILY_VIDEO_SECONDS,
    STUDY_PLAN_END,
    STUDY_PLAN_START,
    STUDY_PLAN_SUBJECTS,
    _call_openai_json,
    _extract_openai_text,
    _is_openai_quota_error,
    _normalize_study_math_markup,
    _openai_error_details,
    _repair_legacy_study_assistant_time_moves,
    _study_plan_business_date,
    _study_plan_nonnegative_number,
    _study_plan_schedule_definitions,
    _study_plan_video_is_complete,
    _study_plan_week_start,
    admin_required,
    app,
    current_user,
    openai_api_key,
    openai_model,
    record_ui_event,
    storage,
):
    def _study_assistant_time_label(seconds: Any) -> str:
        total_minutes = max(0, int(round(_study_plan_nonnegative_number(seconds) / 60)))
        hours, minutes = divmod(total_minutes, 60)
        if hours and minutes:
            return f"{hours} 小時 {minutes} 分鐘"
        if hours:
            return f"{hours} 小時"
        return f"{minutes} 分鐘"

    def _study_assistant_subject_from_text(value: Any) -> str:
        normalized = re.sub(r"\s+", "", str(value or "")).casefold()
        aliases = {
            "線性代數": ("線性代數", "線代"),
            "離散數學": ("離散數學", "離散"),
            "資料結構": ("資料結構", "datastructure", "ds"),
            "作業系統": ("作業系統", "operatingsystem", "os"),
            "計算機組織": ("計算機組織", "計組", "computerorganization"),
            "演算法": ("演算法", "algorithm", "algo"),
        }
        for subject, names in aliases.items():
            if any(name.casefold() in normalized for name in names):
                return subject
        return ""

    def _study_assistant_subject_progress_answer(
        subject: str,
        videos: List[Dict[str, Any]],
    ) -> Optional[str]:
        subject_videos = [
            video for video in videos if str(video.get("subject") or "") == subject
        ]
        if not subject_videos:
            return None
        total_seconds = sum(
            max(0.0, float(video.get("duration_seconds") or 0))
            for video in subject_videos
        )
        watched_seconds = sum(
            min(
                max(0.0, float(video.get("watched_seconds") or 0)),
                max(0.0, float(video.get("duration_seconds") or 0)),
            )
            for video in subject_videos
        )
        completed = sum(
            1
            for video in subject_videos
            if _study_plan_video_is_complete(
                video.get("duration_seconds"), video.get("watched_seconds")
            )
        )
        completion = min(100.0, watched_seconds / total_seconds * 100) if total_seconds else 0.0
        unfinished = [
            video
            for video in subject_videos
            if not _study_plan_video_is_complete(
                video.get("duration_seconds"), video.get("watched_seconds")
            )
        ]
        active = next(
            (
                video
                for video in unfinished
                if float(video.get("watched_seconds") or 0) > 0
            ),
            unfinished[0] if unfinished else None,
        )
        summary = (
            f"{subject}目前已看 {_study_assistant_time_label(watched_seconds)} / "
            f"{_study_assistant_time_label(total_seconds)}，完成 {completion:.1f}%（{completed}/{len(subject_videos)} 支）。"
        )
        if not active:
            return summary + "全部影片都已完成。"
        sequence = int(active.get("sequence") or 0)
        title = str(active.get("title") or "").strip()
        position = float(active.get("watched_seconds") or 0)
        if position > 0:
            return (
                summary
                + f"目前看到影片 {sequence}「{title}」的 {_study_assistant_time_label(position)}。"
            )
        return summary + f"下一支是影片 {sequence}「{title}」。"

    def _sanitize_study_assistant_answer(value: Any) -> str:
        answer = str(value or "")
        replacements = {
            "set_video_progress": "影片進度",
            "set_study_time_session": "學習時間紀錄",
            "update_study_plan": "讀書計畫",
            "rebalance_study_plan": "重新平均安排讀書計畫",
            "recent_study_sessions": "最近學習紀錄",
            "video_sequence": "影片序號",
            "target_minutes": "分鐘數",
            "target_percent": "完成百分比",
            "move_study_time_between_days": "跨日期移轉學習時間",
            "source_date": "來源日期",
            "target_date": "目標日期",
            "weekday_hours": "平日每日時數",
            "weekend_hours": "假日每日時數",
            "start_date": "計畫起日",
            "end_date": "計畫迄日",
            "session_id": "紀錄",
            "subject": "科目",
        }
        for internal_name, display_name in replacements.items():
            answer = re.sub(re.escape(internal_name), display_name, answer, flags=re.IGNORECASE)
        return _normalize_study_math_markup(answer)

    def _study_assistant_claims_execution(value: Any) -> bool:
        """Reject model prose that could be mistaken for a completed site mutation."""
        answer = " ".join(str(value or "").split())
        if not answer:
            return False
        answer = re.sub(
            r"(?:尚未|沒有|並未|尚無法|無法|不能|不會).{0,10}(?:處理|執行|修改|更新|套用|刪除|重排|調整|完成)",
            "",
            answer,
        )
        return bool(re.search(
            r"(?:已|成功|完成).{0,12}(?:處理|執行|修改|更新|套用|刪除|重排|調整|寫入|儲存|完成)|"
            r"(?:處理|執行|修改|更新|套用|刪除|重排|調整|寫入|儲存)(?:成功|完成)",
            answer,
        ))

    def _study_assistant_explicit_video_progress_action(text: str) -> Optional[Dict[str, Any]]:
        """Resolve natural percentage commands without asking for data already in storage."""
        normalized = " ".join(str(text or "").split()).strip()
        subject = _study_assistant_subject_from_text(normalized)
        if not subject or not any(
            term in normalized
            for term in ("調成", "改成", "設成", "調整為", "更新為", "設為", "標成", "標記為", "看完", "完成")
        ):
            return None
        sequence_match = re.search(r"第\s*(\d+)\s*(?:部|支|集|堂|影片)", normalized)
        if not sequence_match:
            sequence_match = re.search(r"影片\s*(?:第\s*)?(\d+)", normalized)
        if not sequence_match:
            return None
        percent_match = re.search(r"(\d+(?:\.\d+)?)\s*(?:%|％)", normalized)
        if not percent_match:
            percent_match = re.search(r"百分之\s*(\d+(?:\.\d+)?)", normalized)
        if percent_match:
            target_percent = float(percent_match.group(1))
        elif any(term in normalized for term in ("看完", "完成", "全看完", "全部看完")):
            target_percent = 100.0
        else:
            return None
        if not math.isfinite(target_percent) or not 0 <= target_percent <= 100:
            return None
        return {
            "type": "set_video_progress",
            "subject": subject,
            "video_sequence": int(sequence_match.group(1)),
            "target_minutes": 0,
            "target_percent": target_percent,
            "reason": f"將影片觀看進度調整為 {target_percent:g}%",
        }

    def _study_assistant_explicit_time_move_action(text: str) -> Optional[Dict[str, Any]]:
        """Resolve common cross-day attribution requests from natural Chinese."""
        normalized = " ".join(str(text or "").split()).strip()
        subject = _study_assistant_subject_from_text(normalized)
        if not subject or not any(term in normalized for term in ("算在", "移到", "挪到", "記到", "歸到", "改到")):
            return None
        sequence_match = re.search(r"第\s*(\d+)\s*(?:部|支|集|堂|影片)", normalized)
        if not sequence_match:
            sequence_match = re.search(r"影片\s*(?:第\s*)?(\d+)", normalized)
        if not sequence_match:
            subject_position = normalized.find(subject)
            if subject_position >= 0:
                sequence_match = re.search(r"(?:第\s*)?(\d+)\s*(?:部|支|集|堂|影片)?", normalized[subject_position + len(subject):])
        minutes_match = re.search(r"(\d+(?:\.\d+)?)\s*分鐘", normalized)
        move_all = any(term in normalized for term in ("全部", "整筆", "所有", "整段"))
        if not sequence_match or (not minutes_match and not move_all):
            return None
        today = _study_plan_business_date()

        def resolve_date_token(token: str) -> str:
            value = str(token or "").strip()
            relative_days = {"今天": 0, "昨天": -1, "前天": -2, "明天": 1}
            if value in relative_days:
                return (today + timedelta(days=relative_days[value])).isoformat()
            try:
                if re.fullmatch(r"20\d{2}-\d{1,2}-\d{1,2}", value):
                    return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
                match = re.fullmatch(r"(\d{1,2})\s*[/月-]\s*(\d{1,2})(?:\s*日)?", value)
                if match:
                    return date(today.year, int(match.group(1)), int(match.group(2))).isoformat()
            except ValueError:
                return ""
            return ""

        date_token_pattern = r"(?:20\d{2}-\d{1,2}-\d{1,2}|\d{1,2}\s*[/月-]\s*\d{1,2}(?:\s*日)?|今天|昨天|前天|明天)"
        move_match = re.search(r"(?:算在|移到|挪到|記到|歸到|改到)\s*(" + date_token_pattern + r")", normalized)
        target_day = resolve_date_token(move_match.group(1)) if move_match else ""
        before_move = normalized[:move_match.start()] if move_match else normalized
        source_matches = re.findall(date_token_pattern, before_move)
        source_day = resolve_date_token(source_matches[-1]) if source_matches else ""
        explicit_dates = re.findall(r"20\d{2}-\d{2}-\d{2}", normalized)
        if len(explicit_dates) >= 2:
            source_day, target_day = explicit_dates[0], explicit_dates[1]
        if not source_day and target_day:
            # In natural requests such as「把這筆移到昨天」the source is the
            # current study day. Moving something to today implies yesterday.
            source_day = (
                (today - timedelta(days=1)).isoformat()
                if target_day == today.isoformat()
                else today.isoformat()
            )
        if not source_day or not target_day or source_day == target_day:
            return None
        minutes = float(minutes_match.group(1)) if minutes_match else 0.0
        if not math.isfinite(minutes) or minutes < 0 or minutes > 24 * 60 or (minutes <= 0 and not move_all):
            return None
        return {
            "type": "move_study_time_between_days",
            "subject": subject,
            "video_sequence": int(sequence_match.group(1)),
            "target_minutes": minutes,
            "move_all": move_all,
            "source_date": source_day,
            "target_date": target_day,
            "reason": (
                f"將來源日的全部觀看時間改記到 {target_day}"
                if move_all
                else f"將 {minutes:g} 分鐘的學習時間改記到 {target_day}"
            ),
        }

    def _study_assistant_plan_metrics(
        videos: List[Dict[str, Any]],
        current_settings: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        today = _study_plan_business_date()
        totals: Dict[str, Dict[str, float]] = {}
        for subject in STUDY_PLAN_SUBJECTS:
            subject_videos = [item for item in videos if str(item.get("subject") or "") == subject]
            total = sum(_study_plan_nonnegative_number(item.get("duration_seconds")) for item in subject_videos)
            watched = sum(
                min(
                    _study_plan_nonnegative_number(item.get("watched_seconds")),
                    _study_plan_nonnegative_number(item.get("duration_seconds")),
                )
                for item in subject_videos
            )
            totals[subject] = {
                "total_seconds": total,
                "watched_seconds": watched,
                "remaining_seconds": max(0.0, total - watched),
                "completed_videos": float(sum(
                    1 for item in subject_videos
                    if _study_plan_video_is_complete(item.get("duration_seconds"), item.get("watched_seconds"))
                )),
                "video_count": float(len(subject_videos)),
            }

        replan_start: Optional[date] = None
        if current_settings:
            try:
                replan_start = datetime.strptime(str(current_settings.get("start_date") or ""), "%Y-%m-%d").date()
            except (TypeError, ValueError):
                replan_start = None
        active_replan = bool(replan_start and today >= replan_start)
        expected = {subject: 0.0 for subject in STUDY_PLAN_SUBJECTS}
        for week in _study_plan_schedule_definitions(
            videos,
            current_settings,
            storage.list_study_plan_rest_days(),
        ):
            for day_item in week.get("daily_targets") or []:
                scheduled_date = day_item.get("date")
                if (
                    scheduled_date
                    and scheduled_date <= today
                    and not (active_replan and replan_start and scheduled_date < replan_start)
                ):
                    for subject, seconds in dict(day_item.get("allocations") or {}).items():
                        if subject in expected:
                            expected[subject] += _study_plan_nonnegative_number(seconds)
        if active_replan:
            for subject, seconds in dict((current_settings or {}).get("baseline_by_subject") or {}).items():
                if subject in expected:
                    expected[subject] += _study_plan_nonnegative_number(seconds)
        backlog_by_subject = {
            subject: max(0.0, expected.get(subject, 0.0) - totals[subject]["watched_seconds"])
            for subject in STUDY_PLAN_SUBJECTS
        }
        return {
            "today": today.isoformat(),
            "subjects": {
                subject: {
                    "total_hours": round(values["total_seconds"] / 3600, 2),
                    "watched_hours": round(values["watched_seconds"] / 3600, 2),
                    "remaining_hours": round(values["remaining_seconds"] / 3600, 2),
                    "scheduled_by_today_hours": round(expected[subject] / 3600, 2),
                    "behind_hours": round(backlog_by_subject[subject] / 3600, 2),
                    "completed_videos": int(values["completed_videos"]),
                    "video_count": int(values["video_count"]),
                }
                for subject, values in totals.items()
            },
            "remaining_hours": round(sum(item["remaining_seconds"] for item in totals.values()) / 3600, 2),
            "behind_hours": round(sum(backlog_by_subject.values()) / 3600, 2),
        }

    def _study_assistant_data_context(
        *,
        username: str,
        videos: List[Dict[str, Any]],
        current_settings: Optional[Dict[str, Any]],
        page_context: Dict[str, str],
    ) -> Dict[str, Any]:
        """Expose bounded, user-owned learning data instead of raw database access."""
        today = _study_plan_business_date()
        recent_start = today - timedelta(days=29)
        recent_sessions: List[Dict[str, Any]] = []
        for offset in range(14):
            day = (today - timedelta(days=offset)).isoformat()
            for item in storage.list_study_time_sessions(day=day, limit=100):
                recent_sessions.append({
                    "record": str(item.get("session_id") or ""),
                    "date": day,
                    "type": str(item.get("kind") or ""),
                    "subject": str(item.get("subject") or ""),
                    "video": int(item.get("sequence") or 0),
                    "label": str(item.get("label") or "")[:90],
                    "minutes": round(float(item.get("elapsed_seconds") or 0) / 60, 2),
                })
        recent_video_activity = [
            {
                "date": str(item.get("day") or ""),
                "subject": str(item.get("subject") or ""),
                "video": int(item.get("sequence") or 0),
                "title": str(item.get("title") or "")[:90],
                "minutes": round(max(0.0, float(item.get("delta_seconds") or 0)) / 60, 2),
            }
            for item in storage.list_study_plan_activity_events(
                start_day=(today - timedelta(days=13)).isoformat(),
                end_day=today.isoformat(),
            )
            if float(item.get("delta_seconds") or 0) > 0
        ]
        note_rows = storage.list_study_recall_sessions(limit=60)
        note_library = []
        for item in note_rows:
            concepts = []
            for concept in (item.get("key_concepts") or [])[:10]:
                if not isinstance(concept, dict):
                    continue
                concepts.append({
                    "term": str(concept.get("concept") or concept.get("topic") or "")[:100],
                    "summary": str(concept.get("core_summary") or concept.get("explanation") or "")[:260],
                })
            note_library.append({
                "id": int(item.get("id") or 0),
                "date": str(item.get("study_date") or ""),
                "subject": str(item.get("subject") or ""),
                "title": str(item.get("title") or "")[:120],
                "summary": str(item.get("summary") or "")[:300],
                "concepts": concepts,
                "next_review": item.get("next_review_at"),
            })
        due_cards = storage.list_due_study_recall_cards(today=today.isoformat(), limit=18)
        review_schedule = storage.list_study_recall_schedule(start_date=today.isoformat(), days=14)
        cache = storage.load_user_cache(username) or {}
        assignments = []
        for item in ((cache.get("result") or {}).get("all_assignments") or [])[:80]:
            assignments.append({
                "course": str(item.get("course_title") or "")[:100],
                "title": str(item.get("title") or "")[:140],
                "due_at": item.get("due_at"),
                "overdue": bool(item.get("overdue")),
                "completed": bool(item.get("completed")),
                "status": str(item.get("raw_status_text") or "")[:100],
            })
        glossary = {}
        for subject in STUDY_PLAN_SUBJECTS:
            entry = storage.get_study_recall_glossary(subject)
            if entry and entry.get("status") == "ready":
                glossary[subject] = [
                    {
                        "term": str(term.get("term") or "")[:80],
                        "definition": str(term.get("definition") or "")[:220],
                    }
                    for term in (entry.get("terms") or [])[:60]
                    if isinstance(term, dict)
                ]
        compact_videos = [{
            "subject": str(video.get("subject") or ""),
            "sequence": int(video.get("sequence") or 0),
            "title": str(video.get("title") or "")[:90],
            "duration_minutes": round(float(video.get("duration_seconds") or 0) / 60, 2),
            "watched_minutes": round(float(video.get("watched_seconds") or 0) / 60, 2),
            "complete": _study_plan_video_is_complete(video.get("duration_seconds"), video.get("watched_seconds")),
        } for video in videos]
        return {
            "today": today.isoformat(),
            "current_page": page_context,
            "plan_progress": _study_assistant_plan_metrics(videos, current_settings),
            "study_plan": ({
                "start_date": current_settings.get("start_date"),
                "end_date": current_settings.get("end_date"),
                "weekday_hours": round(float(current_settings.get("weekday_minutes") or 0) / 60, 2),
                "weekend_hours": round(float(current_settings.get("weekend_minutes") or 0) / 60, 2),
            } if current_settings else {
                "start_date": STUDY_PLAN_START,
                "end_date": STUDY_PLAN_END,
                "weekday_hours": round(STUDY_PLAN_DAILY_VIDEO_SECONDS / 3600, 2),
                "weekend_hours": round(STUDY_PLAN_DAILY_VIDEO_SECONDS / 3600, 2),
            }),
            "videos": compact_videos,
            "recent_video_activity": recent_video_activity,
            "recent_study_records": recent_sessions,
            "study_time_last_30_days": storage.list_study_time_daily_totals(
                start_day=recent_start.isoformat(), end_day=today.isoformat()
            ),
            "note_library": note_library,
            "due_review_cards": due_cards,
            "review_load_next_14_days": review_schedule,
            "course_assignments": assignments,
            "subject_glossary": glossary,
        }

    def _build_study_assistant_proposal(
        *,
        username: str,
        raw_action: Any,
        videos: List[Dict[str, Any]],
        current_settings: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        if not username or not isinstance(raw_action, dict):
            return None
        action_type = str(raw_action.get("type") or "none").strip()
        if action_type == "none":
            return None
        stored_action_type = action_type
        action_id = secrets.token_urlsafe(24)[:48]
        reason = " ".join(str(raw_action.get("reason") or "").split()).strip()[:240]
        action_payload: Dict[str, Any]
        before_state: Dict[str, Any]
        title: str
        summary: str
        changes: List[Dict[str, str]]

        if action_type == "set_video_progress":
            subject = str(raw_action.get("subject") or "").strip()
            try:
                sequence = int(raw_action.get("video_sequence") or 0)
            except (TypeError, ValueError):
                return None
            candidates = [
                video
                for video in videos
                if str(video.get("subject") or "") == subject
                and int(video.get("sequence") or 0) == sequence
            ]
            if len(candidates) != 1:
                return None
            video = candidates[0]
            duration_seconds = max(0.0, float(video.get("duration_seconds") or 0))
            try:
                raw_target_percent = raw_action.get("target_percent")
                if raw_target_percent not in (None, ""):
                    target_percent = float(raw_target_percent)
                    if not math.isfinite(target_percent) or not 0 <= target_percent <= 100:
                        return None
                    target_seconds = duration_seconds * target_percent / 100
                else:
                    target_seconds = float(raw_action.get("target_minutes") or 0) * 60
                    target_percent = (target_seconds / duration_seconds * 100) if duration_seconds > 0 else 0.0
            except (TypeError, ValueError):
                return None
            if not math.isfinite(target_seconds):
                return None
            if target_seconds < 0 or target_seconds > duration_seconds + 1:
                return None
            target_seconds = min(target_seconds, duration_seconds)
            before_state = {
                "video_id": int(video["id"]),
                "watched_seconds": max(0.0, float(video.get("watched_seconds") or 0)),
                "progress_version": max(0, int(video.get("progress_version") or 0)),
            }
            action_payload = {
                "video_id": int(video["id"]),
                "target_seconds": target_seconds,
                "expected_version": before_state["progress_version"],
            }
            title = f"修正 {subject}影片 {sequence} 進度"
            summary = (
                f"將觀看進度調整為 {target_percent:g}%"
                f"（{_study_assistant_time_label(target_seconds)} / {_study_assistant_time_label(duration_seconds)}）"
            )
            before_percent = (
                min(100.0, before_state["watched_seconds"] / duration_seconds * 100)
                if duration_seconds > 0 else 0.0
            )
            changes = [{
                "label": "影片進度",
                "before": f"{before_percent:.1f}% · {_study_assistant_time_label(before_state['watched_seconds'])}",
                "after": f"{target_percent:g}% · {_study_assistant_time_label(target_seconds)}",
            }]
        elif action_type == "set_study_time_session":
            session_id = str(raw_action.get("session_id") or "").strip()
            try:
                target_seconds = float(raw_action.get("target_minutes") or 0) * 60
            except (TypeError, ValueError):
                return None
            item = storage.get_study_time_session(session_id)
            if not item or not math.isfinite(target_seconds) or not (0 <= target_seconds <= 24 * 60 * 60):
                return None
            try:
                item_day = datetime.strptime(str(item.get("day") or ""), "%Y-%m-%d").date()
            except (TypeError, ValueError):
                return None
            if (_study_plan_business_date() - item_day).days not in range(14):
                return None
            before_state = dict(item)
            action_payload = {
                "session_id": session_id,
                "target_seconds": target_seconds,
                "expected_updated_at": str(item.get("updated_at") or ""),
            }
            title = "修正實際學習時間"
            summary = f"{item.get('day')} · {item.get('label')}"
            changes = [{
                "label": "實際時間",
                "before": _study_assistant_time_label(item.get("elapsed_seconds")),
                "after": _study_assistant_time_label(target_seconds),
            }]
        elif action_type == "move_study_time_between_days":
            subject = str(raw_action.get("subject") or "").strip()
            try:
                sequence = int(raw_action.get("video_sequence") or 0)
                requested_seconds = float(raw_action.get("target_minutes") or 0) * 60
                move_all = bool(raw_action.get("move_all"))
                source_day = datetime.strptime(str(raw_action.get("source_date") or ""), "%Y-%m-%d").date()
                target_day = datetime.strptime(str(raw_action.get("target_date") or ""), "%Y-%m-%d").date()
            except (TypeError, ValueError):
                return None
            today = _study_plan_business_date()
            if (
                not subject
                or not math.isfinite(requested_seconds)
                or requested_seconds < 0
                or requested_seconds > 24 * 60 * 60
                or source_day == target_day
                or abs((today - source_day).days) > 14
                or abs((today - target_day).days) > 14
            ):
                return None
            video = next((
                item for item in videos
                if str(item.get("subject") or "") == subject
                and int(item.get("sequence") or 0) == sequence
            ), None)
            if not video:
                return None
            source_activity_seconds = sum(
                max(0.0, float(item.get("delta_seconds") or 0))
                for item in storage.list_study_plan_activity_events(day=source_day.isoformat())
                if int(item.get("video_id") or 0) == int(video.get("id") or 0)
            )
            target_activity_seconds = sum(
                max(0.0, float(item.get("delta_seconds") or 0))
                for item in storage.list_study_plan_activity_events(day=target_day.isoformat())
                if int(item.get("video_id") or 0) == int(video.get("id") or 0)
            )
            target_seconds = source_activity_seconds if move_all else requested_seconds
            # Spoken/displayed minutes are rounded. For an "all" request, move the
            # exact stored seconds so no invisible remainder is left behind.
            if move_all and requested_seconds > 0:
                displayed_minutes = round(source_activity_seconds / 60)
                requested_minutes = round(requested_seconds / 60)
                if abs(displayed_minutes - requested_minutes) > 1:
                    return None
            if target_seconds <= 0 or target_seconds > 24 * 60 * 60:
                return None
            if source_activity_seconds + 0.5 < target_seconds:
                return None
            candidates = [
                item for item in storage.list_study_time_sessions(day=source_day.isoformat(), limit=100)
                if str(item.get("subject") or "") == subject
                and int(item.get("sequence") or 0) == sequence
                and float(item.get("elapsed_seconds") or 0) > 0
            ]
            remaining = target_seconds
            selected = []
            for item in candidates:
                amount = min(remaining, float(item.get("elapsed_seconds") or 0))
                if amount > 0:
                    selected.append({
                        "session_id": str(item.get("session_id") or ""),
                        "seconds": amount,
                        "expected_updated_at": str(item.get("updated_at") or ""),
                    })
                    remaining -= amount
                if remaining <= 0.05:
                    break
            if remaining > 0.05:
                selected = []
            before_state = {
                "source_sessions": [
                    dict(storage.get_study_time_session(item["session_id"]) or {})
                    for item in selected
                ],
                "source_day": source_day.isoformat(),
                "target_day": target_day.isoformat(),
                "source_activity_seconds": source_activity_seconds,
                "target_activity_seconds": target_activity_seconds,
            }
            action_payload = {
                "moves": selected,
                "source_day": source_day.isoformat(),
                "target_day": target_day.isoformat(),
                "target_session_id": f"assistant-move-{action_id}"[:80],
                "target_seconds": target_seconds,
                "move_all": move_all,
                "subject": subject,
                "video_sequence": sequence,
                "video_id": int(video.get("id") or 0),
                "expected_source_activity_seconds": source_activity_seconds,
                "expected_target_activity_seconds": target_activity_seconds,
            }
            title = f"移轉 {subject}影片 {sequence} 學習時間"
            summary = f"將 {_study_assistant_time_label(target_seconds)}從 {source_day.isoformat()} 改記到 {target_day.isoformat()}"
            changes = [{
                "label": "日期歸屬",
                "before": f"{source_day.isoformat()} · {_study_assistant_time_label(target_seconds)}",
                "after": f"{target_day.isoformat()} · {_study_assistant_time_label(target_seconds)}",
            }]
        elif action_type in {"update_study_plan", "rebalance_study_plan"}:
            is_rebalance = action_type == "rebalance_study_plan"
            start_default = (
                str(current_settings.get("start_date"))
                if current_settings
                else (_study_plan_week_start(_study_plan_business_date()) + timedelta(days=7)).isoformat()
            )
            end_default = str((current_settings or {}).get("end_date") or STUDY_PLAN_END)
            start_value = (
                _study_plan_business_date().isoformat()
                if is_rebalance
                else (str(raw_action.get("start_date") or "").strip() or start_default)
            )
            end_value = str(raw_action.get("end_date") or "").strip() or end_default
            try:
                start_day = datetime.strptime(start_value, "%Y-%m-%d").date()
                end_day = datetime.strptime(end_value, "%Y-%m-%d").date()
                weekday_hours = float(raw_action.get("weekday_hours") or 0)
                weekend_hours = float(raw_action.get("weekend_hours") or 0)
            except (TypeError, ValueError):
                return None
            current_weekday = float((current_settings or {}).get("weekday_minutes") or 180) / 60
            current_weekend = float((current_settings or {}).get("weekend_minutes") or 120) / 60
            weekday_hours = weekday_hours or current_weekday
            weekend_hours = weekend_hours or current_weekend
            if (
                end_day < start_day + (timedelta(0) if is_rebalance else timedelta(days=6))
                or end_day > start_day + timedelta(days=366)
                or not (0.25 <= weekday_hours <= 12)
                or not (0.25 <= weekend_hours <= 12)
            ):
                return None
            if current_settings and not is_rebalance:
                baseline_by_subject = dict(current_settings.get("baseline_by_subject") or {})
                subject_targets = dict(current_settings.get("subject_targets") or {})
            else:
                baseline_by_subject = {}
                subject_targets = {}
                for subject in STUDY_PLAN_SUBJECTS:
                    subject_videos = [video for video in videos if str(video.get("subject") or "") == subject]
                    watched = sum(
                        min(float(video.get("watched_seconds") or 0), float(video.get("duration_seconds") or 0))
                        for video in subject_videos
                    )
                    remaining = sum(float(video.get("duration_seconds") or 0) for video in subject_videos) - watched
                    baseline_by_subject[subject] = max(0.0, watched)
                    if remaining > 0.001:
                        subject_targets[subject] = remaining
            if not subject_targets:
                return None
            if is_rebalance:
                remaining_days = (end_day - start_day).days + 1
                required_daily_hours = sum(subject_targets.values()) / max(1, remaining_days) / 3600
                # Equal weights make the scheduler distribute the exact remaining load evenly;
                # the load ratio handles values outside the visual 15 min–12 h preference range.
                equal_weight_hours = min(12.0, max(0.25, required_daily_hours))
                weekday_hours = equal_weight_hours
                weekend_hours = equal_weight_hours
            before_state = {
                "exists": bool(current_settings),
                "settings": dict(current_settings or {}),
            }
            action_payload = {
                "start_date": start_day.isoformat(),
                "end_date": end_day.isoformat(),
                "weekday_minutes": weekday_hours * 60,
                "weekend_minutes": weekend_hours * 60,
                "baseline_by_subject": baseline_by_subject,
                "subject_targets": subject_targets,
            }
            stored_action_type = "update_study_plan"
            title = "平均分攤剩餘讀書計畫" if is_rebalance else "調整讀書計畫"
            summary = (
                f"保留 {end_day.isoformat()} 截止日，從今天起平均安排剩餘 {_study_assistant_time_label(sum(subject_targets.values()))}"
                if is_rebalance
                else (reason or "更新計畫日期與每日影片時數")
            )
            changes = []
            old_start = str((current_settings or {}).get("start_date") or start_default)
            old_end = str((current_settings or {}).get("end_date") or end_default)
            for label, before, after in (
                ("計畫起日", old_start, start_day.isoformat()),
                ("計畫迄日", old_end, end_day.isoformat()),
                ("平日每日", f"{current_weekday:g} 小時", f"{weekday_hours:g} 小時"),
                ("假日每日", f"{current_weekend:g} 小時", f"{weekend_hours:g} 小時"),
            ):
                if before != after:
                    changes.append({"label": label, "before": before, "after": after})
            if is_rebalance:
                metrics = _study_assistant_plan_metrics(videos, current_settings)
                changes = [
                    {"label": "原截止日", "before": old_end, "after": end_day.isoformat() + "（保留）"},
                    {
                        "label": "目前落後",
                        "before": f"{float(metrics.get('behind_hours') or 0):.1f} 小時",
                        "after": f"分攤至 {(end_day - start_day).days + 1} 天",
                    },
                    {
                        "label": "剩餘影片",
                        "before": f"{float(metrics.get('remaining_hours') or 0):.1f} 小時",
                        "after": f"平均每日約 {required_daily_hours:.2f} 小時",
                    },
                ]
            if not changes:
                return None
        else:
            return None

        stored = storage.create_study_assistant_action(
            action_id=action_id,
            username=username,
            action_type=stored_action_type,
            action_payload=action_payload,
            before_state=before_state,
            summary=summary,
        )
        if not stored:
            return None
        return {
            "id": action_id,
            "title": title,
            "summary": summary,
            "reason": reason,
            "changes": changes,
            "apply_url": url_for("admin_study_assistant_action_apply", action_id=action_id),
        }

    @app.post("/admin/study-recall/assistant/ask")
    @admin_required
    def admin_study_recall_ask_general():
        if not openai_api_key:
            return {"ok": False, "error": "AI 問答尚未啟用，請先設定 OPENAI_API_KEY。"}, 503
        user = current_user() or {}
        _repair_legacy_study_assistant_time_moves(str(user.get("username") or ""))
        payload = request.get_json(silent=True) or {}
        question = " ".join(str(payload.get("question") or "").split()).strip()
        if not question:
            return {"ok": False, "error": "請輸入想詢問的內容。"}, 400
        if len(question) > 800:
            return {"ok": False, "error": "問題請控制在 800 字以內。"}, 400
        raw_page_context = payload.get("page_context")
        page_context = {
            "path": str((raw_page_context or {}).get("path") or request.referrer or "")[:240],
            "title": str((raw_page_context or {}).get("title") or "")[:120],
        } if isinstance(raw_page_context, dict) else {"path": "", "title": ""}

        response_style = str(payload.get("response_style") or "concise").strip().lower()
        if response_style not in {"concise", "detailed"}:
            response_style = "concise"
        style_instruction = (
            "採詳細回答：先給結論，再補上必要理由、步驟與一個具體例子，控制在 5 至 10 個短段落。"
            if response_style == "detailed"
            else "採精簡回答：直接回答核心問題並保留必要條件，控制在 2 至 5 個短段落。"
        )

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

        conversation = "\n".join(
            f"{'使用者' if entry['role'] == 'user' else '助理'}：{entry['content']}"
            for entry in history
        )
        previous_user_messages = [
            entry["content"] for entry in history if entry.get("role") == "user"
        ]
        short_confirmation = question.casefold() in {
            "確認", "確定", "好", "可以", "是", "對", "沒錯", "就這個", "照這個",
            "a", "選a", "選項a", "a選項",
        }
        numbered_followup = question.casefold() in {"1", "2", "選1", "選2", "選項1", "選項2"}
        intent_text = question
        if (short_confirmation or numbered_followup) and previous_user_messages:
            intent_text = f"{previous_user_messages[-1]} {question}"
        data_nouns = (
            "影片", "進度", "觀看", "學習", "讀書", "時數", "分鐘", "小時", "計畫", "資料庫", "紀錄",
            "筆記", "重點卡", "複習", "考題", "作業", "截止", "課程", "月曆", "行程", "今日任務",
            *STUDY_PLAN_SUBJECTS,
        )
        mutation_verbs = (
            "修正", "更正", "修改", "調整", "調成", "改成", "設成", "更新", "刪除", "重設", "對調",
            "重排", "重新安排", "平攤", "分攤", "整理", "規劃", "安排", "延後", "提前",
            "算在", "移到", "挪到", "記到", "歸到", "改到", "新增", "建立", "加入", "移除",
        )
        personal_markers = (
            "我的", "我今天", "我昨天", "幫我", "替我", "網站", "目前", "現在", "資料庫", "當前",
        )
        read_markers = (
            "查", "多少", "完成率", "看到哪", "目前進度", "現在進度", "進度", "落後", "待補",
            "有哪些", "哪一個", "什麼時候", "分析", "比較", "建議",
        )
        has_data_noun = any(term in intent_text for term in data_nouns)
        has_mutation_intent = any(term in intent_text for term in mutation_verbs)
        data_request = (
            has_data_noun and (
                has_mutation_intent
                or any(term in intent_text for term in personal_markers)
                or any(term in intent_text for term in read_markers)
            )
        ) or any(term in intent_text for term in personal_markers)
        if not data_request:
            prompt = (
                "你是可靠、直接且善於教學的繁體中文 AI 助手。這是一般問答模式，沒有指定筆記或重點卡；"
                "請依可靠知識回答使用者的實際問題，不要假裝看過使用者的筆記，也不要自行捏造來源。"
                "一般問答模式不能修改網站資料，也絕對不能聲稱已更新、已執行或已套用任何網站變更。"
                "若資訊不足或問題有多種合理解讀，先說明必要假設；若內容可能過時，清楚提醒需要查證。"
                f"回答要完整收尾並避免冗長。{style_instruction}數學表達式使用 LaTeX：行內公式用 \\( ... \\)，"
                "獨立公式用 \\[ ... \\]。除非使用者需要，否則不要加入多餘標題或結尾邀請。\n\n"
                f"最近對話：\n{conversation or '尚無'}\n\n"
                f"使用者這次的問題：{question}"
            )
            try:
                response = requests.post(
                    "https://api.openai.com/v1/responses",
                    headers={"Authorization": f"Bearer {openai_api_key}", "Content-Type": "application/json"},
                    json={
                        "model": openai_model,
                        "store": False,
                        "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}],
                        "reasoning": {"effort": normalize_openai_reasoning_effort(openai_model, "low")},
                        "max_output_tokens": 3200,
                    },
                    timeout=90,
                )
                response.raise_for_status()
                response_payload = response.json()
                answer = _extract_openai_text(response_payload).strip()[:8000]
                incomplete = response_payload.get("status") == "incomplete"
            except requests.HTTPError as exc:
                error_code, error_type, error_message = _openai_error_details(exc.response)
                if _is_openai_quota_error(error_code, error_type, error_message):
                    return {"ok": False, "error": "OpenAI API 額度不足，請管理員補充額度後再使用 AI 助手。"}, 503
                return {"ok": False, "error": "AI 助手暫時無法回答，請稍後再試。"}, 502
            except (requests.RequestException, ValueError, TypeError):
                return {"ok": False, "error": "AI 助手暫時無法回答，請稍後再試。"}, 502
            if incomplete:
                return {"ok": False, "error": "回答內容過長，請縮小問題範圍後再試。"}, 502
            if not answer:
                return {"ok": False, "error": "AI 助手沒有產生有效回答，請換個方式提問。"}, 502
            record_ui_event(
                "study_recall_general_question",
                meta={"history_count": len(history), "response_style": response_style, "data_mode": False},
            )
            return {"ok": True, "answer": _normalize_study_math_markup(answer), "proposal": None}

        business_today = _study_plan_business_date()
        videos = storage.list_study_plan_videos_with_records()
        replan_settings = storage.get_study_plan_replan_settings()
        explicit_video_action = _study_assistant_explicit_video_progress_action(intent_text)
        if explicit_video_action:
            proposal = _build_study_assistant_proposal(
                username=str((current_user() or {}).get("username") or ""),
                raw_action=explicit_video_action,
                videos=videos,
                current_settings=replan_settings,
            )
            if proposal:
                record_ui_event(
                    "study_recall_general_question",
                    meta={"data_mode": True, "proposed_action": "set_video_progress", "deterministic": True},
                )
                return {
                    "ok": True,
                    "answer": "我已讀取這部影片的真實總長與目前進度。請先確認下方變更，按下套用後才會實際更新。",
                    "proposal": proposal,
                }
            return {
                "ok": True,
                "answer": "找不到唯一對應的科目與影片編號，因此沒有修改任何資料。請檢查科目名稱與影片編號。",
                "proposal": None,
            }
        explicit_time_move_action = _study_assistant_explicit_time_move_action(intent_text)
        if explicit_time_move_action:
            proposal = _build_study_assistant_proposal(
                username=str((current_user() or {}).get("username") or ""),
                raw_action=explicit_time_move_action,
                videos=videos,
                current_settings=replan_settings,
            )
            if proposal:
                record_ui_event(
                    "study_recall_general_question",
                    meta={"data_mode": True, "proposed_action": "move_study_time_between_days", "deterministic": True},
                )
                return {
                    "ok": True,
                    "answer": "我已核對來源日期的實際觀看紀錄。請確認下方日期與分鐘數；按下套用前不會修改資料。",
                    "proposal": proposal,
                }
            return {
                "ok": True,
                "answer": "來源日期找不到足夠且唯一對應的觀看紀錄，因此沒有修改任何資料。",
                "proposal": None,
            }
        explicit_rebalance_request = bool(
            "計畫" in intent_text
            and any(term in intent_text for term in ("重排", "重新安排", "平攤", "分攤", "排回"))
            and any(term in intent_text for term in ("請", "幫我", "替我", "我要", "調整", "重排"))
        )
        if explicit_rebalance_request:
            proposal = _build_study_assistant_proposal(
                username=str((current_user() or {}).get("username") or ""),
                raw_action={
                    "type": "rebalance_study_plan",
                    "end_date": str((replan_settings or {}).get("end_date") or STUDY_PLAN_END),
                    "weekday_hours": 0,
                    "weekend_hours": 0,
                    "reason": "將目前未完成與落後的影片量平均分攤至原截止日前",
                },
                videos=videos,
                current_settings=replan_settings,
            )
            if proposal:
                record_ui_event(
                    "study_recall_general_question",
                    meta={"data_mode": True, "proposed_action": "rebalance_study_plan", "deterministic": True},
                )
                return {
                    "ok": True,
                    "answer": "我已依照目前實際進度重新計算，保留原截止日，並把所有未完成影片與落後量平均分攤到剩餘日期。以下是套用前的完整預覽。",
                    "proposal": proposal,
                }
            return {
                "ok": True,
                "answer": "目前沒有尚未完成的影片可重新分配，或原截止日已經早於今天。",
                "proposal": None,
            }
        requested_subject = _study_assistant_subject_from_text(intent_text)
        subject_progress_request = bool(
            requested_subject
            and not has_mutation_intent
            and any(term in intent_text for term in read_markers)
        )
        if subject_progress_request:
            answer = _study_assistant_subject_progress_answer(requested_subject, videos)
            if answer:
                record_ui_event(
                    "study_recall_general_question",
                    meta={
                        "history_count": len(history),
                        "response_style": response_style,
                        "data_mode": True,
                        "direct_subject_progress": requested_subject,
                    },
                )
                return {"ok": True, "answer": answer, "proposal": None}

        vague_time_correction = bool(
            has_mutation_intent
            and any(term in intent_text for term in ("時數", "學習時間", "讀書時間", "分鐘", "小時"))
            and not re.search(r"\d", intent_text)
            and not requested_subject
        )
        if vague_time_correction:
            return {
                "ok": True,
                "answer": "可以。請告訴我哪一天、哪一筆學習紀錄，以及正確應該是多少分鐘。",
                "proposal": None,
            }

        data_context = _study_assistant_data_context(
            username=str((current_user() or {}).get("username") or ""),
            videos=videos,
            current_settings=replan_settings,
            page_context=page_context,
        )
        prompt = (
            "你是可靠、直接且善於教學的繁體中文 AI 助手，也能協助管理這個學習網站的資料。"
            "這是一般問答模式，沒有指定筆記或重點卡。"
            "請依可靠知識回答使用者的實際問題，不要假裝看過使用者的筆記，也不要自行捏造來源。"
            "網站目前可用資料是伺服器從使用者自己的資料庫即時整理出的真實資料，涵蓋計畫與落後量、"
            "所有影片進度、近 30 天學習時間、近 14 天可修正紀錄、筆記庫與概念、複習負荷、作業及專有名詞。"
            "你要自行交叉比對需要的部分，不得聲稱無法存取、要求使用者再次授權或要求貼上已存在的資料。"
            "唯讀查詢要直接回答，不得要求確認。修改資訊不足時，只能用一句自然中文追問一個必要問題，"
            "不得列出選單、操作格式或必要欄位清單。能從對話與資料判斷時就直接判斷。"
            "資料查詢與修改回答最多三個短句，不重述使用者整段問題。"
            "數學表達式使用 LaTeX：行內公式用 \\( ... \\)，"
            "獨立公式用 \\[ ... \\]。除非使用者需要，否則不要加入多餘標題或結尾邀請。\n\n"
            "對使用者只能使用自然名稱。絕對不可顯示英文動作名稱、資料欄位、session id、JSON、資料表或 SQL；"
            "不可提到 set_video_progress、set_study_time_session、update_study_plan、session_id、subject、"
            "video_sequence、target_minutes、start_date、end_date、weekday_hours、weekend_hours。\n\n"
            "你是能自行完成任務的學習 Agent。先查網站目前可用資料、消除可由資料解決的歧義，再規劃安全動作；"
            "所有寫入只能提出一個待確認動作，由伺服器在使用者確認後執行並回讀驗證，"
            "不能聲稱已經修改，也不能要求或產生 SQL。若目前工具無法安全完成要求，action.type 必須為 none，"
            "並誠實說明這次沒有改動資料：\n"
            "1. set_video_progress：修正某科目某支影片的最後觀看位置或完成百分比。"
            "subject、video_sequence 必須明確；使用者說百分比或看完時填 target_percent，說分鐘時填 target_minutes。"
            "影片總長已存在網站資料中，不得要求使用者提供。\n"
            "2. set_study_time_session：修正最近 14 天內某筆實際學習時間。session_id、target_minutes 必須明確。\n"
            "3. move_study_time_between_days：把某科目某支影片的學習分鐘從一個日期改記到另一日期，可向前或向後移；"
            "subject、video_sequence、source_date、target_date 必須明確，日期格式為 YYYY-MM-DD。"
            "指定分鐘時填 target_minutes 且 move_all=false；說全部、整筆或所有時填 move_all=true，"
            "target_minutes 可填使用者說的約數或 0，實際執行會以來源日資料庫的精確秒數為準。\n"
            "4. update_study_plan：調整計畫起日、迄日、平日或假日每日影片時數；未變更欄位使用空字串或 0。\n"
            "5. rebalance_study_plan：保留原截止日，依目前真實觀看進度把所有剩餘影片與落後量從今天起平均分攤；"
            "使用者要求重排、追回落後或平均分攤時優先使用，不必追問他已經提供過的時數。\n"
            "只有使用者明確要求修改，而且資料能唯一對應時，action.type 才能不是 none。"
            "如果使用者只是在詢問、診斷、比較、描述錯誤，或對象／數值不明確，action.type 必須是 none，"
            "並在 answer 中說明還需要哪個資訊。answer 不得說已完成、已修正或已套用，只能說準備修改或請確認。\n\n"
            f"網站目前可用資料：\n{json.dumps(data_context, ensure_ascii=False)}\n\n"
            f"最近對話：\n{conversation or '尚無'}\n\n"
            f"使用者這次的問題：{question}"
        )
        schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["answer", "action"],
            "properties": {
                "answer": {"type": "string", "maxLength": 8000},
                "action": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "type", "subject", "video_sequence", "session_id", "target_minutes",
                        "target_percent", "move_all", "source_date", "target_date", "start_date", "end_date", "weekday_hours", "weekend_hours", "reason",
                    ],
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": [
                                "none", "set_video_progress", "set_study_time_session",
                                "move_study_time_between_days", "update_study_plan", "rebalance_study_plan",
                            ],
                        },
                        "subject": {"type": "string"},
                        "video_sequence": {"type": "integer", "minimum": 0},
                        "session_id": {"type": "string"},
                        "target_minutes": {"type": "number", "minimum": 0},
                        "target_percent": {"type": "number", "minimum": 0, "maximum": 100},
                        "move_all": {"type": "boolean"},
                        "source_date": {"type": "string"},
                        "target_date": {"type": "string"},
                        "start_date": {"type": "string"},
                        "end_date": {"type": "string"},
                        "weekday_hours": {"type": "number", "minimum": 0},
                        "weekend_hours": {"type": "number", "minimum": 0},
                        "reason": {"type": "string", "maxLength": 240},
                    },
                },
            },
        }
        try:
            ai_result = _call_openai_json(
                name="study_data_assistant",
                schema=schema,
                content=[{"type": "input_text", "text": prompt}],
                timeout=90,
                reasoning_effort="low",
                max_output_tokens=3600,
            )
        except requests.HTTPError as exc:
            error_code, error_type, error_message = _openai_error_details(exc.response)
            if _is_openai_quota_error(error_code, error_type, error_message):
                return {"ok": False, "error": "OpenAI API 額度不足，請管理員補充額度後再使用 AI 助手。"}, 503
            return {"ok": False, "error": "AI 助手暫時無法回答，請稍後再試。"}, 502
        except (requests.RequestException, ValueError, TypeError):
            return {"ok": False, "error": "AI 助手暫時無法回答，請稍後再試。"}, 502
        answer = str(ai_result.get("answer") or "").strip()[:8000]
        if not answer:
            return {"ok": False, "error": "AI 助手沒有產生有效回答，請換個方式提問。"}, 502
        answer = _sanitize_study_assistant_answer(answer)
        proposal = _build_study_assistant_proposal(
            username=str((current_user() or {}).get("username") or ""),
            raw_action=ai_result.get("action"),
            videos=videos,
            current_settings=replan_settings,
        )
        proposed_type = str((ai_result.get("action") or {}).get("type") or "none")
        if proposed_type != "none" and not proposal:
            answer = {
                "set_video_progress": "請告訴我要修改的科目、影片序號，以及正確的觀看分鐘數。",
                "set_study_time_session": "請告訴我哪一天、哪一筆學習紀錄，以及正確的分鐘數。",
                "move_study_time_between_days": "來源日期找不到足夠且唯一對應的觀看紀錄，因此沒有修改任何資料。",
                "update_study_plan": "請告訴我要調整的日期或每日可讀時數。",
                "rebalance_study_plan": "目前沒有可安全重新安排的未完成影片，或原截止日已經早於今天。",
            }.get(proposed_type, "這筆資料目前無法安全修改，請再說明要改哪一筆與正確數值。")
        elif proposal:
            answer = "已整理好變更，確認後就會套用。"
        elif has_mutation_intent:
            if _study_assistant_claims_execution(answer):
                answer = "這次沒有修改任何資料；目前無法把要求安全轉成可驗證的變更。"
            elif "沒有修改" not in answer and "尚未修改" not in answer:
                answer = f"尚未修改任何資料。{answer}"
        elif _study_assistant_claims_execution(answer):
            answer = "這次沒有修改任何資料。"
        record_ui_event(
            "study_recall_general_question",
            meta={
                "history_count": len(history),
                "response_style": response_style,
                "proposed_action": proposed_type,
            },
        )
        return {"ok": True, "answer": answer, "proposal": proposal}

    return (
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
    )
