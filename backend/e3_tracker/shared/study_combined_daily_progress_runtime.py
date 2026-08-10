from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from flask import session


def _safe_seconds(value: Any) -> float:
    try:
        parsed = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    if parsed != parsed or parsed in {float("inf"), float("-inf")}:
        return 0.0
    return max(0.0, parsed)


def _completion_percent(target_seconds: float, watched_seconds: float) -> float:
    target = _safe_seconds(target_seconds)
    watched = min(_safe_seconds(watched_seconds), target)
    if target <= 0:
        return 0.0
    if watched >= target - 5.0 or watched / target >= 0.995:
        return 100.0
    return min(100.0, watched / target * 100.0)


def _day_state(day: date, today: date, *, target_seconds: float, completion: float) -> Tuple[str, str]:
    if target_seconds <= 0:
        if day == today:
            return "active", "彈性日"
        return "upcoming", "未排程"
    if completion >= 100.0:
        if day > today:
            return "early", "提早完成"
        return "complete", "完成"
    if completion > 0.0:
        if day > today:
            return "early", "超前"
        return "partial", "部分"
    if day == today:
        return "active", "進行中"
    if day < today:
        return "behind", "待補"
    return "upcoming", "未開始"


def _week_state(start: date, end: date, today: date, completion: float) -> Tuple[str, str]:
    if completion >= 100.0:
        if start > today:
            return "early", "提早完成"
        return "complete", "已達標"
    if start <= today <= end:
        return "active", "進行中"
    if end < today:
        return "behind", "待補"
    return "upcoming", "未開始"


def _combined_daily_progress_rows(
    definitions: Iterable[Dict[str, Any]],
    activity_events: Iterable[Dict[str, Any]],
    *,
    today: date,
) -> Dict[str, Any]:
    """Build daily progress from actual same-day video changes, pooled across planned subjects.

    A day's subjects share one target pool. For example, if the target is
    Linear Algebra 1.45h + DS 2h, a 3h DS change counts as 3h toward the
    combined 3.45h day instead of being capped at the DS 2h sub-target.
    Progress never migrates to a different calendar day.
    """

    activity_by_day_subject: Dict[Tuple[str, str], float] = {}
    for event in activity_events:
        day_key = str(event.get("day") or "").strip()
        subject = str(event.get("subject") or "").strip()
        if not day_key or not subject:
            continue
        try:
            delta = float(event.get("delta_seconds") or 0)
        except (TypeError, ValueError):
            continue
        if delta != delta or delta in {float("inf"), float("-inf")}:
            continue
        key = (day_key, subject)
        activity_by_day_subject[key] = activity_by_day_subject.get(key, 0.0) + delta

    weeks: List[Dict[str, Any]] = []
    flat_days: List[Dict[str, Any]] = []
    current_week_start = ""

    for definition in definitions:
        start = definition.get("start")
        end = definition.get("end")
        if not isinstance(start, date) or not isinstance(end, date):
            continue

        week_days: List[Dict[str, Any]] = []
        week_target_seconds = 0.0
        week_watched_seconds = 0.0

        for daily_target in definition.get("daily_targets") or []:
            current_day = daily_target.get("date")
            if not isinstance(current_day, date):
                continue
            day_key = current_day.isoformat()
            allocations = {
                str(subject): _safe_seconds(seconds)
                for subject, seconds in (daily_target.get("allocations") or {}).items()
                if str(subject or "")
            }
            target_seconds = sum(allocations.values())

            # Pool the subjects together before applying the daily cap. Negative
            # corrections remain negative until the final day total is clamped.
            raw_watched_seconds = sum(
                activity_by_day_subject.get((day_key, subject), 0.0)
                for subject in allocations
            )
            watched_seconds = min(target_seconds, max(0.0, raw_watched_seconds))
            completion = _completion_percent(target_seconds, watched_seconds)
            state, state_label = _day_state(
                current_day,
                today,
                target_seconds=target_seconds,
                completion=completion,
            )
            row = {
                "date": day_key,
                "target_seconds": round(target_seconds, 1),
                "watched_seconds": round(watched_seconds, 1),
                "target_hours": round(target_seconds / 3600.0, 2),
                "watched_hours": round(watched_seconds / 3600.0, 2),
                "completion": round(completion, 1),
                "state": state,
                "state_label": state_label,
                "subjects": list(allocations),
            }
            week_days.append(row)
            flat_days.append(row)
            week_target_seconds += target_seconds
            week_watched_seconds += watched_seconds

        week_completion = _completion_percent(week_target_seconds, week_watched_seconds)
        state, state_label = _week_state(start, end, today, week_completion)
        week_row = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "target_seconds": round(week_target_seconds, 1),
            "watched_seconds": round(week_watched_seconds, 1),
            "target_hours": round(week_target_seconds / 3600.0, 2),
            "watched_hours": round(week_watched_seconds / 3600.0, 2),
            "remaining_hours": round(max(0.0, week_target_seconds - week_watched_seconds) / 3600.0, 2),
            "completion": round(week_completion, 1),
            "state": state,
            "state_label": state_label,
            "days": week_days,
        }
        weeks.append(week_row)
        if start <= today <= end:
            current_week_start = start.isoformat()

    if not current_week_start and weeks:
        if today < date.fromisoformat(weeks[0]["start"]):
            current_week_start = weeks[0]["start"]
        else:
            current_week_start = weeks[-1]["start"]

    current_week = next(
        (week for week in weeks if week["start"] == current_week_start),
        weeks[0] if weeks else {},
    )
    return {
        "today": today.isoformat(),
        "current_week_start": current_week_start,
        "current_week": current_week,
        "weeks": weeks,
        "days": flat_days,
    }


def _register_combined_daily_route(app: Any, storage: Any, web_module: Any) -> None:
    endpoint = "admin_study_plan_combined_daily_progress"
    if endpoint in app.view_functions:
        return

    def combined_daily_progress():
        username = str(session.get("username") or "").strip()
        token = str(session.get("session_token") or "").strip()
        if not (
            username
            and token
            and storage.is_valid_web_session(token, username)
        ):
            return {"ok": False, "error": "unauthorized"}, 401
        if not session.get("is_admin"):
            return {"ok": False, "error": "forbidden"}, 403

        today = web_module._study_plan_business_date()
        videos = storage.list_study_plan_videos_with_records()
        replan_settings = storage.get_study_plan_replan_settings()
        definitions = web_module._study_plan_schedule_definitions(videos, replan_settings)
        events = storage.list_study_plan_activity_events(
            start_day=web_module.STUDY_PLAN_START,
            end_day=today.isoformat(),
        )
        payload = _combined_daily_progress_rows(definitions, events, today=today)
        return {"ok": True, **payload}

    app.add_url_rule(
        "/admin/study-plan/combined-daily-progress.json",
        endpoint=endpoint,
        view_func=combined_daily_progress,
        methods=["GET"],
    )


def install_study_combined_daily_progress_runtime(web_module: Any) -> None:
    if getattr(web_module, "__e3_combined_daily_progress_installed", False):
        return
    web_module.__e3_combined_daily_progress_installed = True

    root_dir = Path(__file__).resolve().parents[3]
    partial_path = root_dir / "frontend" / "templates" / "_study_combined_daily_progress.html"
    if partial_path.exists():
        partial = partial_path.read_text(encoding="utf-8")
        template = str(web_module.STUDY_PLAN_TEMPLATE)
        if "__e3CombinedDailyProgressInstalled" not in template:
            web_module.STUDY_PLAN_TEMPLATE = (
                template.replace("</body>", partial + "\n</body>", 1)
                if "</body>" in template
                else template + "\n" + partial
            )

    original_create_app = web_module.create_app

    def create_app_with_combined_daily_progress(*args: Any, **kwargs: Any):
        app = original_create_app(*args, **kwargs)
        storage = app.extensions.get("e3_storage")
        if storage is not None:
            _register_combined_daily_route(app, storage, web_module)
        return app

    web_module.create_app = create_app_with_combined_daily_progress
