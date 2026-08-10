from __future__ import annotations

import copy
from datetime import date
from typing import Any, Dict, Iterable, List


def _number(value: Any) -> float:
    try:
        return max(0.0, float(value or 0))
    except (TypeError, ValueError):
        return 0.0


def _zero_subject_progress(items: Iterable[Dict[str, Any]]) -> None:
    for item in items or []:
        if not isinstance(item, dict):
            continue
        for key in ("watched_seconds", "credited_seconds", "watched_minutes", "watched_hours"):
            if key in item:
                item[key] = 0.0
        if "completion" in item:
            item["completion"] = 0.0


def _clear_future_week_progress(
    week_rows: Iterable[Dict[str, Any]],
    *,
    today: date,
) -> List[Dict[str, Any]]:
    """Keep schedule targets visible while forbidding progress credit before a week starts."""

    rows = copy.deepcopy(list(week_rows or []))
    for week in rows:
        if not isinstance(week, dict):
            continue
        try:
            week_start = date.fromisoformat(str(week.get("start") or ""))
        except ValueError:
            continue
        if week_start <= today:
            continue

        target_seconds = _number(week.get("target_seconds"))
        if not target_seconds:
            target_seconds = _number(week.get("target_minutes")) * 60

        week["watched_seconds"] = 0.0
        if "watched_minutes" in week:
            week["watched_minutes"] = 0.0
        week["watched_hours"] = 0.0
        week["completion"] = 0.0
        week["remaining_seconds"] = target_seconds
        if "remaining_minutes" in week:
            week["remaining_minutes"] = target_seconds / 60
        week["remaining_hours"] = round(target_seconds / 3600, 1)
        week["state"] = "upcoming"
        week["state_label"] = "未開始"

        _zero_subject_progress(week.get("subject_mix") or [])

        for day in week.get("daily_recommendations") or []:
            if not isinstance(day, dict):
                continue
            day["credited_seconds"] = 0.0
            if "watched_seconds" in day:
                day["watched_seconds"] = 0.0
            if "watched_minutes" in day:
                day["watched_minutes"] = 0.0
            if "watched_hours" in day:
                day["watched_hours"] = 0.0
            day["completion"] = 0.0
            if day.get("has_target", _number(day.get("target_seconds")) > 0):
                day["state"] = "upcoming"
                day["state_label"] = "未開始"
            _zero_subject_progress(day.get("subject_progress") or [])

    return rows


def install_study_week_progress_runtime(web_module: Any) -> None:
    """Clamp future week progress at the template boundary without changing saved video positions."""

    if getattr(web_module, "__e3_study_week_progress_runtime_installed", False):
        return
    web_module.__e3_study_week_progress_runtime_installed = True

    original_render = web_module.render_template_string

    def guarded_render(template: str, *args: Any, **context: Any):
        week_rows = context.get("week_rows")
        if isinstance(week_rows, (list, tuple)):
            context = dict(context)
            context["week_rows"] = _clear_future_week_progress(
                week_rows,
                today=web_module._study_plan_business_date(),
            )
        return original_render(template, *args, **context)

    web_module.render_template_string = guarded_render
