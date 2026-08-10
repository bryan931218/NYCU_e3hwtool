from __future__ import annotations

from copy import deepcopy
from datetime import date
from typing import Any, Dict, List, Tuple


def _pct(done: float, target: float) -> float:
    if target <= 0:
        return 0.0
    return min(100.0, max(0.0, done / target * 100.0))


def _day_state(day: Dict[str, Any], today: date, credited: float, target: float) -> Tuple[str, str]:
    raw_date = str(day.get("date") or "")
    try:
        day_date = date.fromisoformat(raw_date)
    except ValueError:
        day_date = today
    completion = _pct(credited, target)
    if target <= 0:
        return "upcoming", "彈性"
    if completion >= 99.5:
        return ("early", "提前完成") if day_date > today else ("complete", "完成")
    if credited > 0:
        return "partial", "進行中"
    if day_date < today:
        return "behind", "待補"
    if day_date == today:
        return "active", "今天"
    return "upcoming", "未開始"


def _redistribute_progress_to_earliest_gap(
    week_rows: List[Dict[str, Any]],
    *,
    today: date,
) -> List[Dict[str, Any]]:
    """Redistribute existing total credit chronologically into the earliest open slots.

    The original planner allocates credit per subject. That can skip an earlier empty day
    and place progress on a later day where the watched subject happens to appear. This
    helper keeps the exact same total credited seconds, but fills schedule slots from the
    beginning of the timeline first.
    """
    rows = week_rows
    remaining = sum(max(0.0, float(row.get("watched_seconds") or 0)) for row in rows)

    for row in rows:
        week_done = 0.0
        subject_done: Dict[str, float] = {}
        for day in row.get("daily_recommendations") or []:
            target = max(0.0, float(day.get("target_seconds") or 0))
            credited = min(remaining, target)
            remaining = max(0.0, remaining - credited)
            week_done += credited
            completion = _pct(credited, target)
            day["credited_seconds"] = credited
            day["watched_seconds"] = credited
            day["completion"] = completion
            state, label = _day_state(day, today, credited, target)
            day["state"] = state
            day["state_label"] = label

            allocations = day.get("allocations") or {}
            for subject, seconds in allocations.items():
                subject_target = max(0.0, float(seconds or 0))
                subject_credit = subject_target * completion / 100.0
                subject_done[str(subject)] = subject_done.get(str(subject), 0.0) + subject_credit

            for item in day.get("subject_progress") or []:
                item_target = max(0.0, float(item.get("target_seconds") or 0))
                item["watched_seconds"] = item_target * completion / 100.0
                item["completion"] = completion

        week_target = max(0.0, float(row.get("target_seconds") or 0))
        week_completion = _pct(week_done, week_target)
        row["watched_seconds"] = week_done
        row["watched_hours"] = round(week_done / 3600.0, 1)
        row["remaining_seconds"] = max(0.0, week_target - week_done)
        row["remaining_hours"] = round(max(0.0, week_target - week_done) / 3600.0, 1)
        row["completion"] = week_completion

        try:
            start = date.fromisoformat(str(row.get("start") or ""))
            end = date.fromisoformat(str(row.get("end") or ""))
        except ValueError:
            start = end = today
        if week_completion >= 99.5:
            row["state"], row["state_label"] = (("early", "提前完成") if start > today else ("complete", "完成"))
        elif week_done > 0:
            row["state"], row["state_label"] = "active", "進行中"
        elif end < today:
            row["state"], row["state_label"] = "behind", "待補"
        elif start <= today <= end:
            row["state"], row["state_label"] = "active", "進行中"
        else:
            row["state"], row["state_label"] = "upcoming", "未開始"

        for mix in row.get("subject_mix") or []:
            name = str(mix.get("name") or "")
            target = max(0.0, float(mix.get("target_seconds") or 0))
            done = min(target, subject_done.get(name, 0.0))
            mix["watched_seconds"] = done
            mix["watched_hours"] = round(done / 3600.0, 1)
            mix["completion"] = _pct(done, target)

    return rows


def install_study_gap_fill_runtime(web_module: Any) -> None:
    if getattr(web_module, "__e3_study_gap_fill_runtime_installed", False):
        return
    web_module.__e3_study_gap_fill_runtime_installed = True

    original = web_module._study_plan_week_rows

    def wrapped(videos):
        result = original(videos)
        if not isinstance(result, tuple) or not result or not isinstance(result[0], list):
            return result
        rows = result[0]
        today = web_module._study_plan_business_date()
        _redistribute_progress_to_earliest_gap(rows, today=today)

        # current_week / calendar_week are usually references into rows, but sync any
        # detached mirrors as well so every panel shows the same values.
        by_start = {str(row.get("start") or ""): row for row in rows}
        for extra in result[1:]:
            if isinstance(extra, dict):
                match = by_start.get(str(extra.get("start") or ""))
                if match is not None and match is not extra:
                    extra.update(deepcopy(match))
        return result

    web_module._study_plan_week_rows = wrapped
