from __future__ import annotations

import copy
from functools import wraps
from typing import Any, Dict, Iterable, List, Tuple


_INSTALL_MARKER = "__e3_rest_day_redistribution_installed__"


def _day_key(value: Any) -> str:
    if hasattr(value, "isoformat"):
        try:
            return str(value.isoformat())
        except Exception:
            pass
    return str(value or "")


def _positive_number(value: Any) -> float:
    try:
        parsed = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, parsed)


def _entries(weeks: List[Dict[str, Any]]) -> List[Tuple[str, Dict[str, Any], bool]]:
    rows: List[Tuple[str, Dict[str, Any], bool]] = []
    for week in weeks:
        week_segment = bool(week.get("is_replanned"))
        for day in week.get("daily_targets") or []:
            key = _day_key(day.get("date"))
            if not key:
                continue
            segment = bool(day.get("replanned")) or week_segment
            rows.append((key, day, segment))
    rows.sort(key=lambda item: item[0])
    return rows


def _rebuild_week_totals(weeks: List[Dict[str, Any]]) -> None:
    for week in weeks:
        subject_targets: Dict[str, float] = {}
        subject_order: List[str] = []
        for day in week.get("daily_targets") or []:
            for subject, raw_seconds in dict(day.get("allocations") or {}).items():
                seconds = _positive_number(raw_seconds)
                if seconds <= 0.001:
                    continue
                if subject not in subject_order:
                    subject_order.append(subject)
                subject_targets[subject] = subject_targets.get(subject, 0.0) + seconds
        week["subject_targets"] = subject_targets
        week["subjects"] = [subject for subject in subject_order if subject_targets.get(subject, 0.0) > 0.001]


def redistribute_rest_day_allocations(
    weeks: Iterable[Dict[str, Any]],
    rest_days: Iterable[str] | None,
) -> List[Dict[str, Any]]:
    """Move each rest day's load across every later planned day in its schedule segment.

    The original implementation redistributed each subject only to later dates that
    already contained that subject. In an interleaved plan this could concentrate
    several hours into just one or two days even when many planned days remained.

    This version treats the day as one pool: every later date that already has a
    study target, is in the same schedule segment, and is not itself a rest day
    receives an equal share. Subject totals are still preserved exactly.
    """

    result = copy.deepcopy(list(weeks))
    entries = _entries(result)
    by_key = {key: (day, segment) for key, day, segment in entries}
    originally_planned = {
        key
        for key, day, _segment in entries
        if sum(_positive_number(value) for value in dict(day.get("allocations") or {}).values()) > 0.001
    }
    requested = {
        str(value)
        for value in (rest_days or [])
        if str(value) in by_key and str(value) in originally_planned
    }

    def receivers(rest_key: str, segment: bool, blocked: set[str]) -> List[Dict[str, Any]]:
        return [
            day
            for key, day, candidate_segment in entries
            if key > rest_key
            and key in originally_planned
            and key not in blocked
            and candidate_segment == segment
        ]

    # Resolve from the end. A requested rest day is effective only if at least
    # one later planned date can receive its work. Invalid late requests remain
    # normal study days and can therefore receive an earlier rest day's load.
    effective: set[str] = set()
    for rest_key in sorted(requested, reverse=True):
        _day, segment = by_key[rest_key]
        if receivers(rest_key, segment, effective):
            effective.add(rest_key)

    # All effective rest days are excluded as receivers from the start so one
    # rest day never hands work to another rest day.
    for rest_key in sorted(effective):
        rest_row, segment = by_key[rest_key]
        moved_allocations = {
            str(subject): _positive_number(seconds)
            for subject, seconds in dict(rest_row.get("allocations") or {}).items()
            if _positive_number(seconds) > 0.001
        }
        candidates = receivers(rest_key, segment, effective)
        if not candidates:
            continue

        for subject, seconds in moved_allocations.items():
            addition = seconds / len(candidates)
            for candidate in candidates:
                allocations = candidate.setdefault("allocations", {})
                allocations[subject] = _positive_number(allocations.get(subject)) + addition

        rest_row["allocations"] = {}
        rest_row["focus"] = "休息日"
        rest_row["is_rest_day"] = True
        rest_row["redistributed_seconds"] = sum(moved_allocations.values())
        rest_row["redistributed_day_count"] = len(candidates)

    for key, day, segment in entries:
        day.setdefault("is_rest_day", False)
        day.setdefault("redistributed_seconds", 0.0)
        day.setdefault("redistributed_day_count", 0)
        day["rest_day_requested"] = key in requested
        if key in effective:
            day["can_be_rest_day"] = False
            continue
        has_target = sum(
            _positive_number(value)
            for value in dict(day.get("allocations") or {}).values()
        ) > 0.001
        day["can_be_rest_day"] = bool(
            has_target and receivers(key, segment, effective)
        )

    _rebuild_week_totals(result)
    return result


def _patch_rest_day_toggle_template(template: str) -> str:
    patched = str(template or "")
    patched = patched.replace(
        'aria-label="恢復 {{ day.date }} 的原定安排" title="恢復原定安排">復</button>',
        'aria-label="取消 {{ day.date }} 的休息日" title="取消休息日" aria-pressed="true">休</button>',
    )
    patched = patched.replace(
        "並把原定 {{ day.hours }} 小時平均分攤到後續日期？",
        "並把原定 {{ day.hours }} 小時平均分攤到所有剩餘計畫日？",
    )
    return patched


def install_study_rest_day_runtime(web_module: Any) -> None:
    """Install all-day redistribution and make the rest button a true toggle."""

    original = getattr(web_module, "_study_plan_schedule_definitions", None)
    if not callable(original) or getattr(original, _INSTALL_MARKER, False):
        return

    @wraps(original)
    def schedule_definitions(
        videos: Iterable[Dict[str, Any]],
        replan_settings: Dict[str, Any] | None = None,
        rest_days: Iterable[str] | None = None,
    ) -> List[Dict[str, Any]]:
        # Always ask the original builder for the untouched plan. Its built-in
        # rest-day logic is subject-specific; applying our redistribution on top
        # of it would double-move the same hours.
        base_weeks = original(videos, replan_settings, None)
        return redistribute_rest_day_allocations(base_weeks, rest_days)

    setattr(schedule_definitions, _INSTALL_MARKER, True)
    web_module._study_plan_schedule_definitions = schedule_definitions

    template = getattr(web_module, "STUDY_PLAN_TEMPLATE", None)
    if isinstance(template, str):
        web_module.STUDY_PLAN_TEMPLATE = _patch_rest_day_toggle_template(template)
