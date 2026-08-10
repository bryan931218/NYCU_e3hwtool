from __future__ import annotations

from datetime import datetime, time as datetime_time, timedelta
from typing import Any, Dict, Optional

from .constants import TAIPEI_TZ


_LEARNING_DAY_START_HOUR = 8
_PROGRESS_TOLERANCE_MINUTES = 10.0


def _nonnegative_number(value: Any) -> float:
    try:
        parsed = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    if parsed != parsed or parsed in {float("inf"), float("-inf")}:
        return 0.0
    return max(0.0, parsed)


def _learning_day_fraction(now: Optional[datetime] = None) -> float:
    """Return how far the current 08:00 -> next-day 08:00 learning day has elapsed."""

    current = now or datetime.now(TAIPEI_TZ)
    if current.tzinfo is None:
        current = current.replace(tzinfo=TAIPEI_TZ)
    else:
        current = current.astimezone(TAIPEI_TZ)

    business_day = (current - timedelta(hours=_LEARNING_DAY_START_HOUR)).date()
    learning_day_start = datetime.combine(
        business_day,
        datetime_time(hour=_LEARNING_DAY_START_HOUR),
        tzinfo=TAIPEI_TZ,
    )
    elapsed_seconds = (current - learning_day_start).total_seconds()
    return min(1.0, max(0.0, elapsed_seconds / (24 * 60 * 60)))


def _format_duration(minutes: float) -> str:
    full_hours, remainder_minutes = divmod(int(round(abs(minutes))), 60)
    if full_hours and remainder_minutes:
        return f"{full_hours} 小時 {remainder_minutes} 分鐘"
    if full_hours:
        return f"{full_hours} 小時"
    return f"{remainder_minutes} 分鐘"


def _time_aware_progress_race(
    watched_minutes: Any,
    target_minutes_by_today: Any,
    total_target_minutes: Any,
    today_target_minutes: Any = 0,
    *,
    now: Optional[datetime] = None,
    tolerance_minutes: float = _PROGRESS_TOLERANCE_MINUTES,
) -> Dict[str, Any]:
    """Compare watched progress with what should reasonably be done *at this moment*.

    The old implementation compared progress with the whole current day's target and
    then treated any deficit smaller than that full-day target as "normal". That made
    the judgement asymmetric: several hours behind could still be normal while being
    only a minute ahead became "early". Here the current day's target accrues smoothly
    through the app's existing 08:00 -> next-day 08:00 learning day.
    """

    watched = _nonnegative_number(watched_minutes)
    total = _nonnegative_number(total_target_minutes)
    full_target = _nonnegative_number(target_minutes_by_today)
    if total > 0:
        full_target = min(full_target, total)
    today_target = min(_nonnegative_number(today_target_minutes), full_target)

    elapsed_fraction = _learning_day_fraction(now)
    target_before_today = max(0.0, full_target - today_target)
    expected_today = today_target * elapsed_fraction
    expected_target = target_before_today + expected_today
    if total > 0:
        expected_target = min(expected_target, total)

    actual_percent = min(100.0, (watched / total * 100) if total else 0.0)
    target_percent = min(100.0, (expected_target / total * 100) if total else 0.0)
    delta_minutes = watched - expected_target
    delta_hours = delta_minutes / 60
    tolerance = max(1.0, _nonnegative_number(tolerance_minutes))

    delta_label = _format_duration(delta_minutes)
    today_target_label = _format_duration(today_target)
    expected_today_label = _format_duration(expected_today)

    if delta_minutes < -tolerance:
        state = "behind"
        state_label = "落後計畫"
        status_label = f"落後 {delta_label}"
        action = f"截至此刻約差 {delta_label}，先補回這段即可回到應有節奏。"
        headline_message = "目前進度落後。"
        headline_unit = "小時待補"
    elif delta_minutes > tolerance:
        state = "early"
        state_label = "超前計畫"
        status_label = f"領先 {delta_label}"
        action = f"截至此刻約領先 {delta_label}，可保留作後續緩衝。"
        headline_message = "目前進度超前。"
        headline_unit = "小時領先"
    else:
        state = "active"
        state_label = "進度同步"
        status_label = "與此刻進度同步"
        action = f"與此刻應有進度的差距在 {int(round(tolerance))} 分鐘內。"
        headline_message = "目前進度與時間同步。"
        headline_unit = "小時差距"

    runner_position = min(97.5, max(2.5, actual_percent))
    plan_position = min(97.5, max(2.5, target_percent))
    return {
        "watched_hours": round(watched / 60, 1),
        "target_hours": round(expected_target / 60, 1),
        "scheduled_target_hours": round(full_target / 60, 1),
        "actual_percent": round(actual_percent, 1),
        "target_percent": round(target_percent, 1),
        "delta_hours": round(delta_hours, 1),
        "delta_minutes": round(delta_minutes, 1),
        "absolute_delta_hours": round(abs(delta_hours), 1),
        "delta_label": delta_label,
        "today_target_hours": round(today_target / 60, 1),
        "today_target_label": today_target_label,
        "expected_today_hours": round(expected_today / 60, 1),
        "expected_today_label": expected_today_label,
        "today_elapsed_percent": round(elapsed_fraction * 100, 1),
        "tolerance_minutes": round(tolerance, 1),
        # Retained for compatibility with the existing context builder. The visible
        # message is supplied by this runtime instead of its old full-day allowance.
        "within_daily_allowance": state == "active",
        "state": state,
        "state_label": state_label,
        "status_label": status_label,
        "status_detail": action,
        "headline_message": headline_message,
        "headline_value": f"{abs(delta_hours):.1f}",
        "headline_unit": headline_unit,
        "action": action,
        "runner_position": round(runner_position, 1),
        "plan_position": round(plan_position, 1),
        "gap_start": round(min(runner_position, plan_position), 1),
        "gap_width": round(abs(runner_position - plan_position), 1),
        "comparison_mode": "time_aware",
    }


def _patch_progress_template(template: str) -> str:
    replacements = (
        (
            "{{ progress_race.headline_message }}{{ pace_insight.action }}",
            "{{ progress_race.headline_message }}{{ progress_race.action }}",
        ),
        ("截至今天應看", "截至此刻應看"),
        (
            "<div class=\"race-plan-label\">今日目標 {{ progress_race.target_hours }}h</div>",
            "<div class=\"race-plan-label\">此刻應到 {{ progress_race.target_hours }}h</div>",
        ),
    )
    for old, new in replacements:
        template = template.replace(old, new)
    return template


def install_study_progress_runtime(web_module: Any) -> None:
    """Install time-aware pace judgement before the Flask app is created."""

    if getattr(web_module, "__e3_study_progress_runtime_installed", False):
        return
    web_module.__e3_study_progress_runtime_installed = True

    web_module._study_plan_progress_race = _time_aware_progress_race
    for attribute in ("STUDY_HOME_TEMPLATE", "PUBLIC_STUDY_TEMPLATE"):
        template = getattr(web_module, attribute, None)
        if isinstance(template, str):
            setattr(web_module, attribute, _patch_progress_template(template))
