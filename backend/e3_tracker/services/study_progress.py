"""Pure study progress, completion, and pacing calculations."""

import math
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qs, urlsplit, urlunsplit
from ..shared.constants import TAIPEI_TZ

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


def _study_plan_default_video(
    videos: Iterable[Dict[str, Any]],
    *,
    last_watched_video_id: int = 0,
    requested_video_id: int = 0,
    allow_completed_fallback: bool = True,
) -> Optional[Dict[str, Any]]:
    ordered_videos = list(videos)
    requested_video = next(
        (
            video
            for video in ordered_videos
            if int(video.get("id") or 0) == int(requested_video_id or 0)
        ),
        None,
    )
    if requested_video:
        return requested_video

    last_watched_video = next(
        (
            video
            for video in ordered_videos
            if int(video.get("id") or 0) == int(last_watched_video_id or 0)
        ),
        None,
    )
    if last_watched_video and not _study_plan_video_is_complete(
        last_watched_video.get("duration_seconds"),
        last_watched_video.get("watched_seconds"),
    ):
        return last_watched_video

    unfinished_video = next(
        (
            video
            for video in ordered_videos
            if not _study_plan_video_is_complete(
                video.get("duration_seconds"),
                video.get("watched_seconds"),
            )
        ),
        None,
    )
    if unfinished_video:
        return unfinished_video
    if not allow_completed_fallback:
        return None
    return last_watched_video or (ordered_videos[0] if ordered_videos else None)


def _study_plan_credited_video_seconds(duration_seconds: Any, watched_seconds: Any) -> float:
    """Credit a completed video at full length so tiny player end gaps do not leak into the schedule."""
    duration = _study_plan_nonnegative_number(duration_seconds)
    watched = min(_study_plan_nonnegative_number(watched_seconds), duration)
    if _study_plan_video_is_complete(duration, watched):
        return duration
    return watched


def _study_plan_range_credited_seconds(
    video_ranges: Iterable[Tuple[float, float, Dict[str, Any]]],
    range_start: Any,
    range_end: Any,
) -> float:
    """Credit only watched video portions that physically overlap a scheduled range."""
    start = _study_plan_nonnegative_number(range_start)
    end = max(start, _study_plan_nonnegative_number(range_end))
    credited = 0.0
    for video_start, video_end, video in video_ranges:
        if video_end <= start or video_start >= end:
            continue
        watched_end = min(
            video_end,
            video_start
            + _study_plan_credited_video_seconds(
                video.get("duration_seconds"),
                video.get("watched_seconds"),
            ),
        )
        credited += max(0.0, min(end, watched_end) - max(start, video_start))
    return min(max(0.0, end - start), credited)


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
    # Ratio tolerance is appropriate for a single player near its final frame,
    # but on a long weekly total it can hide several genuinely unwatched minutes.
    return watched >= target - STUDY_PLAN_COMPLETE_TOLERANCE_SECONDS


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
        watched_seconds = _study_plan_credited_video_seconds(
            duration_seconds,
            item.get("watched_seconds"),
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


def _study_plan_task_video_queue(
    videos: Iterable[Dict[str, Any]],
    required_seconds: Any,
) -> List[Dict[str, Any]]:
    """Return every unfinished sequential video needed to cover a task window."""
    target = _study_plan_nonnegative_number(required_seconds)
    queued_seconds = 0.0
    queue: List[Dict[str, Any]] = []
    for video in videos:
        duration = _study_plan_nonnegative_number(video.get("duration_seconds"))
        watched = min(
            _study_plan_nonnegative_number(video.get("watched_seconds")),
            duration,
        )
        if duration <= 0 or _study_plan_video_is_complete(duration, watched):
            continue
        queue.append(video)
        queued_seconds += max(0.0, duration - watched)
        if target <= 0 or queued_seconds >= target:
            break
    return queue


def _study_plan_interleave_video_queues(
    queues: Iterable[Iterable[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """Keep every subject visible by alternating videos across task queues."""
    rows = [list(queue) for queue in queues if queue]
    interleaved: List[Dict[str, Any]] = []
    for index in range(max((len(queue) for queue in rows), default=0)):
        for queue in rows:
            if index < len(queue):
                interleaved.append(queue[index])
    return interleaved


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


def _study_plan_subject_status(
    subject_weeks: Iterable[Dict[str, Any]],
    today: date,
    *,
    completion: Any,
    watched_seconds: Any,
) -> Tuple[str, str]:
    displayed_completion = _study_plan_nonnegative_number(completion)
    watched = _study_plan_nonnegative_number(watched_seconds)
    if displayed_completion >= 100:
        return "complete", "已達標"
    if watched > 0:
        return "active", "進行中"

    weeks = list(subject_weeks)
    today_key = today.isoformat()
    if any(
        str(row.get("start") or "")
        and str(row.get("start") or "") <= today_key
        for row in weeks
    ):
        return "behind", "待補"
    return "upcoming", "未開始"


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


def _study_plan_pace_history(
    week_rows: Iterable[Dict[str, Any]],
    daily_snapshots: Iterable[Dict[str, Any]],
    *,
    today: date,
    current_watched_minutes: Any,
    limit: int = 14,
) -> Dict[str, Any]:
    """Build a daily cumulative watched-vs-plan delta series in hours."""
    today_key = today.isoformat()
    daily_targets: Dict[str, float] = {}
    candidate_days = {today_key}
    for week in week_rows:
        for item in week.get("daily_recommendations") or []:
            day_key = str(item.get("date") or "")
            if not day_key or day_key > today_key:
                continue
            daily_targets[day_key] = daily_targets.get(day_key, 0.0) + _study_plan_nonnegative_number(
                item.get("target_seconds")
            )
            candidate_days.add(day_key)

    snapshots_by_day: Dict[str, float] = {}
    for item in daily_snapshots:
        day_key = str(item.get("day") or "")
        if not day_key or day_key > today_key:
            continue
        snapshots_by_day[day_key] = _study_plan_nonnegative_number(item.get("total_watched_seconds"))
        candidate_days.add(day_key)

    parsed_days: List[date] = []
    for day_key in candidate_days:
        try:
            parsed_days.append(date.fromisoformat(day_key))
        except ValueError:
            continue
    start_day = min(parsed_days) if parsed_days else today
    current_watched_seconds = _study_plan_nonnegative_number(current_watched_minutes) * 60
    cumulative_target = 0.0
    cumulative_actual: Optional[float] = None
    rows: List[Dict[str, Any]] = []
    cursor = start_day
    while cursor <= today:
        day_key = cursor.isoformat()
        cumulative_target += daily_targets.get(day_key, 0.0)
        if day_key in snapshots_by_day:
            cumulative_actual = snapshots_by_day[day_key]
        if cursor == today:
            cumulative_actual = current_watched_seconds
        if cumulative_actual is not None:
            delta_hours = (cumulative_actual - cumulative_target) / 3600
            if delta_hours > 0.05:
                state = "early"
            elif delta_hours < -0.05:
                state = "behind"
            else:
                state = "active"
            rows.append(
                {
                    "date": day_key,
                    "short_date": f"{cursor.month}/{cursor.day}",
                    "delta_hours": round(delta_hours, 1),
                    "value_label": f"{delta_hours:+.1f}",
                    "state": state,
                    "is_today": cursor == today,
                    "accessible_label": (
                        f"{cursor.month} 月 {cursor.day} 日，"
                        f"{'領先' if delta_hours >= 0 else '落後'} {abs(delta_hours):.1f} 小時"
                    ),
                }
            )
        cursor += timedelta(days=1)

    display_rows = rows[-max(2, min(int(limit or 14), 31)) :]
    if not display_rows:
        return {
            "rows": [],
            "points": "",
            "range_label": "尚無每日紀錄",
            "trend_label": "開始記錄影片進度後會顯示曲線。",
            "latest_label": "+0.0h",
            "latest_state": "active",
            "scale_label": "1.0h",
        }

    max_absolute = max(1.0, max(abs(float(row["delta_hours"])) for row in display_rows))
    scale_hours = math.ceil(max_absolute * 2) / 2
    left, right, zero_y, amplitude = 64.0, 980.0, 78.0, 48.0
    denominator = max(1, len(display_rows) - 1)
    for index, row in enumerate(display_rows):
        x = left + ((right - left) * index / denominator)
        normalized = max(-1.0, min(1.0, float(row["delta_hours"]) / scale_hours))
        y = zero_y - normalized * amplitude
        row["x"] = round(x, 1)
        row["y"] = round(y, 1)
        row["value_y"] = round(max(16.0, y - 9) if normalized >= 0 else min(143.0, y + 15), 1)
    first = display_rows[0]
    latest = display_rows[-1]
    trend_hours = float(latest["delta_hours"]) - float(first["delta_hours"])
    if abs(trend_hours) < 0.05:
        trend_label = f"相較 {first['short_date']} 持平"
    elif trend_hours > 0:
        trend_label = f"相較 {first['short_date']} 改善 {abs(trend_hours):.1f} 小時"
    else:
        trend_label = f"相較 {first['short_date']} 惡化 {abs(trend_hours):.1f} 小時"
    return {
        "rows": display_rows,
        "points": " ".join(f"{row['x']},{row['y']}" for row in display_rows),
        "range_label": f"{first['short_date']} - {latest['short_date']}",
        "trend_label": trend_label,
        "latest_label": f"{float(latest['delta_hours']):+.1f}h",
        "latest_state": str(latest["state"]),
        "scale_label": f"{scale_hours:.1f}h",
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
