from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Optional


_INSTALL_MARKER = "__e3_unique_activity_progress_installed__"
_HISTORY_START = "1970-01-01"


def _finite_number(value: Any) -> float:
    try:
        parsed = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    return parsed if math.isfinite(parsed) else 0.0


def credit_only_new_video_progress(events: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return activity events with delta_seconds limited to lifetime-new progress.

    Video records intentionally follow the player's saved position, so rewinding,
    replaying, or reconstructing an old saved position can create a large raw
    delta even though no new course material was watched. Daily study progress
    must never credit the same part of a video twice.

    The first position seen in an event is also treated as already-known history.
    That keeps an older video from being credited merely because activity logging
    started after its progress record already existed.
    """

    ordered = sorted(
        (dict(item) for item in events),
        key=lambda item: (
            str(item.get("day") or ""),
            str(item.get("updated_at") or ""),
            int(item.get("video_id") or 0),
        ),
    )
    high_water_by_video: Dict[int, float] = {}
    credited: List[Dict[str, Any]] = []

    for item in ordered:
        try:
            video_id = int(item.get("video_id") or 0)
        except (TypeError, ValueError):
            video_id = 0
        previous = max(0.0, _finite_number(item.get("previous_watched_seconds")))
        current = max(0.0, _finite_number(item.get("watched_seconds")))
        raw_delta = _finite_number(item.get("delta_seconds"))

        historical_high = high_water_by_video.get(video_id, 0.0)
        already_credited_through = max(historical_high, previous)
        new_progress = max(0.0, current - already_credited_through)
        high_water_by_video[video_id] = max(historical_high, previous, current)

        item["raw_delta_seconds"] = raw_delta
        item["credited_delta_seconds"] = new_progress
        # Existing study-page consumers read delta_seconds. Expose the safe value
        # there so today's chart, task completion, timeline heat, and video list all
        # agree and cannot count replayed material a second time.
        item["delta_seconds"] = new_progress
        credited.append(item)

    return credited


def _within_requested_range(
    item: Dict[str, Any],
    *,
    start_day: Optional[str],
    end_day: Optional[str],
) -> bool:
    day = str(item.get("day") or "")
    if start_day and day < start_day:
        return False
    if end_day and day > end_day:
        return False
    return True


def install_unique_study_activity_progress(storage_cls: Any) -> None:
    """Make daily progress represent only never-before-credited video progress."""

    if getattr(storage_cls, _INSTALL_MARKER, False):
        return
    original = getattr(storage_cls, "list_study_plan_activity_events", None)
    if not callable(original):
        return

    def list_study_plan_activity_events(
        self,
        *,
        day: Optional[str] = None,
        start_day: Optional[str] = None,
        end_day: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        requested_start = str(day or start_day or "").strip() or None
        requested_end = str(day or end_day or "").strip() or None
        if requested_start is None and requested_end is None:
            return original(self, day=day, start_day=start_day, end_day=end_day)

        # Read earlier activity too so a video completed on a previous day forms a
        # high-water mark. The returned list is then trimmed back to the caller's
        # requested range.
        history_end = requested_end or requested_start
        history_events = original(
            self,
            start_day=_HISTORY_START,
            end_day=history_end,
        )
        safe_events = credit_only_new_video_progress(history_events)
        return [
            item
            for item in safe_events
            if _within_requested_range(
                item,
                start_day=requested_start,
                end_day=requested_end,
            )
        ]

    setattr(storage_cls, "list_study_plan_activity_events", list_study_plan_activity_events)
    setattr(storage_cls, _INSTALL_MARKER, True)
