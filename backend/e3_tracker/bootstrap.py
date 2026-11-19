"""Build one application and register each feature explicitly."""
import os
import warnings

from e3_tracker.platform import application as web
from e3_tracker.application_storage import ApplicationStorage
from e3_tracker.study.features import register_application_features
from e3_tracker.study.domain.study_rest_day_runtime import redistribute_rest_day_allocations
from e3_tracker.study.application import (
    STUDY_PLAN_SUBJECTS,
    _study_plan_schedule_definitions,
)
from e3_tracker.study.services.study_progress import _study_plan_credited_video_seconds


def _calibrate_history_through_today(weeks, videos, replan_settings):
    """Freeze history as complete through Nov 19 without inventing a lead."""
    if str((replan_settings or {}).get("start_date") or "") != "2026-11-20":
        return weeks
    cutoff = "2026-11-19"
    historical_days = [
        day
        for week in weeks
        for day in week.get("daily_targets") or []
        if str(day.get("date") or "")[:10] <= cutoff
    ]
    watched_by_subject = {subject: 0.0 for subject in STUDY_PLAN_SUBJECTS}
    for video in videos:
        subject = str(video.get("subject") or "")
        if subject not in watched_by_subject:
            continue
        watched_by_subject[subject] += _study_plan_credited_video_seconds(
            video.get("duration_seconds"),
            video.get("watched_seconds"),
        )
    for subject, seconds in dict(
        (replan_settings or {}).get("baseline_by_subject") or {}
    ).items():
        if str(subject) in watched_by_subject:
            watched_by_subject[str(subject)] = max(0.0, float(seconds or 0))

    desired_targets = dict(watched_by_subject)

    original_targets = {subject: 0.0 for subject in STUDY_PLAN_SUBJECTS}
    for day in historical_days:
        for subject, seconds in dict(day.get("allocations") or {}).items():
            if subject in original_targets:
                original_targets[subject] += max(0.0, float(seconds or 0))
    for day in historical_days:
        allocations = dict(day.get("allocations") or {})
        calibrated = {}
        for subject, seconds in allocations.items():
            original = original_targets.get(subject, 0.0)
            target = desired_targets.get(subject, 0.0)
            if original > 0.001 and target > 0.001:
                calibrated[subject] = max(0.0, float(seconds or 0)) * target / original
        day["allocations"] = calibrated

    for week in weeks:
        subject_targets = {}
        subjects = []
        for day in week.get("daily_targets") or []:
            for subject, seconds in dict(day.get("allocations") or {}).items():
                if float(seconds or 0) <= 0.001:
                    continue
                subject_targets[subject] = subject_targets.get(subject, 0.0) + float(seconds)
                if subject not in subjects:
                    subjects.append(subject)
        week["subject_targets"] = subject_targets
        week["subjects"] = subjects
    return weeks


def _build_schedule(videos, replan_settings=None, rest_days=None):
    weeks = _study_plan_schedule_definitions(videos, replan_settings, None)
    # Rest-day redistribution for dates before a replan must keep using the
    # original plan's receiver count.  Otherwise shortening the future plan
    # retroactively changes the required hours shown on historical dates.
    historical_reference = (
        _study_plan_schedule_definitions(videos, None, None)
        if replan_settings
        else weeks
    )
    distributed = redistribute_rest_day_allocations(
        weeks,
        rest_days,
        reference_weeks=historical_reference,
    )
    return _calibrate_history_through_today(
        distributed,
        videos,
        replan_settings,
    )


def create_app(**kwargs):
    if os.getenv("RAILWAY_ENVIRONMENT") and not any(
        str(os.getenv(name) or "").strip()
        for name in ("E3_DATABASE_URL", "DATABASE_URL", "RAILWAY_VOLUME_MOUNT_PATH")
    ):
        warnings.warn(
            "Railway has no configured persistent database or volume; SQLite data is ephemeral.",
            RuntimeWarning,
            stacklevel=2,
        )
    app = web.create_app(
        storage_class=ApplicationStorage,
        schedule_builder=_build_schedule,
        **kwargs,
    )
    register_application_features(app)
    return app
