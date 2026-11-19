"""Build one application and register each feature explicitly."""
import os
import warnings

from e3_tracker.platform import application as web
from e3_tracker.application_storage import ApplicationStorage
from e3_tracker.study.features import register_application_features
from e3_tracker.study.domain.study_rest_day_runtime import redistribute_rest_day_allocations
from e3_tracker.study.application import _study_plan_schedule_definitions


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
    return redistribute_rest_day_allocations(
        weeks,
        rest_days,
        reference_weeks=historical_reference,
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
