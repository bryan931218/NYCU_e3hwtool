from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

_MAX_CALENDAR_RANGE_DAYS = 550


def _study_calendar_time_rows(
    storage: Any,
    *,
    start_day: str,
    end_day: str,
) -> List[Dict[str, Any]]:
    """Return total recorded learning time grouped by learning day."""
    return storage.list_study_time_daily_totals(
        start_day=start_day,
        end_day=end_day,
    )


def _register_study_calendar_routes(app: Any, storage: Any) -> None:
    from ..api.routes.study_calendar import register_study_calendar_routes
    return register_study_calendar_routes(app, storage)


def install_study_calendar_runtime(web_module: Any) -> None:
    """Use total learning time for study-home calendar heat and summaries."""

    if getattr(web_module, "__e3_study_calendar_runtime_installed", False):
        return
    web_module.__e3_study_calendar_runtime_installed = True

    root_dir = Path(__file__).resolve().parents[3]
    partial_path = root_dir / "frontend" / "templates" / "_study_calendar_time_split.html"
    if partial_path.exists():
        partial = partial_path.read_text(encoding="utf-8")
        marker = "__e3StudyCalendarTimeSplitInstalled"
        template = str(web_module.STUDY_HOME_TEMPLATE)
        if marker not in template:
            if "</body>" in template:
                web_module.STUDY_HOME_TEMPLATE = template.replace(
                    "</body>",
                    partial + "\n</body>",
                    1,
                )
            else:
                web_module.STUDY_HOME_TEMPLATE = template + "\n" + partial

    original_create_app = web_module.create_app

    def create_app_with_study_calendar(*args: Any, **kwargs: Any):
        app = original_create_app(*args, **kwargs)
        storage = app.extensions.get("e3_storage")
        if storage is not None:
            _register_study_calendar_routes(app, storage)
        return app

    web_module.create_app = create_app_with_study_calendar
