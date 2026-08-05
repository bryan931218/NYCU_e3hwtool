from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from flask import session
from sqlalchemy import text


_MAX_CALENDAR_RANGE_DAYS = 550


def _study_calendar_time_rows(
    storage: Any,
    *,
    start_day: str,
    end_day: str,
) -> List[Dict[str, Any]]:
    """Return actual video/practice elapsed time grouped by learning day."""

    statement = text(
        "SELECT day, "
        "SUM(CASE WHEN kind = 'video' THEN elapsed_seconds ELSE 0 END) AS video_seconds, "
        "SUM(CASE WHEN kind = 'practice' THEN elapsed_seconds ELSE 0 END) AS practice_seconds, "
        "COUNT(*) AS session_count "
        "FROM study_time_sessions "
        "WHERE day >= :start_day AND day <= :end_day "
        "GROUP BY day ORDER BY day"
    )
    with storage._lock, storage._engine.connect() as conn:
        rows = conn.execute(
            statement,
            {"start_day": start_day, "end_day": end_day},
        ).mappings().all()

    result: List[Dict[str, Any]] = []
    for row in rows:
        video_seconds = max(0.0, float(row.get("video_seconds") or 0))
        practice_seconds = max(0.0, float(row.get("practice_seconds") or 0))
        result.append(
            {
                "date": str(row.get("day") or ""),
                "video_seconds": round(video_seconds, 1),
                "study_seconds": round(practice_seconds, 1),
                "total_seconds": round(video_seconds + practice_seconds, 1),
                "session_count": max(0, int(row.get("session_count") or 0)),
            }
        )
    return result


def _register_study_calendar_routes(app: Any, storage: Any) -> None:
    if "admin_study_calendar_time_summary" in app.view_functions:
        return

    def admin_study_calendar_time_summary():
        username = str(session.get("username") or "").strip()
        session_token = str(session.get("session_token") or "").strip()
        authenticated = bool(
            username
            and session_token
            and storage.is_valid_web_session(session_token, username)
        )
        if not authenticated:
            return {"ok": False, "error": "unauthorized"}, 401
        if not session.get("is_admin"):
            return {"ok": False, "error": "forbidden"}, 403

        from flask import request

        try:
            start = date.fromisoformat(str(request.args.get("start") or ""))
            end = date.fromisoformat(str(request.args.get("end") or ""))
        except ValueError:
            return {"ok": False, "error": "invalid_date"}, 400
        if end < start or (end - start).days > _MAX_CALENDAR_RANGE_DAYS:
            return {"ok": False, "error": "invalid_range"}, 400

        days = _study_calendar_time_rows(
            storage,
            start_day=start.isoformat(),
            end_day=end.isoformat(),
        )
        return {
            "ok": True,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "days": days,
        }

    app.add_url_rule(
        "/admin/study-calendar/time-summary.json",
        endpoint="admin_study_calendar_time_summary",
        view_func=admin_study_calendar_time_summary,
        methods=["GET"],
    )


def install_study_calendar_runtime(web_module: Any) -> None:
    """Add actual video/study time to the study-home calendar."""

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
