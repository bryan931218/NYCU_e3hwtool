"""Authenticated calendar summaries."""
from datetime import date
from typing import Any
from flask import request
from e3_tracker.platform.auth import admin_access

MAX_CALENDAR_RANGE_DAYS = 550


def register_study_calendar_routes(app: Any, storage: Any) -> None:
    if "admin_study_calendar_time_summary" in app.view_functions:
        return

    def admin_study_calendar_time_summary():
        authenticated, is_admin = admin_access(storage)
        if not authenticated:
            return {"ok": False, "error": "unauthorized"}, 401
        if not is_admin:
            return {"ok": False, "error": "forbidden"}, 403

        try:
            start = date.fromisoformat(str(request.args.get("start") or ""))
            end = date.fromisoformat(str(request.args.get("end") or ""))
        except ValueError:
            return {"ok": False, "error": "invalid_date"}, 400
        if end < start or (end - start).days > MAX_CALENDAR_RANGE_DAYS:
            return {"ok": False, "error": "invalid_range"}, 400

        days = storage.list_study_time_daily_totals(
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
