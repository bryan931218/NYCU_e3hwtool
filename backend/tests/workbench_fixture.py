"""Render synthetic assignment data without connecting to E3 or a real account."""
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flask import g, render_template
from e3_tracker.platform.application import create_app

with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {
    "E3_CACHE_DIR": folder, "E3_DATABASE_URL": "", "DATABASE_URL": "", "E3_CANONICAL_HOST": "",
    "E3_ENV": "development", "RAILWAY_ENVIRONMENT_ID": "", "RAILWAY_ENVIRONMENT_NAME": "",
    "E3_NOTIFICATIONS_WORKER": "0", "E3_YOUTUBE_AUTO_SYNC_ENABLED": "0", "E3_SESSION_PROFILE_WORKER": "0",
    "E3_GA4_MEASUREMENT_ID": "", "E3_GA4_PROPERTY_ID": "", "E3_SESSION_COOKIE_SECURE": "0",
}):
    app = create_app()
    try:
        now = int(datetime(2026, 10, 15, 12, tzinfo=timezone(timedelta(hours=8))).timestamp())
        courses, assignments = [], []
        for index in range(12):
            semester = "115-1" if index < 8 else "114-2"
            title = f"{semester} Test course {index + 1}"
            rows = [{"course_id": str(index + 1), "course_title": title, "semester_key": semester,
                     "title": f"Assignment {index + 1}-{item + 1}", "url": "https://e3p.nycu.edu.tw/mod/assign/view.php?id=1",
                     "due_ts": now + (item + 1) * 86400, "due_at": "2026-11-01 23:59",
                     "completed": item % 5 == 0, "overdue": False,
                     "participant_count": 40, "submitted_count": 10, "raw_status_text": ""}
                    for item in range(25)]
            assignments.extend(rows)
            courses.append({"id": str(index + 1), "title": title, "semester_key": semester, "assignments": rows})
        with app.test_request_context("/"):
            g.csp_nonce = "workbench-fixture"
            print(render_template("assignments/web.html", user={"username": "fixture", "is_admin": False},
                viewed_username="fixture", result={"courses": courses, "all_assignments": assignments,
                    "available_semesters": [{"key": "115-1", "label": "115 上學期", "course_count": 8},
                                            {"key": "114-2", "label": "114 下學期", "course_count": 4}]},
                preferences={"view_mode": "calendar" if "--calendar" in sys.argv else "course" if "--course" in sys.argv else "due",
                             "semester_filter": ["115-1"], "status_filter": ["pending"]},
                stats={"online": 1, "total": 10}, stats_version=0, now_ts=now,
                announcements=[], guest_mode=False, is_admin_view=False, admin_view_options=[],
                cache_ts=now, last_updated_label="2026-10-15 12:00", excel_data=None,
                google_ready=False, google_linked=False, app_home_url="/", support_email="test@example.invalid"))
    finally:
        app.extensions["e3_storage"]._engine.dispose()
