"""E3 assignment dashboard entry route."""

import io
from zipfile import ZipFile, ZIP_DEFLATED

from flask import render_template, render_template_string, send_file
from e3_tracker.assignments.services.google_calendar import GOOGLE_CALENDAR_SCOPE
from e3_tracker.platform.paths import FRONTEND_ROOT

EXTENSION_FILES = ("manifest.json", "background.js", "core.js", "tracker.js", "probe.js", "e3-page.js", "README.html")


def register_dashboard_routes(
    *,
    app,
    current_user,
    HOME_TEMPLATE,
    WEB_TEMPLATE,
    _build_dashboard_context,
    usage_stats,
    current_stats_version,
    app_home_url,
    support_email
):
    @app.get("/e3-auto-navigation")
    def e3_navigation_extension():
        return render_template("assignments/pages/e3_navigation_extension.html")

    @app.get("/e3-auto-navigation/download")
    def e3_navigation_extension_download():
        source = FRONTEND_ROOT / "assignments" / "static" / "e3-navigation-extension"
        archive = io.BytesIO()
        with ZipFile(archive, "w", compression=ZIP_DEFLATED) as package:
            for filename in EXTENSION_FILES:
                package.write(source / filename, arcname=f"e3-auto-navigation/{filename}")
        archive.seek(0)
        return send_file(archive, mimetype="application/zip", as_attachment=True,
                         download_name="e3-auto-navigation.zip", max_age=0)

    @app.route("/", methods=["GET"])
    def index():
        user = current_user()
        if not user:
            return render_template_string(
                HOME_TEMPLATE,
                stats=usage_stats(),
                stats_version=current_stats_version(),
                google_scope=GOOGLE_CALENDAR_SCOPE,
                app_home_url=app_home_url,
                support_email=support_email,
            )
        context = _build_dashboard_context(user)
        return render_template_string(
            WEB_TEMPLATE,
            **context,
            app_home_url=app_home_url,
            support_email=support_email,
        )
