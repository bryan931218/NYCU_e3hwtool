"""E3 assignment dashboard entry route."""

from flask import render_template_string
from e3_tracker.assignments.services.google_calendar import GOOGLE_CALENDAR_SCOPE


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
