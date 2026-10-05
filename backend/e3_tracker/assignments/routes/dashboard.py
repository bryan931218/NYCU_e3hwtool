"""E3 assignment dashboard entry route."""

import posixpath

from flask import abort, render_template, render_template_string, request, send_file
from e3_tracker.assignments.services.google_calendar import GOOGLE_CALENDAR_SCOPE
from e3_tracker.assignments.services.e3_navigation_extension import (
    EXTENSION_FILES, build_extension_archive, extension_details, official_store_url,
)


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
    private_extension_assets = {
        ('assignments/e3-navigation-extension/' + name).casefold()
        for name in EXTENSION_FILES
        if name != 'guide.css' and not name.startswith('icons/')
    }

    def is_private_extension_request():
        filename = (request.view_args or {}).get('filename', '')
        normalized = posixpath.normpath(filename.replace('\\', '/')).casefold()
        return request.endpoint in {'e3_navigation_extension', 'e3_navigation_extension_download'} or (
            request.endpoint == 'frontend_asset' and normalized in private_extension_assets
        )

    @app.before_request
    def restrict_e3_navigation_installation():
        if not is_private_extension_request():
            return
        user = current_user()
        if not user or not user.get('is_admin') or user.get('is_guest'):
            abort(403)

    @app.after_request
    def private_e3_navigation_response(response):
        if is_private_extension_request():
            response.headers['Cache-Control'] = 'private, no-store'
        return response

    @app.get("/e3-auto-navigation")
    def e3_navigation_extension():
        return render_template("assignments/pages/e3_navigation_extension.html",
            extension=extension_details(),
            chrome_store_url=official_store_url(app.config.get("E3_CHROME_EXTENSION_URL"), "chrome"),
            edge_store_url=official_store_url(app.config.get("E3_EDGE_EXTENSION_URL"), "edge"))

    @app.get("/e3-auto-navigation/privacy")
    def e3_navigation_extension_privacy():
        user = current_user()
        return render_template("assignments/pages/e3_navigation_extension_privacy.html",
                               support_email=support_email,
                               can_install=bool(user and user.get('is_admin') and not user.get('is_guest')))

    @app.get("/e3-auto-navigation/download")
    def e3_navigation_extension_download():
        return send_file(build_extension_archive(), mimetype="application/zip", as_attachment=True,
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
