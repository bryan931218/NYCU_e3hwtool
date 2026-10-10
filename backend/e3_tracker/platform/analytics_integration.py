"""Default-enabled E3 analytics with saved opt-outs and isolated collection."""

from flask import current_app, redirect, render_template, request, session, url_for
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from .services.google_analytics import GoogleAnalyticsReports, analytics_config


PAGES = {
    "/": "首頁", "/login": "登入", "/settings/notifications": "通知設定",
    "/courses/messages": "課程訊息", "/courses/announcements": "課程公告",
    "/courses/mail": "課程信件", "/feedback": "問題回報",
}

CONSENT_COOKIE = "e3_analytics_consent"
CONSENT_MAX_AGE = 180 * 86400


def consent_choice():
    # Consent belongs to this browser, not to the login session rotated at sign-in.
    value = request.cookies.get(CONSENT_COOKIE, "")
    try:
        choice = URLSafeTimedSerializer(current_app.secret_key, salt="e3-analytics-consent").loads(value, max_age=CONSENT_MAX_AGE)
        return choice if choice in {"granted", "denied"} else ""
    except (BadSignature, SignatureExpired, TypeError):
        return ""


def register_google_analytics(app):
    reports = GoogleAnalyticsReports(analytics_config())
    app.extensions["e3_google_analytics"] = reports

    @app.context_processor
    def analytics_navigation_context():
        context = {"ga4": reports.status()}
        if request.path == "/privacy" and reports.config["measurement_id"]:
            context["analytics_preferences"] = {
                "measurement": reports.config["measurement_id"], "consent": consent_choice(),
                "default_enabled": True,
                "campaigns": [], "events": [], "path": "/privacy", "title": "隱私權政策",
                "settings": True,
            }
        return context

    @app.post("/analytics/consent")
    def analytics_consent():
        data = request.get_json(silent=True) or {}
        if not isinstance(data, dict) or data.get("choice") not in {"granted", "denied"}:
            return {"ok": False}, 400
        response = app.json.response({"ok": True})
        value = URLSafeTimedSerializer(app.secret_key, salt="e3-analytics-consent").dumps(data["choice"])
        response.set_cookie(CONSENT_COOKIE, value, max_age=CONSENT_MAX_AGE, httponly=True,
                            secure=app.config["SESSION_COOKIE_SECURE"], samesite="Lax", path="/")
        return response

    @app.get("/admin/analytics/ga4")
    def google_analytics_report():
        storage = app.extensions["e3_storage"]
        user = storage.load_web_session(session.get("session_token", ""))
        if not user or not user.get("is_admin") or user.get("is_guest"):
            return {"error": "forbidden"}, 403
        response = app.json.response(reports.get(request.args.get("days", 30, type=int)))
        response.headers["Cache-Control"] = "private, no-store"
        return response

    @app.get("/admin/ga4")
    def admin_google_analytics():
        user = app.extensions["e3_storage"].load_web_session(session.get("session_token", ""))
        if not user:
            return redirect(url_for("login"))
        if not user.get("is_admin") or user.get("is_guest"):
            return redirect(url_for("index"))
        response = app.make_response(render_template("shared/admin_google_analytics.html", admin_user=user))
        response.headers["Cache-Control"] = "private, no-store"
        return response

    @app.after_request
    def inject_google_analytics(response):
        try:
            return inject_markup(response)
        except Exception:
            app.logger.warning("Optional analytics markup unavailable")
            return response

    def inject_markup(response):
        if (request.method != "GET" or response.status_code != 200 or response.mimetype != "text/html"
                or request.path not in PAGES or request.args.get("view_user")):
            return response
        config = reports.config
        if not config["measurement_id"]:
            return response
        storage = app.extensions["e3_storage"]
        user = storage.load_web_session(session.get("session_token", ""))
        if user and user.get("is_admin"):
            return response
        page = "作業工作台" if request.path == "/" and user else PAGES[request.path]
        events = session.pop("ga4_login_events", [])
        choice = consent_choice()
        snippet = render_template("shared/components/google_analytics.html", analytics={
            "measurement": config["measurement_id"], "campaigns": config["campaigns"],
            "consent": choice, "path": request.path, "title": page,
            "default_enabled": True, "events": events if choice != "denied" else [],
        })
        html = response.get_data(as_text=True)
        position = html.lower().rfind("</body>")
        if position >= 0:
            response.set_data(html[:position] + snippet + html[position:])
        return response
