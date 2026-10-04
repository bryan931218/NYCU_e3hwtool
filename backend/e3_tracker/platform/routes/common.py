"""Shared HTTP, session, telemetry and deployment health endpoints."""

import os
from urllib.parse import urlsplit, urlunsplit
from flask import redirect, render_template_string, request


def register_platform_routes(
    *,
    app,
    canonical_host,
    study_health,
    usage_stats,
    current_stats_version,
    PRIVACY_TEMPLATE,
    TERMS_TEMPLATE,
    app_home_url,
    support_email,
    google_scope,
    legal_entity_name,
    legal_effective_date,
    login_required,
    record_ui_event,
    current_user
):
    @app.before_request
    def enforce_canonical_host():
        if not canonical_host or request.path == "/healthz":
            return
        host = request.host
        normalized_host = host.lower().rstrip(".")
        desired_host = canonical_host.lower().strip().rstrip("./")
        if "://" in desired_host:
            desired_host = (urlsplit(desired_host).netloc or desired_host).rstrip(".")
        proto = request.scheme
        needs_host_redirect = normalized_host != desired_host
        needs_proto_redirect = proto != "https"
        if needs_host_redirect or needs_proto_redirect:
            parts = urlsplit(request.url)
            new_url = urlunsplit(
                (
                    "https",
                    desired_host,
                    parts.path,
                    parts.query,
                    parts.fragment,
                )
            )
            return redirect(new_url, code=301)

    @app.after_request
    def add_no_store_headers(resp):
        cache_control = resp.headers.get("Cache-Control", "")
        content_disposition = resp.headers.get("Content-Disposition", "")
        if "attachment" in content_disposition.lower() or cache_control.startswith(
            "public"
        ):
            return resp
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp

    @app.route("/healthz", methods=["GET"])
    def health_check():
        return {
            "status": "ok",
            "release": str(os.getenv("RAILWAY_GIT_COMMIT_SHA") or "")[:12],
            "study_plan_youtube": study_health(),
        }, 200

    @app.route("/traffic/stats", methods=["GET"])
    def traffic_stats():
        stats = usage_stats()
        payload = {
            "version": current_stats_version(),
            "online": stats["online"],
            "total": stats["total"],
        }
        return payload, 200, {"Cache-Control": "no-store, max-age=0"}

    @app.route("/privacy", methods=["GET"])
    def privacy_policy():
        return render_template_string(
            PRIVACY_TEMPLATE,
            app_home_url=app_home_url,
            support_email=support_email,
            google_scope=google_scope,
            legal_entity_name=legal_entity_name,
            effective_date=legal_effective_date,
        )

    @app.route("/terms", methods=["GET"])
    def terms_of_service():
        return render_template_string(
            TERMS_TEMPLATE,
            app_home_url=app_home_url,
            support_email=support_email,
            google_scope=google_scope,
            legal_entity_name=legal_entity_name,
        )

    @app.post("/ui-event")
    @login_required
    def ui_event():
        payload = request.get_json(silent=True) or {}
        action = str(payload.get("action") or "").strip()
        status = str(payload.get("status") or "info").strip() or "info"
        meta = payload.get("meta")
        if not isinstance(meta, dict):
            meta = None
        elif "is_new_user" in meta:
            meta = {key: value for key, value in meta.items() if key != "is_new_user"}
        if not action:
            return {"ok": False, "error": "action required"}, 400
        if action.lower().startswith("notification_"):
            return {"ok": False, "error": "server-confirmed action required"}, 400
        record_ui_event(action, status, meta)
        return {"ok": True}

    @app.get("/session/status")
    @login_required
    def session_status():
        user = current_user()
        return {"ok": True, "username": user["username"] if user else None}
