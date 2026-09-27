"""Shared request protections, enabled for both sites and all mutating routes."""

import os
import secrets
from flask import g, request, session
from flask_wtf.csrf import CSRFError, CSRFProtect
from werkzeug.middleware.proxy_fix import ProxyFix

from .security import production_mode


def _request_limit(name, default):
    value = int(os.getenv(name, str(default)))
    if not 1 <= value <= 100000:
        raise RuntimeError(f"{name} must be between 1 and 100000")
    return value


def configure_http_security(app, storage):
    production = production_mode()
    hops = int(os.getenv("E3_PROXY_HOPS", "0"))
    if not 0 <= hops <= 3:
        raise RuntimeError("E3_PROXY_HOPS must match 0-3 trusted reverse proxies")
    if hops:
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=hops, x_proto=hops, x_host=hops)
    if production:
        if not app.config["SESSION_COOKIE_SECURE"]:
            raise RuntimeError("Production requires secure session cookies")
        if os.getenv("E3_DEV_RELOAD", "").lower() in {"1", "true", "yes", "on"}:
            raise RuntimeError("Debug/reload is forbidden in production")
        hosts = [
            part.strip()
            for part in os.getenv("E3_TRUSTED_HOSTS", "").split(",")
            if part.strip()
        ]
        if not hosts or any(
            "*" in host or "/" in host or ":" in host for host in hosts
        ):
            raise RuntimeError("Production requires E3_TRUSTED_HOSTS")
        if not os.getenv("E3_ADMIN_USER_ID", "").strip():
            raise RuntimeError("Production requires an explicit E3_ADMIN_USER_ID")
        app.config["TRUSTED_HOSTS"] = hosts
        app.config["SESSION_COOKIE_NAME"] = "__Host-e3_session"
        if app.config["SESSION_COOKIE_SAMESITE"] != "Lax":
            raise RuntimeError(
                "Use SameSite=Lax in production for secure Google OAuth compatibility"
            )
    app.config.update(
        WTF_CSRF_TIME_LIMIT=None,
        SECURITY_REQUEST_LIMIT=_request_limit("E3_REQUESTS_PER_MINUTE", 3000),
        SECURITY_LOGIN_LIMIT=_request_limit("E3_LOGIN_IP_LIMIT", 120),
        SECURITY_LOGIN_ACCOUNT_LIMIT=_request_limit("E3_LOGIN_ACCOUNT_LIMIT", 20),
        SECURITY_LOGIN_BROWSER_LIMIT=_request_limit("E3_LOGIN_BROWSER_LIMIT", 10),
    )

    @app.before_request
    def protect_request():
        g.csp_nonce = secrets.token_urlsafe(24)
        if request.path.startswith(("/assets/", "/static/")) or request.path in {
            "/healthz",
            "/favicon.ico",
        }:
            return None
        ip = request.remote_addr or "unknown"
        if not storage.consume_security_limit(
            f"requests:{ip}", app.config["SECURITY_REQUEST_LIMIT"], 60
        ):
            return (
                {"ok": False, "error": "too_many_requests"},
                429,
                {"Retry-After": "60"},
            )
        if request.method not in {"GET", "HEAD", "OPTIONS"}:
            if not request.path.startswith("/admin/study-recall/upload"):
                request.max_content_length = min(
                    app.config["MAX_CONTENT_LENGTH"], 4 * 1024 * 1024
                )
            origin = request.headers.get("Origin")
            if origin and origin != request.host_url.rstrip("/"):
                return {"ok": False, "error": "cross_origin_request"}, 403
            if request.headers.get("Sec-Fetch-Site") == "cross-site":
                return {"ok": False, "error": "cross_site_request"}, 403
            if request.path in {"/login", "/guest-login"}:
                request.max_content_length = 16 * 1024
                limit = app.config["SECURITY_LOGIN_LIMIT"]
                if not storage.consume_security_limit(f"login:{ip}", limit, 300):
                    return (
                        {"ok": False, "error": "login_rate_limited"},
                        429,
                        {"Retry-After": "300"},
                    )
                username = (
                    str(request.form.get("username") or "").strip().casefold()[:191]
                )
                browser_token = session.get("csrf_token")
                if browser_token and not storage.consume_security_limit(
                    f"login-browser:{browser_token}",
                    app.config["SECURITY_LOGIN_BROWSER_LIMIT"],
                    300,
                ):
                    return (
                        {"ok": False, "error": "login_rate_limited"},
                        429,
                        {"Retry-After": "300"},
                    )
                if username and not storage.consume_security_limit(
                    f"login-account:{username}",
                    app.config["SECURITY_LOGIN_ACCOUNT_LIMIT"],
                    300,
                ):
                    return (
                        {"ok": False, "error": "login_rate_limited"},
                        429,
                        {"Retry-After": "300"},
                    )

    CSRFProtect(app)
    app.jinja_env.globals["csp_nonce"] = lambda: getattr(g, "csp_nonce", "")

    @app.errorhandler(CSRFError)
    def csrf_error(_error):
        return {
            "ok": False,
            "error": "csrf_validation_failed",
            "message": "Please reload the page and retry.",
        }, 400

    @app.after_request
    def security_headers(response):
        nonce = getattr(g, "csp_nonce", "")
        response.headers.setdefault(
            "Content-Security-Policy",
            (
                "default-src 'self'; "
                f"script-src 'nonce-{nonce}' 'strict-dynamic' 'self' https://cdn.jsdelivr.net https://www.youtube.com; "
                "script-src-attr 'none'; style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://fonts.googleapis.com; "
                "img-src 'self' data: blob: https://i.ytimg.com https://img.youtube.com; "
                "font-src 'self' data: https://cdn.jsdelivr.net https://fonts.gstatic.com; "
                "connect-src 'self' https://www.youtube.com https://www.youtube-nocookie.com; "
                "frame-src https://www.youtube.com https://www.youtube-nocookie.com; "
                "worker-src 'self'; media-src 'self' blob:; object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'"
            ),
        )
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = (
            "camera=(), microphone=(), geolocation=()"
        )
        response.headers["Cross-Origin-Resource-Policy"] = "same-origin"
        if production or request.is_secure:
            response.headers["Strict-Transport-Security"] = "max-age=31536000"
        # Even images/attachments returned from authenticated pages must not be public-cacheable.
        if session.get("session_token") or request.path.startswith(
            ("/admin/", "/study-progress/notes/")
        ):
            response.headers["Cache-Control"] = "private, no-store, max-age=0"
        return response
