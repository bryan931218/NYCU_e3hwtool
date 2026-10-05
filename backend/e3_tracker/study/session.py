"""Keep study authentication separate from the E3 credential session."""

import secrets
from datetime import datetime, timedelta, timezone

from flask import redirect, render_template, request, session, url_for
from flask.sessions import SecureCookieSessionInterface, SessionInterface
from itsdangerous import BadSignature
from flask_wtf.csrf import generate_csrf


STUDY_SESSION_LIFETIME = timedelta(days=30)


def is_study_request(path):
    return path.startswith("/admin/study-") or path == "/study-progress" or path.startswith("/study-progress/")


class StudyCookieSessionInterface(SecureCookieSessionInterface):
    salt = "e3-study-cookie-session"

    def get_cookie_name(self, app):
        return "__Host-e3_study_session" if app.config["SESSION_COOKIE_SECURE"] else "e3_study_session"

    def get_cookie_domain(self, app):
        return None

    def get_cookie_path(self, app):
        return "/"

    def get_expiration_time(self, app, browser_session):
        if browser_session.permanent:
            return datetime.now(timezone.utc) + STUDY_SESSION_LIFETIME
        return None

    def open_session(self, app, request):
        serializer = self.get_signing_serializer(app)
        value = request.cookies.get(self.get_cookie_name(app))
        if value and serializer:
            try:
                return self.session_class(serializer.loads(value, max_age=int(STUDY_SESSION_LIFETIME.total_seconds())))
            except BadSignature:
                pass
        return self.session_class()


class SiteSessionInterface(SessionInterface):
    def __init__(self, storage):
        self.storage = storage
        self.assignment = SecureCookieSessionInterface()
        self.study = StudyCookieSessionInterface()

    def assignment_admin(self, app, request):
        browser_session = self.assignment.open_session(app, request)
        user = self.storage.load_web_session(browser_session.get("session_token"))
        if user and user.get("is_admin") and not user.get("is_guest"):
            return user, browser_session
        return None, browser_session

    def start_study_session(self, browser_session, user, legacy_session):
        token = secrets.token_urlsafe(24)
        self.storage.save_web_session(
            token, user["username"], is_admin=True,
            lifetime=int(STUDY_SESSION_LIFETIME.total_seconds()),
        )
        browser_session.clear()
        browser_session["session_token"] = token
        # Preserve the initial CSRF seed for an in-flight legacy study POST.
        if legacy_session.get("csrf_token"):
            browser_session["csrf_token"] = legacy_session["csrf_token"]
        browser_session.permanent = True

    def open_session(self, app, request):
        if not is_study_request(request.path):
            return self.assignment.open_session(app, request)
        browser_session = self.study.open_session(app, request)
        if not browser_session.get("session_token") and not browser_session.get("study_signed_out"):
            user, legacy_session = self.assignment_admin(app, request)
            if user:
                self.start_study_session(browser_session, user, legacy_session)
        return browser_session

    def save_session(self, app, browser_session, response):
        interface = self.study if is_study_request(request.path) else self.assignment
        interface.save_session(app, browser_session, response)


def configure_study_sessions(app, storage):
    interface = SiteSessionInterface(storage)
    app.session_interface = interface

    @app.before_request
    def protect_study_account_switch():
        expected_account = request.headers.get("X-Study-Account")
        if is_study_request(request.path) and expected_account and request.method not in {"GET", "HEAD", "OPTIONS"}:
            user = storage.load_web_session(session.get("session_token"))
            if not user or not user.get("is_admin"):
                return {"ok": False, "error": "study_login_required"}, 401
            if user["username"] != expected_account:
                return {"ok": False, "error": "study_account_changed"}, 403

    @app.get("/admin/study-auth/state")
    def study_auth_state():
        user = storage.load_web_session(session.get("session_token"))
        if not user or not user.get("is_admin"):
            return {"ok": False, "error": "study_login_required"}, 401
        return {"ok": True, "username": user["username"], "csrf_token": generate_csrf()}

    @app.get("/admin/study-auth/login")
    def study_login():
        user = storage.load_web_session(session.get("session_token"))
        if user and user.get("is_admin"):
            return redirect(url_for("admin_study_plan"))
        assignment_admin, _ = interface.assignment_admin(app, request)
        return render_template("study/study_login.html", can_resume=bool(assignment_admin))

    @app.post("/admin/study-auth/resume")
    def study_resume():
        user, legacy_session = interface.assignment_admin(app, request)
        if not user:
            return redirect(url_for("study_login"))
        old_token = session.get("session_token")
        if old_token:
            storage.clear_web_session(old_token)
        interface.start_study_session(session, user, legacy_session)
        return redirect(url_for("admin_study_plan"))

    @app.post("/admin/study-auth/logout")
    def study_logout():
        storage.clear_web_session(session.get("session_token"))
        session.clear()
        session["study_signed_out"] = True
        session.permanent = True
        return redirect(url_for("study_login"))
