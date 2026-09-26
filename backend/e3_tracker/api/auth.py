"""Session validation shared by explicitly registered feature routes."""
from flask import session


def admin_access(storage):
    username = str(session.get("username") or "").strip()
    token = str(session.get("session_token") or "").strip()
    authenticated = bool(username and token and storage.is_valid_web_session(token, username))
    return authenticated, bool(authenticated and session.get("is_admin"))


def admin_username(storage):
    _authenticated, is_admin = admin_access(storage)
    return str(session["username"]).strip() if is_admin else None
