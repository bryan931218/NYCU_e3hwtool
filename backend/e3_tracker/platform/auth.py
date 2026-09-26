"""Session validation shared by explicitly registered feature routes."""

from flask import session


def admin_access(storage):
    token = str(session.get("session_token") or "").strip()
    user = storage.load_web_session(token)
    return bool(user), bool(user and user["is_admin"])


def admin_username(storage):
    user = storage.load_web_session(str(session.get("session_token") or ""))
    return user["username"] if user and user["is_admin"] else None
