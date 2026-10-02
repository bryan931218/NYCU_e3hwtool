"""Authenticated CRUD API for account-synced custom todos and reminders."""

import re
import time

from flask import request


_CUSTOM_UID_RE = re.compile(r"custom\|[A-Za-z0-9._|:-]{1,240}")


def _validate_uid(raw):
    uid = str(raw or "").strip()
    if not _CUSTOM_UID_RE.fullmatch(uid):
        raise ValueError("自訂待辦識別碼格式不正確")
    return uid


def _validate_item(raw):
    if not isinstance(raw, dict) or set(raw) - {"uid", "course", "title", "due_ts"}:
        raise ValueError("自訂待辦格式不正確")
    uid = _validate_uid(raw.get("uid"))
    course = str(raw.get("course") or "").strip() or "自訂代辦"
    title = str(raw.get("title") or "").strip()
    if not title or len(title) > 300 or len(course) > 200:
        raise ValueError("請輸入有效的自訂待辦名稱")
    due_ts = raw.get("due_ts")
    if type(due_ts) not in (int, float):
        raise ValueError("截止時間格式不正確")
    due_ts = int(due_ts)
    now = int(time.time())
    if due_ts <= 0 or due_ts > now + 10 * 365 * 86400:
        raise ValueError("截止時間超出可設定範圍")
    return {"uid": uid, "course": course, "title": title, "due_ts": due_ts}


def register_custom_todo_notification_routes(app, storage, account_only):
    @app.route("/api/custom-todos", methods=["GET", "POST", "DELETE"])
    @account_only
    def custom_todos_api(username):
        if request.method == "GET":
            return {"ok": True, "items": storage.list_custom_todos(username)}

        raw = request.get_json(silent=True)
        try:
            if request.method == "DELETE":
                if not isinstance(raw, dict) or set(raw) != {"uid"}:
                    raise ValueError("自訂待辦格式不正確")
                uid = _validate_uid(raw.get("uid"))
                deleted = storage.delete_custom_todo(username, uid)
                cancelled = storage.cancel_custom_todo_notifications(username, uid)
                return {"ok": True, "deleted": deleted, "cancelled": cancelled}

            item = _validate_item(raw)
            item = storage.upsert_custom_todo(username, item)
            scheduled = storage.schedule_custom_todo_notifications(
                username, item, now=int(time.time())
            )
            return {"ok": True, "item": item, "scheduled": scheduled}
        except (TypeError, ValueError) as exc:
            return {"ok": False, "message": str(exc)}, 400
