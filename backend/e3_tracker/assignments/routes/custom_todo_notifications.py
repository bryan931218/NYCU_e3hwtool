"""Authenticated API for scheduling browser-local custom todo reminders."""

import re
import time

from flask import request


_CUSTOM_UID_RE = re.compile(r"custom\|[A-Za-z0-9._|:-]{1,240}")


def _validate_uid(raw):
    uid = str(raw or "").strip()
    if not _CUSTOM_UID_RE.fullmatch(uid):
        raise ValueError("自訂待辦識別碼格式不正確")
    return uid


def register_custom_todo_notification_routes(app, storage, account_only):
    @app.route("/api/notifications/custom-todo", methods=["POST", "DELETE"])
    @account_only
    def notification_custom_todo(username):
        raw = request.get_json(silent=True)
        if not isinstance(raw, dict):
            return {"ok": False, "message": "自訂待辦格式不正確"}, 400

        try:
            uid = _validate_uid(raw.get("uid"))
            if request.method == "DELETE":
                cancelled = storage.cancel_custom_todo_notifications(username, uid)
                return {"ok": True, "cancelled": cancelled}

            if set(raw) - {"uid", "course", "title", "due_ts"}:
                raise ValueError("自訂待辦格式不正確")
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

            scheduled = storage.schedule_custom_todo_notifications(
                username,
                {
                    "uid": uid,
                    "course": course,
                    "title": title,
                    "due_ts": due_ts,
                },
                now=now,
            )
            return {"ok": True, "scheduled": scheduled}
        except (TypeError, ValueError) as exc:
            return {"ok": False, "message": str(exc)}, 400
