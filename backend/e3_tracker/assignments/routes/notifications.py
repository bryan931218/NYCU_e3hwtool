"""Own-account notification settings and signed LINE webhook."""

import base64
import hashlib
import hmac
import json
import re
from functools import wraps
from flask import request, render_template, send_from_directory
from e3_tracker.platform.paths import FRONTEND_ROOT
from e3_tracker.platform.guest_privacy import is_guest_identity
from e3_tracker.assignments.domain.notification_activity import (
    NOTIFICATION_ACTION_LABELS,
    notification_setting_changes,
)
from e3_tracker.assignments.domain.notifications import (
    validate_preferences,
    validate_subscription,
    digest,
)
from e3_tracker.assignments.routes.custom_todo_notifications import (
    register_custom_todo_notification_routes,
)


def register_notification_routes(app, storage, current_user, login_required, service, record_activity):
    def activity(username, action, detail="", status="success"):
        if not username or is_guest_identity(username) or action not in NOTIFICATION_ACTION_LABELS:
            return
        try:
            record_activity(action, status=status, metadata={
                "username": username, "site": "assignments", "action_detail": detail,
            })
        except Exception:
            app.logger.warning("Notification activity unavailable")

    def account_only(fn):
        @login_required
        @wraps(fn)
        def wrapper(*args, **kwargs):
            user = current_user()
            if user.get("is_guest"):
                return {"ok": False, "message": "請登入 E3 帳號後設定通知"}, 403
            return fn(user["username"], *args, **kwargs)
        return wrapper

    def status(username):
        return {"ok": True, **storage.notification_preferences(username), **service.capabilities()}

    def reschedule_custom_todos(username):
        for item in storage.list_custom_todos(username):
            storage.schedule_custom_todo_notifications(username, item)

    register_custom_todo_notification_routes(app, storage, account_only)

    @app.get("/settings/notifications")
    @account_only
    def notification_settings_page(username):
        try:
            storage.record_assignment_usage(username, "usage_notification_settings")
        except Exception:
            app.logger.warning("Assignment usage aggregate unavailable")
        return render_template("assignments/settings/notifications.html", notification_config=status(username))

    @app.route("/api/notifications/settings", methods=["GET", "POST"])
    @account_only
    def notification_settings_api(username):
        if request.method == "POST":
            try:
                prefs = validate_preferences(request.get_json(silent=True))
                previous = storage.notification_preferences(username)
                if prefs["browser_enabled"] and not service.browser_ready:
                    raise ValueError("瀏覽器推播服務尚未啟用")
                if prefs["browser_enabled"] and not previous["browser_devices"]:
                    raise ValueError("請先啟用至少一個瀏覽器裝置")
                if prefs["line_enabled"] and (
                    not service.line_ready or not previous["line_linked"]
                ):
                    raise ValueError("請先綁定 LINE")
                storage.save_notification_preferences(username, prefs)
                changes = notification_setting_changes(previous["preferences"], prefs)
                if changes:
                    activity(username, "notification_settings_updated", changes)
                cache = storage.load_user_cache(username) or {}
                if cache.get("result"):
                    service.observe(username, cache["result"], baseline=True)
                reschedule_custom_todos(username)
            except ValueError as exc:
                return {"ok": False, "message": str(exc)}, 400
        return status(username)

    @app.route("/api/notifications/browser", methods=["POST", "DELETE"])
    @account_only
    def notification_browser_api(username):
        try:
            if request.method == "POST":
                if not service.browser_ready:
                    return {"ok": False, "message": "瀏覽器推播服務尚未啟用"}, 503
                subscription = validate_subscription(request.get_json(silent=True))
                existing = storage.notification_preferences(username)["browser_endpoint_hashes"]
                storage.store_push_subscription(username, subscription)
                if digest(subscription["endpoint"]) not in existing:
                    activity(username, "notification_browser_enabled")
                reschedule_custom_todos(username)
            else:
                raw = request.get_json(silent=True) or {}
                endpoint = raw.get("endpoint", "")
                if not isinstance(endpoint, str):
                    raise ValueError("訂閱格式不正確")
                if storage.remove_push_subscription(username, digest(endpoint)):
                    activity(username, "notification_browser_disabled")
        except ValueError as exc:
            return {"ok": False, "message": str(exc)}, 400
        return status(username)

    @app.post("/api/notifications/test")
    @account_only
    def notification_test(username):
        raw = request.get_json(silent=True)
        if not isinstance(raw, dict) or set(raw) - {"channel", "endpoint_hash"}:
            return {"ok": False, "message": "測試格式不正確"}, 400
        channel = raw.get("channel")
        endpoint_hash = raw.get("endpoint_hash", "")
        if channel not in ("browser", "line") or not isinstance(endpoint_hash, str) or (
            channel == "browser" and not re.fullmatch(r"[0-9a-f]{64}", endpoint_hash)
        ):
            return {"ok": False, "message": "請選擇有效的通知方式"}, 400
        if not storage.consume_security_limit(f"notification-test:{username}", 10, 600):
            return {"ok": False, "message": "測試次數已達上限，請 10 分鐘後再試"}, 429
        try:
            test_tag = service.send_test(username, channel, endpoint_hash)
        except ValueError as exc:
            return {"ok": False, "message": str(exc)}, 400
        except Exception:
            app.logger.warning("Notification test delivery unavailable (%s)", channel)
            activity(username, "notification_test_failed", "瀏覽器推播" if channel == "browser" else "LINE", "error")
            return {"ok": False, "message": "傳送失敗，請確認服務設定後再試"}, 503
        activity(username, "notification_test_sent", ("瀏覽器推播" if channel == "browser" else "LINE") + "：已交給推播服務，非裝置收件確認")
        return {"ok": True, "message": "測試通知已交給推播服務，請確認是否收到", **({"test_tag": test_tag} if channel == "browser" else {})}

    @app.post("/api/notifications/line/link")
    @account_only
    def notification_line_link(username):
        if not service.line_ready:
            return {"ok": False, "message": "LINE 通知服務尚未啟用"}, 503
        if not storage.consume_security_limit(f"line-link:{username}", 10, 600):
            return {"ok": False, "message": "請稍後再產生綁定碼"}, 429
        code = storage.create_line_link_code(username)
        activity(username, "notification_line_link_started")
        return {"ok": True, "code": code, "expires_in": 600, "friend_url": service.capabilities()["line_friend_url"]}

    @app.delete("/api/notifications/line/link")
    @account_only
    def notification_line_unlink(username):
        if storage.unlink_line(username=username):
            activity(username, "notification_line_unlinked", "透過網站解除綁定")
        return status(username)

    @app.get("/assignment-notifications-sw.js")
    def assignment_notification_worker():
        response = send_from_directory(FRONTEND_ROOT / "assignments" / "static" / "js", "notifications-sw.js", max_age=0)
        response.headers["Cache-Control"] = "no-cache"
        return response

    @app.get("/assignment.webmanifest")
    def assignment_manifest():
        return send_from_directory(FRONTEND_ROOT / "assignments" / "static", "assignment.webmanifest", mimetype="application/manifest+json", max_age=0)

    @app.post("/api/notifications/line/webhook")
    def notification_line_webhook():
        if not service.line_ready:
            return {"ok": False}, 503
        if request.content_length and request.content_length > 65536:
            return {"ok": False}, 413
        raw = request.get_data()
        if len(raw) > 65536:
            return {"ok": False}, 413
        expected = base64.b64encode(hmac.new(service.line_secret.encode(), raw, hashlib.sha256).digest()).decode()
        if not hmac.compare_digest(expected.encode(), request.headers.get("X-Line-Signature", "").encode()):
            return {"ok": False}, 403
        try:
            payload = json.loads(raw)
            events = payload.get("events", [])
            if not isinstance(events, list) or len(events) > 20:
                raise ValueError()
            for event in events:
                source = event.get("source") or {}
                target = source.get("userId", "")
                if source.get("type") != "user" or not re.fullmatch(r"U[0-9a-f]{32}", target):
                    continue
                if event.get("type") == "unfollow":
                    owner = storage.unlink_line(target_hash=digest(target))
                    if owner:
                        activity(owner, "notification_line_unlinked", "透過 LINE 取消追蹤")
                message = event.get("message") or {}
                if event.get("type") != "message" or message.get("type") != "text":
                    continue
                match = re.fullmatch(r"E3\s+([A-Za-z0-9_-]{24})", str(message.get("text", "")).strip())
                owner = storage.consume_line_link_code(match[1], target, return_username=True) if match else None
                if owner:
                    activity(owner, "notification_line_linked")
                    if event.get("replyToken"):
                        try:
                            service.reply_linked(event["replyToken"])
                        except Exception:
                            app.logger.warning("LINE binding reply unavailable")
        except (ValueError, TypeError, AttributeError):
            return {"ok": False}, 400
        return {"ok": True}

    app.extensions["csrf"].exempt(notification_line_webhook)
