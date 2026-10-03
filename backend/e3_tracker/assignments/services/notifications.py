"""Web Push / LINE delivery and periodic E3 polling, independent of page visits."""

import json
import logging
import os
import threading
import time
import uuid
from urllib.parse import quote

import requests
from e3_tracker.assignments.domain.notifications import (
    active_assignments,
    validate_subscription,
)
from e3_tracker.assignments.services.collector import (
    current_semester_key,
    annotate_result_semesters,
)

logger = logging.getLogger(__name__)


class NoRedirectSession(requests.Session):
    def request(self, *args, **kwargs):
        kwargs["allow_redirects"] = False
        return super().request(*args, **kwargs)


class NotificationService:
    def __init__(self, storage, *, app_home_url):
        self.storage = storage
        self.home_url = app_home_url
        self.vapid_private = os.getenv("E3_PUSH_VAPID_PRIVATE_KEY", "").strip()
        self.vapid_public = os.getenv("E3_PUSH_VAPID_PUBLIC_KEY", "").strip()
        self.vapid_subject = os.getenv("E3_PUSH_VAPID_SUBJECT", "").strip()
        self.line_token = os.getenv("E3_LINE_CHANNEL_ACCESS_TOKEN", "").strip()
        self.line_secret = os.getenv("E3_LINE_CHANNEL_SECRET", "").strip()
        self.line_bot_id = os.getenv("E3_LINE_BOT_BASIC_ID", "").strip()
        self.stop = threading.Event()

    @property
    def browser_ready(self):
        return bool(self.vapid_private and self.vapid_public and self.vapid_subject)

    @property
    def line_ready(self):
        return bool(self.line_token and self.line_secret and self.line_bot_id)

    def capabilities(self):
        return {
            "browser_ready": self.browser_ready,
            "vapid_public_key": self.vapid_public if self.browser_ready else "",
            "line_ready": self.line_ready,
            "line_friend_url": (
                f"https://line.me/R/ti/p/{quote(self.line_bot_id, safe='@')}"
                if self.line_ready
                else ""
            ),
        }

    def observe(self, username, result, *, baseline=False):
        annotate_result_semesters(result)
        self.storage.observe_notification_assignments(
            username, result, current_semester_key(), baseline=baseline
        )

    def line_request(self, method, payload, retry_key=None):
        headers = {"Authorization": f"Bearer {self.line_token}"}
        if retry_key:
            headers["X-Line-Retry-Key"] = retry_key
        response = requests.post(
            f"https://api.line.me/v2/bot/message/{method}",
            json=payload,
            headers=headers,
            timeout=10,
            allow_redirects=False,
        )
        if (
            retry_key
            and response.status_code == 409
            and response.headers.get("x-line-accepted-request-id")
        ):
            return
        response.raise_for_status()

    def reply_linked(self, reply_token):
        self.line_request(
            "reply",
            {
                "replyToken": reply_token,
                "messages": [
                    {
                        "type": "text",
                        "text": "E3 帳號已綁定。請回到網站的通知設定，開啟 LINE 通知。",
                    }
                ],
            },
        )

    def deliver(self, job, payload, target):
        public = {key: payload[key] for key in ("title", "body", "url")}
        public["tag"] = job["event_key"]
        if job["channel"] == "line":
            text = f"{payload['title']}\n{payload['body']}\n{self.home_url}"
            self.line_request(
                "push",
                {"to": target, "messages": [{"type": "text", "text": text}]},
                str(uuid.UUID(job["id"][:32])),
            )
        else:
            from pywebpush import webpush

            with NoRedirectSession() as transport:
                webpush(
                    validate_subscription(target),
                    data=json.dumps(public, ensure_ascii=False),
                    vapid_private_key=self.vapid_private,
                    vapid_claims={"sub": self.vapid_subject},
                    ttl=max(1, min(3600, int(job["expires_at"] - time.time()))),
                    timeout=10,
                    requests_session=transport,
                )

    def send_test(self, username, channel, endpoint_hash=""):
        if not (self.browser_ready if channel == "browser" else self.line_ready):
            raise ValueError("通知服務尚未啟用")
        target = self.storage.notification_test_target(username, channel, endpoint_hash)
        if not target:
            raise ValueError("請先啟用此裝置或綁定 LINE")
        identifier = uuid.uuid4().hex
        try:
            self.deliver(
                {
                    "id": identifier,
                    "event_key": f"test:{identifier}",
                    "channel": channel,
                    "expires_at": time.time() + 600,
                },
                {"title": "E3 通知測試", "body": "這是你的通知測試訊息。", "url": "/"},
                target,
            )
        except Exception:
            raise RuntimeError("notification_test_failed") from None
        return f"test:{identifier}"

    def dispatch(self, *, now=None):
        now = time.time() if now is None else now
        for job in self.storage.claim_notification_jobs(now):
            username = None
            try:
                delivery = self.storage.notification_delivery(job)
                if not delivery:
                    self.storage.finish_notification_job(job, "cancelled", now=now)
                    continue
                username, prefs, payload, target = delivery
                ready = (
                    self.browser_ready
                    if job["channel"] == "browser"
                    else self.line_ready
                )
                if not ready:
                    self.storage.finish_notification_job(
                        job, "pending", "not_configured", now=now
                    )
                    continue
                cache = self.storage.load_user_cache(username) or {}
                result = cache.get("result") or {}
                annotate_result_semesters(result)
                items = active_assignments(
                    result,
                    current_semester_key(),
                    self.storage.assignment_uid,
                    self.storage.load_user_preferences(username).get(
                        "ignored_assignment_uids", []
                    ),
                )
                item = items.get(payload["uid_hash"])
                if not item or (
                    payload["kind"] == "due" and item.get("due_ts") != payload["due_ts"]
                ):
                    self.storage.finish_notification_job(job, "cancelled", now=now)
                    continue
                self.deliver(job, payload, target)
                self.storage.finish_notification_job(job, "sent", now=now)
            except Exception as exc:
                response = getattr(exc, "response", None)
                status = (
                    response.status_code
                    if response is not None
                    else getattr(exc, "status_code", None)
                )
                if job["channel"] == "browser" and status in (404, 410):
                    if username:
                        self.storage.remove_push_subscription(
                            username, job["target_hash"]
                        )
                    self.storage.finish_notification_job(
                        job, "cancelled", "subscription_expired", now=now
                    )
                else:
                    self.storage.finish_notification_job(
                        job, "pending", "delivery_failed", now=now
                    )
                # Provider exceptions may embed tokens/endpoints; never log their text.
                logger.warning(
                    "Assignment notification delivery failed (%s)", job["channel"]
                )

    def refresh_once(self, fetch_assignments_for, save_cache):
        for username in self.storage.notification_users():
            try:
                user = self.storage.notification_sync_user(username, time.time())
                if user:
                    result, excel = fetch_assignments_for(user)
                    # Partial collections cannot authoritatively remove pending reminders.
                    if result.get("errors"):
                        self.storage.notification_sync_error(username)
                    else:
                        save_cache(username, result, excel)
            except Exception:
                self.storage.notification_sync_error(username)
                logger.warning("Assignment notification refresh failed")

    def scan_once(self):
        for username in self.storage.notification_users():
            try:
                cache = self.storage.load_user_cache(username) or {}
                if cache.get("result"):
                    self.observe(username, cache["result"])
            except Exception:
                logger.warning("Assignment notification scan will retry")
        self.dispatch()

    def tick(self, fetch_assignments_for, save_cache):
        self.refresh_once(fetch_assignments_for, save_cache)
        self.scan_once()

    def start(self, app, fetch_assignments_for, save_cache):
        if os.getenv("E3_NOTIFICATIONS_WORKER", "1").lower() in {
            "0",
            "false",
            "off",
        } or not (self.browser_ready or self.line_ready):
            return

        def run(callback):
            while not self.stop.wait(60):
                try:
                    with app.app_context():
                        callback()
                except Exception:
                    logger.warning("Assignment notification worker will retry")

        # E3 collection can be slow; do not block cached due reminders behind it.
        for name, callback in (
            ("assignment-notifications", self.scan_once),
            (
                "assignment-notification-sync",
                lambda: self.refresh_once(fetch_assignments_for, save_cache),
            ),
        ):
            threading.Thread(
                target=run, args=(callback,), name=name, daemon=True
            ).start()
