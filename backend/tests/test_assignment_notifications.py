"""Notification rules, tenant isolation, durable delivery and signed LINE linking."""

import base64
import hashlib
import hmac
import json
import os
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from cryptography.hazmat.primitives.serialization import PrivateFormat, NoEncryption
import requests
from sqlalchemy import select, update

from e3_tracker.platform.application import create_app
from e3_tracker.platform.storage import PersistentStorage
from e3_tracker.assignments.domain.notifications import (
    validate_preferences,
    validate_subscription,
    digest,
)
from e3_tracker.assignments.services.collector import current_semester_key
from e3_tracker.assignments.persistence.notification_schema import (
    notification_jobs as jobs,
    notification_settings as settings,
    push_subscriptions,
    line_link_codes,
)


def subscription(suffix="a"):
    key = (
        ec.generate_private_key(ec.SECP256R1())
        .public_key()
        .public_bytes(Encoding.X962, PublicFormat.UncompressedPoint)
    )
    b64 = lambda value: base64.urlsafe_b64encode(value).decode().rstrip("=")
    return {
        "endpoint": f"https://fcm.googleapis.com/fcm/send/test-{suffix}",
        "keys": {"p256dh": b64(key), "auth": b64(b"0123456789abcdef")},
    }


class NotificationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(
            os.environ,
            {
                "E3_ENV": "development",
                "RAILWAY_ENVIRONMENT_ID": "",
                "RAILWAY_ENVIRONMENT_NAME": "",
                "E3_CACHE_DIR": self.directory.name,
                "E3_DATABASE_URL": "",
                "DATABASE_URL": "",
                "E3_CANONICAL_HOST": "",
                "E3_SESSION_COOKIE_SECURE": "0",
                "E3_DEV_RELOAD": "0",
                "E3_NOTIFICATIONS_WORKER": "0",
                "E3_PUSH_VAPID_PRIVATE_KEY": "",
                "E3_PUSH_VAPID_PUBLIC_KEY": "",
                "E3_LINE_CHANNEL_ACCESS_TOKEN": "",
                "E3_LINE_CHANNEL_SECRET": "",
                "E3_LINE_BOT_BASIC_ID": "",
            },
        )
        self.environment.start()
        self.app = create_app()
        self.storage = self.app.extensions["e3_storage"]
        self.service = self.app.extensions["e3_notifications"]
        self.service.vapid_private = "test-private-not-a-real-key"
        self.service.vapid_public = "test-public"
        self.service.vapid_subject = "mailto:test@example.test"
        self.service.line_token = "test-line-token"
        self.service.line_secret = "test-line-secret"
        self.service.line_bot_id = "@test-bot"
        from tests.security_helpers import csrf_client

        self.client = csrf_client(self.app)
        self.storage.save_web_session(
            "notifications-test", "student", moodle_session="test-moodle"
        )
        with self.client.session_transaction() as session:
            session["session_token"] = "notifications-test"
        self.now = int(time.time())
        self.sub = subscription()
        self.storage.store_push_subscription("student", self.sub)
        self.prefs = validate_preferences(
            {
                "new_assignment": True,
                "due_reminder": True,
                "days_before": [3, 1],
                "browser_enabled": True,
            }
        )

    def tearDown(self):
        self.service.stop.set()
        self.storage._engine.dispose()
        self.environment.stop()
        self.directory.cleanup()

    def item(self, title, *, days=5, **extra):
        return {
            "course_id": 1,
            "course_title": "1151.Test",
            "semester_key": current_semester_key(),
            "title": title,
            "url": f"https://e3p.nycu.edu.tw/mod/assign/view.php?id={digest(title)[:10]}",
            "due_ts": self.now + days * 86400,
            "completed": False,
            **extra,
        }

    def result(self, *items):
        return {
            "courses": [
                {
                    "id": 1,
                    "title": "1151.Test",
                    "semester_key": current_semester_key(),
                    "assignments": list(items),
                }
            ],
            "all_assignments": list(items),
            "errors": [],
        }

    def save(self, result):
        self.storage.save_user_cache("student", {"result": result, "ts": self.now})

    def observe(self, result, *, now=None):
        self.storage.observe_notification_assignments(
            "student", result, current_semester_key(), now=now or self.now
        )

    def job_rows(self):
        with self.storage._engine.connect() as conn:
            return conn.execute(select(jobs)).mappings().all()

    def enable(self):
        self.storage.save_notification_preferences("student", self.prefs)

    def test_first_sync_baseline_and_new_assignment_are_deduplicated(self):
        self.enable()
        self.observe(self.result(self.item("old")))
        self.assertEqual(self.job_rows(), [])
        result = self.result(self.item("old"), self.item("new"))
        self.observe(result)
        self.observe(result)
        self.assertEqual(len(self.job_rows()), 1)
        self.assertEqual(json.loads(self.job_rows()[0]["payload"])["kind"], "new")

    def test_multiple_due_thresholds_and_nearest_catchup(self):
        self.enable()
        result = self.result(self.item("due"))
        self.observe(result)
        self.observe(result, now=self.now + 2 * 86400)
        self.observe(result, now=self.now + 2 * 86400 + 60)
        self.observe(result, now=self.now + 4 * 86400)
        self.assertEqual(
            [json.loads(row["payload"])["days"] for row in self.job_rows()], [3, 1]
        )
        self.observe(result, now=self.now + 6 * 86400)
        self.assertEqual(len(self.job_rows()), 2)

    def test_completed_graded_ignored_and_archived_assignments_are_skipped(self):
        self.enable()
        self.observe(self.result())
        ignored = self.item("ignored", days=1)
        uid = self.storage.assignment_uid(
            ignored["course_id"], ignored["title"], ignored["url"]
        )
        self.storage.save_user_preferences("student", {"ignored_overdue_uids": [uid]})
        self.observe(
            self.result(
                self.item("completed", days=1, completed=True),
                self.item("graded", days=1, grade_text="85"),
                ignored,
                self.item("archive", days=1, semester_key="114-2"),
            )
        )
        self.assertEqual(self.job_rows(), [])

    def test_change_of_due_date_cancels_old_reminder_before_delivery(self):
        self.enable()
        result = self.result(self.item("due", days=1))
        self.save(result)
        self.observe(result)
        self.save(self.result(self.item("due", days=8)))
        with patch.object(self.service, "deliver") as send:
            self.service.dispatch(now=self.now)
        send.assert_not_called()
        self.assertEqual(self.job_rows()[0]["state"], "cancelled")

    def test_pending_ignored_assignment_suppresses_new_and_due_jobs(self):
        self.enable()
        self.observe(self.result())
        item = self.item("future ignored", days=1)
        uid = self.storage.assignment_uid(item["course_id"], item["title"], item["url"])
        self.storage.save_user_preferences("student", {"ignored_assignment_uids": [uid]})
        self.observe(self.result(item))
        self.assertEqual(self.job_rows(), [])

    def test_ignoring_pending_assignment_cancels_already_queued_notification(self):
        self.enable()
        result = self.result(self.item("future queued", days=1))
        self.save(result)
        self.observe(result)
        self.assertEqual(len(self.job_rows()), 1)
        item = result["all_assignments"][0]
        uid = self.storage.assignment_uid(item["course_id"], item["title"], item["url"])
        self.storage.save_user_preferences("student", {"ignored_assignment_uids": [uid]})
        with patch.object(self.service, "deliver") as send:
            self.service.dispatch(now=self.now)
        send.assert_not_called()
        self.assertEqual(self.job_rows()[0]["state"], "cancelled")

    def test_successful_delivery_is_not_repeated(self):
        self.enable()
        self.observe(self.result())
        result = self.result(self.item("new"))
        self.save(result)
        self.observe(result)
        with patch.object(self.service, "deliver") as send:
            self.service.dispatch(now=self.now)
            self.service.dispatch(now=self.now + 60)
        send.assert_called_once()
        self.assertEqual(self.job_rows()[0]["state"], "sent")

    def test_delivery_failure_retries_without_losing_job(self):
        self.enable()
        self.observe(self.result())
        result = self.result(self.item("new"))
        self.save(result)
        self.observe(result)
        with patch.object(
            self.service,
            "deliver",
            side_effect=TimeoutError("private-provider-endpoint"),
        ):
            self.service.dispatch(now=self.now)
        self.assertEqual(self.job_rows()[0]["state"], "pending")
        with patch.object(self.service, "deliver") as send:
            self.service.dispatch(now=self.now + 121)
        send.assert_called_once()
        self.assertEqual(self.job_rows()[0]["error"], None)

    def test_separate_workers_claim_each_job_once(self):
        self.enable()
        self.observe(self.result())
        self.observe(self.result(self.item("new")))
        other = PersistentStorage(str(self.storage._engine.url))
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(
                    pool.map(
                        lambda repo: repo.claim_notification_jobs(self.now),
                        [self.storage, other],
                    )
                )
            self.assertEqual(sum(len(rows) for rows in results), 1)
        finally:
            other._engine.dispose()

    def test_worker_refreshes_without_web_request_and_only_once_per_interval(self):
        self.enable()
        fetch = Mock(return_value=(self.result(), None))
        save = Mock()
        self.service.tick(fetch, save)
        self.service.tick(fetch, save)
        fetch.assert_called_once()
        save.assert_called_once()
        self.assertEqual(fetch.call_args.args[0]["moodle_session"], "test-moodle")

    def test_expired_session_still_delivers_cached_due_reminders(self):
        self.enable()
        self.save(self.result(self.item("due", days=1)))
        self.storage.clear_web_session("notifications-test")
        fetch = Mock()
        with patch.object(self.service, "deliver") as send:
            self.service.tick(fetch, Mock())
        fetch.assert_not_called()
        send.assert_called_once()
        self.assertEqual(
            self.storage.notification_preferences("student")["sync_error"],
            "session_expired",
        )

    def test_api_validates_rules_and_is_scoped_to_authenticated_account(self):
        response = self.client.post(
            "/api/notifications/settings?username=other", json=self.prefs
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(
            self.storage.notification_preferences("student")["preferences"][
                "new_assignment"
            ]
        )
        self.assertFalse(
            self.storage.notification_preferences("other")["preferences"][
                "new_assignment"
            ]
        )
        for days in ([0], [31], [True], [1.5], [], [1, 2, 3, 4, 5, 6]):
            response = self.client.post(
                "/api/notifications/settings", json={**self.prefs, "days_before": days}
            )
            self.assertEqual(response.status_code, 400)
        self.assertEqual(
            self.app.test_client()
            .post("/api/notifications/settings", json=self.prefs)
            .status_code,
            400,
        )

    def test_guests_cannot_retain_notifications_and_anonymous_requests_redirect(self):
        self.storage.save_web_session("notifications-test", "student", is_guest=True)
        self.assertEqual(
            self.client.get("/api/notifications/settings").status_code, 403
        )
        self.assertEqual(
            self.client.post("/api/notifications/browser", json=self.sub).status_code,
            403,
        )
        self.assertEqual(
            self.app.test_client().get("/settings/notifications").status_code, 302
        )

    def test_push_secrets_are_encrypted_and_other_user_cannot_delete_subscription(self):
        self.storage.remove_push_subscription("other", digest(self.sub["endpoint"]))
        with self.storage._engine.connect() as conn:
            raw = conn.execute(select(push_subscriptions.c.subscription)).scalar_one()
        self.assertTrue(raw.startswith("enc:v1:"))
        self.assertNotIn("fcm.googleapis.com", raw)
        response = self.client.get("/api/notifications/settings")
        self.assertEqual(response.json["browser_devices"], 1)
        self.assertNotIn(self.sub["keys"]["auth"], response.get_data(as_text=True))
        self.assertNotIn(
            self.service.line_token,
            self.client.get("/settings/notifications").get_data(as_text=True),
        )

    def test_web_push_blocks_ssrf_and_invalid_keys(self):
        for endpoint in (
            "https://127.0.0.1/",
            "https://fcm.googleapis.com.evil.test/",
            "http://fcm.googleapis.com/",
            "https://fcm.googleapis.com:8443/",
            "https://user:pass@fcm.googleapis.com/",
            "https://metadata.google.internal/",
        ):
            with self.assertRaises(ValueError):
                validate_subscription({**self.sub, "endpoint": endpoint})
        with self.assertRaises(ValueError):
            validate_subscription(
                {**self.sub, "keys": {"p256dh": "bad", "auth": "bad"}}
            )

    def webhook(self, events, *, signature=True):
        raw = json.dumps({"events": events}).encode()
        sig = (
            base64.b64encode(
                hmac.new(
                    self.service.line_secret.encode(), raw, hashlib.sha256
                ).digest()
            ).decode()
            if signature
            else "invalid"
        )
        return self.app.test_client().post(
            "/api/notifications/line/webhook",
            data=raw,
            content_type="application/json",
            headers={"X-Line-Signature": sig},
        )

    def line_event(self, code):
        return {
            "type": "message",
            "source": {"type": "user", "userId": "U" + "a" * 32},
            "message": {"type": "text", "text": f"E3 {code}"},
            "replyToken": "test-reply",
        }

    def test_line_link_is_signed_expiring_and_one_time_only(self):
        code = self.client.post("/api/notifications/line/link").json["code"]
        event = self.line_event(code)
        self.assertEqual(self.webhook([event], signature=False).status_code, 403)
        self.assertFalse(
            self.storage.notification_preferences("student")["line_linked"]
        )
        with patch.object(self.service, "reply_linked") as reply:
            self.assertEqual(self.webhook([event]).status_code, 200)
            self.assertEqual(self.webhook([event]).status_code, 200)
        reply.assert_called_once()
        self.assertTrue(self.storage.notification_preferences("student")["line_linked"])
        expired = self.storage.create_line_link_code("other")
        with self.storage._engine.begin() as conn:
            conn.execute(
                update(line_link_codes)
                .where(line_link_codes.c.code_hash == digest(expired))
                .values(expires_at=0)
            )
        self.assertFalse(self.storage.consume_line_link_code(expired, "U" + "b" * 32))

    def test_line_cannot_bind_another_account_and_unfollow_revokes_delivery(self):
        code = self.storage.create_line_link_code("student")
        self.assertTrue(self.storage.consume_line_link_code(code, "U" + "a" * 32))
        code = self.storage.create_line_link_code("other")
        self.assertFalse(self.storage.consume_line_link_code(code, "U" + "a" * 32))
        response = self.webhook(
            [{"type": "unfollow", "source": {"type": "user", "userId": "U" + "a" * 32}}]
        )
        self.assertEqual(response.status_code, 200)
        self.assertFalse(
            self.storage.notification_preferences("student")["line_linked"]
        )

    def test_service_worker_has_correct_scope_and_csp(self):
        response = self.client.get("/assignment-notifications-sw.js")
        self.assertEqual(response.status_code, 200)
        self.assertIn("javascript", response.content_type)
        self.assertIn("worker-src 'self'", response.headers["Content-Security-Policy"])
        response.close()
        manifest = self.client.get("/assignment.webmanifest")
        self.assertEqual(manifest.status_code, 200)
        self.assertEqual(manifest.json["display"], "standalone")
        manifest.close()

    def test_settings_page_keeps_push_test_without_local_test_button(self):
        response = self.client.get("/settings/notifications")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn('id="testBrowser"', html)
        self.assertIn('id="testLine"', html)
        self.assertIn('id="browserDiagnostic"', html)
        self.assertNotIn('id="testLocalBrowser"', html)
        self.assertNotIn("測試本機通知", html)

    def test_browser_transport_encrypts_payload_and_never_follows_redirects(self):
        private = ec.generate_private_key(ec.SECP256R1())
        self.service.vapid_private = (
            base64.urlsafe_b64encode(
                private.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())
            )
            .decode()
            .rstrip("=")
        )
        response = requests.Response()
        response.status_code = 201
        job = {
            "id": "a" * 64,
            "channel": "browser",
            "event_key": "new:test",
            "expires_at": time.time() + 600,
        }
        payload = {"title": "新作業", "body": "private-assignment-title", "url": "/"}
        with patch("requests.sessions.Session.request", return_value=response) as send:
            self.service.deliver(job, payload, self.sub)
        self.assertFalse(send.call_args.kwargs["allow_redirects"])
        self.assertEqual(send.call_args.kwargs["timeout"], 10)
        self.assertNotIn(b"private-assignment-title", send.call_args.kwargs["data"])

    def test_line_transport_has_stable_retry_key_and_fixed_destination(self):
        response = requests.Response()
        response.status_code = 200
        job = {"id": "a" * 64, "channel": "line", "event_key": "new:test"}
        payload = {"title": "新作業", "body": "HW1", "url": "/"}
        with patch("requests.post", return_value=response) as send:
            self.service.deliver(job, payload, "U" + "a" * 32)
            self.service.deliver(job, payload, "U" + "a" * 32)
        self.assertEqual(
            send.call_args.args[0], "https://api.line.me/v2/bot/message/push"
        )
        self.assertEqual(
            send.call_args_list[0].kwargs["headers"]["X-Line-Retry-Key"],
            send.call_args_list[1].kwargs["headers"]["X-Line-Retry-Key"],
        )

    def test_switching_accounts_transfers_only_verified_browser_subscription(self):
        self.storage.store_push_subscription("other", self.sub)
        self.assertEqual(
            self.storage.notification_preferences("student")["browser_devices"], 0
        )
        self.assertEqual(
            self.storage.notification_preferences("other")["browser_devices"], 1
        )
        with self.assertRaises(ValueError):
            self.storage.store_push_subscription("student", subscription())

    def test_manual_test_only_sends_to_own_device_and_preserves_preferences(self):
        before = self.storage.notification_preferences("student")
        with patch.object(self.service, "deliver") as send:
            response = self.client.post(
                "/api/notifications/test?username=other",
                json={
                    "channel": "browser",
                    "endpoint_hash": digest(self.sub["endpoint"]),
                },
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(send.call_args.args[2], self.sub)
        self.assertEqual(send.call_args.args[1]["title"], "E3 通知測試")
        self.assertRegex(response.json["test_tag"], r"^test:[0-9a-f]{32}$")
        self.assertEqual(response.json["test_tag"], send.call_args.args[0]["event_key"])
        self.assertEqual(self.storage.notification_preferences("student"), before)
        self.assertEqual(self.job_rows(), [])
        other = subscription("other")
        self.storage.store_push_subscription("other", other)
        with patch.object(self.service, "deliver") as send:
            response = self.client.post(
                "/api/notifications/test",
                json={"channel": "browser", "endpoint_hash": digest(other["endpoint"])},
            )
        self.assertEqual(response.status_code, 400)
        send.assert_not_called()

    def test_manual_line_test_requires_binding_and_is_rate_limited(self):
        code = self.storage.create_line_link_code("student")
        target = "U" + "a" * 32
        self.assertTrue(self.storage.consume_line_link_code(code, target))
        with patch.object(self.service, "deliver") as send:
            for _ in range(10):
                response = self.client.post(
                    "/api/notifications/test", json={"channel": "line"}
                )
                self.assertEqual(response.status_code, 200)
                self.assertNotIn("test_tag", response.json)
            self.assertEqual(
                self.client.post(
                    "/api/notifications/test", json={"channel": "line"}
                ).status_code,
                429,
            )
        self.assertEqual(send.call_count, 10)
        self.assertEqual(send.call_args.args[2], target)

    def test_manual_test_rejects_invalid_requests_and_hides_provider_errors(self):
        with patch.object(self.service, "deliver") as send:
            for body in (
                [],
                {},
                {"channel": []},
                {"channel": "line", "to": "other"},
                {"channel": "browser", "endpoint_hash": "bad"},
            ):
                self.assertEqual(
                    self.client.post("/api/notifications/test", json=body).status_code,
                    400,
                )
            self.assertEqual(
                self.client.post(
                    "/api/notifications/test", json={"channel": "line"}
                ).status_code,
                400,
            )
            send.assert_not_called()
        with patch.object(
            self.service, "deliver", side_effect=ValueError("private-provider-token")
        ):
            response = self.client.post(
                "/api/notifications/test",
                json={
                    "channel": "browser",
                    "endpoint_hash": digest(self.sub["endpoint"]),
                },
            )
        self.assertEqual(response.status_code, 503)
        self.assertNotIn("private-provider-token", response.get_data(as_text=True))
        self.assertEqual(
            self.app.test_client()
            .post("/api/notifications/test", json={"channel": "line"})
            .status_code,
            400,
        )
        self.storage.save_web_session("notifications-test", "student", is_guest=True)
        self.assertEqual(
            self.client.post(
                "/api/notifications/test", json={"channel": "line"}
            ).status_code,
            403,
        )

    def test_test_budget_is_shared_by_channels_and_resets_after_ten_minutes(self):
        code = self.storage.create_line_link_code("student")
        self.storage.consume_line_link_code(code, "U" + "a" * 32)
        browser = {"channel": "browser", "endpoint_hash": digest(self.sub["endpoint"])}
        with patch.object(self.service, "deliver") as send, patch(
            "e3_tracker.platform.persistence.accounts.time.time", return_value=self.now
        ):
            for index in range(10):
                body = browser if index % 2 else {"channel": "line"}
                self.assertEqual(
                    self.client.post("/api/notifications/test", json=body).status_code,
                    200,
                )
            self.assertEqual(
                self.client.post("/api/notifications/test", json=browser).status_code,
                429,
            )
            self.assertEqual(send.call_count, 10)
            self.assertTrue(
                self.storage.consume_security_limit("notification-test:other", 10, 600)
            )
        with patch.object(self.service, "deliver") as send, patch(
            "e3_tracker.platform.persistence.accounts.time.time", return_value=self.now + 601
        ):
            self.assertEqual(
                self.client.post("/api/notifications/test", json=browser).status_code,
                200,
            )
            send.assert_called_once()

    def test_unlink_revokes_pending_line_code_even_without_existing_binding(self):
        code = self.storage.create_line_link_code("student")
        self.storage.unlink_line(username="student")
        self.assertFalse(self.storage.consume_line_link_code(code, "U" + "a" * 32))

    def activities(self, action=None):
        return [event for event in self.storage.recent_traffic_events(500)
                if event["action"].startswith("notification_")
                and (action is None or event["action"] == action)]

    def test_settings_activity_records_changed_switches_and_reminder_days_only(self):
        self.assertEqual(self.client.post("/api/notifications/settings", json=self.prefs).status_code, 200)
        events = self.activities("notification_settings_updated")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["meta"]["username"], "student")
        self.assertIn("瀏覽器通知：啟用", events[0]["meta"]["action_detail"])
        self.assertIn("新作業通知：啟用", events[0]["meta"]["action_detail"])
        self.assertIn("到期前 3、1 天提醒", events[0]["meta"]["action_detail"])
        self.client.get("/api/notifications/settings")
        self.client.post("/api/notifications/settings", json=self.prefs)
        self.client.post("/api/notifications/settings", json={**self.prefs, "days_before": [99]})
        self.client.post("/api/notifications/settings", json={**self.prefs, "line_enabled": True})
        self.assertEqual(len(self.activities()), 1)
        self.client.post("/api/notifications/settings", json={**self.prefs, "browser_enabled": False})
        self.assertEqual(self.activities()[-1]["meta"]["action_detail"], "瀏覽器通知：關閉")

    def test_browser_activity_logs_new_device_and_real_removal_not_repeat_or_other_owner(self):
        sub = subscription("activity")
        self.assertEqual(self.client.post("/api/notifications/browser", json=sub).status_code, 200)
        self.client.post("/api/notifications/browser", json=sub)
        self.assertEqual(len(self.activities("notification_browser_enabled")), 1)
        other = subscription("other-activity")
        self.storage.store_push_subscription("other", other)
        self.client.delete("/api/notifications/browser", json={"endpoint": other["endpoint"]})
        self.assertEqual(len(self.activities()), 1)
        self.assertEqual(self.storage.notification_preferences("other")["browser_devices"], 1)
        self.client.delete("/api/notifications/browser", json={"endpoint": sub["endpoint"]})
        self.client.delete("/api/notifications/browser", json={"endpoint": sub["endpoint"]})
        self.assertEqual(len(self.activities("notification_browser_disabled")), 1)
        serialized = json.dumps(self.activities())
        for private in (sub["endpoint"], *sub["keys"].values(), digest(sub["endpoint"])):
            self.assertNotIn(private, serialized)

    def test_line_activity_is_signed_owned_one_time_and_does_not_store_credentials(self):
        code = self.client.post("/api/notifications/line/link").json["code"]
        self.assertEqual(len(self.activities("notification_line_link_started")), 1)
        event = self.line_event(code)
        self.webhook([event], signature=False)
        self.assertEqual(len(self.activities()), 1)
        with patch.object(self.service, "reply_linked"):
            self.webhook([event])
            self.webhook([event])
        linked = self.activities("notification_line_linked")
        self.assertEqual(len(linked), 1)
        self.assertEqual(linked[0]["meta"]["username"], "student")
        self.assertIsNone(linked[0]["ip"])
        self.client.delete("/api/notifications/line/link")
        self.client.delete("/api/notifications/line/link")
        self.assertEqual(len(self.activities("notification_line_unlinked")), 1)
        serialized = json.dumps(self.activities())
        for private in (code, digest(code), "U" + "a" * 32, digest("U" + "a" * 32), "test-reply", self.service.line_token, self.service.line_secret):
            self.assertNotIn(private, serialized)

    def test_line_unfollow_activity_records_actual_binding_owner_not_webhook_session(self):
        self.storage.save_user_profile("Session-owner", "示範", "示")
        self.storage.save_student_number("Session-owner", "113550092")
        code = self.storage.create_line_link_code("Session-owner")
        self.storage.consume_line_link_code(code, "U" + "a" * 32)
        raw = json.dumps({"events": [{"type": "unfollow", "source": {"type": "user", "userId": "U" + "a" * 32}}]}).encode()
        signature = base64.b64encode(hmac.new(self.service.line_secret.encode(), raw, hashlib.sha256).digest()).decode()
        for _ in range(2):
            self.client.post("/api/notifications/line/webhook", data=raw, content_type="application/json", headers={"X-Line-Signature": signature})
        events = self.activities("notification_line_unlinked")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["meta"]["username"], "Session-owner")
        self.assertEqual(events[0]["meta"]["action_detail"], "透過 LINE 取消追蹤")

    def test_test_activity_distinguishes_provider_acceptance_from_failure_without_error_secrets(self):
        raw = {"channel": "browser", "endpoint_hash": digest(self.sub["endpoint"])}
        with patch.object(self.service, "send_test", return_value="private-test-tag"):
            self.assertEqual(self.client.post("/api/notifications/test", json=raw).status_code, 200)
        self.assertIn("非裝置收件確認", self.activities()[-1]["meta"]["action_detail"])
        with patch.object(self.service, "send_test", side_effect=RuntimeError("private-error-token")):
            self.assertEqual(self.client.post("/api/notifications/test", json=raw).status_code, 503)
        self.assertEqual(self.activities()[-1]["status"], "error")
        self.assertNotIn("private-error-token", json.dumps(self.activities()))
        self.assertNotIn("private-test-tag", json.dumps(self.activities()))

    def test_guests_cannot_create_notification_activity_or_forge_server_actions(self):
        self.assertEqual(self.client.post("/ui-event", json={"action": "notification_line_linked"}).status_code, 400)
        self.storage.save_web_session("notifications-test", "student", is_guest=True)
        for method, url, body in [("post", "/api/notifications/settings", self.prefs),
                                  ("post", "/api/notifications/browser", self.sub),
                                  ("post", "/api/notifications/line/link", {}),
                                  ("delete", "/api/notifications/line/link", {})]:
            self.assertEqual(getattr(self.client, method)(url, json=body).status_code, 403)
        self.assertEqual(self.activities(), [])

    def test_notification_activity_is_rendered_in_chinese_after_restart(self):
        from tests.security_helpers import csrf_client
        self.client.post("/api/notifications/settings", json=self.prefs)
        self.storage.save_web_session("activity-admin", "admin", is_admin=True)
        reloaded = create_app()
        try:
            client = csrf_client(reloaded)
            with client.session_transaction() as session:
                session["session_token"] = "activity-admin"
            response = client.get("/admin/traffic")
            from bs4 import BeautifulSoup
            activity = BeautifulSoup(response.get_data(as_text=True), "html.parser").select_one(".events").get_text()
            self.assertIn("更新通知設定", activity)
            self.assertIn("瀏覽器通知：啟用", activity)
            self.assertIn("成功", activity)
            self.assertNotIn("activity_only", activity)
            self.assertNotIn("action_detail", activity)
        finally:
            reloaded.extensions["e3_notifications"].stop.set()
            reloaded.extensions["e3_storage"]._engine.dispose()


if __name__ == "__main__":
    unittest.main()
