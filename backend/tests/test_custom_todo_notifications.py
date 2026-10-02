"""Account-synced custom todos reuse the durable assignment notification queue."""

import json
import os
import tempfile
import time
import unittest
from unittest.mock import patch

from sqlalchemy import select

from e3_tracker.assignments.domain.notifications import validate_preferences
from e3_tracker.assignments.persistence.notification_schema import notification_jobs as jobs
from e3_tracker.platform.application import create_app


class CustomTodoNotificationTests(unittest.TestCase):
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
        self.storage.save_web_session(
            "custom-todo-test", "student", moodle_session="test-moodle"
        )
        self.storage.store_push_subscription(
            "student",
            {
                "endpoint": "https://fcm.googleapis.com/fcm/send/custom-todo-test",
                "keys": {"p256dh": "unused-in-mocked-delivery", "auth": "unused"},
            },
        )
        self.storage.save_notification_preferences(
            "student",
            validate_preferences(
                {
                    "new_assignment": True,
                    "due_reminder": True,
                    "days_before": [3, 1],
                    "browser_enabled": True,
                    "line_enabled": False,
                }
            ),
        )
        self.now = int(time.time())

    def tearDown(self):
        self.service.stop.set()
        self.storage._engine.dispose()
        self.environment.stop()
        self.directory.cleanup()

    def rows(self):
        with self.storage._engine.connect() as conn:
            return conn.execute(select(jobs).order_by(jobs.c.created_at)).mappings().all()

    def todo(self, *, due_days=5):
        return {
            "uid": "custom|1234567890|abc123",
            "course": "自訂代辦",
            "title": "完成研究所備審",
            "due_ts": self.now + due_days * 86400,
        }

    def persist_and_schedule(self, item):
        saved = self.storage.upsert_custom_todo("student", item)
        self.storage.schedule_custom_todo_notifications("student", saved, now=self.now)
        return saved

    def test_custom_todo_is_account_persistent_for_other_devices(self):
        item = self.storage.upsert_custom_todo("student", self.todo())
        self.assertEqual(self.storage.list_custom_todos("student"), [item])
        self.assertEqual(self.storage.get_custom_todo("student", item["uid"]), item)

        updated = {**item, "title": "跨裝置更新後的待辦", "due_ts": item["due_ts"] + 3600}
        self.storage.upsert_custom_todo("student", updated)
        self.assertEqual(self.storage.list_custom_todos("student"), [updated])

        self.assertTrue(self.storage.delete_custom_todo("student", item["uid"]))
        self.assertEqual(self.storage.list_custom_todos("student"), [])

    def test_session_logins_with_same_student_number_share_todos(self):
        first = "Session-first-device"
        second = "Session-second-device"
        self.storage.save_web_session("session-a", first, moodle_session="moodle-a")
        self.storage.save_web_session("session-b", second, moodle_session="moodle-b")
        self.assertTrue(self.storage.save_student_number(first, "113550001"))
        self.assertTrue(self.storage.save_student_number(second, "113550001"))

        item = self.storage.upsert_custom_todo(first, self.todo())
        self.assertEqual(self.storage.list_custom_todos(second), [item])

        updated = {**item, "title": "手機修改後"}
        self.storage.upsert_custom_todo(second, updated)
        self.assertEqual(self.storage.get_custom_todo(first, item["uid"]), updated)

        self.assertTrue(self.storage.delete_custom_todo(second, item["uid"]))
        self.assertEqual(self.storage.list_custom_todos(first), [])

    def test_schedule_creates_future_thresholds_and_is_idempotent(self):
        item = self.todo(due_days=5)
        self.storage.upsert_custom_todo("student", item)
        self.assertEqual(
            self.storage.schedule_custom_todo_notifications(
                "student", item, now=self.now
            ),
            2,
        )
        first = self.rows()
        self.assertEqual(len(first), 2)
        payloads = [json.loads(row["payload"]) for row in first]
        self.assertEqual([payload["days"] for payload in payloads], [3, 1])
        self.assertTrue(all(payload["custom_todo"] for payload in payloads))
        self.assertTrue(all(payload["custom_uid"] == item["uid"] for payload in payloads))
        self.assertEqual(
            [int(row["retry_at"]) for row in first],
            [item["due_ts"] - 3 * 86400, item["due_ts"] - 86400],
        )

        self.storage.schedule_custom_todo_notifications("student", item, now=self.now)
        active = [row for row in self.rows() if row["state"] == "pending"]
        self.assertEqual(len(active), 2)

    def test_due_change_cancels_old_schedule_and_builds_new_one(self):
        old = self.todo(due_days=5)
        self.persist_and_schedule(old)
        updated = {**old, "due_ts": self.now + 8 * 86400}
        self.storage.upsert_custom_todo("student", updated)
        self.storage.schedule_custom_todo_notifications(
            "student", updated, now=self.now
        )
        rows = self.rows()
        self.assertEqual(len([row for row in rows if row["state"] == "pending"]), 2)
        self.assertEqual(len([row for row in rows if row["state"] == "cancelled"]), 2)

    def test_custom_due_job_delivers_without_e3_cache_membership(self):
        item = self.todo(due_days=1)
        self.persist_and_schedule(item)
        with patch.object(self.service, "deliver") as send:
            self.service.dispatch(now=self.now)
        send.assert_called_once()
        row = self.rows()[0]
        self.assertEqual(row["state"], "sent")
        self.assertTrue(json.loads(row["payload"])["custom_todo"])

    def test_deleted_todo_is_cancelled_before_delivery(self):
        item = self.todo(due_days=1)
        self.persist_and_schedule(item)
        self.storage.delete_custom_todo("student", item["uid"])
        with patch.object(self.service, "deliver") as send:
            self.service.dispatch(now=self.now)
        send.assert_not_called()
        self.assertEqual(self.rows()[0]["state"], "cancelled")


if __name__ == "__main__":
    unittest.main()
