"""Custom todo reminders reuse the durable assignment notification queue."""

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

    def test_schedule_creates_future_thresholds_and_is_idempotent(self):
        item = self.todo(due_days=5)
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
        self.assertEqual(
            [int(row["retry_at"]) for row in first],
            [item["due_ts"] - 3 * 86400, item["due_ts"] - 86400],
        )

        self.storage.schedule_custom_todo_notifications("student", item, now=self.now)
        active = [row for row in self.rows() if row["state"] == "pending"]
        self.assertEqual(len(active), 2)

    def test_due_change_cancels_old_schedule_and_builds_new_one(self):
        old = self.todo(due_days=5)
        self.storage.schedule_custom_todo_notifications("student", old, now=self.now)
        updated = {**old, "due_ts": self.now + 8 * 86400}
        self.storage.schedule_custom_todo_notifications(
            "student", updated, now=self.now
        )
        rows = self.rows()
        self.assertEqual(len([row for row in rows if row["state"] == "pending"]), 2)
        self.assertEqual(len([row for row in rows if row["state"] == "cancelled"]), 2)

    def test_custom_due_job_delivers_without_e3_cache_membership(self):
        item = self.todo(due_days=1)
        self.storage.schedule_custom_todo_notifications("student", item, now=self.now)
        with patch.object(self.service, "deliver") as send:
            self.service.dispatch(now=self.now)
        send.assert_called_once()
        row = self.rows()[0]
        self.assertEqual(row["state"], "sent")
        self.assertTrue(json.loads(row["payload"])["custom_todo"])


if __name__ == "__main__":
    unittest.main()
