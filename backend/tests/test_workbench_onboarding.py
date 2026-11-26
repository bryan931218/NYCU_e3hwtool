import json
import os
import tempfile
import unittest
from unittest.mock import patch

from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from e3_tracker.platform.persistence.migrations import run_migrations
from e3_tracker.platform.application import create_app
from tests.security_helpers import csrf_client


class WorkbenchOnboardingTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(os.environ, {
            "E3_ENV": "development", "RAILWAY_ENVIRONMENT_ID": "", "RAILWAY_ENVIRONMENT_NAME": "",
            "RAILWAY_ENVIRONMENT": "", "E3_CACHE_DIR": self.directory.name, "E3_DATABASE_URL": "", "DATABASE_URL": "",
            "E3_CANONICAL_HOST": "", "E3_SESSION_COOKIE_SECURE": "0", "E3_SESSION_PROFILE_WORKER": "0",
            "E3_NOTIFICATIONS_WORKER": "0", "E3_YOUTUBE_AUTO_SYNC_ENABLED": "0", "E3_SERVER_SSO_COUNTS": "",
        })
        self.environment.start()
        self.app = create_app()
        self.storage = self.app.extensions["e3_storage"]
        self.first = self.client_for("first", "student-a")
        self.second = self.client_for("second", "student-b")

    def client_for(self, token, user, admin=False):
        self.storage.save_web_session(token, user, is_admin=admin)
        self.storage.save_user_cache(user, {"ts": 1, "result": {"courses": [], "all_assignments": [], "errors": []}})
        client = csrf_client(self.app)
        with client.session_transaction() as browser:
            browser["session_token"] = token
        return client

    def config(self, client, path="/"):
        page = BeautifulSoup(client.get(path).get_data(as_text=True), "html.parser")
        return page, json.loads(page.select_one("#workbench-config").string)

    def tearDown(self):
        self.storage._engine.dispose()
        self.environment.stop()
        self.directory.cleanup()

    def test_completion_is_account_scoped_durable_and_preserves_other_preferences(self):
        page, before = self.config(self.first)
        self.assertIsNotNone(page.select_one("#workbenchTour"))
        self.assertIsNotNone(page.select_one("#onboardingReplay"))
        self.assertFalse(before["preferences"]["onboarding_completed"])
        self.assertNotIn("student-a", before["onboardingKey"])
        response = self.first.post("/preferences", json={"onboarding_completed": True, "viewMode": "calendar"})
        self.assertEqual(response.status_code, 200)
        _, another = self.config(self.second)
        self.assertFalse(another["preferences"]["onboarding_completed"])
        self.assertNotEqual(before["onboardingKey"], another["onboardingKey"])
        _, later = self.config(self.client_for("another-device", "student-a"))
        self.assertTrue(later["preferences"]["onboarding_completed"])
        self.assertEqual(later["preferences"]["view_mode"], "calendar")
        self.assertEqual(later["onboardingKey"], before["onboardingKey"])
        self.first.post("/preferences", json={"statusFilters": ["pending", "graded"]})
        self.assertTrue(self.storage.load_user_preferences("student-a")["onboarding_completed"])
        self.storage.save_user_preferences("student-a", {"view_mode": "due", "onboarding_completed": False})
        self.assertTrue(self.storage.load_user_preferences("student-a")["onboarding_completed"])

    def test_invalid_completion_values_do_not_mark_the_guide_done(self):
        for value in [1, "true", [], {}, None]:
            response = self.first.post("/preferences", json={"onboarding_completed": value})
            self.assertFalse(response.get_json()["preferences"]["onboarding_completed"])

    def test_upgrade_adds_completion_without_changing_existing_preferences(self):
        engine = create_engine("sqlite://")
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE TABLE user_preferences (user_id INTEGER PRIMARY KEY, view_mode VARCHAR(32), status_filter VARCHAR(32), semester_filter TEXT, include_ignored_overdue INTEGER DEFAULT 0, show_overdue INTEGER DEFAULT 0, show_completed INTEGER DEFAULT 0, show_graded INTEGER DEFAULT 0, ignored_overdue_uids TEXT, updated_at VARCHAR(64))"))
                conn.execute(text("INSERT INTO user_preferences(user_id, view_mode) VALUES(1, 'calendar')"))
            self.assertIn("0020_workbench_onboarding", run_migrations(engine))
            with engine.connect() as conn:
                row = conn.execute(text("SELECT view_mode, onboarding_completed FROM user_preferences WHERE user_id=1")).one()
                self.assertEqual(tuple(row), ("calendar", 0))
            self.assertEqual(run_migrations(engine), [])
        finally:
            engine.dispose()

    def test_readonly_view_does_not_show_or_complete_someone_elses_guide(self):
        admin = self.client_for("owner", "admin", admin=True)
        page, config = self.config(admin, "/?view_user=student-a")
        self.assertIsNone(page.select_one("#workbenchTour"))
        self.assertIsNone(page.select_one("#onboardingReplay"))
        self.assertEqual(admin.post("/preferences?view_user=student-a", json={"onboarding_completed": True}).status_code, 403)
        self.assertFalse(config["preferences"]["onboarding_completed"])


if __name__ == "__main__":
    unittest.main()
