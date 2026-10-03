"""Calendar is a shared-data view, not a new authorization or storage boundary."""
import os
import tempfile
import unittest
from unittest.mock import patch

from bs4 import BeautifulSoup
from e3_tracker.platform.application import create_app
from tests.security_helpers import csrf_client


class AssignmentCalendarTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(os.environ, {
            "E3_ENV": "development", "RAILWAY_ENVIRONMENT_ID": "",
            "RAILWAY_ENVIRONMENT_NAME": "", "RAILWAY_ENVIRONMENT": "",
            "E3_CACHE_DIR": self.directory.name, "E3_DATABASE_URL": "",
            "DATABASE_URL": "", "E3_CANONICAL_HOST": "",
            "E3_SESSION_COOKIE_SECURE": "0", "E3_YOUTUBE_AUTO_SYNC_ENABLED": "0",
            "E3_SESSION_PROFILE_WORKER": "0", "E3_NOTIFICATIONS_WORKER": "0",
            "OPENAI_API_KEY": "",
        })
        self.environment.start()
        self.app = create_app()
        self.storage = self.app.extensions["e3_storage"]
        self.client = csrf_client(self.app)
        self.storage.save_web_session("calendar-test", "calendar-user", is_guest=True)
        with self.client.session_transaction() as browser:
            browser["session_token"] = "calendar-test"

    def tearDown(self):
        self.storage._engine.dispose()
        self.environment.stop()
        self.directory.cleanup()

    def test_calendar_preference_round_trip_and_unknown_mode_rejection(self):
        response = self.client.post("/preferences", json={"viewMode": "calendar"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["preferences"]["view_mode"], "calendar")
        self.assertEqual(self.storage.load_user_preferences("calendar-user")["view_mode"], "calendar")
        cache = self.client.get("/api/cache").get_json()
        self.assertEqual(cache["preferences"]["view_mode"], "calendar")
        response = self.client.post("/preferences", json={"viewMode": "invalid"})
        self.assertEqual(response.get_json()["preferences"]["view_mode"], "calendar")

    def test_generalized_ignore_preferences_preserve_legacy_records_and_restore(self):
        uid = "1|Future assignment|https://example.test/task"
        self.storage.save_user_preferences("calendar-user", {"ignored_overdue_uids": [uid]})
        response = self.client.post("/preferences", json={"viewMode": "calendar"})
        self.assertEqual(response.get_json()["preferences"]["ignored_assignment_uids"], [uid])
        self.assertEqual(self.storage.load_user_preferences("calendar-user")["ignored_assignment_uids"], [uid])
        response = self.client.post("/preferences", json={"ignoredAssignmentUids": [uid, uid, " another "]})
        prefs = response.get_json()["preferences"]
        self.assertEqual(prefs["ignored_assignment_uids"], [uid, "another"])
        self.assertEqual(prefs["ignored_overdue_uids"], prefs["ignored_assignment_uids"])
        self.assertEqual(self.client.get("/api/cache").get_json()["preferences"]["ignored_assignment_uids"], [uid, "another"])
        # An already-open old client can still restore ignored assignments.
        response = self.client.post("/preferences", json={"ignoredOverdueUids": []})
        self.assertEqual(response.get_json()["preferences"]["ignored_assignment_uids"], [])
        self.assertEqual(self.storage.load_user_preferences("calendar-user")["ignored_assignment_uids"], [])

    def test_calendar_shell_assets_and_controls_work_without_a_cache(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        page = BeautifulSoup(response.get_data(as_text=True), "html.parser")
        for identifier in ("viewCalendarBtn", "viewCalendar", "assignmentCalendar", "calendarDayList", "calendarAddTask"):
            self.assertIsNotNone(page.select_one(f"#{identifier}"))
        script = page.select_one("script[src*='fullcalendar-6.1.21.min.js']")
        self.assertTrue(script.get("nonce"))
        for url in (script["src"], "/assets/assignments/css/deadline-calendar.css", "/assets/assignments/js/workbench/deadline-calendar.js", "/assets/assignments/vendor/lucide-calendar/plus.svg"):
            asset = self.client.get(url)
            self.assertEqual(asset.status_code, 200, url)
            asset.close()

    def test_calendar_does_not_remove_csrf_or_anonymous_preference_checks(self):
        self.assertEqual(self.app.test_client().post("/preferences", json={"viewMode": "calendar"}).status_code, 400)
        anonymous = csrf_client(self.app)
        self.assertEqual(anonymous.post("/preferences", json={"viewMode": "calendar"}).status_code, 302)

    def test_readonly_calendar_hides_add_controls_and_rejects_preference_writes(self):
        self.storage.save_web_session("calendar-admin", "admin", is_admin=True)
        self.storage.save_user_cache("calendar-user", {"ts": 1, "result": {"courses": [], "all_assignments": [], "errors": []}})
        with self.client.session_transaction() as browser:
            browser["session_token"] = "calendar-admin"
        response = self.client.get("/?view_user=calendar-user")
        self.assertEqual(response.status_code, 200)
        page = BeautifulSoup(response.get_data(as_text=True), "html.parser")
        self.assertIsNotNone(page.select_one("#viewCalendar"))
        self.assertIsNone(page.select_one("#calendarAddTask"))
        self.assertEqual(self.client.post("/preferences?view_user=calendar-user", json={"viewMode": "calendar"}).status_code, 403)


if __name__ == "__main__":
    unittest.main()
