"""Only an authenticated account may hand its own E3 session to its browser."""

import os
import tempfile
import unittest
from unittest.mock import patch

from e3_tracker.platform.application import create_app
from tests.security_helpers import csrf_client


class E3NavigationSessionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(os.environ, {
            "E3_CACHE_DIR": self.directory.name, "E3_DATABASE_URL": "",
            "E3_CANONICAL_HOST": "", "E3_SESSION_COOKIE_SECURE": "0",
            "E3_SESSION_PROFILE_WORKER": "0", "E3_NOTIFICATIONS_WORKER": "0",
            "E3_YOUTUBE_AUTO_SYNC_ENABLED": "0",
        })
        self.environment.start()
        self.app = create_app()
        self.storage = self.app.extensions["e3_storage"]
        self.client = csrf_client(self.app)
        self.username = "Session-navigation-test"
        self.credential = "synthetic-credential-1234567890"
        self.url = "https://e3p.nycu.edu.tw/mod/assign/view.php?id=42"
        self.storage.save_web_session("navigation-test", self.username, moodle_session=self.credential, is_admin=True)
        with self.client.session_transaction() as cookie:
            cookie["session_token"] = "navigation-test"
        item = {"course_id": 20, "title": "Assignment", "url": self.url}
        self.storage.save_user_cache(self.username, {"result": {
            "courses": [{"id": 20, "title": "Course", "url": "https://e3p.nycu.edu.tw/course/view.php?id=20", "assignments": [item]}],
            "all_assignments": [item],
        }})

    def tearDown(self):
        self.storage._engine.dispose()
        self.environment.stop()
        self.directory.cleanup()

    def restore(self, **changes):
        return self.client.post("/api/e3-navigation/session", json={
            "username": self.username, "url": self.url, **changes,
        })

    def test_own_session_requires_post_and_response_is_private(self):
        response = self.restore()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, {"ok": True, "session": self.credential})
        self.assertIn("private", response.headers["Cache-Control"])
        self.assertIn("no-store", response.headers["Cache-Control"])
        self.assertEqual(self.client.get("/api/e3-navigation/session").status_code, 405)
        response = self.restore(url="https://e3p.nycu.edu.tw/course/view.php?id=20")
        self.assertEqual(response.status_code, 200)

    def test_destinations_must_belong_to_the_current_account(self):
        for url in (
            "https://attacker.example/mod/assign/view.php?id=42",
            "https://e3p.nycu.edu.tw.attacker.example/mod/assign/view.php?id=42",
            "https://user@e3p.nycu.edu.tw/mod/assign/view.php?id=42",
            "https://e3p.nycu.edu.tw:444/mod/assign/view.php?id=42",
            "https://e3p.nycu.edu.tw/login/logout.php?id=42",
            self.url + "&id=43", self.url + "\n",
        ):
            with self.subTest(url=url):
                self.assertEqual(self.restore(url=url).status_code, 400)
        self.assertEqual(self.restore(url=self.url.replace("42", "43")).status_code, 403)
        self.assertEqual(self.restore(username="another-user").status_code, 409)

    def test_normal_users_guests_and_readonly_admins_cannot_export_credentials(self):
        for flags, path in (
            ({}, "/api/e3-navigation/session"),
            ({"is_guest": True}, "/api/e3-navigation/session"),
            ({"is_admin": True}, "/api/e3-navigation/session?view_user=another-user"),
        ):
            self.storage.save_web_session("navigation-test", self.username, moodle_session=self.credential, **flags)
            response = self.client.post(path, json={"username": self.username, "url": self.url})
            self.assertEqual(response.status_code, 403)
            self.assertNotIn(self.credential, response.get_data(as_text=True))

    def test_expired_web_sessions_missing_credentials_and_stale_pages_fail_closed(self):
        self.storage.save_web_session("navigation-test", self.username, lifetime=-1)
        self.assertEqual(self.restore().status_code, 302)
        for credential in (None, "invalid\r\nvalue", "short"):
            self.storage.save_web_session("navigation-test", self.username, moodle_session=credential, is_admin=True)
            with self.client.session_transaction() as cookie:
                cookie["session_token"] = "navigation-test"
            self.assertEqual(self.restore().status_code, 409)

    def test_csrf_and_cross_origin_requests_cannot_read_session(self):
        self.assertEqual(self.app.test_client().post("/api/e3-navigation/session", json={}).status_code, 400)
        response = self.client.post("/api/e3-navigation/session", json={"username": self.username, "url": self.url},
                                    headers={"Origin": "https://attacker.example"})
        self.assertEqual(response.status_code, 403)
        self.assertNotIn(self.credential, response.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()
