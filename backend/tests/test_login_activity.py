"""Failed logins remain visible to admins without creating verified-user activity."""

import json
import os
import tempfile
import unittest
from unittest.mock import patch

import requests
from bs4 import BeautifulSoup

from e3_tracker.assignments.services.http import E3LoginError
from e3_tracker.platform.application import create_app
from tests.security_helpers import csrf_client


class LoginActivityTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(os.environ, {
            "E3_ENV": "development", "RAILWAY_ENVIRONMENT_ID": "", "RAILWAY_ENVIRONMENT_NAME": "",
            "E3_CACHE_DIR": self.directory.name, "E3_DATABASE_URL": "", "DATABASE_URL": "",
            "E3_CANONICAL_HOST": "", "E3_SESSION_COOKIE_SECURE": "0",
            "E3_SESSION_PROFILE_WORKER": "0", "E3_NOTIFICATIONS_WORKER": "0",
            "E3_YOUTUBE_AUTO_SYNC_ENABLED": "0",
        })
        self.environment.start()
        self.app = create_app()
        self.storage = self.app.extensions["e3_storage"]
        self.client = csrf_client(self.app)
        self.admin = csrf_client(self.app)
        self.storage.save_web_session("activity-admin-token", "activity-admin", is_admin=True)
        with self.admin.session_transaction() as session:
            session["session_token"] = "activity-admin-token"

    def tearDown(self):
        self.storage._engine.dispose()
        self.environment.stop()
        self.directory.cleanup()

    def failures(self):
        return [event for event in self.storage.recent_activity_events() if event["action"] == "login_failed"]

    def password_failure(self, error, username="test-account"):
        with patch("e3_tracker.assignments.routes.assignments.login_with_password", side_effect=error):
            return self.client.post("/login", data={
                "username": username, "password": "private-password-must-not-be-recorded",
            })

    def test_rejection_is_persistent_and_visible_in_both_admin_activity_views(self):
        error = E3LoginError("login_rejected", page="login", form=True, notice="none", trace=[
            {"method": "POST", "status": 303, "page": "home", "session_cookie": False},
            {"method": "GET", "status": 200, "page": "login", "session_cookie": False},
        ])
        self.assertEqual(self.password_failure(error).status_code, 200)
        event, = self.failures()
        self.assertEqual(event["status"], "error")
        self.assertEqual(event["meta"]["attempted_username"], "test-account")
        self.assertEqual(event["meta"]["error_code"], "login_rejected")
        self.assertEqual(event["meta"]["login_stage"], "e3_authentication")
        self.assertEqual(event["meta"]["response_trace"][0]["status"], 303)
        self.assertIsNone(event["meta"].get("username"))
        for path in ("/admin/traffic", "/admin/traffic/activity"):
            page = self.admin.get(path)
            self.assertEqual(page.status_code, 200)
            activity = BeautifulSoup(page.get_data(as_text=True), "html.parser").select_one(".events")
            text = activity.get_text()
            for expected in ("登入失敗", "test-account", "E3 未建立已驗證的登入狀態", "login_rejected", "帳密表單：有", "POST 303", "E3 頁面沒有提供錯誤提示"):
                self.assertIn(expected, text)
            self.assertIsNotNone(activity.select_one(".status-badge.error"))
            self.assertNotIn("private-password", text)
        self.assertFalse(any(row["username"] == "test-account" for row in self.storage.list_user_profiles()))
        self.assertEqual(self.client.get("/session/status").status_code, 302)

    def test_network_errors_keep_distinct_codes_without_raw_exception_data(self):
        for error, code in (
            (requests.ReadTimeout("private-exception-with-password"), "upstream_timeout"),
            (requests.exceptions.SSLError("private-credential-url"), "upstream_tls"),
            (requests.ConnectionError("private-cookie-value"), "upstream_connection"),
        ):
            with self.subTest(code=code):
                self.password_failure(error)
                self.assertEqual(self.failures()[0]["meta"]["error_code"], code)
        stored = json.dumps(self.failures())
        for secret in ("private-exception", "private-credential", "private-cookie", "private-password"):
            self.assertNotIn(secret, stored)

    def test_session_failure_never_records_the_cookie_or_a_derived_identity(self):
        credential = "private-session-cookie-1234567890"
        with patch("e3_tracker.assignments.application.collect_assignments", side_effect=requests.ReadTimeout("private-upstream-body")):
            self.assertEqual(self.client.post("/login", data={"login_type": "session", "moodle_session": credential}).status_code, 200)
        event, = self.failures()
        self.assertEqual(event["meta"]["login_method"], "session")
        self.assertEqual(event["meta"]["login_stage"], "session_validation")
        self.assertEqual(event["meta"]["error_code"], "upstream_timeout")
        stored = json.dumps(event)
        for secret in (credential, "private-upstream-body", "Session-"):
            self.assertNotIn(secret, stored)

    def test_empty_forms_and_an_invalid_csrf_have_distinct_failure_stages(self):
        for method in ("password", "session"):
            self.assertEqual(self.client.post("/login", data={"login_type": method}).status_code, 200)
        self.assertEqual(len(self.failures()), 2)
        self.assertTrue(all(event["meta"]["error_code"] == "missing_fields" for event in self.failures()))
        self.assertEqual(self.app.test_client().post("/login", data={"username": "test-account", "password": "private"}).status_code, 400)
        self.assertEqual(len(self.failures()), 3)
        self.assertEqual(self.failures()[0]["meta"]["error_code"], "csrf_validation_failed")
        self.assertEqual(self.failures()[0]["meta"]["login_stage"], "request_validation")

    def test_site_rate_limit_is_visible_without_claiming_e3_rejected_credentials(self):
        self.app.config["SECURITY_LOGIN_BROWSER_LIMIT"] = 1
        self.client.post("/login", data={"login_type": "password"})
        with patch("e3_tracker.assignments.routes.assignments.login_with_password") as upstream:
            self.assertEqual(self.client.post("/login", data={"username": "test-account", "password": "private"}).status_code, 429)
            upstream.assert_not_called()
        event = self.failures()[0]
        self.assertEqual(event["meta"]["error_code"], "login_rate_limited")
        self.assertIn("本站", event["meta"]["message"])
        self.assertNotIn("attempted_username", event["meta"])

    def test_claimed_account_is_escaped_and_failure_cannot_be_forged_by_ui_events(self):
        self.password_failure(E3LoginError("login_rejected"), username='<img src=x onerror=alert(1)>')
        html = self.admin.get("/admin/traffic/activity").get_data(as_text=True)
        self.assertNotIn('<img src=x onerror=alert(1)>', html)
        self.assertIn('&lt;img', html)
        self.assertEqual(self.admin.post("/ui-event", json={"action": "login_failed", "meta": {"message": "forged"}}).status_code, 400)
        self.assertEqual(len(self.failures()), 1)
        regular = csrf_client(self.app)
        self.storage.save_web_session("regular-token", "regular", is_admin=False)
        with regular.session_transaction() as session:
            session["session_token"] = "regular-token"
        self.assertEqual(regular.get("/admin/traffic/activity").status_code, 403)


if __name__ == "__main__":
    unittest.main()
