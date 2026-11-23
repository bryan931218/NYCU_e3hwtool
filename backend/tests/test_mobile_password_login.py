"""API authentication must yield the matching real web session, not just a token."""

import os
import tempfile
import time
import unittest
from unittest.mock import patch

import requests

from e3_tracker.assignments.services.http import E3LoginError, login_failure_reason, login_with_password
from e3_tracker.assignments.services.mobile_auth import login_with_mobile_api
from e3_tracker.platform.application import create_app
from tests.security_helpers import csrf_client


BASE = "https://e3p.nycu.edu.tw"
USER = "synthetic-user"
TOKEN, PRIVATE, KEY = "t" * 32, "p" * 64, "k" * 32
MENU = '<div class="usermenu"><a href="/user/profile.php?id=42">Profile</a><a href="/login/logout.php?sesskey=synthetic">Logout</a></div>'


def page(path, html="", status=200, location=None):
    response = requests.Response()
    response.status_code = status
    response.url = BASE + path
    response._content = html.encode()
    if location:
        response.headers["Location"] = location
    return response


class MobilePasswordLoginTests(unittest.TestCase):
    def setUp(self):
        self.reset()

    def reset(self):
        if hasattr(self, "session"):
            self.session.close()
        self.session = requests.Session()
        self.results = [
            {"token": TOKEN, "privatetoken": PRIVATE},
            {"userid": 42, "username": USER, "siteurl": BASE},
            {"key": KEY, "autologinurl": BASE + "/admin/tool/mobile/autologin.php"},
            page("/admin/tool/mobile/autologin.php", status=303, location="/my/"),
            page("/my/", MENU),
        ]

    def tearDown(self):
        self.session.close()

    def respond(self, sess, method, url, **kwargs):
        if not self.results:
            self.fail("Unexpected additional request")
        result = self.results.pop(0)
        if isinstance(result, dict):
            response = page("/webservice/rest/server.php")
            response.json = lambda: result
            return response
        if self.return_cookie and result.status_code == 303:
            sess.cookies.set("MoodleSession", "synthetic-web-session", domain="e3p.nycu.edu.tw", path="/")
        return result

    def perform(self, *, cookie=True):
        self.return_cookie = cookie
        with patch("e3_tracker.assignments.services.mobile_auth.safe_request", side_effect=self.respond) as probe:
            self.request = probe
            login_with_mobile_api(self.session, BASE, USER, "private-password", timeout=8)

    def test_nycu_dispatches_to_the_api_without_submitting_the_incompatible_html_form(self):
        with patch("e3_tracker.assignments.services.mobile_auth.login_with_mobile_api") as api, patch("e3_tracker.assignments.services.http.safe_request") as html:
            login_with_password(self.session, BASE, USER, "private-password", timeout=8)
        api.assert_called_once_with(self.session, BASE, USER, "private-password", timeout=8)
        html.assert_not_called()

    def test_complete_exchange_keeps_credentials_in_post_bodies_and_returns_a_web_cookie(self):
        self.perform()
        calls = self.request.call_args_list
        self.assertEqual(len(calls), 5)
        self.assertEqual(self.session.cookies.get("MoodleSession"), "synthetic-web-session")
        self.assertEqual(calls[0].args[1:3], ("POST", BASE + "/login/token.php"))
        self.assertEqual(calls[0].kwargs["data"]["service"], "moodle_mobile_app")
        self.assertEqual(calls[1].kwargs["data"]["wsfunction"], "core_webservice_get_site_info")
        self.assertEqual(calls[2].kwargs["data"]["privatetoken"], PRIVATE)
        self.assertEqual(calls[3].kwargs["data"], {"userid": "42", "key": KEY, "urltogo": BASE + "/my/"})
        self.assertEqual(calls[4].args[1:3], ("GET", BASE + "/my/"))
        for call in calls:
            self.assertFalse(call.kwargs["allow_redirects"])
            for secret in (TOKEN, PRIVATE, KEY, "private-password"):
                self.assertNotIn(secret, call.args[2])

    def test_invalid_credentials_and_handoff_cooldown_are_not_retried(self):
        for code, reason, count in (("invalidlogin", "login_rejected", 1), ("autologinkeygenerationlockout", "session_handoff_limited", 3)):
            with self.subTest(code=code):
                self.reset()
                self.results[count - 1] = {"error": "private upstream body and credentials", "errorcode": code}
                with self.assertRaises(E3LoginError) as raised:
                    self.perform()
                self.assertEqual(login_failure_reason(raised.exception), reason)
                self.assertEqual(raised.exception.api_code, code)
                self.assertEqual(self.request.call_count, count)
                self.assertNotIn("private", str(raised.exception))
                self.session.close()

    def test_identity_mismatch_stops_before_any_session_handoff(self):
        for change in ({"userid": True}, {"userid": 0}, {"username": "other-person"}, {"siteurl": "https://other.example"}):
            with self.subTest(change=change):
                self.reset()
                self.results[1].update(change)
                with self.assertRaises(E3LoginError) as raised:
                    self.perform()
                self.assertEqual(raised.exception.reason, "identity_mismatch")
                self.assertEqual(self.request.call_count, 2)
                self.session.close()

    def test_foreign_handoff_and_redirects_never_receive_credentials(self):
        for value in ("https://other.example/admin/tool/mobile/autologin.php", BASE + "/login/index.php", BASE + "/admin/tool/mobile/autologin.php?key=private"):
            with self.subTest(value=value):
                self.reset()
                self.results[2]["autologinurl"] = value
                with self.assertRaises(E3LoginError):
                    self.perform()
                self.assertEqual(self.request.call_count, 3)
                self.session.close()
        self.reset()
        self.results[3] = page("/admin/tool/mobile/autologin.php", status=303, location="https://other.example/my/")
        with self.assertRaises(E3LoginError):
            self.perform()
        self.assertEqual(self.request.call_count, 4)

    def test_a_token_or_a_menu_for_another_user_never_counts_as_a_web_login(self):
        for html in (MENU.replace("id=42", "id=99"), MENU + '<input name="password">', MENU + '<div class="usermenu"><a href="/user/profile.php?id=99">Other</a></div>'):
            with self.subTest(html=html):
                self.reset()
                self.results[4] = page("/my/", html)
                with self.assertRaises(E3LoginError) as raised:
                    self.perform()
                self.assertEqual(raised.exception.reason, "mobile_session_unavailable")
                self.session.close()
        self.reset()
        with self.assertRaises(E3LoginError) as raised:
            self.perform(cookie=False)
        self.assertEqual(raised.exception.reason, "session_cookie_missing")

    def test_unknown_api_error_details_are_not_retained_in_the_exception(self):
        self.results[0] = {"error": "private upstream response", "errorcode": "private-secret"}
        with self.assertRaises(E3LoginError) as raised:
            self.perform()
        self.assertIsNone(raised.exception.api_code)
        self.assertNotIn("private", str(vars(raised.exception)))

    def test_original_password_form_establishes_the_original_administrator_and_activity_access(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
            "E3_ENV": "development", "RAILWAY_ENVIRONMENT_ID": "", "RAILWAY_ENVIRONMENT_NAME": "",
            "E3_CACHE_DIR": directory, "E3_DATABASE_URL": "", "DATABASE_URL": "",
            "E3_CANONICAL_HOST": "", "E3_SESSION_COOKIE_SECURE": "0", "E3_ADMIN_USER_ID": USER,
            "E3_BASE_URL": BASE, "E3_SESSION_PROFILE_WORKER": "0", "E3_NOTIFICATIONS_WORKER": "0",
            "E3_YOUTUBE_AUTO_SYNC_ENABLED": "0",
        }):
            app = create_app()
            storage = app.extensions["e3_storage"]
            try:
                storage.save_user_cache(USER, {"ts": time.time(), "result": {"courses": [], "all_assignments": []}})
                client = csrf_client(app)
                self.return_cookie = True
                with patch("e3_tracker.assignments.services.mobile_auth.safe_request", side_effect=self.respond):
                    result = client.post("/login", data={"username": USER, "password": "private-password"})
                self.assertEqual(result.status_code, 302)
                with client.session_transaction() as browser:
                    account = storage.load_web_session(browser["session_token"])
                self.assertEqual(account["username"], USER)
                self.assertTrue(account["is_admin"])
                self.assertEqual(account["moodle_session"], "synthetic-web-session")
                activity = client.get("/admin/traffic/activity")
                self.assertEqual(activity.status_code, 200)
                self.assertIn("登入成功", activity.get_data(as_text=True))
                self.assertNotIn("private-password", activity.get_data(as_text=True))
            finally:
                storage._engine.dispose()


if __name__ == "__main__":
    unittest.main()
