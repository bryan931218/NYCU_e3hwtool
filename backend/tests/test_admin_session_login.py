"""Admin Session login requires a fresh, explicitly bound Moodle principal."""

import hashlib
import os
import tempfile
import unittest
from unittest.mock import patch

import requests
from sqlalchemy import select

from e3_tracker.assignments.services.session_auth import authenticated_moodle_user_id
from e3_tracker.bootstrap import create_app
from e3_tracker.platform.persistence.core_schema import web_sessions_table
from tests.security_helpers import csrf_client


BASE = "https://e3.example.test"
MENU = '<div class="usermenu"><a href="/user/profile.php?id=42">Profile</a><a href="/login/logout.php?sesskey=synthetic">Logout</a></div>'


def response(html=MENU, *, path="/my/", status=200, location=None):
    result = requests.Response()
    result.url = BASE + path
    result.status_code = status
    result._content = html.encode()
    result.encoding = "utf-8"
    if location:
        result.headers["Location"] = location
    return result


class MoodlePrincipalTests(unittest.TestCase):
    def test_principal_comes_from_own_authenticated_menu_only(self):
        for html in (MENU, MENU + MENU):
            with patch("e3_tracker.assignments.services.session_auth.safe_request", return_value=response(html)) as read:
                self.assertEqual(authenticated_moodle_user_id(object(), BASE), "42")
                self.assertFalse(read.call_args.kwargs["allow_redirects"])

    def test_missing_conflicting_external_or_login_page_principals_are_rejected(self):
        for html in (
            '<a href="/user/profile.php?id=42">Profile</a>',
            MENU.replace('sesskey=synthetic', 'sesskey='),
            MENU.replace('/user/profile.php', 'https://outside.test/user/profile.php'),
            MENU.replace('id=42', 'id=42&id=43'),
            MENU.replace('id=42', 'id=invalid'),
            MENU + MENU.replace('id=42', 'id=43'),
            MENU + '<input name="password">',
            MENU + '<input name="logintoken">',
        ):
            with self.subTest(html=html), patch("e3_tracker.assignments.services.session_auth.safe_request", return_value=response(html)):
                self.assertEqual(authenticated_moodle_user_id(object(), BASE), "")

    def test_only_bounded_same_origin_dashboard_redirects_are_followed(self):
        with patch("e3_tracker.assignments.services.session_auth.safe_request", side_effect=[
            response(status=302, location="/my/index.php"), response(path="/my/index.php"),
        ]) as read:
            self.assertEqual(authenticated_moodle_user_id(object(), BASE), "42")
            self.assertEqual(read.call_count, 2)
        for location in ("/login/index.php", "https://outside.test/my/", "/my/?token=private"):
            with self.subTest(location=location), patch("e3_tracker.assignments.services.session_auth.safe_request", return_value=response(status=302, location=location)) as read:
                self.assertEqual(authenticated_moodle_user_id(object(), BASE), "")
                read.assert_called_once()


class AdminSessionLoginTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(os.environ, {
            "E3_ENV": "development", "RAILWAY_ENVIRONMENT_ID": "", "RAILWAY_ENVIRONMENT_NAME": "",
            "E3_CACHE_DIR": self.directory.name, "E3_DATABASE_URL": "", "DATABASE_URL": "",
            "E3_CANONICAL_HOST": "", "E3_SESSION_COOKIE_SECURE": "0", "E3_DEV_RELOAD": "0",
            "E3_SESSION_PROFILE_WORKER": "0", "E3_NOTIFICATIONS_WORKER": "0", "E3_YOUTUBE_AUTO_SYNC_ENABLED": "0",
            "E3_ADMIN_USER_ID": "112550103", "E3_ADMIN_MOODLE_USER_ID": "42", "OPENAI_API_KEY": "",
        })
        self.environment.start()
        self.app = create_app(default_base_url=BASE)
        self.storage = self.app.extensions["e3_storage"]
        self.client = csrf_client(self.app)
        self.cookie = "private-cookie-test-value"
        self.storage.save_user_cache("112550103", {"ts": 123, "result": {"courses": [], "all_assignments": []}})
        self.storage.save_user_preferences("112550103", {"view_mode": "calendar"})

    def tearDown(self):
        self.app.extensions["e3_session_identity"].stop.set()
        self.storage._engine.dispose()
        self.environment.stop()
        self.directory.cleanup()

    def post(self, **extra):
        return self.client.post("/login", data={"login_type": "session", "moodle_session": self.cookie,
                                                 "admin_session": "1", **extra})

    def session_count(self):
        with self.storage._engine.connect() as conn:
            return len(conn.execute(select(web_sessions_table.c.session_token)).all())

    def test_matching_principal_reuses_canonical_admin_and_existing_data(self):
        with patch("e3_tracker.assignments.routes.assignments.authenticated_moodle_user_id", return_value="42") as verify, \
                patch.object(self.app.extensions["e3_session_identity"], "schedule_user"):
            result = self.post(username="attacker-supplied", is_admin="0")
        self.assertEqual(result.status_code, 302)
        verify.assert_called_once()
        with self.client.session_transaction() as state:
            user = self.storage.load_web_session(state["session_token"])
        self.assertEqual(user["username"], "112550103")
        self.assertTrue(user["is_admin"])
        self.assertEqual(user["moodle_session"], self.cookie)
        self.assertEqual(self.storage.load_user_cache("112550103")["ts"], 123)
        self.assertEqual(self.storage.load_user_preferences("112550103")["view_mode"], "calendar")
        self.assertEqual(self.client.get("/admin/traffic").status_code, 200)

    def test_wrong_missing_and_expired_principals_never_start_admin_session(self):
        for principal in ("43", "", "042"):
            with self.subTest(principal=principal), patch("e3_tracker.assignments.routes.assignments.authenticated_moodle_user_id", return_value=principal):
                result = self.post(username="112550103", is_admin="1", student_number="112550103", moodle_user_id="42")
            self.assertEqual(result.status_code, 200)
            self.assertEqual(self.session_count(), 0)
            self.assertNotIn(self.cookie, result.get_data(as_text=True))
        with patch("e3_tracker.assignments.routes.assignments.authenticated_moodle_user_id", side_effect=requests.Timeout("private")):
            self.assertEqual(self.post().status_code, 200)
        self.assertEqual(self.session_count(), 0)

    def test_verified_admin_does_not_depend_on_assignment_collection_to_log_in(self):
        with patch("e3_tracker.assignments.routes.assignments.authenticated_moodle_user_id", return_value="42"), \
                patch.object(self.app.extensions["e3_session_identity"], "schedule_user"), \
                patch.object(self.storage, "load_user_cache", return_value=None):
            result = self.post()
        self.assertEqual(result.status_code, 302)
        with self.client.session_transaction() as state:
            self.assertTrue(self.storage.load_web_session(state["session_token"])["is_admin"])

    def test_disabled_binding_hides_option_and_rejects_forged_request(self):
        with patch.dict(os.environ, {"E3_ADMIN_MOODLE_USER_ID": ""}), patch(
            "e3_tracker.assignments.routes.assignments.authenticated_moodle_user_id"
        ) as verify:
            self.assertNotIn('name="admin_session"', self.client.get("/login").get_data(as_text=True))
            self.assertEqual(self.post().status_code, 200)
            verify.assert_not_called()
        self.assertEqual(self.session_count(), 0)

    def test_ordinary_session_enrichment_cannot_grant_admin_even_for_admin_number(self):
        username = "Session-" + hashlib.sha1(self.cookie.encode()).hexdigest()[:10]
        self.storage.save_user_cache(username, {"ts": 123, "result": {"courses": [], "all_assignments": []}})
        with patch("e3_tracker.assignments.routes.assignments.authenticated_moodle_user_id") as verify, patch(
            "e3_tracker.assignments.services.session_identity.fetch_session_identity", return_value={"name": "Test User", "student_number": "112550103"}
        ):
            self.assertEqual(self.post(admin_session="0", is_admin="1").status_code, 302)
            verify.assert_not_called()
        with self.client.session_transaction() as state:
            user = self.storage.load_web_session(state["session_token"])
        self.assertEqual(user["username"], username)
        self.assertFalse(user["is_admin"])

    def test_admin_session_login_keeps_csrf_checks(self):
        with patch("e3_tracker.assignments.routes.assignments.authenticated_moodle_user_id") as verify:
            result = self.app.test_client().post("/login", data={"login_type": "session", "admin_session": "1", "moodle_session": self.cookie})
            self.assertEqual(result.status_code, 400)
            verify.assert_not_called()


if __name__ == "__main__":
    unittest.main()
