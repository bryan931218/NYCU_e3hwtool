import os
import tempfile
import unittest
from unittest.mock import patch

from e3_tracker.platform.application import create_app
from e3_tracker.study.session import STUDY_SESSION_LIFETIME
from tests.security_helpers import csrf_client


class StudySessionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        with patch.dict(os.environ, {"E3_CACHE_DIR": self.temp.name, "E3_DATABASE_URL": "", "E3_SESSION_COOKIE_SECURE": "0"}):
            self.app = create_app()
        self.storage = self.app.extensions["e3_storage"]
        self.addCleanup(self.storage._engine.dispose)
        self.client = csrf_client(self.app)

    def assignment_login(self, username="test-admin", admin=True):
        self.storage.save_web_session("assignment-token", username, is_admin=admin, moodle_session="e3-credential")
        with self.client.session_transaction() as cookie:
            cookie.clear()
            cookie["session_token"] = "assignment-token"
            cookie.permanent = True

    def study_token(self):
        with self.client.session_transaction(path="/admin/study-plan") as cookie:
            return cookie["session_token"]

    def test_bootstrap_creates_independent_cookie_without_e3_credential(self):
        self.assignment_login()
        response = self.client.get("/admin/study-auth/state")
        self.assertEqual(response.status_code, 200)
        self.assertIn("e3_study_session=", response.headers["Set-Cookie"])
        token = self.study_token()
        self.assertNotEqual(token, "assignment-token")
        self.assertIsNone(self.storage.load_web_session(token)["moodle_session"])
        self.assertEqual(self.app.permanent_session_lifetime.days, 1)
        self.assertEqual(STUDY_SESSION_LIFETIME.days, 30)

    def test_assignment_logout_does_not_interrupt_study_progress_or_csrf(self):
        self.assignment_login()
        auth = self.client.get("/admin/study-auth/state").get_json()
        study_token = self.study_token()
        self.assertEqual(self.client.post("/logout").status_code, 302)
        self.assertIsNone(self.storage.load_web_session("assignment-token"))
        self.assertIsNotNone(self.storage.load_web_session(study_token))
        video = self.storage.list_study_plan_videos_with_records()[0]
        response = self.client.post("/admin/study-plan/video-progress", json={
            "video_id": video["id"], "watched_seconds": 127, "expected_version": video["progress_version"],
        }, headers={"X-Study-Account": "test-admin"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["playback_seconds"], 127)
        # The actual token from the still-open study page remains valid as well.
        real = self.app.test_client()
        real.set_cookie("e3_study_session", self.client.get_cookie("e3_study_session").value)
        response = real.post("/admin/study-plan/study-time", json={
            "session_id": "video_test_123", "kind": "video", "video_id": video["id"], "elapsed_seconds": 30,
        }, headers={"X-CSRFToken": auth["csrf_token"], "Referer": "https://localhost/admin/study-plan"})
        self.assertEqual(response.status_code, 200)

    def test_assignment_session_expiry_does_not_expire_study_session(self):
        self.assignment_login()
        self.client.get("/admin/study-auth/state")
        self.storage.save_web_session("assignment-token", "test-admin", is_admin=True, lifetime=-1)
        self.assertIsNone(self.storage.load_web_session("assignment-token"))
        self.assertEqual(self.client.get("/admin/study-auth/state").status_code, 200)
        self.assertEqual(self.client.get("/admin/study-plan").status_code, 200)

    def test_study_logout_does_not_logout_assignment_or_automatically_sign_back_in(self):
        self.assignment_login()
        self.client.get("/admin/study-auth/state")
        token = self.study_token()
        self.assertEqual(self.client.post("/admin/study-auth/logout").status_code, 302)
        self.assertIsNone(self.storage.load_web_session(token))
        self.assertIsNotNone(self.storage.load_web_session("assignment-token"))
        self.assertEqual(self.client.get("/admin/study-auth/state").status_code, 401)
        self.assertEqual(self.client.post("/admin/study-auth/resume").status_code, 302)
        self.assertEqual(self.client.get("/admin/study-auth/state").status_code, 200)
        self.assertNotEqual(self.study_token(), token)

    def test_guest_non_admin_and_forged_flags_cannot_bootstrap(self):
        self.assignment_login(admin=False)
        self.assertEqual(self.client.get("/admin/study-auth/state").status_code, 401)
        with self.client.session_transaction() as cookie:
            cookie["is_admin"] = True
        self.assertEqual(self.client.get("/admin/study-auth/state").status_code, 401)
        self.assertEqual(self.client.post("/admin/study-auth/resume").status_code, 302)
        self.assertEqual(self.client.get("/admin/study-auth/state").status_code, 401)
        self.storage.save_web_session("assignment-token", "guest_test", is_guest=True, is_admin=True)
        self.assertEqual(self.client.get("/admin/study-auth/state").status_code, 401)

    def test_revoked_study_token_cannot_write(self):
        self.assignment_login()
        self.client.get("/admin/study-auth/state")
        self.storage.clear_web_session(self.study_token())
        response = self.client.post("/admin/study-plan/video-progress", json={"video_id": 1, "watched_seconds": 10, "expected_version": 0})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["error"], "study_login_required")

    def test_study_cookie_cannot_authenticate_assignment_admin_pages(self):
        self.assignment_login()
        self.client.get("/admin/study-auth/state")
        study_cookie = self.client.get_cookie("e3_study_session").value
        other = self.app.test_client()
        other.set_cookie(self.app.config["SESSION_COOKIE_NAME"], study_cookie)
        self.assertEqual(other.get("/admin/traffic").status_code, 302)

    def test_cross_account_stale_tab_cannot_sync_to_new_account(self):
        self.assignment_login()
        self.client.get("/admin/study-auth/state")
        response = self.client.post("/admin/study-plan/video-progress", json={"video_id": 1, "watched_seconds": 10, "expected_version": 0}, headers={"X-Study-Account": "other-admin"})
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["error"], "study_account_changed")

    def test_csrf_still_required_for_study_writes_and_logout(self):
        self.assignment_login()
        self.client.get("/admin/study-auth/state")
        other = self.app.test_client()
        other.set_cookie("e3_study_session", self.client.get_cookie("e3_study_session").value)
        for path in ("/admin/study-plan/video-progress", "/admin/study-auth/logout", "/admin/study-auth/resume"):
            self.assertEqual(other.post(path, json={}).status_code, 400)

    def test_secure_cookie_is_host_only_http_only_and_lax(self):
        self.app.config["SESSION_COOKIE_SECURE"] = True
        self.assignment_login()
        response = self.client.get("/admin/study-auth/state")
        cookie = response.headers["Set-Cookie"]
        self.assertIn("__Host-e3_study_session=", cookie)
        for attribute in ("Secure", "HttpOnly", "Path=/", "SameSite=Lax"):
            self.assertIn(attribute, cookie)
        self.assertNotIn("Domain=", cookie)
