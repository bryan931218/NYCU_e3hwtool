"""Server SSO stays isolated, session-bound, encrypted, and optional."""
import os
import queue
import tempfile
import time
import unittest
from unittest.mock import MagicMock, Mock, patch

from sqlalchemy import select, update

from e3_tracker.assignments.services.server_sso import ServerSso, allowed_browser_url
from e3_tracker.platform.moodle_credentials import pack_moodle_credential, unpack_moodle_credential
from e3_tracker.platform.persistence.core_schema import web_sessions_table
from e3_tracker.platform.storage import PersistentStorage
from tests import test_mobile_password_login as api_fixtures


class ServerSsoTests(unittest.TestCase):
    def test_browser_network_is_limited_to_school_and_verification_providers(self):
        for url in ("https://portal.nycu.edu.tw/portal/newe3p", "https://e3p.nycu.edu.tw/my/",
                    "https://www.google.com/recaptcha/api.js"):
            self.assertTrue(allowed_browser_url(url))
        for url in ("http://portal.nycu.edu.tw/", "https://127.0.0.1/", "http://169.254.169.254/",
                    "https://portal.nycu.edu.tw.evil.example/", "https://portal.nycu.edu.tw:8443/",
                    "https://user:password@portal.nycu.edu.tw/", "file:///etc/passwd", "https://[bad"):
            self.assertFalse(allowed_browser_url(url))

    def test_feature_is_disabled_by_default_and_can_be_limited_to_administrator(self):
        service = ServerSso(None, None, None, admin_user_id="owner")
        with patch.dict(os.environ, {"E3_SERVER_SSO_COUNTS": ""}):
            self.assertFalse(service.enabled("owner"))
        with patch.dict(os.environ, {"E3_SERVER_SSO_COUNTS": "admin"}):
            self.assertTrue(service.enabled("owner"))
            self.assertFalse(service.enabled("another-user"))

    def test_verification_code_is_bound_to_one_browser_session_and_attempt_limit(self):
        service = ServerSso(None, None, None)
        token = "browser-one"
        job = {"started": time.monotonic(), "state": "otp_required", "attempts": 0, "codes": queue.Queue(maxsize=1)}
        service._jobs[service._key(token)] = job
        self.assertFalse(service.verify("browser-two", "123456"))
        self.assertFalse(service.verify(token, 123456))
        self.assertFalse(service.verify(token, "123456<script>"))
        self.assertTrue(service.verify(token, "123456"))
        self.assertEqual(job["codes"].get_nowait(), "123456")
        self.assertEqual(service.status(token), {"state": "verifying", "attempts": 0})
        job.update(state="otp_required", attempts=3)
        self.assertFalse(service.verify(token, "123456"))
        job.update(attempts=0, started=time.monotonic() - 601)
        self.assertFalse(service.verify(token, "123456"))

    def test_logout_wakes_a_waiting_verification_without_retaining_a_code(self):
        service = ServerSso(None, None, None)
        job = {"started": time.monotonic(), "state": "otp_required", "attempts": 0, "codes": queue.Queue(maxsize=1)}
        service._jobs[service._key("browser")] = job
        service.cancel("browser")
        self.assertIsNone(job["codes"].get_nowait())
        self.assertEqual(job["state"], "cancelled")
        self.assertFalse(service.verify("browser", "123456"))

    def test_thread_failure_discards_password_and_releases_capacity(self):
        service = ServerSso(None, None, None)
        with patch.dict(os.environ, {"E3_SERVER_SSO_COUNTS": "all"}), patch(
                "e3_tracker.assignments.services.server_sso.threading.Thread") as thread:
            thread.return_value.start.side_effect = RuntimeError("worker unavailable")
            self.assertFalse(service.start("browser", "student", "private-password", 42))
            self.assertEqual(thread.call_args.kwargs["args"][3], {})
            self.assertFalse(service.start("browser-two", "student", "private-password", 42))
        self.assertEqual(service.status("browser")["state"], "unavailable")
        self.assertNotIn("private-password", str(service._jobs))

    def test_browser_queue_is_bounded_and_reports_busy_without_starting_more_workers(self):
        service = ServerSso(None, None, None)
        with patch.dict(os.environ, {"E3_SERVER_SSO_COUNTS": "all"}), patch(
                "e3_tracker.assignments.services.server_sso.threading.Thread") as thread:
            for index in range(3):
                self.assertTrue(service.start(str(index), "student", "private-password", 42))
            self.assertFalse(service.start("extra", "student", "private-password", 42))
            self.assertEqual(thread.call_count, 3)
            self.assertEqual(service.status("extra")["state"], "busy")
        self.assertNotIn("private-password", str(service._jobs))

    def test_public_preflight_never_enters_credentials_or_clicks_login(self):
        service = ServerSso(None, None, None)
        runtime = MagicMock()
        manager = MagicMock()
        manager.__enter__.return_value = runtime
        browser = runtime.chromium.launch.return_value
        page = browser.new_context.return_value.new_page.return_value
        with patch("playwright.sync_api.sync_playwright", return_value=manager):
            self.assertEqual(service.preflight(), "ready")
        page.get_by_role.return_value.click.assert_not_called()
        page.get_by_role.return_value.fill.assert_not_called()
        page.locator.assert_not_called()
        browser.close.assert_called_once()

    def test_worker_completes_ordinary_otp_flow_and_attaches_only_verified_identity(self):
        storage, app, connected = Mock(), MagicMock(), Mock()
        service = ServerSso(storage, app, connected)
        runtime, manager = MagicMock(), MagicMock()
        manager.__enter__.return_value = runtime
        browser = runtime.chromium.launch.return_value
        context = browser.new_context.return_value
        page = context.new_page.return_value
        page.url = "https://e3p.nycu.edu.tw/my/"
        context.pages = [page]
        context.cookies.side_effect = [[], [{"name": "MoodleSession", "value": "s" * 32}]]
        job = {"started": time.monotonic(), "state": "queued", "attempts": 0, "codes": queue.Queue(maxsize=1)}
        job["codes"].put("123456")
        secret = {"password": "private-password"}
        service._slots.acquire()
        with patch("playwright.sync_api.sync_playwright", return_value=manager), patch(
                "e3_tracker.assignments.services.server_sso.verified_cookie", return_value=True) as verify:
            service._run(job, "browser", "student", secret, 42)
        self.assertEqual(secret, {})
        self.assertEqual((job["state"], job["attempts"]), ("complete", 1))
        verify.assert_called_once_with("s" * 32, 42)
        storage.attach_moodle_web_session.assert_called_once_with("browser", "student", 42, "s" * 32)
        connected.assert_called_once_with("student", "browser")
        browser.close.assert_called_once()
        self.assertTrue(service._capacity.acquire(blocking=False))
        self.assertTrue(service._slots.acquire(blocking=False))

    def test_worker_rejects_wrong_school_identity_and_cancels_after_logout(self):
        storage = Mock()
        storage.is_valid_web_session.side_effect = [True, True, False]
        service = ServerSso(storage, MagicMock(), Mock())
        runtime, manager = MagicMock(), MagicMock()
        manager.__enter__.return_value = runtime
        browser = runtime.chromium.launch.return_value
        context = browser.new_context.return_value
        page = context.new_page.return_value
        page.url = "https://e3p.nycu.edu.tw/my/"
        page.locator.return_value.count.return_value = 0
        context.pages = [page]
        context.cookies.return_value = [{"name": "MoodleSession", "value": "s" * 32}]
        job = {"started": time.monotonic(), "state": "queued", "attempts": 0, "codes": queue.Queue(maxsize=1)}
        service._slots.acquire()
        with patch("playwright.sync_api.sync_playwright", return_value=manager), patch(
                "e3_tracker.assignments.services.server_sso.verified_cookie", return_value=False):
            service._run(job, "browser", "student", {"password": "private-password"}, 42)
        self.assertEqual(job["state"], "cancelled")
        storage.attach_moodle_web_session.assert_not_called()
        service.on_connected.assert_not_called()
        browser.close.assert_called_once()

    def test_both_api_and_web_credentials_remain_encrypted_and_owner_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = PersistentStorage("sqlite:///" + os.path.join(directory, "sso.sqlite3"))
            try:
                storage.save_web_session("browser", "student", moodle_token="t" * 32, moodle_user_id=42, is_admin=True)
                with storage._engine.connect() as conn:
                    expires = conn.execute(select(web_sessions_table.c.expires_at)).scalar_one()
                self.assertFalse(storage.attach_moodle_web_session("browser", "someone-else", 42, "s" * 32))
                self.assertFalse(storage.attach_moodle_web_session("browser", "student", 99, "s" * 32))
                self.assertFalse(storage.attach_moodle_web_session("browser", "student", True, "s" * 32))
                self.assertTrue(storage.attach_moodle_web_session("browser", "student", 42, "s" * 32))
                user = storage.load_web_session("browser", "student")
                self.assertEqual(user["moodle_token"], "t" * 32)
                self.assertEqual(user["moodle_session"], "s" * 32)
                self.assertTrue(user["is_admin"])
                with storage._engine.connect() as conn:
                    row = conn.execute(select(web_sessions_table)).mappings().one()
                self.assertEqual(row["expires_at"], expires)
                self.assertNotIn("s" * 32, row["moodle_credential"])
                self.assertNotIn("t" * 32, row["moodle_credential"])
                storage.clear_web_session("browser")
                self.assertFalse(storage.attach_moodle_web_session("browser", "student", 42, "s" * 32))
                self.assertIsNone(storage.load_web_session("browser"))
            finally:
                storage._engine.dispose()

    def test_expired_web_session_cannot_be_reanimated_by_background_login(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = PersistentStorage("sqlite:///" + os.path.join(directory, "expired.sqlite3"))
            try:
                storage.save_web_session("browser", "student", moodle_token="t" * 32, moodle_user_id=42)
                with storage._engine.begin() as conn:
                    conn.execute(update(web_sessions_table).values(expires_at=time.time() - 1))
                self.assertFalse(storage.attach_moodle_web_session("browser", "student", 42, "s" * 32))
            finally:
                storage._engine.dispose()

    def test_old_credentials_are_compatible_and_invalid_companion_cookie_is_rejected(self):
        self.assertEqual(unpack_moodle_credential("legacy-cookie"), {"moodle_session": "legacy-cookie"})
        packed = pack_moodle_credential("s" * 32, token="t" * 32, user_id=42)
        self.assertEqual(unpack_moodle_credential(packed)["moodle_session"], "s" * 32)
        with self.assertRaises(ValueError):
            pack_moodle_credential("bad", token="t" * 32, user_id=42)

    def test_student_api_reads_real_totals_from_matching_verified_web_account(self):
        fixture = api_fixtures.ApiPasswordTests()
        fixture.setUp()
        response = Mock(status_code=200, text='''<table><tr><th>Name</th><th>status</th><th>Function</th></tr>
            <tr><td>HW</td><td>3 Submitted<br>90 Not Submitted</td><td><a href="/mod/assign/view.php?id=456">View</a></td></tr></table>''')
        from e3_tracker.assignments.services.collector import collect_assignments
        with fixture.mocked(), patch("e3_tracker.assignments.services.submission_counts.verified_cookie", return_value=True) as verify, patch(
                "e3_tracker.assignments.services.submission_counts.safe_request", return_value=response):
            item, = collect_assignments(fixture.options(moodle_session="s" * 32))["all_assignments"]
        self.assertEqual((item["submitted_count"], item["participant_count"]), (3, 93))
        verify.assert_called_once_with("s" * 32, 42, 8)
        with fixture.mocked(), patch("e3_tracker.assignments.services.submission_counts.verified_cookie", return_value=False), patch(
                "e3_tracker.assignments.services.submission_counts.safe_request") as request:
            item, = collect_assignments(fixture.options(moodle_session="s" * 32))["all_assignments"]
        self.assertIsNone(item["submitted_count"])
        request.assert_not_called()

    def test_late_counts_update_both_result_views_and_preserve_zero(self):
        from e3_tracker.assignments.services.submission_counts import apply_class_counts
        flat = {"course_id": 10, "url": "https://e3p.nycu.edu.tw/mod/assign/view.php?id=456", "submitted_count": None}
        grouped = dict(flat)
        result = {"all_assignments": [flat], "courses": [{"assignments": [grouped]}]}
        apply_class_counts(result, {(10, 456): (0, 93)})
        self.assertEqual(flat["submitted_count"], 0)
        self.assertEqual(grouped["participant_count"], 93)
        foreign = {"course_id": 10, "url": "https://foreign.example/mod/assign/view.php?id=456"}
        apply_class_counts({"all_assignments": [foreign]}, {(10, 456): (3, 93)})
        self.assertNotIn("submitted_count", foreign)

    def test_routes_protect_verification_ownership_guest_access_and_logout(self):
        from e3_tracker.platform.application import create_app
        from tests.security_helpers import csrf_client
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
            "E3_ENV": "development", "RAILWAY_ENVIRONMENT_ID": "", "RAILWAY_ENVIRONMENT_NAME": "",
            "E3_CACHE_DIR": directory, "E3_DATABASE_URL": "", "DATABASE_URL": "", "E3_CANONICAL_HOST": "",
            "E3_SESSION_COOKIE_SECURE": "0", "E3_SESSION_PROFILE_WORKER": "0", "E3_NOTIFICATIONS_WORKER": "0",
            "E3_YOUTUBE_AUTO_SYNC_ENABLED": "0", "E3_SERVER_SSO_COUNTS": "",
        }):
            app = create_app()
            storage = app.extensions["e3_storage"]
            service = app.extensions["e3_server_sso"]
            try:
                self.assertEqual(app.test_client().get("/api/e3-connection").status_code, 302)
                first, second, guest = csrf_client(app), csrf_client(app), csrf_client(app)
                for client, token, username, is_guest in ((first, "one", "student", False),
                        (second, "two", "student", False), (guest, "guest", "visitor", True)):
                    storage.save_web_session(token, username, is_guest=is_guest,
                        moodle_token="t" * 32 if not is_guest else None, moodle_user_id=42 if not is_guest else None)
                    with client.session_transaction() as browser:
                        browser["session_token"] = token
                job = {"started": time.monotonic(), "state": "otp_required", "attempts": 0, "codes": queue.Queue(maxsize=1)}
                service._jobs[service._key("one")] = job
                self.assertEqual(second.post("/api/e3-connection/verify", json={"code": "123456"}).status_code, 400)
                self.assertEqual(job["state"], "otp_required")
                self.assertEqual(guest.get("/api/e3-connection").status_code, 403)
                self.assertEqual(guest.post("/api/e3-connection/verify", json={"code": "123456"}).status_code, 403)
                self.assertEqual(first.post("/api/e3-connection/verify", json={"code": "123456"}).status_code, 200)
                self.assertEqual(job["codes"].get_nowait(), "123456")
                self.assertEqual(first.post("/logout").status_code, 302)
                self.assertEqual(job["state"], "cancelled")
                self.assertIsNone(storage.load_web_session("one"))
            finally:
                storage._engine.dispose()


if __name__ == "__main__":
    unittest.main()
