"""New-user markers come only from a first successful, non-guest login."""

import hashlib
import os
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from bs4 import BeautifulSoup
from sqlalchemy import delete, select, text

from e3_tracker.assignments.persistence.membership import assignment_memberships
from e3_tracker.platform.application import create_app
from e3_tracker.platform.persistence import migrations
from e3_tracker.platform.persistence.core_schema import users_table
from e3_tracker.platform.storage import PersistentStorage
from tests.security_helpers import csrf_client


class AssignmentMembershipTests(unittest.TestCase):
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
        self.storage.save_web_session("member-admin", "member-admin", is_admin=True)
        self.admin = csrf_client(self.app)
        with self.admin.session_transaction() as session:
            session["session_token"] = "member-admin"

    def tearDown(self):
        self.storage._engine.dispose()
        self.environment.stop()
        self.directory.cleanup()

    def page(self):
        response = self.admin.get("/admin/traffic")
        self.assertEqual(response.status_code, 200)
        return BeautifulSoup(response.get_data(as_text=True), "html.parser")

    def login(self, username):
        client = csrf_client(self.app)
        self.storage.save_user_cache(username, {"ts": time.time(), "result": {"courses": [], "all_assignments": []}})
        def authenticate(sess, *args, **kwargs):
            sess.cookies.set("MoodleSession", "synthetic-test-only")
        with patch("e3_tracker.assignments.routes.assignments.login_with_password", side_effect=authenticate):
            response = client.post("/login", data={"username": username, "password": "synthetic-test-only"})
        self.assertEqual(response.status_code, 302)
        return client

    def test_only_first_password_login_has_new_user_event_and_account_badge(self):
        self.login("112550001")
        self.login("112550001")
        events = [event for event in self.storage.recent_traffic_events(500) if event["action"] == "login_success"]
        self.assertEqual([event["meta"]["is_new_user"] for event in events], [True, False])
        page = self.page()
        self.assertEqual(len(page.select(".events .new-user-badge")), 1)
        badge = page.select_one(".account-label .new-user-badge")
        self.assertEqual(badge.get_text(), "新用戶")
        self.assertIn("首次加入：", badge["title"])
        self.assertNotIn("is_new_user", page.select_one(".events").get_text())

    def test_failed_login_and_guest_login_do_not_create_memberships(self):
        client = csrf_client(self.app)
        with patch("e3_tracker.assignments.routes.assignments.login_with_password", side_effect=RuntimeError("test failure")):
            self.assertEqual(client.post("/login", data={"username": "113550001", "password": "synthetic"}).status_code, 200)
        self.assertEqual(client.post("/guest-login").status_code, 302)
        self.assertFalse(self.storage.claim_assignment_membership("訪客_synthetic"))
        with self.storage._engine.connect() as conn:
            self.assertEqual(conn.execute(select(assignment_memberships)).all(), [])
        self.assertIsNone(self.page().select_one(".new-user-badge"))

    def test_verified_session_aliases_and_password_login_share_first_join(self):
        self.login("114550001")
        for raw in ("synthetic-session-one", "synthetic-session-two"):
            username = "Session-" + hashlib.sha1(raw.encode()).hexdigest()[:10]
            self.storage.save_user_cache(username, {"ts": time.time(), "result": {"courses": [], "all_assignments": []}})
            client = csrf_client(self.app)
            with patch("e3_tracker.assignments.services.session_identity.fetch_session_identity", return_value={"student_number": "114550001", "name": "示範用戶"}):
                self.assertEqual(client.post("/login", data={"login_type": "session", "moodle_session": raw}).status_code, 302)
        events = [event for event in self.storage.recent_traffic_events(500) if event["action"] == "login_success"]
        self.assertEqual([event["meta"]["is_new_user"] for event in events], [True, False, False])

    def test_session_only_new_student_is_marked_once(self):
        for username in ("Session-member-one", "Session-member-two"):
            self.storage.save_student_number(username, "115550001")
        self.assertTrue(self.storage.claim_assignment_membership("Session-member-one"))
        self.assertFalse(self.storage.claim_assignment_membership("Session-member-two"))
        self.assertFalse(self.storage.claim_assignment_membership("115550001"))

    def test_unknown_session_identity_does_not_guess_another_students_account(self):
        self.assertTrue(self.storage.claim_assignment_membership("Session-unknown"))
        self.assertFalse(self.storage.claim_assignment_membership("Session-unknown"))
        self.assertTrue(self.storage.claim_assignment_membership("112550002"))

    def test_traffic_reset_and_storage_restart_do_not_make_returning_user_new(self):
        self.login("112550003")
        self.assertEqual(self.admin.post("/admin/traffic/reset").status_code, 302)
        reloaded = PersistentStorage(str(self.storage._engine.url))
        try:
            self.assertFalse(reloaded.claim_assignment_membership("112550003"))
        finally:
            reloaded._engine.dispose()
        self.login("112550003")
        self.assertIsNone(self.page().select_one(".events .new-user-badge"))

    def test_account_badge_expires_after_seven_days(self):
        self.storage.save_user_profile("113550003", "Old", "O")
        self.assertTrue(self.storage.claim_assignment_membership("113550003", now=time.time() - 8 * 86400))
        self.assertIsNone(self.page().select_one(".account-label .new-user-badge"))

    def test_browser_cannot_forge_first_join_badge(self):
        self.assertEqual(self.admin.post("/ui-event", json={"action": "login_success", "meta": {"is_new_user": True}}).status_code, 200)
        self.assertNotIn("is_new_user", self.storage.recent_traffic_events(500)[-1]["meta"])
        self.assertIsNone(self.page().select_one(".events .new-user-badge"))

    def test_upgrade_baselines_existing_accounts_without_marking_them_new(self):
        self.storage.save_user_profile("112550004", "Existing", "E")
        self.storage.save_student_number("Session-legacy", "112550004")
        self.storage.save_web_session("legacy-only-token", "Session-without-profile")
        with self.storage._engine.begin() as conn:
            conn.execute(users_table.insert().values(username="訪客_legacy", created_at="2026-01-01", is_guest=1, is_admin=0))
            conn.execute(delete(assignment_memberships))
            conn.execute(text("DELETE FROM e3_schema_migrations WHERE version='0011_assignment_memberships'"))
        self.assertEqual(migrations.run_migrations(self.storage._engine), ["0011_assignment_memberships"])
        self.assertFalse(self.storage.claim_assignment_membership("112550004"))
        self.assertFalse(self.storage.claim_assignment_membership("Session-legacy"))
        self.assertFalse(self.storage.claim_assignment_membership("Session-without-profile"))
        self.assertEqual(migrations.run_migrations(self.storage._engine), [])
        with self.storage._engine.connect() as conn:
            self.assertTrue(all(row.is_new == 0 for row in conn.execute(select(assignment_memberships))))
        self.assertIsNone(self.page().select_one(".new-user-badge"))

    def test_separate_workers_cannot_claim_same_student_twice(self):
        other = PersistentStorage(str(self.storage._engine.url))
        try:
            with ThreadPoolExecutor(max_workers=2) as workers:
                results = list(workers.map(lambda store: store.claim_assignment_membership("115550005"), [self.storage, other]))
            self.assertEqual(sorted(results), [False, True])
        finally:
            other._engine.dispose()


if __name__ == "__main__":
    unittest.main()
