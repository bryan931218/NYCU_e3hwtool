"""Guest privacy regressions, including legacy rows with an incorrect role flag."""

import io
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import delete, select, text, update

from e3_tracker.platform.application import create_app
from e3_tracker.platform.guest_privacy import without_guest_traffic
from e3_tracker.platform.persistence import migrations
from e3_tracker.platform.persistence.core_schema import users_table, web_sessions_table
from e3_tracker.platform.services.traffic import TrafficTracker
from e3_tracker.platform.storage import PersistentStorage
from tests.security_helpers import csrf_client

GUEST = "\u8a2a\u5ba2_abcdef"


def cache_payload():
    return {
        "ts": 1,
        "excel_data": "temporary-excel",
        "result": {
            "courses": [
                {"id": 101, "title": "Course", "assignments": [{"title": "Homework"}]}
            ],
            "all_assignments": [],
            "errors": [{"message": "temporary error"}],
        },
        "preferences": {"view_mode": "course"},
    }


class GuestStorageTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = str(Path(self.directory.name) / "test.sqlite3")
        self.storage = PersistentStorage(self.path)

    def tearDown(self):
        self.storage._engine.dispose()
        self.directory.cleanup()

    def populate_guest(self, *, active=True):
        if active:
            self.storage.save_web_session("guest-token", GUEST, is_guest=True)
        self.storage.save_user_cache(GUEST, cache_payload())
        self.storage.mark_assignment_views(GUEST, ["homework"], seen_ts=1)
        self.storage.add_feedback({"username": GUEST, "message": "temporary feedback"})
        with self.storage._engine.begin() as conn:
            user_id = conn.execute(
                select(users_table.c.id).where(users_table.c.username == GUEST)
            ).scalar_one()
            conn.execute(
                text(
                    "INSERT INTO announcement_votes (announcement_id, user_id, vote_type, created_at, updated_at) VALUES ('test', :id, 'up', 'test', 'test')"
                ),
                {"id": user_id},
            )
            conn.execute(
                text("INSERT INTO google_tokens (user_id) VALUES (:id)"),
                {"id": user_id},
            )

    def assert_no_guest_data(self):
        for table in (
            "users",
            "courses",
            "assignments",
            "assignment_views",
            "user_fetch_state",
            "fetch_errors",
            "user_preferences",
            "google_tokens",
            "web_sessions",
            "announcement_votes",
            "feedback",
        ):
            with self.subTest(table=table), self.storage._engine.connect() as conn:
                self.assertEqual(
                    conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar(), 0
                )

    def test_active_guest_is_temporary_and_never_listed_as_an_account(self):
        self.populate_guest()
        self.storage.purge_expired_guest_data(force=True)
        self.assertIsNotNone(self.storage.load_user_cache(GUEST))
        self.assertEqual(
            self.storage.load_user_preferences(GUEST)["view_mode"], "course"
        )
        self.assertEqual(self.storage.list_user_profiles(), [])
        self.assertEqual(self.storage.list_cached_users(), [])
        with self.storage._engine.connect() as conn:
            self.assertEqual(
                conn.execute(select(users_table.c.is_guest)).scalar_one(), 1
            )

    def test_logout_removes_all_related_rows_and_is_idempotent(self):
        self.populate_guest()
        self.storage.clear_web_session("guest-token")
        self.storage.clear_web_session("guest-token")
        self.assert_no_guest_data()

    def test_expired_session_load_removes_related_rows(self):
        self.populate_guest()
        with self.storage._engine.begin() as conn:
            conn.execute(update(web_sessions_table).values(expires_at=0))
        self.assertIsNone(self.storage.load_web_session("guest-token"))
        self.assert_no_guest_data()

    def test_startup_removes_abandoned_cache_even_without_session(self):
        self.populate_guest(active=False)
        other = PersistentStorage(self.path)
        other._engine.dispose()
        self.assert_no_guest_data()

    def test_another_valid_session_keeps_the_guest_cache_until_last_logout(self):
        self.populate_guest()
        self.storage.save_web_session("second-token", GUEST, is_guest=True)
        self.storage.clear_web_session("guest-token")
        self.assertIsNotNone(self.storage.load_user_cache(GUEST))
        self.storage.clear_web_session("second-token")
        self.assert_no_guest_data()

    def test_cleanup_preserves_students_and_cookie_login_accounts(self):
        self.populate_guest(active=False)
        for username in ("112550103", "Session-673ffeaeac", "visitor_regular"):
            self.storage.save_user_cache(username, cache_payload())
            self.storage.save_user_profile(username, "Test Name", "Test")
        self.storage.purge_expired_guest_data(force=True)
        self.assertIsNone(self.storage.load_user_cache(GUEST))
        self.assertEqual(
            {row["username"] for row in self.storage.list_user_profiles()},
            {"112550103", "Session-673ffeaeac", "visitor_regular"},
        )
        for username in ("112550103", "Session-673ffeaeac", "visitor_regular"):
            self.assertIsNotNone(self.storage.load_user_cache(username))
            self.assertEqual(
                self.storage.load_user_profile(username)["name"], "Test Name"
            )

    def test_cleanup_runs_again_after_throttle_window(self):
        self.populate_guest()
        self.storage.purge_expired_guest_data(force=True)
        with self.storage._engine.begin() as conn:
            conn.execute(update(web_sessions_table).values(expires_at=0))
        self.storage.purge_expired_guest_data()
        self.assertIsNotNone(self.storage.load_user_cache(GUEST))
        with patch(
            "e3_tracker.platform.persistence.accounts.time.monotonic",
            return_value=time.monotonic() + 61,
        ):
            self.storage.purge_expired_guest_data()
        self.assert_no_guest_data()

    def test_migration_cleans_incorrect_legacy_flags_and_traffic_once(self):
        self.populate_guest(active=False)
        legacy_state = {
            "total": 42,
            "ip_users": {"guest-ip": GUEST, "student-ip": "112550103"},
            "user_flags": {GUEST: False, "112550103": False},
            "user_totals": {GUEST: 3, "112550103": 5},
            "ip_totals": {"guest-ip": 3, "student-ip": 5},
        }
        with self.storage._engine.begin() as conn:
            conn.execute(
                update(users_table)
                .where(users_table.c.username == GUEST)
                .values(is_guest=0)
            )
            conn.execute(
                text("INSERT INTO traffic_state (id, payload) VALUES (1, :payload)"),
                {"payload": json.dumps(legacy_state)},
            )
            conn.execute(
                text(
                    "INSERT INTO traffic_events (username, is_guest, action, meta) VALUES (:name, 0, 'guest_import', :meta)"
                ),
                {"name": GUEST, "meta": json.dumps({"username": GUEST})},
            )
            conn.execute(
                text(
                    "INSERT INTO traffic_events (username, is_guest, action, meta) VALUES ('112550103', 0, 'login_success', '{}')"
                )
            )
            conn.execute(
                delete(migrations.history).where(
                    migrations.history.c.version == "0006_guest_retention"
                )
            )
        self.assertEqual(
            migrations.run_migrations(self.storage._engine), ["0006_guest_retention"]
        )
        self.assertEqual(migrations.run_migrations(self.storage._engine), [])
        self.assert_no_guest_data()
        state = self.storage.load_traffic_state()
        self.assertEqual(state["total"], 42)
        self.assertEqual(state["ip_totals"], {"student-ip": 5})
        self.assertNotIn(GUEST, json.dumps(state, ensure_ascii=False))
        self.assertEqual(
            [event["action"] for event in self.storage.recent_traffic_events(500)],
            ["login_success"],
        )

    def test_guest_traffic_never_reaches_event_or_state_storage(self):
        tracker = TrafficTracker(
            state_saver=self.storage.save_traffic_state,
            event_writer=lambda event: self.storage.append_traffic_event(event, 500),
        )
        tracker.record_visit(
            "guest-ip",
            action="heartbeat",
            metadata={"username": GUEST, "is_guest": False},
        )
        tracker.record_visit(
            "guest-ip", action="guest_login", metadata={"username": GUEST}
        )
        tracker.record_visit(
            "guest-ip",
            action="page_view",
            metadata={"username": "anonymous", "is_guest": True},
        )
        self.storage.append_traffic_event(
            {"action": "page_view", "meta": {"username": GUEST}}, 500
        )
        self.assertEqual(self.storage.recent_traffic_events(500), [])
        self.assertIsNone(self.storage.load_traffic_state())
        tracker.record_visit(
            "student-ip", action="login_success", metadata={"username": "112550103"}
        )
        self.assertEqual(len(self.storage.recent_traffic_events(500)), 1)
        self.assertNotIn("guest-ip", json.dumps(self.storage.load_traffic_state()))

    def test_state_filter_removes_flagged_guests_and_hourly_members(self):
        state = {
            "total": 10,
            "user_flags": {"flagged": True},
            "user_last_seen": {GUEST: 1, "flagged": 2, "student": 3},
            "hourly_buckets": {"123": [GUEST, "flagged", "student"]},
            "hourly_series": [{"ts": 123, "count": 3}],
        }
        filtered = without_guest_traffic(state)
        self.assertEqual(filtered["user_last_seen"], {"student": 3})
        self.assertEqual(filtered["hourly_series"], [{"ts": 123, "count": 1}])
        self.assertEqual(state["hourly_series"][0]["count"], 3)


class GuestFlowTests(unittest.TestCase):
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
                "E3_WEB_SECRET": "",
                "E3_DATA_ENCRYPTION_KEY": "",
                "E3_DATA_ENCRYPTION_PREVIOUS_KEYS": "",
                "E3_SESSION_COOKIE_SECURE": "0",
                "E3_CANONICAL_HOST": "",
                "E3_PROXY_HOPS": "0",
                "E3_TRUSTED_HOSTS": "",
                "E3_DEV_RELOAD": "0",
                "OPENAI_API_KEY": "",
            },
        )
        self.environment.start()
        self.app = create_app()
        self.storage = self.app.extensions["e3_storage"]
        self.client = csrf_client(self.app)

    def tearDown(self):
        self.storage._engine.dispose()
        self.environment.stop()
        self.directory.cleanup()

    def import_guest(self):
        self.assertEqual(self.client.post("/guest-login").status_code, 302)
        with self.client.session_transaction() as cookie:
            token = cookie["session_token"]
        name = self.storage.load_web_session(token)["username"]
        payload = cache_payload()
        payload["mode"] = "guest_export_v1"
        response = self.client.post(
            "/guest/import",
            data={
                "guest_file": (io.BytesIO(json.dumps(payload).encode()), "guest.json")
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertIsNotNone(self.storage.load_user_cache(name))
        self.assertEqual(self.client.get("/").status_code, 200)
        return token, name

    def test_guest_import_and_logout_leave_no_account_or_cache(self):
        token, name = self.import_guest()
        self.client.post(
            "/ui-event",
            json={
                "action": "page_view",
                "meta": {"username": "spoofed", "is_guest": False},
            },
        )
        self.assertEqual(self.client.post("/logout").status_code, 302)
        self.assertIsNone(self.storage.load_user_cache(name))
        self.assertIsNone(self.storage.load_web_session(token))
        self.assertEqual(self.storage.list_user_profiles(), [])
        self.assertEqual(self.storage.recent_traffic_events(500), [])

    def test_admin_account_table_excludes_even_active_legacy_guest_rows(self):
        self.import_guest()
        self.storage.save_user_profile("112550103", "Test Name", "Test")
        self.storage.save_web_session("admin-token", "112550103", is_admin=True)
        with self.storage._engine.begin() as conn:
            conn.execute(
                update(users_table)
                .where(
                    users_table.c.username.startswith("\u8a2a\u5ba2_", autoescape=True)
                )
                .values(is_guest=0)
            )
        admin = csrf_client(self.app)
        with admin.session_transaction() as cookie:
            cookie["session_token"] = "admin-token"
        response = admin.get("/admin/traffic")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("112550103", html)
        self.assertIn("Test Name", html)
        self.assertNotIn("\u8a2a\u5ba2_", html)
        self.assertNotIn("\u7e3d\u8a2a\u5ba2\u4eba\u6578", html)

    def test_switching_to_a_new_guest_session_cleans_old_import(self):
        token, name = self.import_guest()
        self.assertEqual(self.client.post("/guest-login").status_code, 302)
        self.assertIsNone(self.storage.load_user_cache(name))
        self.assertIsNone(self.storage.load_web_session(token))

    def test_next_request_cleans_abandoned_expired_guest(self):
        token, name = self.import_guest()
        with self.storage._engine.begin() as conn:
            conn.execute(update(web_sessions_table).values(expires_at=0))
        self.storage._guest_cleanup_at = 0
        self.assertEqual(csrf_client(self.app).get("/login").status_code, 200)
        self.assertIsNone(self.storage.load_user_cache(name))
        self.assertIsNone(self.storage.load_web_session(token))
