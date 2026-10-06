"""Admin-only aggregate adoption, identity deduplication and persistent feature usage."""

import json
import os
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from unittest.mock import patch
from bs4 import BeautifulSoup
from sqlalchemy import delete, select
from e3_tracker.platform.application import create_app
from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.platform.persistence.core_schema import users_table, traffic_events_table
from e3_tracker.platform.persistence import migrations
from e3_tracker.platform.storage import PersistentStorage
from e3_tracker.assignments.domain.usage import feature_for_event, student_identity
from e3_tracker.assignments.persistence.usage_schema import feature_usage, usage_state
from e3_tracker.assignments.persistence.notification_schema import line_bindings, push_subscriptions, notification_settings
from e3_tracker.assignments.services.admin_analytics import analytics_window, build_assignment_analytics
from tests.security_helpers import csrf_client


class AssignmentAnalyticsTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(os.environ, {
            "E3_ENV": "development", "RAILWAY_ENVIRONMENT_ID": "", "RAILWAY_ENVIRONMENT_NAME": "",
            "RAILWAY_ENVIRONMENT": "", "E3_CACHE_DIR": self.directory.name, "E3_DATABASE_URL": "",
            "DATABASE_URL": "", "E3_CANONICAL_HOST": "", "E3_SESSION_COOKIE_SECURE": "0",
            "E3_YOUTUBE_AUTO_SYNC_ENABLED": "0", "E3_SESSION_PROFILE_WORKER": "0",
            "E3_NOTIFICATIONS_WORKER": "0", "OPENAI_API_KEY": "",
        })
        self.environment.start()
        self.app = create_app()
        self.storage = self.app.extensions["e3_storage"]
        self.client = csrf_client(self.app)
        self.storage.save_web_session("analytics-admin", "112550103", is_admin=True)
        self.storage.save_web_session("analytics-session", "Session-one")
        self.storage.save_student_number("Session-one", "112550103")
        self.storage.save_web_session("analytics-second", "113550092")
        self.storage.save_web_session("analytics-third", "114513001")
        self.storage.save_web_session("analytics-unknown", "Session-unknown")
        self.storage.save_web_session("analytics-guest", "訪客_demo", is_guest=True)
        for username in ("112550103", "Session-one", "113550092", "114513001", "Session-unknown", "訪客_demo"):
            self.storage.save_user_profile(username, "Test account", "T")
        self.login("analytics-admin")

    def login(self, token):
        with self.client.session_transaction() as browser:
            browser["session_token"] = token

    def tearDown(self):
        self.storage._engine.dispose()
        self.environment.stop()
        self.directory.cleanup()

    def snapshot(self, **params):
        window = analytics_window(params)
        return build_assignment_analytics(self.storage.assignment_usage_snapshot(window["start"], window["end"]), window)

    def ids(self):
        with self.storage._engine.connect() as conn:
            return {row.username: row.id for row in conn.execute(select(users_table.c.id, users_table.c.username))}

    def test_students_and_codes_are_deduplicated_with_unknowns_separate(self):
        result = self.snapshot()
        self.assertEqual((result["total"], result["identified"], result["unidentified"]), (4, 3, 1))
        self.assertEqual({row["code"]: row["count"] for row in result["cohorts"]}, {"112": 1, "113": 1, "114": 1})
        self.assertEqual({row["code"]: row["count"] for row in result["departments"]}, {"550": 2, "513": 1})
        self.assertEqual(student_identity({"username": "Session-112550103"}), "")
        self.assertEqual(student_identity({"username": "１１２５５０１０３"}), "")

    def test_daily_users_are_taipei_calendar_days_persistent_and_deduplicated(self):
        midnight = TAIPEI_TZ.localize(datetime(2026, 10, 6)).timestamp()
        for username, action, ts in (
            ("114513001", "usage_calendar", midnight - 1),
            ("112550103", "login_success", midnight + 1),
            ("Session-one", "heartbeat", midnight + 2),
            ("113550092", "usage_search", midnight + 3),
            ("Session-unknown", "heartbeat", midnight + 4),
        ):
            self.storage.record_assignment_usage(username, action, meta={"site": "assignments"}, now=ts)
        self.storage.record_assignment_usage("訪客_demo", "heartbeat", now=midnight + 5)
        self.storage.record_assignment_usage("114513001", "study_plan_saved", meta={"site": "study"}, now=midnight + 5)
        self.storage.record_assignment_usage("114513001", "notification_line_linked", meta={"activity_only": True}, now=midnight + 5)
        self.assertEqual(self.storage.assignment_daily_user_count(now=midnight - 1), 1)
        self.assertEqual(self.storage.assignment_daily_user_count(now=midnight + 100), 3)
        self.assertEqual(self.storage.assignment_daily_user_count(now=midnight + 86400), 0)
        self.storage.clear_traffic_events()
        other = PersistentStorage(str(self.storage._engine.url))
        try:
            self.assertEqual(other.assignment_daily_user_count(now=midnight + 100), 3)
        finally:
            other._engine.dispose()
        self.assertTrue(all(row["feature"] != "__presence" for row in self.storage.assignment_usage_snapshot("2026-10-06", "2026-10-06")["usage"]))

    def test_quiet_feature_usage_updates_public_daily_users_without_activity_noise(self):
        self.login("analytics-second")
        self.client.post("/ui-event", json={"action": "usage_calendar"})
        self.client.post("/ui-event", json={"action": "usage_calendar"})
        self.assertEqual(self.storage.recent_traffic_events(500), [])
        response = self.app.test_client().get("/")
        values = BeautifulSoup(response.get_data(as_text=True), "html.parser").select(".home-stats dd")
        self.assertEqual(values[0].get_text(strip=True), "1人")

    def test_bindings_count_people_not_devices_and_only_effective_enabled_channels(self):
        ids = self.ids()
        with self.storage._engine.begin() as conn:
            for index, username in enumerate(("112550103", "Session-one", "113550092", "訪客_demo")):
                conn.execute(line_bindings.insert().values(user_id=ids[username], target_hash=f"hash{index}", target="SECRET_LINE_TARGET"))
            for index in range(3):
                conn.execute(push_subscriptions.insert().values(user_id=ids["113550092"], endpoint_hash=f"endpoint{index}", subscription="SECRET_PUSH_KEY"))
            for username, prefs in (
                ("112550103", {"line_enabled": True, "new_assignment": True, "due_reminder": True}),
                ("Session-one", {"line_enabled": True, "new_assignment": True, "due_reminder": True}),
                ("113550092", {"browser_enabled": True, "new_assignment": True, "due_reminder": False}),
                ("114513001", {"line_enabled": True, "new_assignment": True, "due_reminder": True}),
            ):
                conn.execute(notification_settings.insert().values(user_id=ids[username], preferences=json.dumps(prefs), initialized=0, sync_after=0))
        result = self.snapshot()
        self.assertEqual((result["line"], result["browser"]), (2, 1))
        self.assertEqual(result["enabled"], {"line": 1, "browser": 1, "new_assignment": 2, "due_reminder": 1})
        self.storage.save_google_tokens("Session-one", {"refresh_token": "GOOGLE_SECRET"})
        self.storage.save_google_tokens("112550103", {"refresh_token": "GOOGLE_SECRET_TWO"})
        self.assertEqual(self.snapshot()["google"], 1)
        response = self.client.get("/admin/traffic")
        html = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        for secret in ("SECRET_LINE_TARGET", "SECRET_PUSH_KEY", "GOOGLE_SECRET"):
            self.assertNotIn(secret, html)

    def test_session_setting_without_own_binding_does_not_enable_another_identity_binding(self):
        ids = self.ids()
        with self.storage._engine.begin() as conn:
            conn.execute(line_bindings.insert().values(user_id=ids["112550103"], target_hash="line", target="secret"))
            conn.execute(notification_settings.insert().values(user_id=ids["Session-one"], preferences='{"line_enabled":true,"new_assignment":true}', initialized=0, sync_after=0))
        self.assertEqual(self.snapshot()["enabled"]["line"], 0)

    def test_usage_persists_beyond_event_rotation_and_deduplicates_session_identities(self):
        self.storage.record_assignment_usage("112550103", "usage_calendar")
        self.storage.record_assignment_usage("Session-one", "usage_calendar")
        self.storage.record_assignment_usage("113550092", "usage_calendar")
        self.storage.clear_traffic_events()
        other = PersistentStorage(str(self.storage._engine.url))
        try:
            window = analytics_window({})
            snapshot = other.assignment_usage_snapshot(window["start"], window["end"])
            result = build_assignment_analytics(snapshot, window)
            row = next(item for item in result["features"] if item["key"] == "calendar")
            self.assertEqual((row["users"], row["count"], row["percent"]), (2, 3, 50.0))
        finally:
            other._engine.dispose()

    def test_range_includes_taipei_days_and_retention_prunes_only_old_aggregates(self):
        now = time.time()
        for age in (0, 10, 60, 731):
            self.storage.record_assignment_usage("112550103", "usage_search", now=now - age * 86400)
        for value, count in (("7d", 1), ("30d", 2), ("90d", 3), ("all", 3)):
            row = next(item for item in self.snapshot(usage_range=value)["features"] if item["key"] == "search")
            self.assertEqual(row["count"], count)
        with self.storage._engine.connect() as conn:
            self.assertEqual(len(conn.execute(select(feature_usage)).all()), 3)
        window = analytics_window({"usage_range": "invalid"}, now=TAIPEI_TZ.localize(datetime(2026, 10, 4)))
        self.assertEqual((window["range"], window["start"], window["end"]), ("30d", "2026-09-05", "2026-10-04"))

    def test_only_whitelisted_successful_assignment_account_actions_are_saved(self):
        for action, status, meta, username in (
            ("usage_calendar", "error", {}, "112550103"),
            ("study_calendar", "success", {}, "112550103"),
            ("usage_calendar", "success", {"site": "study"}, "112550103"),
            ("usage_calendar", "success", {}, "訪客_demo"),
            ("usage_calendar", "success", {}, "missing-user"),
        ):
            self.storage.record_assignment_usage(username, action, status, meta)
        with self.storage._engine.connect() as conn:
            self.assertEqual(conn.execute(select(feature_usage)).all(), [])
        self.assertIsNone(feature_for_event("google_sync", "start", {}))

    def test_ui_event_cannot_impersonate_another_account_or_leak_query_contents(self):
        self.login("analytics-second")
        response = self.client.post("/ui-event", json={"action": "usage_search", "meta": {"username": "114513001", "query": "PRIVATE_QUERY"}})
        self.assertEqual(response.status_code, 200)
        with self.storage._engine.connect() as conn:
            row = conn.execute(select(feature_usage)).mappings().one()
            self.assertEqual(row["user_id"], self.ids()["113550092"])
            self.assertEqual(set(row), {"user_id", "feature", "day", "count"})

    def test_first_use_before_cache_or_profile_exists_is_not_lost(self):
        self.storage.save_web_session("analytics-cold", "115550001")
        self.login("analytics-cold")
        self.assertEqual(self.client.post("/ui-event", json={"action": "usage_calendar"}).status_code, 200)
        self.assertIn("115550001", self.ids())
        row = next(item for item in self.snapshot()["features"] if item["key"] == "calendar")
        self.assertEqual((row["users"], row["count"]), (1, 1))

    def test_admin_only_stats_render_and_clear_operations_reset_usage(self):
        self.storage.record_assignment_usage("113550092", "usage_calendar")
        response = self.client.get("/admin/traffic")
        document = BeautifulSoup(response.get_data(as_text=True), "html.parser")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(document.select_one('[data-feature="calendar"] [data-users]').get_text(strip=True), "1")
        self.assertIsNotNone(document.select_one('[data-distribution="department"] [data-code="550"]'))
        self.client.post("/admin/traffic/reset-user", data={"username": "113550092"})
        self.assertEqual(sum(row["count"] for row in self.snapshot()["features"]), 0)
        self.storage.record_assignment_usage("112550103", "usage_calendar")
        self.client.post("/admin/traffic/reset")
        self.assertEqual(sum(row["count"] for row in self.snapshot()["features"]), 0)
        for token in ("analytics-second", "analytics-guest"):
            self.login(token)
            response = self.client.get("/admin/traffic")
            self.assertEqual(response.status_code, 302)
            self.assertNotIn("LINE 綁定成功", response.get_data(as_text=True))
        self.assertEqual(self.app.test_client().get("/admin/traffic").status_code, 302)

    def test_concurrent_workers_atomically_increment_the_same_day(self):
        other = PersistentStorage(str(self.storage._engine.url))
        try:
            with ThreadPoolExecutor(max_workers=4) as executor:
                list(executor.map(lambda index: (self.storage if index % 2 else other).record_assignment_usage("112550103", "usage_calendar"), range(30)))
            row = next(item for item in self.snapshot()["features"] if item["key"] == "calendar")
            self.assertEqual((row["users"], row["count"]), (1, 30))
        finally:
            other._engine.dispose()

    def test_concurrent_first_use_creates_only_one_account(self):
        self.storage.save_web_session("cold-workers", "115550888")
        other = PersistentStorage(str(self.storage._engine.url))
        try:
            with ThreadPoolExecutor(max_workers=4) as executor:
                list(executor.map(lambda index: (self.storage if index % 2 else other).record_assignment_usage("115550888", "usage_calendar"), range(20)))
            row = next(item for item in self.snapshot()["features"] if item["key"] == "calendar")
            self.assertEqual((row["users"], row["count"]), (1, 20))
        finally:
            other._engine.dispose()

    def test_empty_accounts_produce_zero_percentages_not_errors(self):
        window = analytics_window({})
        result = build_assignment_analytics({"accounts": [], "usage": [], "settings": [],
            "linked": {"line": set(), "browser": set(), "google": set()},
            "state": {"started_at": time.time(), "legacy_samples": 0}}, window)
        self.assertEqual((result["total"], result["identified"], result["line"]), (0, 0, 0))
        self.assertEqual(result["cohorts"], [])
        self.assertTrue(all(row["percent"] == 0 for row in result["features"]))

    def test_migration_backfills_only_retained_successes_once(self):
        self.storage.append_traffic_event({"ts": time.time(), "action": "download_excel", "status": "success", "meta": {"username": "112550103"}}, 500)
        self.storage.append_traffic_event({"ts": time.time(), "action": "google_sync", "status": "error", "meta": {"username": "113550092"}}, 500)
        with self.storage._engine.begin() as conn:
            conn.execute(delete(migrations.history).where(migrations.history.c.version == "0010_assignment_usage"))
            conn.execute(delete(usage_state))
        self.assertEqual(migrations.run_migrations(self.storage._engine), ["0010_assignment_usage"])
        self.assertEqual(migrations.run_migrations(self.storage._engine), [])
        result = self.snapshot()
        self.assertEqual(result["legacy_samples"], 1)
        row = next(item for item in result["features"] if item["key"] == "excel")
        self.assertEqual((row["users"], row["count"]), (1, 1))


if __name__ == "__main__":
    unittest.main()
