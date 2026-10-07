"""Session rotation affects accounts, not the number of verified people."""

import os
import tempfile
import time
import unittest
from unittest.mock import patch

from bs4 import BeautifulSoup

from e3_tracker.platform.application import create_app
from e3_tracker.platform.services.account_identity import AccountIdentities
from e3_tracker.platform.services.traffic import TrafficTracker
from e3_tracker.platform.services.traffic_trends import build_traffic_trend
from tests.security_helpers import csrf_client


NUMBER = "112550101"
PROFILES = [{"username": NUMBER, "profile_name": "王小明"},
            {"username": "Session-old", "student_number": NUMBER, "profile_name": ""},
            {"username": "Session-new", "student_number": NUMBER, "profile_name": ""}]


class TrafficIdentityTests(unittest.TestCase):
    def test_retained_session_aliases_deduplicate_without_rewriting_state(self):
        now = time.time()
        tracker = TrafficTracker(state_loader=lambda: {
            "user_totals": {NUMBER: 10, "Session-old": 2, "Session-new": 1},
            "active_users": {name: now for name in (NUMBER, "Session-old", "Session-new")},
            "user_last_seen": {name: now for name in (NUMBER, "Session-old", "Session-new")},
            "hourly_buckets": {int(now): [NUMBER, "Session-old", "Session-new"]},
            "hourly_series": [{"ts": int(now), "count": 3}],
        }, profile_loader=lambda: PROFILES)
        self.assertEqual(tracker.snapshot()["total_users"], 1)
        self.assertEqual(tracker.snapshot()["online"], 1)
        self.assertEqual(tracker.snapshot()["daily_users"], 1)
        rows = tracker.user_breakdown()
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]["username"], rows[0]["count"], rows[0]["profile_name"]), (NUMBER, 13, "王小明"))
        self.assertEqual(tracker.hourly_buckets(), {int(now): {NUMBER}})
        self.assertEqual(tracker.hourly_series(), [{"ts": int(now), "count": 1}])
        self.assertEqual(tracker._user_total_hits, {NUMBER: 10, "Session-old": 2, "Session-new": 1})
        self.assertEqual(tracker._hourly_buckets[int(now)], {NUMBER, "Session-old", "Session-new"})

    def test_late_verified_identity_reclassifies_existing_traffic(self):
        profiles = []
        tracker = TrafficTracker(profile_loader=lambda: profiles)
        for name in ("Session-old", "Session-new"):
            tracker.record_visit("127.0.0.1", action="login_success", metadata={"username": name})
        self.assertEqual(tracker.snapshot()["total_users"], 2)
        old_version = tracker.version()
        profiles.extend(PROFILES)
        tracker.refresh_identities(force=True)
        self.assertEqual(tracker.snapshot()["total_users"], 1)
        self.assertGreater(tracker.version(), old_version)
        self.assertEqual(len(tracker.user_breakdown()), 1)
        self.assertEqual(len(tracker.recent_events()), 2)

    def test_same_name_or_ip_and_unverified_sessions_are_never_merged(self):
        profiles = [{"username": name, "profile_name": "王小明"} for name in ("Session-a", "Session-b", NUMBER, "112550102")]
        tracker = TrafficTracker(profile_loader=lambda: profiles)
        for row in profiles:
            tracker.record_visit("127.0.0.1", action="login_success", metadata=row)
        self.assertEqual(tracker.snapshot()["total_users"], 4)
        self.assertEqual(tracker.snapshot()["online"], 4)
        self.assertEqual(len(tracker.user_breakdown()), 4)

    def test_legacy_trend_fallback_uses_verified_identity(self):
        now = time.time()
        trend = build_traffic_trend([], {}, [
            {"ts": now, "action": "login_success", "meta": {"username": row["username"]}} for row in PROFILES
        ], {"range": "today", "trend": "hour"}, identity_key=AccountIdentities(PROFILES).key)
        self.assertEqual(max(trend["values"]), 1)

    def test_view_options_use_latest_cache_but_preserve_selected_raw_account(self):
        identities = AccountIdentities(PROFILES)
        options = [{"username": row["username"], "fetched_ts": index} for index, row in enumerate(PROFILES)]
        self.assertEqual(identities.view_options(options)[0]["username"], "Session-new")
        selected = identities.view_options(options, "Session-old")
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["username"], "Session-old")
        self.assertEqual(selected[0]["profile_name"], "王小明")


class TrafficIdentityPageTests(unittest.TestCase):
    def test_dashboard_groups_accounts_while_authentication_and_cache_stay_separate(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
            "E3_ENV": "development", "E3_CACHE_DIR": directory, "E3_DATABASE_URL": "", "DATABASE_URL": "",
            "E3_CANONICAL_HOST": "", "E3_SESSION_COOKIE_SECURE": "0", "E3_SESSION_PROFILE_WORKER": "0",
            "E3_NOTIFICATIONS_WORKER": "0", "E3_YOUTUBE_AUTO_SYNC_ENABLED": "0",
        }):
            app = create_app()
            storage = app.extensions["e3_storage"]
            try:
                storage.save_user_profile(NUMBER, "王小明", "王")
                for index, row in enumerate(PROFILES):
                    username = row["username"]
                    token = f"test-token-{index}"
                    storage.save_web_session(token, username)
                    if username.startswith("Session-"):
                        storage.save_student_number(username, NUMBER)
                    storage.save_user_cache(username, {"ts": index+1, "result": {"courses": [], "all_assignments": []}})
                    client = csrf_client(app)
                    with client.session_transaction() as session:
                        session.update(username=username, session_token=token)
                    client.post("/ui-event", json={"action": "login_success"}, environ_overrides={"REMOTE_ADDR":f"127.0.0.{index+1}"})
                    self.assertEqual(client.get("/admin/traffic").status_code, 302)
                storage.save_web_session("admin-token", "test-admin", is_admin=True)
                admin = csrf_client(app)
                with admin.session_transaction() as session:
                    session.update(username="test-admin", session_token="admin-token")
                html = BeautifulSoup(admin.get("/admin/traffic").get_data(as_text=True), "html.parser")
                rows = html.select("#account-usage tbody tr")
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0].select("td")[3].get_text(strip=True), "3")
                self.assertEqual(html.select_one("#system-summary .metric strong").get_text(strip=True), "1")
                self.assertIn("王小明", rows[0].get_text())
                self.assertEqual(len(html.select("#trafficViewUser option")), 1)
                self.assertEqual(html.select_one("#trafficViewUser option")["value"], "Session-new")
                self.assertEqual(admin.get("/traffic/stats").json["total_users"], 1)
                for index, row in enumerate(PROFILES):
                    self.assertEqual(storage.load_web_session(f"test-token-{index}")["username"], row["username"])
                    self.assertEqual(storage.load_user_cache(row["username"])["ts"], index+1)
            finally:
                storage._engine.dispose()


if __name__ == "__main__":
    unittest.main()
