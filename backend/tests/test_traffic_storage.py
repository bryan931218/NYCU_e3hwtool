import tempfile
import unittest
import time
from pathlib import Path

from e3_tracker.platform.storage import PersistentStorage
from e3_tracker.platform.services.traffic import TrafficTracker
from e3_tracker.platform.persistence import migrations
from e3_tracker.platform.persistence.core_schema import traffic_events_table
from sqlalchemy import select, delete


class TrafficStorageTests(unittest.TestCase):
    def test_server_activity_persists_without_inflating_website_usage(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PersistentStorage(str(Path(temp_dir) / "activity.sqlite3"))
            try:
                tracker = TrafficTracker(event_writer=lambda event: storage.append_traffic_event(event, max_events=200))
                before = tracker.snapshot()
                tracker.record_event("notification_line_linked", metadata={
                    "username": "Session-demo", "site": "assignments",
                })
                self.assertEqual(tracker.snapshot(), before)
                self.assertEqual(tracker.user_breakdown(), [])
                self.assertEqual(tracker.hourly_series(), [])
                reloaded = TrafficTracker(event_loader=storage.recent_traffic_events)
                self.assertEqual(reloaded.recent_events()[0]["meta"]["username"], "Session-demo")
                self.assertIsNone(reloaded.recent_events()[0]["ip"])
                self.assertEqual(reloaded.snapshot(), before)
            finally:
                storage._engine.dispose()

    def test_traffic_event_retention_keeps_the_newest_events(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PersistentStorage(str(Path(temp_dir) / "traffic.sqlite3"))
            try:
                for index in range(4):
                    storage.append_traffic_event(
                        {"ts": float(index), "ip": "127.0.0.1", "action": f"event-{index}", "status": "success", "meta": {"site": "study"}},
                        max_events=2,
                    )
                events = storage.recent_traffic_events(10)
                self.assertEqual([event["action"] for event in events], ["event-2", "event-3"])
            finally:
                storage._engine.dispose()

    def test_traffic_state_can_be_saved_twice(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PersistentStorage(str(Path(temp_dir) / "traffic.sqlite3"))
            try:
                storage.save_traffic_state({"total": 1})
                storage.save_traffic_state({"total": 2})
                self.assertEqual(storage.load_traffic_state(), {"total": 2})
            finally:
                storage._engine.dispose()

    def test_activity_survives_event_rotation_restart_and_expires_after_90_days(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "traffic.sqlite3")
            storage = PersistentStorage(path)
            try:
                now = time.time()
                for age in (91, 89, 70):
                    storage.append_traffic_event({"ts": now - age * 86400, "action": "notification_line_linked",
                                                  "meta": {"username": str(age), "site": "assignments"}}, 2)
                for index in range(10):
                    storage.append_traffic_event({"ts": now, "action": "study_plan_saved", "meta": {"site": "study"}}, 2)
                self.assertEqual([ev["meta"]["username"] for ev in storage.recent_activity_events()], ["70", "89"])
                other = PersistentStorage(path)
                try:
                    self.assertEqual(other.recent_activity_events(), storage.recent_activity_events())
                finally:
                    other._engine.dispose()
                self.assertEqual(storage.delete_traffic_events_for_user("70"), 1)
                self.assertEqual(len(storage.recent_activity_events()), 1)
                storage.clear_traffic_events()
                self.assertEqual(storage.recent_activity_events(), [])
            finally:
                storage._engine.dispose()

    def test_activity_migration_backfills_only_visible_retained_records_once(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = PersistentStorage(str(Path(directory) / "traffic.sqlite3"))
            try:
                now = time.time()
                storage.save_user_profile("112550103", "Test", "T")
                with storage._engine.begin() as conn:
                    conn.execute(delete(migrations.history).where(migrations.history.c.version == "0017_traffic_activity_retention"))
                    conn.execute(traffic_events_table.insert(), [
                        {"ts": now - age * 86400, "action": action, "status": "success", "meta": '{"username":"112550103"}', "retained_activity": 0}
                        for age, action in [(30, "login_success"), (91, "login_success"), (0, "study_plan_saved"), (0, "usage_calendar")]
                    ])
                    conn.execute(traffic_events_table.insert().values(ts=now, action="heartbeat", status="info", meta='{"username":"112550103"}'))
                self.assertEqual(migrations.run_migrations(storage._engine), ["0017_traffic_activity_retention"])
                self.assertEqual(migrations.run_migrations(storage._engine), [])
                self.assertEqual([ev["action"] for ev in storage.recent_activity_events()], ["login_success"])
                self.assertEqual(storage.assignment_daily_user_count(), 1)
                with storage._engine.connect() as conn:
                    self.assertEqual(len(conn.execute(select(traffic_events_table)).all()), 5)
            finally:
                storage._engine.dispose()


if __name__ == "__main__":
    unittest.main()
