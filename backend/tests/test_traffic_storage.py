import tempfile
import unittest
from pathlib import Path

from e3_tracker.shared.storage import PersistentStorage


class TrafficStorageTests(unittest.TestCase):
    def test_traffic_event_retention_keeps_the_newest_events(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PersistentStorage(str(Path(temp_dir) / "traffic.sqlite3"))
            try:
                for index in range(4):
                    storage.append_traffic_event(
                        {"ts": float(index), "ip": "127.0.0.1", "action": f"event-{index}", "status": "success"},
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


if __name__ == "__main__":
    unittest.main()
