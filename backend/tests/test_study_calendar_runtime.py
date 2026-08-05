import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import text

from e3_tracker.shared.deployment_runtime import DeploymentSafeStorage
from e3_tracker.shared.study_calendar_runtime import (
    _study_calendar_time_rows,
    install_study_calendar_runtime,
)


class StudyCalendarRuntimeTests(unittest.TestCase):
    def test_time_rows_sum_all_learning_sessions(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = DeploymentSafeStorage(str(Path(temp_dir) / "calendar.sqlite3"))
            with storage._lock, storage._engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO study_time_sessions ("
                        "session_key, day, kind, video_id, label, elapsed_seconds, "
                        "completed, started_at, updated_at"
                        ") VALUES "
                        "('video-a', '2026-08-05', 'video', NULL, '影片', 1800, 1, 'x', 'x'), "
                        "('study-a', '2026-08-05', 'practice', NULL, '刷題', 900, 1, 'x', 'x'), "
                        "('study-b', '2026-08-06', 'practice', NULL, '複習', 1200, 0, 'x', 'x')"
                    )
                )

            rows = _study_calendar_time_rows(
                storage,
                start_day="2026-08-01",
                end_day="2026-08-31",
            )
            self.assertEqual(
                rows,
                [
                    {
                        "date": "2026-08-05",
                        "total_seconds": 2700.0,
                        "session_count": 2,
                    },
                    {
                        "date": "2026-08-06",
                        "total_seconds": 1200.0,
                        "session_count": 1,
                    },
                ],
            )
            storage._engine.dispose()

    def test_calendar_partial_is_injected_only_once(self):
        module = SimpleNamespace(
            STUDY_HOME_TEMPLATE="<html><body><main>home</main></body></html>",
            create_app=lambda: SimpleNamespace(extensions={}, view_functions={}),
        )
        install_study_calendar_runtime(module)
        first_template = module.STUDY_HOME_TEMPLATE
        install_study_calendar_runtime(module)

        self.assertEqual(module.STUDY_HOME_TEMPLATE, first_template)
        self.assertEqual(
            module.STUDY_HOME_TEMPLATE.count(
                "__e3StudyCalendarTimeSplitInstalled"
            ),
            1,
        )
        self.assertIn("影片觀看時間", module.STUDY_HOME_TEMPLATE)
        self.assertIn("實際學習時間", module.STUDY_HOME_TEMPLATE)
        self.assertIn("calendar-time-summary", module.STUDY_HOME_TEMPLATE)
        self.assertIn("total_seconds", module.STUDY_HOME_TEMPLATE)
        self.assertNotIn("calendar-time-split", module.STUDY_HOME_TEMPLATE)


if __name__ == "__main__":
    unittest.main()
