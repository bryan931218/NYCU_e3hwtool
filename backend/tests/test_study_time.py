import tempfile
import unittest
from pathlib import Path

from e3_tracker.shared.storage import PersistentStorage
from e3_tracker.shared.study_plan_data import STUDY_PLAN_VIDEO_INVENTORY


class StudyTimeTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.storage = PersistentStorage(str(Path(self.temp_dir.name) / "study-time.sqlite3"))
        self.storage.sync_study_plan_videos(STUDY_PLAN_VIDEO_INVENTORY)

    def tearDown(self):
        self.storage._engine.dispose()
        self.temp_dir.cleanup()

    def test_video_and_practice_time_are_summed_separately(self):
        video = self.storage.list_study_plan_videos_with_records()[0]
        video_summary = self.storage.record_study_time_session(
            session_key="video_session_123",
            kind="video",
            video_id=int(video["id"]),
            elapsed_seconds=120,
        )
        summary = self.storage.record_study_time_session(
            session_key="practice_session_123",
            kind="practice",
            label="矩陣題目",
            elapsed_seconds=300,
            completed=True,
        )

        self.assertEqual(video_summary["video_seconds"], 120)
        self.assertEqual(summary["video_seconds"], 120)
        self.assertEqual(summary["practice_seconds"], 300)
        self.assertEqual(summary["total_seconds"], 420)

    def test_retried_pulse_cannot_reduce_or_double_count_session(self):
        for elapsed in (15, 30, 30, 20):
            summary = self.storage.record_study_time_session(
                session_key="practice_session_retry",
                kind="practice",
                elapsed_seconds=elapsed,
            )

        self.assertEqual(summary["practice_seconds"], 30)
        self.assertEqual(summary["session_count"], 1)

    def test_sessions_can_be_listed_and_deleted(self):
        summary = self.storage.record_study_time_session(
            session_key="practice_session_history",
            kind="practice",
            label="線代題庫",
            elapsed_seconds=245,
            completed=True,
        )

        sessions = self.storage.list_study_time_sessions(day=summary["day"])
        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0]["label"], "線代題庫")
        self.assertEqual(sessions[0]["elapsed_seconds"], 245)
        self.assertTrue(sessions[0]["completed"])

        self.assertTrue(self.storage.delete_study_time_session("practice_session_history"))
        self.assertEqual(self.storage.list_study_time_sessions(day=summary["day"]), [])


if __name__ == "__main__":
    unittest.main()
