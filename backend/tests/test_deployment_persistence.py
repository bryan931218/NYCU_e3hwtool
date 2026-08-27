import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import text

from e3_tracker.shared.config import load_env_defaults
from e3_tracker.shared.deployment_runtime import DeploymentSafeStorage


class DeploymentPersistenceTests(unittest.TestCase):
    def test_database_url_uses_standard_railway_postgres_variable(self):
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgres://user:pass@postgres.internal:5432/app",
            },
            clear=True,
        ):
            defaults = load_env_defaults()
        self.assertEqual(
            defaults["database_url"],
            "postgres://user:pass@postgres.internal:5432/app",
        )

    def test_railway_volume_is_used_for_sqlite_and_uploads(self):
        with patch.dict(
            os.environ,
            {"RAILWAY_VOLUME_MOUNT_PATH": "/data"},
            clear=True,
        ):
            defaults = load_env_defaults()
        self.assertEqual(defaults["database_url"], "/data/e3_tracker.sqlite3")
        self.assertEqual(defaults["cache_dir"], "/data")
        self.assertEqual(defaults["study_upload_dir"], "/data/study_note_images")

    def test_restart_inventory_sync_preserves_database_synced_youtube_link(self):
        inventory = [
            {
                "subject": "測試科目",
                "sequence": 1,
                "title": "測試影片",
                "duration_seconds": 600,
                "youtube_video_id": "STATIC00001",
                "youtube_playlist_id": "STATICLIST",
                "youtube_url": "https://www.youtube.com/watch?v=STATIC00001",
            }
        ]
        synced_url = "https://www.youtube.com/watch?v=SYNCVIDEO01&list=SYNCLIST01"

        with tempfile.TemporaryDirectory() as temp_dir:
            storage = DeploymentSafeStorage(str(Path(temp_dir) / "persistent.sqlite3"))
            storage.sync_study_plan_videos(inventory)
            result = storage.sync_study_plan_youtube_links(
                [
                    {
                        "subject": "測試科目",
                        "sequence": 1,
                        "youtube_video_id": "SYNCVIDEO01",
                        "youtube_playlist_id": "SYNCLIST01",
                        "youtube_url": synced_url,
                    }
                ]
            )
            self.assertEqual(result["updated"], 1)

            # Simulate the app startup inventory sync after a redeploy.
            storage.sync_study_plan_videos(inventory)
            video = storage.list_study_plan_videos_with_records()[0]
            self.assertEqual(video["youtube_url"], synced_url)
            self.assertEqual(video["youtube_video_id"], "SYNCVIDEO01")
            storage._engine.dispose()

    def test_discrete_video_history_repair_moves_only_the_misattributed_day(self):
        inventory = [
            {
                "subject": "離散數學",
                "sequence": sequence,
                "title": f"離散數學第 {sequence} 支",
                "duration_seconds": 10000,
            }
            for sequence in (11, 12)
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = DeploymentSafeStorage(str(Path(temp_dir) / "repair.sqlite3"))
            storage.sync_study_plan_videos(inventory)
            videos = {
                item["sequence"]: item
                for item in storage.list_study_plan_videos_with_records()
            }
            video_11_id = int(videos[11]["id"])
            video_12_id = int(videos[12]["id"])
            with storage._lock, storage._engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO study_plan_activity_events "
                        "(day, video_id, previous_watched_seconds, watched_seconds, delta_seconds, updated_at) "
                        "VALUES "
                        "('2026-08-26', :video_11, 0, 1054, 1054, '2026-08-26T15:00:00'), "
                        "('2026-08-27', :video_12, 0, 1860, 1860, '2026-08-27T02:00:00'), "
                        "('2026-08-27', :video_11, 0, 48, 48, '2026-08-27T02:01:00')"
                    ),
                    {"video_11": video_11_id, "video_12": video_12_id},
                )
                conn.execute(
                    text(
                        "INSERT INTO study_time_sessions "
                        "(session_key, day, kind, video_id, label, elapsed_seconds, completed, started_at, updated_at) "
                        "VALUES "
                        "('old-video-session', '2026-08-26', 'video', :video_11, '舊標籤', 900, 1, 'x', 'x'), "
                        "('new-video-session', '2026-08-27', 'video', :video_11, '新標籤', 30, 0, 'x', 'x')"
                    ),
                    {"video_11": video_11_id},
                )

            storage.sync_study_plan_videos(inventory)
            repaired = storage.list_study_plan_activity_events(
                start_day="2026-08-26",
                end_day="2026-08-27",
            )
            repaired_by_day_sequence = {
                (item["day"], item["sequence"]): item
                for item in repaired
            }
            self.assertEqual(repaired_by_day_sequence[("2026-08-26", 12)]["delta_seconds"], 1054)
            self.assertEqual(repaired_by_day_sequence[("2026-08-27", 12)]["delta_seconds"], 806)
            self.assertEqual(repaired_by_day_sequence[("2026-08-27", 11)]["delta_seconds"], 48)
            with storage._lock, storage._engine.connect() as conn:
                sessions = conn.execute(
                    text(
                        "SELECT session_key, video_id, label FROM study_time_sessions "
                        "ORDER BY session_key"
                    )
                ).mappings().all()
                repairs = conn.execute(text("SELECT repair_key FROM e3_data_repairs")).all()
            session_by_key = {row["session_key"]: row for row in sessions}
            self.assertEqual(session_by_key["old-video-session"]["video_id"], video_12_id)
            self.assertEqual(session_by_key["old-video-session"]["label"], "離散數學第 12 支")
            self.assertEqual(session_by_key["new-video-session"]["video_id"], video_11_id)
            self.assertEqual(len(repairs), 1)

            storage.sync_study_plan_videos(inventory)
            self.assertEqual(
                len(storage.list_study_plan_activity_events(start_day="2026-08-26", end_day="2026-08-27")),
                3,
            )
            storage._engine.dispose()


if __name__ == "__main__":
    unittest.main()
