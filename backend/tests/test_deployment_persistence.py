import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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


if __name__ == "__main__":
    unittest.main()
