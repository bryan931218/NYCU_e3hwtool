import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from e3_tracker.api import web
from e3_tracker.shared.storage import PersistentStorage


class YoutubeStoryboardPersistenceTests(unittest.TestCase):
    def test_metadata_survives_storage_restart(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = str(Path(temp_dir) / "storyboards.sqlite3")
            first = PersistentStorage(database_path)
            first.upsert_youtube_storyboard_metadata(
                youtube_video_id="9dXuhVJ-L5k",
                duration_seconds=987.5,
                storyboard_spec="saved-storyboard-spec",
            )
            first._engine.dispose()

            reopened = PersistentStorage(database_path)
            saved = reopened.get_youtube_storyboard_metadata("9dXuhVJ-L5k")
            self.assertIsNotNone(saved)
            self.assertEqual(saved["duration_seconds"], 987.5)
            self.assertEqual(saved["storyboard_spec"], "saved-storyboard-spec")
            reopened._engine.dispose()

    def test_background_index_persists_a_new_video(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PersistentStorage(str(Path(temp_dir) / "storyboards.sqlite3"))
            payload = {
                "youtube_video_id": "9dXuhVJ-L5k",
                "duration_seconds": 321.0,
                "storyboard_spec": "new-video-storyboard-spec",
            }
            with patch.object(
                web,
                "fetch_youtube_storyboard_metadata",
                return_value=payload,
            ) as fetch:
                scheduled = web._start_youtube_storyboard_index(
                    storage,
                    ["9dXuhVJ-L5k", "invalid"],
                    _TestLogger(),
                )
                deadline = time.monotonic() + 2
                saved = None
                while time.monotonic() < deadline and saved is None:
                    saved = storage.get_youtube_storyboard_metadata("9dXuhVJ-L5k")
                    time.sleep(0.01)

            self.assertEqual(scheduled, 1)
            self.assertIsNotNone(saved)
            self.assertEqual(saved["storyboard_spec"], "new-video-storyboard-spec")
            fetch.assert_called_once_with("9dXuhVJ-L5k")
            storage._engine.dispose()


class _TestLogger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def exception(self, *args, **kwargs):
        pass


if __name__ == "__main__":
    unittest.main()
