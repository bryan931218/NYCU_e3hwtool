import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from e3_tracker.api.web import create_app
from e3_tracker.services.youtube_playlists import sync_known_youtube_playlists
from e3_tracker.shared.storage import PersistentStorage
from e3_tracker.shared.study_plan_data import STUDY_PLAN_VIDEO_INVENTORY


class YoutubePlaylistSyncTests(unittest.TestCase):
    def test_database_sync_updates_source_and_preserves_manual_override(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PersistentStorage(str(Path(temp_dir) / "sync.sqlite3"))
            storage.sync_study_plan_videos(STUDY_PLAN_VIDEO_INVENTORY)
            videos = [
                item
                for item in storage.list_study_plan_videos_with_records()
                if item["subject"] == "線性代數"
            ]
            first, second = videos[:2]
            manual_url = "https://www.youtube.com/watch?v=MANUAL00001"
            storage.update_study_plan_video_youtube(
                video_id=first["id"],
                youtube_video_id="MANUAL00001",
                youtube_playlist_id="",
                youtube_url=manual_url,
            )

            result = storage.sync_study_plan_youtube_links(
                [
                    {
                        "subject": "線性代數",
                        "sequence": first["sequence"],
                        "youtube_video_id": "AUTOSYNC001",
                        "youtube_playlist_id": "PLAYLIST001",
                        "youtube_url": "https://www.youtube.com/watch?v=AUTOSYNC001&list=PLAYLIST001",
                    },
                    {
                        "subject": "線性代數",
                        "sequence": second["sequence"],
                        "youtube_video_id": "AUTOSYNC002",
                        "youtube_playlist_id": "PLAYLIST001",
                        "youtube_url": "https://www.youtube.com/watch?v=AUTOSYNC002&list=PLAYLIST001",
                    },
                ]
            )

            self.assertEqual(result["matched"], 2)
            self.assertEqual(result["updated"], 2)
            self.assertEqual(result["manual_overrides_preserved"], 1)
            refreshed = {
                item["id"]: item
                for item in storage.list_study_plan_videos_with_records()
            }
            self.assertEqual(refreshed[first["id"]]["youtube_url"], manual_url)
            self.assertIn("AUTOSYNC002", refreshed[second["id"]]["youtube_url"])
            storage._engine.dispose()

    def test_service_fetches_playlists_and_reports_partial_failure(self):
        class FakeStorage:
            def sync_study_plan_youtube_links(self, links):
                self.links = links
                return {
                    "matched": len(links),
                    "updated": len(links),
                    "unchanged": 0,
                    "unmatched": 0,
                    "manual_overrides_preserved": 0,
                }

        sources = [
            {"subject": "科目甲", "playlist_id": "playlist-a", "url": "https://example.com/a"},
            {"subject": "科目乙", "playlist_id": "playlist-b", "url": "https://example.com/b"},
            {"subject": "科目丙", "playlist_id": "playlist-c", "url": "https://example.com/c"},
        ]

        def fake_fetch(source):
            if source["subject"] == "科目乙":
                raise RuntimeError("temporary failure")
            if source["subject"] == "科目丙":
                return []
            return [
                {
                    "subject": source["subject"],
                    "sequence": 1,
                    "youtube_video_id": "ABCDEFGHIJK",
                    "youtube_playlist_id": source["playlist_id"],
                    "youtube_url": "https://www.youtube.com/watch?v=ABCDEFGHIJK",
                }
            ]

        storage = FakeStorage()
        with patch("e3_tracker.services.youtube_playlists.fetch_youtube_playlist", side_effect=fake_fetch):
            result = sync_known_youtube_playlists(storage, sources)

        self.assertTrue(result["ok"])
        self.assertEqual(result["updated"], 1)
        self.assertEqual(result["fetched_subjects"], ["科目丙", "科目甲"])
        self.assertEqual(result["empty_subjects"], ["科目丙"])
        self.assertEqual(result["errors"][0]["subject"], "科目乙")
        self.assertEqual(result["youtube_video_ids"], ["ABCDEFGHIJK"])
        self.assertEqual(len(storage.links), 1)

    def test_admin_endpoint_returns_sync_result_without_page_reload(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(
                os.environ,
                {
                    "E3_CACHE_DIR": temp_dir,
                    "E3_DATABASE_URL": "",
                    "E3_SESSION_COOKIE_SECURE": "0",
                },
            ):
                app = create_app()

            storage = app.extensions["e3_storage"]
            token = "youtube-sync-test-session"
            storage.save_web_session(token, "test-admin")
            client = app.test_client()
            with client.session_transaction() as browser_session:
                browser_session["username"] = "test-admin"
                browser_session["session_token"] = token
                browser_session["is_admin"] = True

            page = client.get("/admin/study-settings")
            self.assertEqual(page.status_code, 200)
            html = page.get_data(as_text=True)
            self.assertIn("一鍵同步", html)
            self.assertIn("youtube-sync-form", html)
            self.assertIn("data-youtube-input", html)

            fake_result = {
                "ok": True,
                "matched": 62,
                "updated": 3,
                "unchanged": 59,
                "unmatched": 0,
                "manual_overrides_preserved": 2,
                "playlist_count": 6,
                "fetched_subjects": ["線性代數", "離散數學", "資料結構", "作業系統", "計算機組織", "演算法"],
                "empty_subjects": ["作業系統", "計算機組織", "演算法"],
                "errors": [],
                "youtube_video_ids": ["9dXuhVJ-L5k"],
            }
            with patch(
                "e3_tracker.api.web.sync_known_youtube_playlists",
                return_value=fake_result,
            ), patch(
                "e3_tracker.api.web._start_youtube_storyboard_index",
                return_value=1,
            ) as start_index:
                response = client.post(
                    "/admin/study-settings/youtube-sync",
                    headers={"Accept": "application/json"},
                )

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["result"]["updated"], 3)
            self.assertEqual(payload["result"]["storyboard_indexing"], 1)
            self.assertTrue(payload["videos"])
            start_index.assert_called_once()

            video = storage.list_study_plan_videos_with_records()[0]
            with patch(
                "e3_tracker.api.web._start_youtube_storyboard_index",
                return_value=1,
            ) as start_manual_index:
                manual_response = client.post(
                    "/admin/study-settings",
                    data={
                        "video_id": video["id"],
                        "youtube_url": "https://www.youtube.com/watch?v=NEWVIDEO001",
                    },
                )
            self.assertEqual(manual_response.status_code, 302)
            self.assertEqual(start_manual_index.call_args.args[1], ["NEWVIDEO001"])
            storage._engine.dispose()


if __name__ == "__main__":
    unittest.main()
