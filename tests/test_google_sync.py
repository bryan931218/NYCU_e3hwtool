import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.e3_tracker.api.web import create_app
from backend.e3_tracker.shared.storage import PersistentStorage


class GoogleSyncTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = str((Path(self.temp_dir.name) / "google_sync.sqlite3").resolve())
        os.environ["E3_DATABASE_URL"] = self.db_path
        os.environ["E3_SESSION_COOKIE_SECURE"] = "0"
        os.environ["E3_GOOGLE_CLIENT_ID"] = "test-client"
        os.environ["E3_GOOGLE_CLIENT_SECRET"] = "test-secret"
        os.environ["E3_GOOGLE_REDIRECT_URI"] = "http://localhost/google/callback"
        self.app = create_app()
        self.app.testing = True
        self.storage = PersistentStorage(self.db_path)

    def tearDown(self):
        self.storage._engine.dispose()
        self.app = None
        self.temp_dir.cleanup()
        os.environ.pop("E3_DATABASE_URL", None)
        os.environ.pop("E3_SESSION_COOKIE_SECURE", None)
        os.environ.pop("E3_GOOGLE_CLIENT_ID", None)
        os.environ.pop("E3_GOOGLE_CLIENT_SECRET", None)
        os.environ.pop("E3_GOOGLE_REDIRECT_URI", None)

    def _set_session(self, client, username: str):
        with client.session_transaction() as session:
            session["username"] = username
            session["is_guest"] = False
            session["is_admin"] = False
            session["moodle_session"] = "dummy"

    def test_google_sync_uses_cached_assignments_before_refetch(self):
        username = "student"
        client = self.app.test_client()
        self._set_session(client, username)

        assignment = {
            "course_id": 123,
            "course_title": "【114下】測試課程",
            "title": "HW1",
            "url": "https://example.com/assign/1",
            "due_at": "2026-04-10 23:59",
            "due_ts": 1775836740,
            "overdue": False,
            "completed": True,
            "raw_status_text": "已評分",
            "grade_text": "95 / 100",
            "submitted_count": 10,
            "participant_count": 20,
        }
        self.storage.save_user_cache(
            username,
            {
                "result": {
                    "courses": [
                        {
                            "id": 123,
                            "title": "【114下】測試課程",
                            "url": "https://example.com/course/123",
                            "assignments": [assignment],
                            "detected_assign_links": 1,
                        }
                    ],
                    "all_assignments": [assignment],
                    "errors": [],
                },
                "excel_data": None,
                "ts": int(time.time()),
            },
        )
        self.storage.save_google_tokens(
            username,
            {
                "access_token": "token",
                "refresh_token": "refresh",
                "scope": "scope",
                "token_type": "Bearer",
                "expires_at": time.time() + 3600,
            },
        )

        selected_uid = f"{assignment['course_id']}|{assignment['title']}|{assignment['url']}"

        with patch("backend.e3_tracker.api.web.collect_assignments", side_effect=AssertionError("should not refetch")), patch(
            "backend.e3_tracker.api.web.sync_assignments_to_google_calendar",
            return_value=1,
        ) as sync_mock:
            response = client.post(
                "/google/sync",
                data={"selected_uids": json.dumps([selected_uid])},
                follow_redirects=False,
            )

        self.assertEqual(response.status_code, 302)
        sync_mock.assert_called_once()
        synced_assignments = sync_mock.call_args.args[0]
        self.assertEqual(len(synced_assignments), 1)
        self.assertEqual(synced_assignments[0]["title"], "HW1")
        self.assertEqual(synced_assignments[0]["grade_text"], "95 / 100")


if __name__ == "__main__":
    unittest.main()
