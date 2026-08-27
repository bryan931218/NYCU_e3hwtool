import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from e3_tracker.api.web import create_app
from e3_tracker.shared.storage import PersistentStorage


class StudyUploadSyncTests(unittest.TestCase):
    @staticmethod
    def _sign_in(app, storage, username: str, token: str):
        storage.save_web_session(token, username)
        client = app.test_client()
        with client.session_transaction() as browser_session:
            browser_session["username"] = username
            browser_session["session_token"] = token
            browser_session["is_admin"] = True
        return client

    def test_job_survives_storage_reopen_and_is_scoped_to_user(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = str(Path(temp_dir) / "upload-sync.sqlite3")
            now = time.time()
            storage = PersistentStorage(database_path)
            storage.save_study_note_upload_job(
                job_id="job-one",
                username="bryan",
                status="running",
                progress=47,
                message="正在核對公式。",
                created_at=now,
                updated_at=now,
            )

            reopened = PersistentStorage(database_path)
            current = reopened.get_current_study_note_upload_job("bryan")

            self.assertIsNotNone(current)
            self.assertEqual(current["job_id"], "job-one")
            self.assertEqual(current["progress"], 47)
            self.assertIsNone(reopened.get_current_study_note_upload_job("someone-else"))
            reopened._engine.dispose()
            storage._engine.dispose()

    def test_running_job_wins_over_recent_completed_job(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PersistentStorage(str(Path(temp_dir) / "upload-current.sqlite3"))
            now = time.time()
            storage.save_study_note_upload_job(
                job_id="finished",
                username="bryan",
                status="success",
                progress=100,
                message="完成",
                session_id=9,
                created_at=now - 30,
                updated_at=now,
            )
            storage.save_study_note_upload_job(
                job_id="running",
                username="bryan",
                status="running",
                progress=22,
                message="整理中",
                created_at=now - 10,
                updated_at=now - 5,
            )

            current = storage.get_current_study_note_upload_job("bryan")

            self.assertEqual(current["job_id"], "running")
            storage._engine.dispose()

    def test_tracker_discovers_server_job_and_uploads_with_bounded_workers(self):
        root = Path(__file__).resolve().parents[2]
        tracker = (root / "frontend" / "templates" / "_study_upload_tracker.html").read_text(encoding="utf-8")
        recall = (root / "frontend" / "templates" / "study_recall.html").read_text(encoding="utf-8")

        self.assertIn("/admin/study-recall/upload-jobs/current", tracker)
        self.assertIn("visibilitychange", tracker)
        self.assertIn("Math.min(3, files.length - 1)", recall)
        self.assertIn("Promise.all(workers)", recall)

    def test_current_job_endpoint_is_shared_across_devices_but_not_users(self):
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
            try:
                now = time.time()
                storage.save_study_note_upload_job(
                    job_id="cross-device-job",
                    username="bryan",
                    status="running",
                    progress=61,
                    message="正在定位來源。",
                    created_at=now,
                    updated_at=now,
                )
                laptop = self._sign_in(app, storage, "bryan", "laptop-token")
                phone = self._sign_in(app, storage, "bryan", "phone-token")
                stranger = self._sign_in(app, storage, "other", "other-token")

                laptop_job = laptop.get("/admin/study-recall/upload-jobs/current").get_json()
                phone_job = phone.get("/admin/study-recall/upload-jobs/current").get_json()
                stranger_job = stranger.get("/admin/study-recall/upload-jobs/current").get_json()

                self.assertEqual(laptop_job["job_id"], "cross-device-job")
                self.assertEqual(phone_job["progress"], 61)
                self.assertIsNone(stranger_job["job"])
            finally:
                storage._engine.dispose()


if __name__ == "__main__":
    unittest.main()
