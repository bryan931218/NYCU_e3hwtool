import os
import tempfile
import unittest
from pathlib import Path

from backend.e3_tracker.api.web import create_app
from backend.e3_tracker.shared.storage import PersistentStorage


class AnnouncementVoteTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = str((Path(self.temp_dir.name) / "announcements.sqlite3").resolve())
        os.environ["E3_DATABASE_URL"] = self.db_path
        os.environ["E3_SESSION_COOKIE_SECURE"] = "0"
        self.app = create_app()
        self.app.testing = True
        self.storage = PersistentStorage(self.db_path)

    def tearDown(self):
        self.storage._engine.dispose()
        self.app = None
        self.temp_dir.cleanup()
        os.environ.pop("E3_DATABASE_URL", None)
        os.environ.pop("E3_SESSION_COOKIE_SECURE", None)

    def _set_session(self, client, username: str, *, is_admin: bool = False):
        with client.session_transaction() as session:
            session["username"] = username
            session["is_guest"] = False
            session["is_admin"] = is_admin
            session["moodle_session"] = "dummy"

    def test_announcement_votes_are_persisted_and_visible(self):
        admin_client = self.app.test_client()
        self._set_session(admin_client, "admin", is_admin=True)
        response = admin_client.post(
            "/admin/announcements",
            data={"title": "Test Announcement", "content": "Vote here"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)

        announcements = self.storage.list_announcements_with_votes(limit=10)
        self.assertEqual(len(announcements), 1)
        announcement_id = announcements[0]["id"]

        user_one = self.app.test_client()
        self._set_session(user_one, "user-one")
        vote_up = user_one.post(
            f"/announcements/{announcement_id}/vote",
            json={"vote": "up"},
        )
        self.assertEqual(vote_up.status_code, 200)
        payload = vote_up.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["announcement"]["like_count"], 1)
        self.assertEqual(payload["announcement"]["dislike_count"], 0)
        self.assertEqual(payload["announcement"]["user_vote"], "up")

        user_two = self.app.test_client()
        self._set_session(user_two, "user-two")
        vote_down = user_two.post(
            f"/announcements/{announcement_id}/vote",
            json={"vote": "down"},
        )
        self.assertEqual(vote_down.status_code, 200)
        second_payload = vote_down.get_json()
        self.assertEqual(second_payload["announcement"]["like_count"], 1)
        self.assertEqual(second_payload["announcement"]["dislike_count"], 1)
        self.assertEqual(second_payload["announcement"]["user_vote"], "down")

        clear_vote = user_two.post(
            f"/announcements/{announcement_id}/vote",
            json={"vote": "clear"},
        )
        self.assertEqual(clear_vote.status_code, 200)
        cleared_payload = clear_vote.get_json()
        self.assertEqual(cleared_payload["announcement"]["like_count"], 1)
        self.assertEqual(cleared_payload["announcement"]["dislike_count"], 0)
        self.assertFalse(cleared_payload["announcement"]["user_vote"])

        final_announcements = self.storage.list_announcements_with_votes(limit=10, username="user-one")
        self.assertEqual(final_announcements[0]["like_count"], 1)
        self.assertEqual(final_announcements[0]["dislike_count"], 0)
        self.assertEqual(final_announcements[0]["user_vote"], "up")


if __name__ == "__main__":
    unittest.main()
