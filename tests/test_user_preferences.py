import tempfile
import unittest
from pathlib import Path

from backend.e3_tracker.shared.storage import PersistentStorage


class UserPreferencesTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = str((Path(self.temp_dir.name) / "prefs.sqlite3").resolve())
        self.storage = PersistentStorage(self.db_path)

    def tearDown(self):
        self.storage._engine.dispose()
        self.temp_dir.cleanup()

    def test_save_and_load_show_graded_preference(self):
        self.storage.save_user_preferences(
            "student",
            {
                "view_mode": "due",
                "show_overdue": False,
                "show_completed": False,
                "show_graded": True,
            },
        )

        prefs = self.storage.load_user_preferences("student")

        self.assertEqual(prefs["view_mode"], "due")
        self.assertFalse(prefs["show_overdue"])
        self.assertFalse(prefs["show_completed"])
        self.assertTrue(prefs["show_graded"])


if __name__ == "__main__":
    unittest.main()
