import threading
import unittest

from sqlalchemy import create_engine

from e3_tracker.shared.study_recall_favorites_runtime import (
    _ensure_favorite_table,
    _favorite_count,
    _favorite_rows,
    _set_favorite,
)


class FakeStorage:
    def __init__(self):
        self._engine = create_engine("sqlite:///:memory:")
        self._lock = threading.Lock()
        self.sessions = {
            7: {
                "id": 7,
                "subject": "離散數學",
                "title": "Relation 筆記",
                "study_date": "2026-08-19",
                "key_concepts": [
                    {
                        "concept": "反對稱關係",
                        "topic": "Relation",
                        "card_type": "concept",
                        "recall_cue": "xRy 且 yRx 時要想到什麼？",
                        "core_summary": "若 xRy 且 yRx，則 x=y。",
                        "explanation": "反對稱限制雙向關係只能出現在同一元素。",
                        "memory_hint": "雙向就必須同一點",
                    },
                    {
                        "concept": "關係判斷例題",
                        "topic": "Relation",
                        "card_type": "example",
                        "explanation": "判斷給定關係是否反對稱。",
                    },
                ],
            }
        }

    def get_study_recall_session(self, session_id):
        return self.sessions.get(int(session_id))


class StudyRecallFavoritesRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.storage = FakeStorage()
        _ensure_favorite_table(self.storage)

    def tearDown(self):
        self.storage._engine.dispose()

    def test_favorites_are_persistent_and_user_scoped(self):
        self.assertTrue(_set_favorite(self.storage, "bryan", 7, 0, True))
        self.assertEqual(_favorite_count(self.storage, "bryan"), 1)
        self.assertEqual(_favorite_count(self.storage, "other"), 0)

        rows = _favorite_rows(self.storage, "bryan")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "反對稱關係")
        self.assertEqual(rows[0]["subject"], "離散數學")
        self.assertEqual(rows[0]["session_title"], "Relation 筆記")
        self.assertEqual(rows[0]["url"], "/admin/study-recall?session_id=7#concept-0")

    def test_toggle_can_remove_favorite(self):
        _set_favorite(self.storage, "bryan", 7, 1, True)
        self.assertFalse(_set_favorite(self.storage, "bryan", 7, 1, False))
        self.assertEqual(_favorite_rows(self.storage, "bryan"), [])

    def test_missing_cards_are_rejected(self):
        with self.assertRaises(LookupError):
            _set_favorite(self.storage, "bryan", 7, 99, True)

    def test_stale_favorite_is_cleaned_when_note_disappears(self):
        _set_favorite(self.storage, "bryan", 7, 0, True)
        self.storage.sessions.pop(7)
        self.assertEqual(_favorite_rows(self.storage, "bryan"), [])
        self.assertEqual(_favorite_count(self.storage, "bryan"), 0)


if __name__ == "__main__":
    unittest.main()
