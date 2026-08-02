import tempfile
import unittest
from pathlib import Path

from reprocess_study_notes import audit_relation_associations, prune_repeated_relation_associations
from e3_tracker.shared.storage import PersistentStorage


class StudyRecallRelationTests(unittest.TestCase):
    def test_audit_counts_duplicate_associations(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PersistentStorage(str(Path(temp_dir) / "relations.sqlite3"))
            try:
                storage.create_study_recall_session(
                    study_date="2026-07-22",
                    subject="線性代數",
                    title="關聯測試",
                    image_filenames=[],
                    summary="",
                    key_concepts=[
                        {"concept": "甲", "relations": [{"session_id": 1, "concept_index": 1, "association": "同一句。"}]},
                        {"concept": "乙", "relations": [{"session_id": 1, "concept_index": 2, "association": "同一句。"}]},
                        {"concept": "丙", "relations": []},
                    ],
                )

                result = audit_relation_associations(storage)

                self.assertEqual(result["pairs"], 2)
                self.assertEqual(result["unique_associations"], 1)
                self.assertEqual(result["duplicate_groups"], [{"count": 2, "association": "同一句。"}])
            finally:
                storage._engine.dispose()

    def test_prune_repeated_relation_associations_keeps_only_one_pair(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PersistentStorage(str(Path(temp_dir) / "relations.sqlite3"))
            try:
                session_id = storage.create_study_recall_session(
                    study_date="2026-07-22",
                    subject="線性代數",
                    title="關聯測試",
                    image_filenames=[],
                    summary="",
                    key_concepts=[
                        {
                            "concept": "向量空間",
                            "relations": [
                                {
                                    "session_id": 1,
                                    "concept_index": 1,
                                    "title": "子空間",
                                    "association": "向量空間與子空間都依封閉性判斷，可一起檢查。",
                                }
                            ],
                        },
                        {
                            "concept": "子空間",
                            "relations": [
                                {
                                    "session_id": 1,
                                    "concept_index": 0,
                                    "title": "向量空間",
                                    "association": "向量空間與子空間都依封閉性判斷，可一起檢查。",
                                },
                                {
                                    "session_id": 1,
                                    "concept_index": 2,
                                    "title": "線性組合",
                                    "association": "子空間與線性組合都依封閉性判斷，可一起檢查。",
                                },
                            ],
                        },
                        {
                            "concept": "線性組合",
                            "relations": [
                                {
                                    "session_id": 1,
                                    "concept_index": 1,
                                    "title": "子空間",
                                    "association": "子空間與線性組合都依封閉性判斷，可一起檢查。",
                                }
                            ],
                        },
                    ],
                )
                self.assertEqual(session_id, 1)

                result = prune_repeated_relation_associations(storage)
                session = storage.get_study_recall_session(session_id)

                self.assertEqual(result["pairs_removed"], 1)
                self.assertEqual(result["relations_removed"], 2)
                self.assertEqual(len(session["key_concepts"][0]["relations"]), 1)
                self.assertEqual(len(session["key_concepts"][1]["relations"]), 1)
                self.assertEqual(session["key_concepts"][2]["relations"], [])
            finally:
                storage._engine.dispose()

    def test_prune_removes_generic_fallback_relation(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PersistentStorage(str(Path(temp_dir) / "relations.sqlite3"))
            try:
                session_id = storage.create_study_recall_session(
                    study_date="2026-07-22",
                    subject="線性代數",
                    title="關聯測試",
                    image_filenames=[],
                    summary="",
                    key_concepts=[
                        {
                            "concept": "基底",
                            "relations": [
                                {
                                    "session_id": 1,
                                    "concept_index": 1,
                                    "title": "座標",
                                    "association": "這兩張卡屬於同一份筆記中的直接相關觀念；可一起對照複習。",
                                }
                            ],
                        },
                        {"concept": "座標", "relations": []},
                    ],
                )

                result = prune_repeated_relation_associations(storage)
                session = storage.get_study_recall_session(session_id)

                self.assertEqual(result["pairs_removed"], 1)
                self.assertEqual(session["key_concepts"][0]["relations"], [])
            finally:
                storage._engine.dispose()


if __name__ == "__main__":
    unittest.main()
