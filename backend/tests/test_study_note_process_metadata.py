import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from e3_tracker.api.web import create_app
from e3_tracker.shared.storage import PersistentStorage
from e3_tracker.shared.study_note_composer import StudyNoteToolAccumulator, StudyNoteToolError
from e3_tracker.shared.study_note_quality import (
    is_study_note_process_metadata_card,
    is_study_note_process_metadata_text,
)


BAD_TITLE = "來源保留說明"
BAD_TEXT = "來源明文聲明已保留圖上所有可見文字、數字、括號、方框、圓圈、箭頭與結尾符號。"


class StudyNoteProcessMetadataTests(unittest.TestCase):
    def test_detects_source_preservation_workflow_text(self):
        self.assertTrue(is_study_note_process_metadata_text(BAD_TEXT))
        self.assertTrue(
            is_study_note_process_metadata_card(
                {
                    "concept": BAD_TITLE,
                    "core_summary": BAD_TEXT,
                    "explanation": "此卡記錄來源的保留說明文字。",
                }
            )
        )
        self.assertFalse(
            is_study_note_process_metadata_card(
                {
                    "concept": "Heap 刪除最大值",
                    "core_summary": "刪除根節點後，以最後節點補到根並向下調整。",
                    "explanation": "比較兩個子節點，與較大的子節點交換。",
                }
            )
        )

    def test_tool_composer_rejects_process_metadata_block(self):
        accumulator = StudyNoteToolAccumulator(
            source_pages=[{"image_index": 1, "transcription": BAD_TEXT}],
        )
        accumulator.execute(
            "set_note_overview",
            {"detected_topic": "Heap", "summary": "Heap 的操作。"},
        )
        with self.assertRaisesRegex(StudyNoteToolError, "not a study block"):
            accumulator.execute(
                "add_note_block",
                {
                    "block_id": "source-note",
                    "block_type": "fact",
                    "title": BAD_TITLE,
                    "topic": "來源核對",
                    "recall_cue": None,
                    "key_point": BAD_TEXT,
                    "explanation": "此卡記錄來源的保留說明文字。",
                    "details": [],
                    "example": None,
                    "pitfall": None,
                    "memory_hint": None,
                    "keywords": [],
                    "sources": [{"image_index": 1, "evidence": BAD_TEXT}],
                    "coverage_ids": [],
                    "correction": {
                        "applied": False,
                        "original": None,
                        "corrected": None,
                        "reason": None,
                    },
                },
            )

    def test_search_index_omits_existing_process_metadata_card(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PersistentStorage(str(Path(temp_dir) / "recall.sqlite3"))
            try:
                storage.create_study_recall_session(
                    study_date="2026-09-08",
                    subject="資料結構",
                    title="Heap",
                    image_filenames=["heap.jpg"],
                    summary="Heap 操作",
                    source_transcription=[
                        {"image_index": 1, "transcription": f"{BAD_TEXT}\n\nHeap 刪除最大值。"}
                    ],
                    key_concepts=[
                        {
                            "concept": BAD_TITLE,
                            "core_summary": BAD_TEXT,
                            "explanation": "此卡記錄來源的保留說明文字。",
                            "source_refs": [{"image_index": 1, "evidence": BAD_TEXT}],
                        },
                        {
                            "concept": "Heap 刪除最大值",
                            "core_summary": "刪除根節點後向下調整。",
                            "explanation": "Heap 刪除最大值。",
                            "source_refs": [{"image_index": 1, "evidence": "Heap 刪除最大值。"}],
                        },
                    ],
                )

                documents = storage._study_recall_search_documents()
                indexed_titles = {
                    card["concept_title"]
                    for document in documents
                    for card in document.get("cards") or []
                }
                self.assertNotIn(BAD_TITLE, indexed_titles)
                self.assertIn("Heap 刪除最大值", indexed_titles)
            finally:
                storage._engine.dispose()

    def test_final_validator_drops_metadata_but_keeps_real_card(self):
        good_text = "Heap 刪除最大值的方法：將最後節點移到根，並與較大的子節點交換後持續向下調整。"
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
            try:
                validator = app.extensions["study_note_output_validator"]
                coverage_builder = app.extensions["study_note_coverage_builder"]
                source_pages = [
                    {"image_index": 1, "transcription": f"{BAD_TEXT}\n\n{good_text}"}
                ]
                coverage_texts = [item["text"] for item in coverage_builder(source_pages)]
                self.assertFalse(any("可見文字" in text for text in coverage_texts))
                self.assertTrue(any("Heap 刪除最大值" in text for text in coverage_texts))
                base = {
                    "content_kind": "concept",
                    "card_type": "concept",
                    "recall_cue": "Heap",
                    "simple_example": "",
                    "example_problem": "",
                    "example_method": "",
                    "reasoning_steps": [],
                    "common_confusion": "",
                    "memory_hint": "",
                    "related_concepts": [],
                    "search_keywords": [],
                    "coverage_ids": [],
                    "correction": {"applied": False},
                }
                payload = {
                    "detected_topic": "Heap",
                    "summary": "Heap 操作",
                    "key_concepts": [
                        {
                            **base,
                            "concept": BAD_TITLE,
                            "topic": "來源核對",
                            "core_summary": BAD_TEXT,
                            "explanation": "此卡記錄來源的保留說明文字。",
                            "source_refs": [{"image_index": 1, "evidence": BAD_TEXT}],
                        },
                        {
                            **base,
                            "concept": "Heap 刪除最大值",
                            "topic": "Heap",
                            "core_summary": good_text,
                            "explanation": good_text,
                            "source_refs": [{"image_index": 1, "evidence": good_text}],
                        },
                    ],
                }

                validated = validator(payload, source_pages)
                self.assertIsNotNone(validated)
                self.assertEqual(
                    [card["concept"] for card in validated["key_concepts"]],
                    ["Heap 刪除最大值"],
                )
            finally:
                app.extensions["e3_storage"]._engine.dispose()


if __name__ == "__main__":
    unittest.main()
