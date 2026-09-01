import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from e3_tracker.api.web import create_app


class StudyRecallGlossaryTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        with patch.dict(
            os.environ,
            {
                "E3_CACHE_DIR": self.temp_dir.name,
                "E3_DATABASE_URL": "",
                "E3_SESSION_COOKIE_SECURE": "0",
            },
        ):
            self.app = create_app()
        self.storage = self.app.extensions["e3_storage"]
        token = "study-glossary-test-session"
        self.storage.save_web_session(token, "test-admin")
        self.client = self.app.test_client()
        with self.client.session_transaction() as browser_session:
            browser_session["username"] = "test-admin"
            browser_session["session_token"] = token
            browser_session["is_admin"] = True

    def tearDown(self):
        self.storage._engine.dispose()
        self.temp_dir.cleanup()

    def test_glossary_indexes_all_existing_notes_with_card_destinations(self):
        first_id = self.storage.create_study_recall_session(
            study_date="2026-08-30",
            subject="離散數學",
            title="圖論筆記",
            image_filenames=[],
            summary="",
            source_transcription=[],
            key_concepts=[
                {
                    "concept": "Dijkstra 鬆弛",
                    "core_summary": "用目前最短距離嘗試改善相鄰頂點的距離。",
                    "explanation": "若找到更短路徑，就更新該頂點的距離。",
                },
                {"concept": "定義", "explanation": "過度泛用的標題不應成為詞條。"},
            ],
        )
        second_id = self.storage.create_study_recall_session(
            study_date="2026-08-31",
            subject="線性代數",
            title="向量空間筆記",
            image_filenames=[],
            summary="",
            source_transcription=[],
            key_concepts=[
                {
                    "concept": "線性獨立",
                    "explanation": "只有所有係數皆為零時，線性組合才等於零向量。",
                },
                {"concept": r"\(A^T A\)", "explanation": "公式標題不直接連結。"},
            ],
        )

        response = self.client.get("/admin/study-recall/glossary")
        payload = response.get_json()
        terms = {entry["title"]: entry for entry in payload["terms"]}

        self.assertEqual(response.status_code, 200)
        self.assertIn("no-store", response.headers["Cache-Control"])
        self.assertIn("Dijkstra 鬆弛", terms)
        self.assertIn("線性獨立", terms)
        self.assertNotIn("定義", terms)
        self.assertNotIn(r"\(A^T A\)", terms)
        self.assertEqual(
            terms["Dijkstra 鬆弛"]["url"],
            f"/admin/study-recall?session_id={first_id}#concept-0",
        )
        self.assertEqual(terms["線性獨立"]["session_id"], second_id)
        self.assertIn("所有係數", terms["線性獨立"]["explanation"])

    def test_recall_page_loads_the_wikipedia_style_glossary_runtime(self):
        session_id = self.storage.create_study_recall_session(
            study_date="2026-08-31",
            subject="資料結構",
            title="樹狀結構",
            image_filenames=[],
            summary="",
            source_transcription=[],
            key_concepts=[
                {
                    "concept": "平衡二元搜尋樹",
                    "core_summary": "維持樹高，讓搜尋操作保持高效率。",
                    "explanation": "旋轉操作會重新平衡子樹。",
                }
            ],
        )

        page = self.client.get(
            "/admin/study-recall", query_string={"session_id": session_id}
        ).get_data(as_text=True)

        self.assertIn('data-study-glossary-tooltip', page)
        self.assertIn('data-glossary-url="/admin/study-recall/glossary"', page)
        self.assertIn("window.applyStudyGlossary", page)
        self.assertIn(".glossary-term", page)


if __name__ == "__main__":
    unittest.main()
