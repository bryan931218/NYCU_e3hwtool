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
                {
                    "concept": "生成函數（generating function）定義",
                    "explanation": "生成函數是用冪級數的係數編碼一個數列。",
                    "search_keywords": ["母函數", "GF"],
                },
                {
                    "concept": "三階遞迴的特徵方程與係數解法例題",
                    "explanation": "先建立特徵方程，再由初始條件求係數。",
                },
            ],
        )
        third_id = self.storage.create_study_recall_session(
            study_date="2026-09-01",
            subject="資料結構",
            title="另一科的同名詞",
            image_filenames=[],
            summary="",
            source_transcription=[],
            key_concepts=[
                {
                    "concept": "生成函數定義",
                    "explanation": "這張卡用來驗證同名詞依科目分開索引。",
                }
            ],
        )

        response = self.client.get("/admin/study-recall/glossary")
        payload = response.get_json()
        terms = {entry["title"]: entry for entry in payload["terms"]}

        self.assertEqual(response.status_code, 200)
        self.assertIn("no-store", response.headers["Cache-Control"])
        self.assertIn("Dijkstra 鬆弛", terms)
        self.assertIn("線性獨立", terms)
        self.assertIn("生成函數", terms)
        self.assertIn("generating function", terms)
        self.assertIn("母函數", terms)
        self.assertIn("GF", terms)
        self.assertIn("特徵方程", terms)
        self.assertNotIn("定義", terms)
        self.assertNotIn(r"\(A^T A\)", terms)
        self.assertEqual(
            terms["Dijkstra 鬆弛"]["url"],
            f"/admin/study-recall?session_id={first_id}#concept-0",
        )
        self.assertEqual(terms["線性獨立"]["session_id"], second_id)
        self.assertIn("所有係數", terms["線性獨立"]["explanation"])
        scoped_generating_functions = [
            entry for entry in payload["terms"] if entry["title"] == "生成函數"
        ]
        self.assertEqual(
            {entry["subject"] for entry in scoped_generating_functions},
            {"線性代數", "資料結構"},
        )
        self.assertEqual(
            {entry["session_id"] for entry in scoped_generating_functions},
            {second_id, third_id},
        )
        linear_generating_function = next(
            entry
            for entry in scoped_generating_functions
            if entry["subject"] == "線性代數"
        )
        self.assertEqual(
            linear_generating_function["explanation_kind"], "筆記中的定義"
        )
        self.assertEqual(
            linear_generating_function["explanation"],
            "生成函數是用冪級數的係數編碼一個數列。",
        )
        gf_entry = next(
            entry
            for entry in payload["terms"]
            if entry["title"] == "GF" and entry["subject"] == "線性代數"
        )
        self.assertEqual(gf_entry["explanation_kind"], "相關重點卡")
        self.assertIn("生成函數", gf_entry["explanation"])

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
        self.assertIn("subjectBySession", page)
        self.assertIn("entry.subject !== currentSubject", page)
        self.assertIn("data-glossary-kind", page)
        self.assertIn(".glossary-term", page)


if __name__ == "__main__":
    unittest.main()
