import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from e3_tracker.api.web import create_app
from e3_tracker.shared.source_localization import SOURCE_PAGE_INDEX_VERSION
from e3_tracker.shared.storage import PersistentStorage


class StudyRecallSearchTests(unittest.TestCase):
    def test_note_title_can_be_renamed_without_changing_its_content(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PersistentStorage(str(Path(temp_dir) / "rename.sqlite3"))
            try:
                session_id = storage.create_study_recall_session(
                    study_date="2026-07-23",
                    subject="線性代數",
                    title="原始名稱",
                    image_filenames=["page-1.jpg"],
                    summary="原始摘要",
                    source_transcription=[],
                    key_concepts=[{"concept": "向量空間"}],
                )

                self.assertTrue(
                    storage.rename_study_recall_session(session_id, "  新的 筆記名稱  ")
                )
                renamed = storage.get_study_recall_session(session_id)

                self.assertEqual(renamed["title"], "新的 筆記名稱")
                self.assertEqual(renamed["summary"], "原始摘要")
                self.assertEqual(renamed["key_concepts"][0]["concept"], "向量空間")
            finally:
                storage._engine.dispose()

    def test_section_ocr_corrects_a_polluted_transcription_and_source_page(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PersistentStorage(str(Path(temp_dir) / "search.sqlite3"))
            try:
                session_id = storage.create_study_recall_session(
                    study_date="2026-07-22",
                    subject="線性代數",
                    title="跨頁來源測試",
                    image_filenames=["page-1.jpg", "page-2.jpg"],
                    summary="",
                    source_transcription=[
                        {
                            "image_index": 1,
                            "transcription": "秩的重要性質。錯誤混入：rank 1 可寫成外積。",
                            "localization_index": {
                                "version": SOURCE_PAGE_INDEX_VERSION,
                                "kind": "sections",
                                "lines": [
                                    {
                                        "line_id": 1,
                                        "text": "秩的重要性質：rank(A)=rank(A^T)。",
                                    }
                                ],
                            },
                        },
                        {
                            "image_index": 2,
                            "transcription": "若 rank(A)=1，則存在 x,y 使 A=xy^T。",
                            "localization_index": {
                                "version": SOURCE_PAGE_INDEX_VERSION,
                                "kind": "sections",
                                "lines": [
                                    {
                                        "line_id": 1,
                                        "text": "若 rank(A)=1，則存在 x,y 使 A=xy^T。",
                                    }
                                ],
                            },
                        },
                    ],
                    key_concepts=[
                        {
                            "concept": "rank 1 外積形式",
                            "search_keywords": ["rank 1", "xy^T"],
                            "source_refs": [
                                {
                                    "image_index": 1,
                                    "evidence": "若 rank(A)=1，則存在 x,y 使 A=xy^T。",
                                    "bbox": {
                                        "left": 20,
                                        "top": 20,
                                        "right": 900,
                                        "bottom": 300,
                                        "source_image_index": 1,
                                    },
                                }
                            ],
                        }
                    ],
                )

                results = storage.search_study_recall_pages(
                    query="rank 1 xy",
                    subject="線性代數",
                )

                self.assertTrue(results)
                self.assertEqual(results[0]["image_index"], 2)
                self.assertEqual(results[0]["image_filename"], "page-2.jpg")
                self.assertIsNone(results[0]["bbox"])
            finally:
                storage._engine.dispose()

    def test_search_does_not_repeat_expensive_fuzzy_source_localization(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PersistentStorage(str(Path(temp_dir) / "search.sqlite3"))
            try:
                pages = [
                    {
                        "image_index": image_index,
                        "transcription": f"第 {image_index} 頁 線性映射與基底內容",
                    }
                    for image_index in range(1, 24)
                ]
                concepts = [
                    {
                        "concept": f"線性映射重點 {concept_index}",
                        "explanation": "基底矩陣與座標轉換",
                        "source_refs": [
                            {
                                "image_index": concept_index % 23 + 1,
                                "evidence": f"第 {concept_index % 23 + 1} 頁 線性映射與基底內容",
                            }
                        ],
                    }
                    for concept_index in range(70)
                ]
                storage.create_study_recall_session(
                    study_date="2026-07-23",
                    subject="線性代數",
                    title="搜尋效能測試",
                    image_filenames=[f"page-{index}.jpg" for index in range(1, 24)],
                    summary="線性映射",
                    source_transcription=pages,
                    key_concepts=concepts,
                )

                started_at = time.perf_counter()
                results = storage.search_study_recall_pages(query="基底矩陣")
                elapsed = time.perf_counter() - started_at

                self.assertTrue(results)
                self.assertLess(elapsed, 1.0)
            finally:
                storage._engine.dispose()

    def test_hybrid_search_normalizes_latex_and_filters_content_types(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PersistentStorage(str(Path(temp_dir) / "hybrid-search.sqlite3"))
            try:
                session_id = storage.create_study_recall_session(
                    study_date="2026-07-24",
                    subject="線性代數",
                    title="換基底與消去法",
                    image_filenames=["basis.jpg", "elimination.jpg"],
                    summary="基底矩陣與線性方程組",
                    source_transcription=[
                        {
                            "image_index": 1,
                            "transcription": r"換基矩陣公式為 \(P=B'^{-1}B\)。",
                        },
                        {
                            "image_index": 2,
                            "transcription": "例題：使用高斯消去法解聯立方程式。",
                        },
                    ],
                    key_concepts=[
                        {
                            "concept": "換基矩陣",
                            "topic": "基底轉換",
                            "card_type": "concept",
                            "core_summary": r"\(P=B'^{-1}B\)",
                            "search_keywords": ["換基矩陣", "P", "B"],
                            "source_refs": [
                                {
                                    "image_index": 1,
                                    "evidence": r"換基矩陣公式為 \(P=B'^{-1}B\)。",
                                }
                            ],
                        },
                        {
                            "concept": "高斯消去例題",
                            "topic": "線性方程組",
                            "card_type": "example",
                            "example_problem": "解二元聯立方程式",
                            "example_method": "依序消去未知數",
                            "source_refs": [
                                {
                                    "image_index": 2,
                                    "evidence": "例題：使用高斯消去法解聯立方程式。",
                                }
                            ],
                        },
                    ],
                )

                formula_results = storage.search_study_recall_pages(
                    query="P=B'^-1B",
                    content_type="formula",
                )
                precise_results = storage.search_study_recall_pages(
                    query="P=B'^-1B",
                    session_id=session_id,
                )
                example_results = storage.search_study_recall_pages(
                    query="高斯消去",
                    content_type="example",
                    session_id=session_id,
                )
                concept_results = storage.search_study_recall_pages(
                    query="高斯消去",
                    content_type="concept",
                    session_id=session_id,
                )

                self.assertTrue(formula_results)
                self.assertEqual(formula_results[0]["image_index"], 1)
                self.assertEqual(formula_results[0]["match_reason"], "公式與符號相符")
                self.assertIn(r"\(", formula_results[0]["excerpt"])
                self.assertEqual(
                    [result["image_index"] for result in precise_results],
                    [1],
                )
                self.assertTrue(example_results)
                self.assertEqual(example_results[0]["card_type"], "example")
                self.assertFalse(concept_results)
            finally:
                storage._engine.dispose()

    def test_search_cache_refreshes_when_a_new_note_is_added(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PersistentStorage(str(Path(temp_dir) / "search-cache.sqlite3"))
            try:
                storage.create_study_recall_session(
                    study_date="2026-07-24",
                    subject="線性代數",
                    title="第一份",
                    image_filenames=["first.jpg"],
                    summary="向量空間",
                    source_transcription=[
                        {"image_index": 1, "transcription": "向量空間與子空間"}
                    ],
                    key_concepts=[],
                )
                self.assertTrue(storage.search_study_recall_pages(query="向量空間"))

                second_id = storage.create_study_recall_session(
                    study_date="2026-07-25",
                    subject="離散數學",
                    title="第二份",
                    image_filenames=["second.jpg"],
                    summary="最短路徑",
                    source_transcription=[
                        {"image_index": 1, "transcription": "Dijkstra 最短路徑演算法"}
                    ],
                    key_concepts=[],
                )
                refreshed = storage.search_study_recall_pages(query="Dijkstra")

                self.assertTrue(refreshed)
                self.assertEqual(refreshed[0]["session_id"], second_id)
            finally:
                storage._engine.dispose()

    def test_search_api_returns_renderable_mixed_text_latex(self):
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
                session_id = storage.create_study_recall_session(
                    study_date="2026-07-28",
                    subject="線性代數",
                    title="換基矩陣",
                    image_filenames=["basis.jpg"],
                    summary="基底轉換",
                    source_transcription=[
                        {
                            "image_index": 1,
                            "transcription": "Change of basis: P = B'^{-1}B.",
                        }
                    ],
                    key_concepts=[
                        {
                            "concept": "換基矩陣 P = B'^{-1}B",
                            "topic": "基底轉換",
                            "core_summary": "Change of basis: P = B'^{-1}B.",
                            "simple_example": r"單位矩陣是 \begin{bmatrix}1&0\\0&1\end{bmatrix}。",
                            "search_keywords": ["change of basis", "P"],
                            "source_refs": [
                                {
                                    "image_index": 1,
                                    "evidence": "Change of basis: P = B'^{-1}B.",
                                }
                            ],
                        }
                    ],
                )
                token = "study-search-test-session"
                storage.save_web_session(token, "test-admin")
                client = app.test_client()
                with client.session_transaction() as browser_session:
                    browser_session["username"] = "test-admin"
                    browser_session["session_token"] = token
                    browser_session["is_admin"] = True

                response = client.get(
                    "/admin/study-recall/search",
                    query_string={"q": "P=B'^-1B"},
                )
                payload = response.get_json()

                self.assertEqual(response.status_code, 200)
                self.assertTrue(payload["results"])
                excerpt = payload["results"][0]["excerpt"]
                self.assertIn("\\(P = B'^{-1}B\\)", excerpt)
                self.assertIn("Change of basis:", excerpt)
                self.assertNotIn("\\[Change of basis", excerpt)

                page_response = client.get(
                    "/admin/study-recall",
                    query_string={"session_id": session_id},
                )
                rendered_page = page_response.get_data(as_text=True)
                self.assertIn("換基矩陣", rendered_page)
                self.assertIn(r"\(P = B", rendered_page)
            finally:
                storage._engine.dispose()

    def test_public_search_is_anonymous_read_only_and_can_preview_source_page(self):
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
                session_id = storage.create_study_recall_session(
                    study_date="2026-08-28",
                    subject="資料結構",
                    title="最短路徑",
                    image_filenames=["dijkstra.png"],
                    summary="Dijkstra 最短路徑演算法",
                    source_transcription=[
                        {
                            "image_index": 1,
                            "transcription": "Dijkstra 每次選擇距離最小的未確定頂點。",
                        }
                    ],
                    key_concepts=[
                        {
                            "concept": "Dijkstra 鬆弛",
                            "topic": "最短路徑",
                            "core_summary": r"若 \(d[u]+w(u,v)<d[v]\)，就更新距離。",
                            "source_refs": [
                                {
                                    "image_index": 1,
                                    "evidence": "Dijkstra 每次選擇距離最小的未確定頂點。",
                                    "bbox": {
                                        "source_image_index": 1,
                                        "left": 20,
                                        "top": 120,
                                        "right": 980,
                                        "bottom": 360,
                                        "confidence": 90,
                                        "version": 1,
                                    },
                                }
                            ],
                        }
                    ],
                )
                image_dir = Path(temp_dir) / "study_note_images" / str(session_id)
                image_dir.mkdir(parents=True)
                image_bytes = b"public-note-image"
                (image_dir / "dijkstra.png").write_bytes(image_bytes)
                client = app.test_client()

                response = client.get(
                    "/study-progress/notes/search",
                    query_string={"q": "Dijkstra", "subject": "資料結構"},
                )
                payload = response.get_json()

                self.assertEqual(response.status_code, 200)
                self.assertIn("no-store", response.headers["Cache-Control"])
                self.assertIn("max-age=0", response.headers["Cache-Control"])
                self.assertTrue(payload["results"])
                result = payload["results"][0]
                self.assertNotIn("session_id", result)
                self.assertNotIn("note_url", result)
                self.assertNotIn("evidence", result)
                self.assertEqual(result["bbox"]["top"], 120)

                unrelated = client.get(
                    "/study-progress/notes/search",
                    query_string={"q": "completely-unrelated-token"},
                ).get_json()
                self.assertEqual(unrelated["results"], [])

                image_response = client.get(result["image_url"])
                self.assertEqual(image_response.status_code, 200)
                self.assertEqual(image_response.data, image_bytes)
                image_response.close()
                self.assertEqual(
                    client.get(
                        f"/study-progress/notes/{session_id}/image/not-allowed.png"
                    ).status_code,
                    404,
                )

                public_page = client.get("/study-progress").get_data(as_text=True)
                self.assertIn("搜尋筆記庫", public_page)
                self.assertIn("/study-progress/notes/search", public_page)
                self.assertIn("資料結構", public_page)
            finally:
                storage._engine.dispose()


if __name__ == "__main__":
    unittest.main()
