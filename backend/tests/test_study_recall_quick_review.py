import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bs4 import BeautifulSoup

from e3_tracker.api.web import create_app


class StudyRecallQuickReviewTests(unittest.TestCase):
    def _build_client(self, temp_dir: str):
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
        token = "quick-review-test-session"
        storage.save_web_session(token, "test-admin")
        client = app.test_client()
        with client.session_transaction() as browser_session:
            browser_session["username"] = "test-admin"
            browser_session["session_token"] = token
            browser_session["is_admin"] = True
        return app, storage, client

    @staticmethod
    def _card(subject: str, index: int):
        return {
            "concept": f"{subject}觀念 {index}",
            "topic": f"{subject}主題",
            "recall_cue": f"回想 {subject} 的第 {index} 個條件與結論",
            "core_summary": f"{subject}核心答案 {index}",
            "explanation": f"{subject}觀念 {index} 的完整說明。",
            "card_type": "concept",
        }

    def test_quick_review_limits_session_and_interleaves_subjects(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            _app, storage, client = self._build_client(temp_dir)
            try:
                for subject in ("離散數學", "資料結構"):
                    storage.create_study_recall_session(
                        study_date="2025-01-01",
                        subject=subject,
                        title=f"{subject}筆記",
                        image_filenames=[],
                        summary=f"{subject}摘要",
                        key_concepts=[self._card(subject, index) for index in range(4)],
                    )

                response = client.get("/admin/study-recall/quick-review", query_string={"size": 5})
                page = response.get_data(as_text=True)

                self.assertEqual(response.status_code, 200)
                self.assertEqual(page.count('data-quick-card data-card-type='), 5)
                self.assertIn("用研究所考題，把觀念練成得分能力", client.get("/admin/study-recall").get_data(as_text=True))
                card_sections = page.split('data-quick-card data-card-type=')[1:]
                rendered_subjects = [
                    "離散數學" if "離散數學" in section[:700] else "資料結構"
                    for section in card_sections
                ]
                self.assertTrue(
                    all(left != right for left, right in zip(rendered_subjects, rendered_subjects[1:])),
                    rendered_subjects,
                )
                self.assertIn("考場快測", page)
                self.assertTrue("單選題" in page or "是非題" in page)
                self.assertIn("每題都有來源正解", page)
            finally:
                storage._engine.dispose()

    def test_quick_review_rating_uses_existing_fsrs_schedule(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            _app, storage, client = self._build_client(temp_dir)
            try:
                session_id = storage.create_study_recall_session(
                    study_date="2025-01-01",
                    subject="線性代數",
                    title="線性代數筆記",
                    image_filenames=[],
                    summary="摘要",
                    key_concepts=[self._card("線性代數", 0)],
                )

                response = client.post(
                    f"/admin/study-recall/{session_id}/rate-cards",
                    data={"rating_0": "1"},
                    headers={"X-E3-Recall-Rating": "1", "Accept": "application/json"},
                )
                payload = response.get_json()
                session = storage.get_study_recall_session(session_id)

                self.assertEqual(response.status_code, 200)
                self.assertTrue(payload["ok"])
                self.assertEqual(payload["remaining_due_count"], 0)
                self.assertEqual(session["key_concepts"][0]["review"]["last_rating"], 1)
                self.assertTrue(session["key_concepts"][0]["review"]["next_review_at"])
            finally:
                storage._engine.dispose()

    def test_quick_review_builds_task_specific_questions_and_structured_answers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            _app, storage, client = self._build_client(temp_dir)
            try:
                cards = [
                    {
                        **self._card("演算法", 0),
                        "concept": "二分搜尋",
                        "content_kind": "procedure",
                        "core_summary": "每次排除一半不可能的區間。",
                        "reasoning_steps": ["比較中點", "依結果縮小左右邊界", "直到找到或區間為空"],
                    },
                    {
                        **self._card("演算法", 1),
                        "concept": "時間複雜度",
                        "content_kind": "formula",
                        "core_summary": r"\(T(n)=T(n/2)+O(1)=O(\log n)\)",
                        "common_confusion": "只有每輪確實把問題規模減半時才是對數層數。",
                    },
                    {
                        **self._card("演算法", 2),
                        "concept": "穩定排序",
                        "content_kind": "definition",
                        "core_summary": "相同鍵值元素排序後仍維持原本相對順序。",
                    },
                    {
                        **self._card("演算法", 3),
                        "concept": "最短路徑",
                        "card_type": "example",
                        "example_problem": "給定非負權重圖，求起點到所有節點的最短距離。",
                        "example_method": "使用 Dijkstra，每輪確定目前距離最小的未確定節點。",
                        "reasoning_steps": ["初始化距離", "挑選最小者", "鬆弛相鄰邊"],
                    },
                    {
                        **self._card("演算法", 4),
                        "concept": "貪心選擇",
                        "content_kind": "fact",
                        "core_summary": "每一步選擇當下最佳解，仍須另外證明能導向全域最佳解。",
                    },
                ]
                storage.create_study_recall_session(
                    study_date="2025-01-01",
                    subject="演算法",
                    title="演算法觀念整理",
                    image_filenames=[],
                    summary="演算法摘要",
                    key_concepts=cards,
                )

                response = client.get("/admin/study-recall/quick-review", query_string={"size": 5})
                page = response.get_data(as_text=True)

                self.assertEqual(response.status_code, 200)
                self.assertIn("單選題", page)
                self.assertIn("是非題", page)
                self.assertIn("計算／解題題", page)
                self.assertIn("來源正解", page)
                self.assertIn('class="answer-steps"', page)
                self.assertIn("防錯線", page)
                self.assertNotIn("data-answer-point", page)
                document = BeautifulSoup(page, "html.parser")
                rendered_cards = document.select("[data-quick-card]")
                self.assertEqual(len(rendered_cards), 5)
                for rendered_card in rendered_cards:
                    self.assertIsNotNone(rendered_card.select_one(".answer-sheet .answer-core"))
                    self.assertTrue(rendered_card.select_one("[data-question-type]").get_text(strip=True))
                    self.assertTrue(rendered_card.select_one(".answer-core p").get_text(strip=True))
                    options = rendered_card.select("[data-exam-option]")
                    if options:
                        self.assertIn(len(options), {2, 4})
                        self.assertEqual(
                            sum(option.get("data-correct") == "true" for option in options),
                            1,
                        )
                        self.assertIsNotNone(rendered_card.select_one("[data-submit-objective]"))
                        self.assertEqual(len(rendered_card.select("[data-rating]")), 0)
                    else:
                        self.assertEqual(rendered_card.get("data-interaction"), "calculation")
                        self.assertEqual(len(rendered_card.select("[data-rating]")), 3)
                        self.assertIsNotNone(rendered_card.select_one(".answer-method"))
                true_false_cards = [
                    card
                    for card in rendered_cards
                    if card.select_one("[data-question-type]").get_text(strip=True) == "是非題"
                ]
                correct_true_false_answers = {
                    card.select_one('[data-exam-option][data-correct="true"] .option-text').get_text(strip=True)
                    for card in true_false_cards
                }
                self.assertEqual(correct_true_false_answers, {"正確", "錯誤"})
            finally:
                storage._engine.dispose()

    def test_quick_review_quality_gate_replaces_unclear_calculation_question(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            _app, storage, client = self._build_client(temp_dir)
            try:
                unclear_example = {
                    **self._card("線性代數", 0),
                    "concept": "矩陣運算例題",
                    "card_type": "example",
                    "example_problem": "如上圖所示，求出最後答案。",
                    "core_summary": "先依矩陣乘法規則計算每個位置。",
                    "example_method": "",
                    "reasoning_steps": [],
                }
                other_cards = [
                    {
                        **self._card("線性代數", index),
                        "concept": f"矩陣觀念 {index}",
                        "core_summary": f"矩陣觀念 {index} 的明確核心結論。",
                    }
                    for index in range(1, 4)
                ]
                storage.create_study_recall_session(
                    study_date="2025-01-01",
                    subject="線性代數",
                    title="矩陣題目整理",
                    image_filenames=[],
                    summary="矩陣摘要",
                    key_concepts=[unclear_example, *other_cards],
                )

                page = client.get("/admin/study-recall/quick-review", query_string={"size": 5}).get_data(as_text=True)
                document = BeautifulSoup(page, "html.parser")
                fallback_card = document.select_one('[data-concept-index="0"]')

                self.assertIsNotNone(fallback_card)
                self.assertEqual(fallback_card.get("data-quality-status"), "fallback")
                self.assertEqual(fallback_card.get("data-interaction"), "written")
                self.assertEqual(
                    fallback_card.select_one("[data-question-type]").get_text(strip=True),
                    "來源簡答題",
                )
                self.assertNotIn("如上圖", fallback_card.select_one(".prompt-block h2").get_text())
                self.assertTrue(fallback_card.select_one(".answer-core p").get_text(strip=True))
            finally:
                storage._engine.dispose()

    def test_quick_review_quality_gate_rejects_overlapping_choice_options(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            _app, storage, client = self._build_client(temp_dir)
            try:
                summaries = [
                    "節點數量等於輸入的 n 值。",
                    "在這個情況下，節點數量等於輸入的 n 值，因此需要線性空間。",
                    "邊的數量決定鄰接串列的總長度。",
                    "走訪順序會影響深度優先搜尋產生的樹。",
                ]
                cards = [
                    {
                        **self._card("資料結構", index),
                        "concept": f"圖形觀念 {index}",
                        "core_summary": summary,
                    }
                    for index, summary in enumerate(summaries)
                ]
                storage.create_study_recall_session(
                    study_date="2025-01-01",
                    subject="資料結構",
                    title="圖形結構整理",
                    image_filenames=[],
                    summary="圖形摘要",
                    key_concepts=cards,
                )

                page = client.get("/admin/study-recall/quick-review", query_string={"size": 5}).get_data(as_text=True)
                document = BeautifulSoup(page, "html.parser")
                first_card = document.select_one('[data-concept-index="0"]')

                self.assertIsNotNone(first_card)
                self.assertEqual(first_card.get("data-quality-status"), "fallback")
                self.assertEqual(first_card.get("data-interaction"), "written")
                self.assertEqual(len(first_card.select("[data-exam-option]")), 0)
            finally:
                storage._engine.dispose()


if __name__ == "__main__":
    unittest.main()
