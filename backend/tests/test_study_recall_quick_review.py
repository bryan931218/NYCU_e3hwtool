import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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
                self.assertIn("先回想，再看答案", client.get("/admin/study-recall").get_data(as_text=True))
                card_sections = page.split('data-quick-card data-card-type=')[1:]
                rendered_subjects = [
                    "離散數學" if "離散數學" in section[:700] else "資料結構"
                    for section in card_sections
                ]
                self.assertTrue(
                    all(left != right for left, right in zip(rendered_subjects, rendered_subjects[1:])),
                    rendered_subjects,
                )
                self.assertIn("空白鍵揭示", page)
                self.assertIn("忘了", page)
                self.assertIn("模糊", page)
                self.assertIn("記得", page)
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


if __name__ == "__main__":
    unittest.main()
