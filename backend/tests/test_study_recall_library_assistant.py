import os
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from e3_tracker.api.web import create_app


class StudyRecallLibraryAssistantTests(unittest.TestCase):
    def build_app(self, temp_dir: str):
        with patch.dict(
            os.environ,
            {
                "E3_CACHE_DIR": temp_dir,
                "E3_DATABASE_URL": "",
                "E3_SESSION_COOKIE_SECURE": "0",
                "OPENAI_API_KEY": "test-key",
            },
        ):
            return create_app()

    @staticmethod
    def login_admin(app, client):
        storage = app.extensions["e3_storage"]
        token = "library-assistant-test-session"
        storage.save_web_session(token, "test-admin")
        with client.session_transaction() as browser_session:
            browser_session["username"] = "test-admin"
            browser_session["session_token"] = token
            browser_session["is_admin"] = True

    @staticmethod
    def openai_response(answer: str):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "status": "completed",
            "output": [
                {
                    "type": "message",
                    "content": [{"type": "output_text", "text": answer}],
                }
            ],
        }
        return response

    def test_natural_language_subject_and_page_range_use_only_requested_pages(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self.build_app(temp_dir)
            storage = app.extensions["e3_storage"]
            try:
                storage.create_study_recall_session(
                    study_date="2026-08-20",
                    subject="離散數學",
                    title="排列組合完整筆記",
                    image_filenames=[f"page-{index}.jpg" for index in range(1, 21)],
                    summary="箱子分堆與排列組合",
                    source_transcription=[
                        {
                            "image_index": index,
                            "transcription": f"UNIQUE_PAGE_{index}：第 {index} 頁的箱子分堆公式。",
                        }
                        for index in range(1, 21)
                    ],
                    key_concepts=[],
                )
                storage.create_study_recall_session(
                    study_date="2026-08-20",
                    subject="資料結構",
                    title="樹與圖",
                    image_filenames=["tree.jpg"],
                    summary="樹",
                    source_transcription=[{"image_index": 1, "transcription": "DATA_STRUCTURE_ONLY"}],
                    key_concepts=[],
                )
                client = app.test_client()
                self.login_admin(app, client)

                with patch(
                    "e3_tracker.api.web.requests.post",
                    return_value=self.openai_response(
                        "| 公式 | 條件 |\n|---|---|\n| \\(n!\\) | 相異物件 [來源 1] |"
                    ),
                ) as openai_post:
                    response = client.post(
                        "/admin/study-recall/library-ask",
                        json={
                            "question": "請根據我離散數學筆記的第12~16頁整理出箱子分堆問題的公式表格。"
                        },
                    )

                payload = response.get_json()
                self.assertEqual(response.status_code, 200)
                self.assertTrue(payload["ok"])
                self.assertEqual(payload["inferred"]["subject"], "離散數學")
                self.assertTrue(payload["inferred"]["page_range"])
                self.assertEqual([source["page"] for source in payload["sources"]], [12, 13, 14, 15, 16])
                self.assertEqual(payload["scope"], "離散數學・排列組合完整筆記・第 12–16 頁")

                request_prompt = openai_post.call_args.kwargs["json"]["input"][0]["content"][0]["text"]
                self.assertIn("UNIQUE_PAGE_12", request_prompt)
                self.assertIn("UNIQUE_PAGE_16", request_prompt)
                self.assertNotIn("UNIQUE_PAGE_11", request_prompt)
                self.assertNotIn("UNIQUE_PAGE_17", request_prompt)
                self.assertNotIn("DATA_STRUCTURE_ONLY", request_prompt)
            finally:
                storage._engine.dispose()

    def test_explicit_note_scope_and_page_limit_are_validated(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self.build_app(temp_dir)
            storage = app.extensions["e3_storage"]
            try:
                session_id = storage.create_study_recall_session(
                    study_date="2026-08-20",
                    subject="線性代數",
                    title="矩陣",
                    image_filenames=["matrix.jpg"],
                    summary="矩陣",
                    source_transcription=[{"image_index": 1, "transcription": "矩陣乘法"}],
                    key_concepts=[],
                )
                client = app.test_client()
                self.login_admin(app, client)

                too_wide = client.post(
                    "/admin/study-recall/library-ask",
                    json={
                        "question": "整理公式",
                        "session_id": session_id,
                        "page_start": 1,
                        "page_end": 30,
                    },
                )
                wrong_subject = client.post(
                    "/admin/study-recall/library-ask",
                    json={
                        "question": "整理公式",
                        "session_id": session_id,
                        "subject": "離散數學",
                    },
                )

                self.assertEqual(too_wide.status_code, 400)
                self.assertIn("最多分析 24 頁", too_wide.get_json()["error"])
                self.assertEqual(wrong_subject.status_code, 400)
                self.assertIn("不屬於所選科目", wrong_subject.get_json()["error"])
            finally:
                storage._engine.dispose()

    def test_malformed_math_delimiters_and_plain_section_titles_are_repaired(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self.build_app(temp_dir)
            storage = app.extensions["e3_storage"]
            try:
                session_id = storage.create_study_recall_session(
                    study_date="2026-08-21",
                    subject="線性代數",
                    title="SVD",
                    image_filenames=["svd.jpg"],
                    summary="奇異值分解",
                    source_transcription=[
                        {"image_index": 1, "transcription": "A^T A 的特徵值平方根為奇異值。"}
                    ],
                    key_concepts=[],
                )
                client = app.test_client()
                self.login_admin(app, client)
                malformed_answer = r"""步驟總覽（目標：求 SVD）：

1. 建立 (A^T A)

   - 計算矩陣
     [
     A^T A\in\mathbb{R}^{n\times n}
     ]

2. 取 (\lambda\_i) 的平方根

   [
   \sigma\_i=\sqrt{\lambda\_i}
   ]

\[\det\]\(A\)\(=\det\)\(B,\quad \operatorname{tr}\)\(A\)\(=\operatorname{tr}\)\(B\)

（等價於 \(A-\lambda I\) 不可逆或 \(\ker\)\(A-\lambda I\)\neq\{0\}）[來源 1]

| 塊矩陣 | 結果 |
|---|---|
| \\(\\begin{pmatrix}A&0\\\\0&I_n\\end{pmatrix}\\) | \\(\\det(A)\\) |

常見陷阱與判斷線索（考試實用）

- 當 (\sigma\_i>0) 才能使用 (u\_i=\frac{1}{\sigma\_i}Av\_i)。[來源 1]

如需，我可以再提供完整計算。"""
                with patch(
                    "e3_tracker.api.web.requests.post",
                    return_value=self.openai_response(malformed_answer),
                ):
                    response = client.post(
                        "/admin/study-recall/library-ask",
                        json={"question": "整理 SVD", "session_id": session_id},
                    )

                payload = response.get_json()
                answer = payload["answer"]
                self.assertEqual(response.status_code, 200)
                self.assertIn("## 步驟總覽", answer)
                self.assertIn("### 1. 建立", answer)
                self.assertIn("### 2. 取", answer)
                self.assertIn("## 常見陷阱與判斷線索", answer)
                self.assertIn(r"\[", answer)
                self.assertIn(r"\]", answer)
                self.assertIn(r"\lambda_i", answer)
                self.assertNotIn(r"\lambda\_i", answer)
                self.assertIn(
                    r"\[\detA=\detB,\quad \operatorname{tr}A=\operatorname{tr}B\]",
                    answer,
                )
                self.assertNotIn(r"\[\(", answer)
                self.assertNotIn(r"\)\]", answer)
                self.assertIn(
                    r"\(\begin{pmatrix}A&0\\0&I_n\end{pmatrix}\)",
                    answer,
                )
                protected_math = re.compile(r"\\\[.*?\\\]|\\\(.*?\\\)", re.DOTALL)
                outside_math = protected_math.sub("", answer)
                self.assertNotRegex(
                    outside_math,
                    r"\\(?:operatorname|quad|neq|det|ker|lambda)\b",
                )
                self.assertFalse(any(line.strip() in {"[", "]"} for line in answer.splitlines()))
                self.assertNotIn("如需，我可以", answer)
            finally:
                storage._engine.dispose()

    def test_recall_page_contains_library_assistant_controls(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self.build_app(temp_dir)
            storage = app.extensions["e3_storage"]
            try:
                storage.create_study_recall_session(
                    study_date="2026-08-20",
                    subject="演算法",
                    title="動態規劃",
                    image_filenames=["dp.jpg"],
                    summary="DP",
                    source_transcription=[{"image_index": 1, "transcription": "最佳子結構"}],
                    key_concepts=[],
                )
                client = app.test_client()
                self.login_admin(app, client)

                response = client.get("/admin/study-recall")
                html = response.get_data(as_text=True)

                self.assertEqual(response.status_code, 200)
                self.assertIn('id="note-library-assistant"', html)
                self.assertIn("問整個筆記庫", html)
                self.assertIn("公式表", html)
                self.assertIn("動態規劃", html)
                self.assertIn("data-library-ai-answer", html)
            finally:
                storage._engine.dispose()


if __name__ == "__main__":
    unittest.main()
