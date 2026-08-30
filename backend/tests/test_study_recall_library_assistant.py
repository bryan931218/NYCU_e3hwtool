import os
import json
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
                self.assertIn("全文約 700 到 1000 個中文字", request_prompt)
                self.assertEqual(
                    openai_post.call_args.kwargs["json"]["max_output_tokens"],
                    1800,
                )
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

\[
n!\le n^n,\qquad n!\ge\left\frac{n!}{2}\right^{n/2}
\]

\(\[\sum_{i=1}^n \frac{i^2\(i^2+1\)}{2}=\Theta n^5\]\)

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
                self.assertNotIn(r"\(\[", answer)
                self.assertNotIn(r"\]\)", answer)
                self.assertIn(
                    r"\left(\frac{n!}{2}\right)^{n/2}",
                    answer,
                )
                self.assertIn(
                    r"\(\sum_{i=1}^n \frac{i^2(i^2+1)}{2}=\Theta n^5\)",
                    answer,
                )
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
                self.assertIn('data-general-ask-url="/admin/study-recall/assistant/ask"', html)
                self.assertIn("data-ai-general-mode", html)
                self.assertIn("activateConversation(generalKey, 'AI 問答', 'general')", html)
                self.assertIn('data-ai-style="concise"', html)
                self.assertIn('data-ai-style="detailed"', html)
                self.assertIn("data-ai-stop", html)
                self.assertIn("data-ai-retry", html)
                self.assertIn("e3-study-ai-conversations-v2", html)
                self.assertIn("window.normalizeStudyMathText", html)
                self.assertIn("toggle.classList.remove('has-unread')", html)
                self.assertIn("toggle.classList.add('has-unread')", html)
                self.assertIn("data-ai-action-apply", html)
                self.assertIn("確認套用", html)
                self.assertIn("復原這次變更", html)
            finally:
                storage._engine.dispose()

    def test_floating_assistant_answers_without_a_selected_card(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self.build_app(temp_dir)
            storage = app.extensions["e3_storage"]
            try:
                client = app.test_client()
                self.login_admin(app, client)
                with patch(
                    "e3_tracker.api.web.requests.post",
                    return_value=self.openai_response(r"雜湊表的平均查詢時間為 \\(O(1)\\)。"),
                ) as openai_post:
                    response = client.post(
                        "/admin/study-recall/assistant/ask",
                        json={
                            "question": "雜湊表查詢的時間複雜度是什麼？",
                            "response_style": "detailed",
                            "history": [
                                {"role": "user", "content": "我正在複習資料結構。"},
                                {"role": "assistant", "content": "了解。"},
                            ],
                        },
                    )

                payload = response.get_json()
                self.assertEqual(response.status_code, 200)
                self.assertTrue(payload["ok"])
                self.assertIn(r"\(O(1)\)", payload["answer"])
                self.assertNotIn(r"\\(", payload["answer"])
                request_prompt = openai_post.call_args.kwargs["json"]["input"][0]["content"][0]["text"]
                self.assertIn("這是一般問答模式，沒有指定筆記或重點卡", request_prompt)
                self.assertIn("採詳細回答", request_prompt)
                self.assertIn("我正在複習資料結構", request_prompt)
                self.assertNotIn("重點卡資料", request_prompt)
                self.assertNotIn("set_video_progress", request_prompt)
                self.assertEqual(openai_post.call_args.kwargs["json"]["max_output_tokens"], 3200)
                self.assertIsNone(payload["proposal"])
            finally:
                storage._engine.dispose()

    def test_data_assistant_requires_confirmation_and_can_undo_video_progress(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self.build_app(temp_dir)
            storage = app.extensions["e3_storage"]
            try:
                video = storage.list_study_plan_videos_with_records()[0]
                client = app.test_client()
                self.login_admin(app, client)
                ai_payload = {
                    "answer": "已準備把影片位置修正為 12 分鐘，請確認變更。",
                    "action": {
                        "type": "set_video_progress",
                        "subject": video["subject"],
                        "video_sequence": video["sequence"],
                        "session_id": "",
                        "target_minutes": 12,
                        "start_date": "",
                        "end_date": "",
                        "weekday_hours": 0,
                        "weekend_hours": 0,
                        "reason": "修正錯誤位置",
                    },
                }
                with patch(
                    "e3_tracker.api.web.requests.post",
                    return_value=self.openai_response(json.dumps(ai_payload, ensure_ascii=False)),
                ):
                    ask_response = client.post(
                        "/admin/study-recall/assistant/ask",
                        json={"question": f"把{video['subject']}影片{video['sequence']}改成12分鐘"},
                    )

                proposal = ask_response.get_json()["proposal"]
                unchanged = next(
                    item for item in storage.list_study_plan_videos_with_records()
                    if item["id"] == video["id"]
                )
                self.assertEqual(ask_response.status_code, 200)
                self.assertIsNotNone(proposal)
                self.assertEqual(unchanged["watched_seconds"], 0)

                apply_response = client.post(proposal["apply_url"])
                changed = next(
                    item for item in storage.list_study_plan_videos_with_records()
                    if item["id"] == video["id"]
                )
                self.assertEqual(apply_response.status_code, 200)
                self.assertEqual(changed["watched_seconds"], 720)
                self.assertEqual(client.post(proposal["apply_url"]).status_code, 409)

                undo_url = apply_response.get_json()["undo"]["url"]
                undo_response = client.post(undo_url)
                restored = next(
                    item for item in storage.list_study_plan_videos_with_records()
                    if item["id"] == video["id"]
                )
                self.assertEqual(undo_response.status_code, 200)
                self.assertEqual(restored["watched_seconds"], 0)
                self.assertEqual(client.post(undo_url).status_code, 409)
            finally:
                storage._engine.dispose()

    def test_data_assistant_can_correct_and_undo_study_time(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self.build_app(temp_dir)
            storage = app.extensions["e3_storage"]
            try:
                session_id = "assistant_time_session"
                storage.record_study_time_session(
                    session_key=session_id,
                    kind="practice",
                    label="演算法練習",
                    elapsed_seconds=3600,
                )
                client = app.test_client()
                self.login_admin(app, client)
                ai_payload = {
                    "answer": "找到這筆紀錄，準備改為 35 分鐘。",
                    "action": {
                        "type": "set_study_time_session",
                        "subject": "",
                        "video_sequence": 0,
                        "session_id": session_id,
                        "target_minutes": 35,
                        "start_date": "",
                        "end_date": "",
                        "weekday_hours": 0,
                        "weekend_hours": 0,
                        "reason": "實際只讀了35分鐘",
                    },
                }
                with patch(
                    "e3_tracker.api.web.requests.post",
                    return_value=self.openai_response(json.dumps(ai_payload, ensure_ascii=False)),
                ):
                    proposal = client.post(
                        "/admin/study-recall/assistant/ask",
                        json={"question": "演算法練習其實只有35分鐘，幫我修正"},
                    ).get_json()["proposal"]

                self.assertEqual(storage.get_study_time_session(session_id)["elapsed_seconds"], 3600)
                applied = client.post(proposal["apply_url"])
                self.assertEqual(applied.status_code, 200)
                self.assertEqual(storage.get_study_time_session(session_id)["elapsed_seconds"], 2100)
                self.assertEqual(client.post(applied.get_json()["undo"]["url"]).status_code, 200)
                self.assertEqual(storage.get_study_time_session(session_id)["elapsed_seconds"], 3600)
            finally:
                storage._engine.dispose()

    def test_data_assistant_does_not_overwrite_newer_video_progress(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self.build_app(temp_dir)
            storage = app.extensions["e3_storage"]
            try:
                video = storage.list_study_plan_videos_with_records()[0]
                client = app.test_client()
                self.login_admin(app, client)
                ai_payload = {
                    "answer": "準備修正觀看位置。",
                    "action": {
                        "type": "set_video_progress",
                        "subject": video["subject"],
                        "video_sequence": video["sequence"],
                        "session_id": "",
                        "target_minutes": 12,
                        "start_date": "",
                        "end_date": "",
                        "weekday_hours": 0,
                        "weekend_hours": 0,
                        "reason": "修正位置",
                    },
                }
                with patch(
                    "e3_tracker.api.web.requests.post",
                    return_value=self.openai_response(json.dumps(ai_payload, ensure_ascii=False)),
                ):
                    proposal = client.post(
                        "/admin/study-recall/assistant/ask",
                        json={"question": f"把{video['subject']}影片{video['sequence']}改成12分鐘"},
                    ).get_json()["proposal"]

                newer = storage.update_study_plan_video_progress(
                    video_id=video["id"],
                    watched_seconds=300,
                    expected_version=0,
                )
                self.assertFalse(newer["stale"])
                response = client.post(proposal["apply_url"])
                current = next(
                    item for item in storage.list_study_plan_videos_with_records()
                    if item["id"] == video["id"]
                )
                self.assertEqual(response.status_code, 409)
                self.assertEqual(current["watched_seconds"], 300)
            finally:
                storage._engine.dispose()

    def test_data_assistant_can_update_and_undo_study_plan(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self.build_app(temp_dir)
            storage = app.extensions["e3_storage"]
            try:
                client = app.test_client()
                self.login_admin(app, client)
                ai_payload = {
                    "answer": "準備將平日調整為 3.5 小時、假日 4 小時。",
                    "action": {
                        "type": "update_study_plan",
                        "subject": "",
                        "video_sequence": 0,
                        "session_id": "",
                        "target_minutes": 0,
                        "start_date": "2026-09-07",
                        "end_date": "2026-12-31",
                        "weekday_hours": 3.5,
                        "weekend_hours": 4,
                        "reason": "調整每日可用時間",
                    },
                }
                with patch(
                    "e3_tracker.api.web.requests.post",
                    return_value=self.openai_response(json.dumps(ai_payload, ensure_ascii=False)),
                ):
                    proposal = client.post(
                        "/admin/study-recall/assistant/ask",
                        json={"question": "把計畫改成平日3.5小時，假日4小時，到年底"},
                    ).get_json()["proposal"]

                self.assertIsNone(storage.get_study_plan_replan_settings())
                applied = client.post(proposal["apply_url"])
                settings = storage.get_study_plan_replan_settings()
                self.assertEqual(applied.status_code, 200)
                self.assertEqual(settings["weekday_minutes"], 210)
                self.assertEqual(settings["weekend_minutes"], 240)
                self.assertEqual(client.post(applied.get_json()["undo"]["url"]).status_code, 200)
                self.assertIsNone(storage.get_study_plan_replan_settings())
            finally:
                storage._engine.dispose()

    def test_card_assistant_uses_requested_answer_detail(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self.build_app(temp_dir)
            storage = app.extensions["e3_storage"]
            try:
                session_id = storage.create_study_recall_session(
                    study_date="2026-08-27",
                    subject="資料結構",
                    title="雜湊表",
                    image_filenames=["hash.jpg"],
                    summary="雜湊表查詢",
                    source_transcription=[{"image_index": 1, "transcription": "平均查詢為 O(1)"}],
                    key_concepts=[
                        {
                            "topic": "雜湊表",
                            "concept": "平均查詢時間",
                            "core_summary": r"平均為 \(O(1)\)",
                            "explanation": "雜湊函數可直接定位儲存位置。",
                            "simple_example": "依 key 查詢資料。",
                            "memory_hint": "直接定位",
                        }
                    ],
                )
                client = app.test_client()
                self.login_admin(app, client)
                with patch(
                    "e3_tracker.api.web.requests.post",
                    return_value=self.openai_response(r"奇異值為 \\(\sigma_i=\sqrt{\lambda_i}\\)。"),
                ) as openai_post:
                    response = client.post(
                        f"/admin/study-recall/{session_id}/cards/0/ask",
                        json={"question": "為什麼？", "response_style": "detailed"},
                    )

                self.assertEqual(response.status_code, 200)
                answer = response.get_json()["answer"]
                self.assertIn(r"\(\sigma_i=\sqrt{\lambda_i}\)", answer)
                self.assertNotIn(r"\\(", answer)
                request_prompt = openai_post.call_args.kwargs["json"]["input"][0]["content"][0]["text"]
                self.assertIn("採詳細回答", request_prompt)
                self.assertIn("重點卡資料", request_prompt)
            finally:
                storage._engine.dispose()


if __name__ == "__main__":
    unittest.main()
