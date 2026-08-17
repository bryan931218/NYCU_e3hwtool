import io
import os
import tempfile
import unittest
from unittest.mock import Mock, patch

from PIL import Image

from e3_tracker.api.web import create_app


class VideoFrameQuestionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.env_patch = patch.dict(
            os.environ,
            {
                "E3_CACHE_DIR": self.temp_dir.name,
                "E3_DATABASE_URL": "",
                "E3_SESSION_COOKIE_SECURE": "0",
                "OPENAI_API_KEY": "test-key",
            },
        )
        self.env_patch.start()
        self.app = create_app()
        self.storage = self.app.extensions["e3_storage"]
        self.video = next(
            video
            for video in self.storage.list_study_plan_videos_with_records()
            if video.get("youtube_video_id")
        )
        token = "video-frame-question-session"
        self.storage.save_web_session(token, "test-admin")
        self.client = self.app.test_client()
        with self.client.session_transaction() as browser_session:
            browser_session["username"] = "test-admin"
            browser_session["session_token"] = token
            browser_session["is_admin"] = True

    def tearDown(self):
        self.storage._engine.dispose()
        self.env_patch.stop()
        self.temp_dir.cleanup()

    @staticmethod
    def _frame():
        image = Image.new("RGB", (640, 360), (20, 60, 120))
        output = io.BytesIO()
        image.save(output, format="JPEG")
        return {
            "bytes": output.getvalue(),
            "mime_type": "image/jpeg",
            "requested_seconds": 123.4,
            "frame_seconds": 120.0,
            "width": 640,
            "height": 360,
        }

    def test_sends_current_frame_and_question_to_responses_api(self):
        openai_response = Mock()
        openai_response.raise_for_status.return_value = None
        openai_response.json.return_value = {
            "status": "completed",
            "output": [
                {"type": "message", "content": [{"type": "output_text", "text": "因為這一步使用了反證法。"}]}
            ],
        }
        with patch(
            "e3_tracker.api.web.fetch_youtube_storyboard_frame", return_value=self._frame()
        ) as fetch_frame, patch("e3_tracker.api.web.requests.post", return_value=openai_response) as post:
            response = self.client.post(
                "/admin/study-plan/video-question",
                json={
                    "video_id": self.video["id"],
                    "playback_seconds": 123.4,
                    "question": "這一步為什麼成立？",
                },
            )

        self.assertEqual(response.status_code, 200)
        result = response.get_json()
        self.assertTrue(result["ok"])
        self.assertEqual(result["answer"], "因為這一步使用了反證法。")
        self.assertTrue(result["frame_image"].startswith("data:image/jpeg;base64,"))
        fetch_frame.assert_called_once_with(self.video["youtube_video_id"], 123.4)
        request_json = post.call_args.kwargs["json"]
        content = request_json["input"][0]["content"]
        self.assertEqual(content[0]["type"], "input_text")
        self.assertEqual(content[1]["type"], "input_image")
        self.assertTrue(content[1]["image_url"].startswith("data:image/jpeg;base64,"))
        self.assertIn("這一步為什麼成立", content[0]["text"])

    def test_validates_question_and_video(self):
        missing_question = self.client.post(
            "/admin/study-plan/video-question",
            json={"video_id": self.video["id"], "playback_seconds": 20, "question": ""},
        )
        missing_video = self.client.post(
            "/admin/study-plan/video-question",
            json={"video_id": 999999, "playback_seconds": 20, "question": "這是什麼？"},
        )
        self.assertEqual(missing_question.status_code, 400)
        self.assertEqual(missing_video.status_code, 404)

    def test_page_renders_desktop_mobile_and_fullscreen_controls(self):
        page = self.client.get(
            f"/admin/study-plan?subject={self.video['subject']}&video_id={self.video['id']}"
        )
        html = page.get_data(as_text=True)
        self.assertEqual(page.status_code, 200)
        self.assertIn('id="frame-question-open"', html)
        self.assertIn('id="focus-frame-question"', html)
        self.assertIn('id="frame-question-dialog"', html)
        self.assertIn("Q 問這一幕", html)
        self.assertIn("max-height:calc(100dvh - 18px)", html)
        self.assertIn("/admin/study-plan/video-question", html)


if __name__ == "__main__":
    unittest.main()
