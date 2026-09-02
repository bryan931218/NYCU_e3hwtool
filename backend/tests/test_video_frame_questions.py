import io
import json
import os
import re
import tempfile
import unittest
from unittest.mock import Mock, patch

import requests
from PIL import Image

from e3_tracker.api.web import create_app
from e3_tracker.services.youtube_frames import YoutubeAudioError


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
            "e3_tracker.api.web.fetch_youtube_cached_frame", return_value=self._frame()
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
        fetch_frame.assert_called_once_with(
            self.video["youtube_video_id"],
            123.4,
            storyboard_metadata=None,
        )
        request_json = post.call_args.kwargs["json"]
        content = request_json["input"][0]["content"]
        self.assertEqual(content[0]["type"], "input_text")
        self.assertEqual(content[1]["type"], "input_image")
        self.assertTrue(content[1]["image_url"].startswith("data:image/jpeg;base64,"))
        self.assertIn("這一步為什麼成立", content[0]["text"])

    def test_prefetches_frame_without_calling_openai(self):
        frame = self._frame()
        frame["source"] = "exact"
        with patch(
            "e3_tracker.api.web.fetch_youtube_cached_frame", return_value=frame
        ) as fetch_frame, patch("e3_tracker.api.web.requests.post") as post:
            response = self.client.post(
                "/admin/study-plan/video-frame",
                json={"video_id": self.video["id"], "playback_seconds": 123.4},
            )

        self.assertEqual(response.status_code, 200)
        result = response.get_json()
        self.assertTrue(result["frame_image"].startswith("data:image/jpeg;base64,"))
        self.assertEqual(result["frame_source"], "exact")
        fetch_frame.assert_called_once_with(
            self.video["youtube_video_id"],
            123.4,
            storyboard_metadata=None,
        )
        post.assert_not_called()

    def test_persists_storyboard_metadata_returned_by_frame_capture(self):
        frame = self._frame()
        frame["storyboard_metadata"] = {
            "youtube_video_id": self.video["youtube_video_id"],
            "duration_seconds": 456.0,
            "storyboard_spec": "https://i.example.test/storyboard/$L/$N.jpg|160#90#10#5#10#0#M$M#sig",
        }
        with patch(
            "e3_tracker.api.web.fetch_youtube_cached_frame",
            return_value=frame,
        ):
            response = self.client.post(
                "/admin/study-plan/video-frame",
                json={"video_id": self.video["id"], "playback_seconds": 123.4},
            )

        self.assertEqual(response.status_code, 200)
        saved = self.storage.get_youtube_storyboard_metadata(
            self.video["youtube_video_id"]
        )
        self.assertIsNotNone(saved)
        self.assertEqual(saved["duration_seconds"], 456.0)
        self.assertIn("storyboard", saved["storyboard_spec"])

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

    def test_renaming_marker_generates_summary_from_nearby_frames(self):
        marker_response = self.client.post(
            "/admin/study-plan/video-markers",
            json={
                "video_id": self.video["id"],
                "playback_seconds": 123.4,
                "note": "",
                "auto_summary": False,
            },
        )
        marker_id = marker_response.get_json()["marker"]["id"]
        transcription_response = Mock()
        transcription_response.raise_for_status.return_value = None
        transcription_response.json.return_value = {
            "text": "老師說奇異值分解會把線性轉換拆成旋轉、伸縮與旋轉。"
        }
        summary_response = Mock()
        summary_response.raise_for_status.return_value = None
        summary_response.json.return_value = {
            "status": "completed",
            "output": [
                {
                    "type": "message",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "先建立矩陣，再用 \\(A=U\\Sigma V^{T}\\) 分解比較各方向的伸縮。",
                        }
                    ],
                }
            ],
        }

        with patch(
            "e3_tracker.api.web.fetch_youtube_audio_clip",
            return_value={
                "bytes": b"RIFF" + b"\x00" * 2048,
                "mime_type": "audio/wav",
                "filename": "marker.wav",
                "start_seconds": 108.4,
                "end_seconds": 138.4,
                "duration_seconds": 30.0,
            },
        ) as fetch_audio, patch(
            "e3_tracker.api.web.fetch_youtube_cached_frame",
            side_effect=lambda _video_id, seconds, **_kwargs: {
                **self._frame(),
                "frame_seconds": seconds,
            },
        ) as fetch_frame, patch(
            "e3_tracker.api.web.requests.post",
            side_effect=[transcription_response, summary_response],
        ) as post:
            response = self.client.patch(
                f"/admin/study-plan/video-markers/{marker_id}",
                json={"note": "SVD 的幾何意義"},
            )

        self.assertEqual(response.status_code, 200)
        result = response.get_json()
        self.assertTrue(result["ok"])
        self.assertIsNone(result["summary_error"])
        self.assertEqual(result["marker"]["summary_status"], "ready")
        self.assertIn("A=U", result["marker"]["summary"])
        fetch_audio.assert_called_once_with(
            self.video["youtube_video_id"],
            123.4,
            radius_seconds=15.0,
        )
        self.assertEqual(fetch_frame.call_count, 5)
        transcription_call = post.call_args_list[0]
        self.assertEqual(
            transcription_call.args[0],
            "https://api.openai.com/v1/audio/transcriptions",
        )
        self.assertEqual(transcription_call.kwargs["data"]["language"], "zh")
        self.assertIn("file", transcription_call.kwargs["files"])
        content = post.call_args_list[1].kwargs["json"]["input"][0]["content"]
        self.assertEqual(
            len([item for item in content if item["type"] == "input_image"]),
            5,
        )
        self.assertIn("SVD 的幾何意義", content[0]["text"])
        self.assertIn("旋轉、伸縮與旋轉", content[0]["text"])
        self.assertIn("108.4 至 138.4 秒", content[0]["text"])

        page = self.client.get(
            f"/admin/study-plan?subject={self.video['subject']}&video_id={self.video['id']}"
        ).get_data(as_text=True)
        self.assertNotIn('id="marker-library-data"', page)
        self.assertIn("/admin/study-plan/markers", page)

        marker_page = self.client.get("/admin/study-plan/markers").get_data(as_text=True)
        self.assertIn("關鍵點總覽", marker_page)
        self.assertIn("SVD 的幾何意義", marker_page)
        self.assertIn("A=U", marker_page)
        self.assertIn('class="library-stats"', marker_page)
        self.assertIn('id="load-more"', marker_page)
        self.assertIn("filterRows();", marker_page)
        self.assertIn("前往片段", marker_page)
        self.assertIn(
            f"video_id={self.video['id']}&amp;marker_id={marker_id}",
            marker_page,
        )

    def test_marker_name_is_saved_when_summary_generation_fails(self):
        marker_response = self.client.post(
            "/admin/study-plan/video-markers",
            json={
                "video_id": self.video["id"],
                "playback_seconds": 80,
                "note": "",
                "auto_summary": False,
            },
        )
        marker_id = marker_response.get_json()["marker"]["id"]
        failed_response = Mock()
        failed_response.status_code = 400
        failed_response.json.return_value = {
            "error": {"code": "invalid_request", "type": "invalid_request_error", "message": "bad request"}
        }
        failed_response.raise_for_status.side_effect = requests.HTTPError(
            response=failed_response
        )
        with patch(
            "e3_tracker.api.web.fetch_youtube_audio_clip",
            side_effect=YoutubeAudioError("audio unavailable"),
        ), patch(
            "e3_tracker.api.web.fetch_youtube_cached_frame",
            return_value=self._frame(),
        ), patch(
            "e3_tracker.api.web.requests.post",
            return_value=failed_response,
        ):
            response = self.client.patch(
                f"/admin/study-plan/video-markers/{marker_id}",
                json={"note": "失敗時仍保留名稱"},
            )

        self.assertEqual(response.status_code, 200)
        marker = response.get_json()["marker"]
        self.assertEqual(marker["note"], "失敗時仍保留名稱")
        self.assertEqual(marker["summary_status"], "failed")
        self.assertTrue(response.get_json()["summary_error"])
        saved = self.storage.get_study_plan_video_marker(marker_id)
        self.assertEqual(saved["note"], "失敗時仍保留名稱")

    def test_page_renders_desktop_mobile_and_fullscreen_controls(self):
        page = self.client.get(
            f"/admin/study-plan?subject={self.video['subject']}&video_id={self.video['id']}"
        )
        html = page.get_data(as_text=True)
        self.assertEqual(page.status_code, 200)
        self.assertIn('id="frame-question-open"', html)
        self.assertIn('id="focus-frame-question"', html)
        self.assertIn('id="frame-question-dialog"', html)
        self.assertIn("max-height: 214px", html)
        self.assertIn("scrollbar-gutter: stable", html)
        self.assertIn("Q 問這一幕", html)
        self.assertIn("max-height:calc(100dvh - 18px)", html)
        self.assertIn("/admin/study-plan/video-question", html)
        self.assertIn("/admin/study-plan/video-frame", html)
        self.assertIn("prefetchFrameQuestionImage", html)
        self.assertIn("正在背景準備這一幕", html)
        self.assertNotIn("getDisplayMedia", html)
        self.assertNotIn("frame_image: frameQuestionContext.frameImage", html)
        self.assertNotIn('id="frame-question-upload"', html)
        self.assertNotIn('id="frame-question-capture"', html)
        self.assertNotIn("leaveFullscreenBeforeCapturePrompt", html)
        self.assertNotIn("分享已自動停止", html)
        self.assertNotIn("RestrictionTarget", html)
        self.assertNotIn("restrictTo(", html)


if __name__ == "__main__":
    unittest.main()
