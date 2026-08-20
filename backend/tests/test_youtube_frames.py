import io
import unittest
from unittest.mock import Mock, patch

import requests
from PIL import Image, ImageDraw

from e3_tracker.services import youtube_frames
from e3_tracker.services.youtube_frames import (
    YoutubeFrameError,
    _fetch_youtube_storyboard_frame,
    fetch_youtube_precise_frame,
    fetch_youtube_storyboard_frame,
)


class YoutubeStoryboardFrameTests(unittest.TestCase):
    def setUp(self):
        youtube_frames._metadata_cache.clear()

    @staticmethod
    def _sprite_bytes(width, height, color=(25, 60, 225)):
        sprite = Image.new("RGB", (width, height), color)
        output = io.BytesIO()
        sprite.save(output, format="PNG")
        return output.getvalue()

    @staticmethod
    def _response(content):
        response = Mock()
        response.content = content
        response.raise_for_status.return_value = None
        return response

    @staticmethod
    def _frame_bytes(color=(20, 60, 120), size=(960, 540)):
        frame = Image.new("RGB", size, color)
        output = io.BytesIO()
        frame.save(output, format="JPEG")
        return output.getvalue()

    def test_fetches_tile_matching_playback_time(self):
        sprite = Image.new("RGB", (960, 540), "black")
        draw = ImageDraw.Draw(sprite)
        colors = [
            (220, 30, 30),
            (30, 210, 40),
            (25, 60, 225),
            (220, 180, 25),
            (180, 30, 210),
            (25, 200, 210),
            (110, 60, 30),
            (210, 100, 140),
            (215, 215, 215),
        ]
        for index, color in enumerate(colors):
            left = (index % 3) * 320
            top = (index // 3) * 180
            draw.rectangle((left, top, left + 319, top + 179), fill=color)
        sprite_bytes = io.BytesIO()
        sprite.save(sprite_bytes, format="PNG")

        response = self._response(sprite_bytes.getvalue())
        storyboard = {
            "duration": 90.0,
            "format": {
                "protocol": "mhtml",
                "width": 320,
                "height": 180,
                "rows": 3,
                "columns": 3,
                "fragments": [{"url": "https://example.test/sprite.jpg", "duration": 90.0}],
            },
        }
        with patch.object(youtube_frames, "_storyboard_info", return_value=storyboard), patch.object(
            youtube_frames.requests, "get", return_value=response
        ):
            result = _fetch_youtube_storyboard_frame("9dXuhVJ-L5k", 25.0)

        self.assertAlmostEqual(result["frame_seconds"], 20.0)
        self.assertEqual(result["source"], "storyboard")
        self.assertEqual((result["width"], result["height"]), (640, 360))
        with Image.open(io.BytesIO(result["bytes"])) as frame:
            red, green, blue = frame.resize((1, 1)).getpixel((0, 0))
        self.assertGreater(blue, 180)
        self.assertLess(red, 70)
        self.assertLess(green, 100)

    def test_uses_fps_for_partially_filled_final_storyboard_sheet(self):
        response = self._response(self._sprite_bytes(640, 180, (30, 210, 40)))
        storyboard_format = {
            "protocol": "mhtml",
            "width": 320,
            "height": 180,
            "rows": 2,
            "columns": 2,
            "fps": 0.1,
            "fragments": [
                {"url": "https://example.test/s0.jpg", "duration": 40.0},
                {"url": "https://example.test/s1.jpg", "duration": 40.0},
                {"url": "https://example.test/s2.jpg", "duration": 20.0},
            ],
        }
        storyboard = {
            "duration": 100.0,
            "format": storyboard_format,
            "formats": [storyboard_format],
        }
        with patch.object(youtube_frames, "_storyboard_info", return_value=storyboard), patch.object(
            youtube_frames.requests, "get", return_value=response
        ) as get:
            result = _fetch_youtube_storyboard_frame("9dXuhVJ-L5k", 95.0)

        self.assertAlmostEqual(result["frame_seconds"], 90.0)
        self.assertIn("s2.jpg", get.call_args.args[0])
        self.assertEqual((result["width"], result["height"]), (640, 360))

    def test_preserves_yt_dlp_http_headers_for_storyboard_download(self):
        response = self._response(self._sprite_bytes(320, 180))
        storyboard_format = {
            "protocol": "mhtml",
            "width": 320,
            "height": 180,
            "rows": 1,
            "columns": 1,
            "fps": 0.1,
            "http_headers": {
                "User-Agent": "googlebot",
                "Referer": "youtube.com",
                "Accept-Language": "en-us,en;q=0.5",
            },
            "fragments": [{"url": "https://example.test/sprite.jpg", "duration": 10.0}],
        }
        with patch.object(
            youtube_frames,
            "_storyboard_info",
            return_value={"duration": 10.0, "format": storyboard_format, "formats": [storyboard_format]},
        ), patch.object(youtube_frames.requests, "get", return_value=response) as get:
            _fetch_youtube_storyboard_frame("9dXuhVJ-L5k", 4.0)

        headers = get.call_args.kwargs["headers"]
        self.assertEqual(headers["User-Agent"], "googlebot")
        self.assertEqual(headers["Referer"], "youtube.com")
        self.assertEqual(headers["Accept-Language"], "en-us,en;q=0.5")

    def test_falls_back_to_lower_resolution_storyboard(self):
        high = {
            "protocol": "mhtml",
            "width": 320,
            "height": 180,
            "rows": 1,
            "columns": 1,
            "fps": 0.1,
            "fragments": [{"url": "https://example.test/high.jpg", "duration": 10.0}],
        }
        low = {
            "protocol": "mhtml",
            "width": 160,
            "height": 90,
            "rows": 1,
            "columns": 1,
            "fps": 0.1,
            "fragments": [{"url": "https://example.test/low.jpg", "duration": 10.0}],
        }
        bad_response = Mock()
        bad_response.content = b""
        bad_response.raise_for_status.side_effect = requests.HTTPError("403")
        good_response = self._response(self._sprite_bytes(160, 90))

        def get(url, **_kwargs):
            return bad_response if "high.jpg" in url else good_response

        with patch.object(
            youtube_frames,
            "_storyboard_info",
            return_value={"duration": 10.0, "format": high, "formats": [high, low]},
        ), patch.object(youtube_frames.requests, "get", side_effect=get) as request_get:
            result = _fetch_youtube_storyboard_frame("9dXuhVJ-L5k", 4.0)

        self.assertEqual((result["width"], result["height"]), (640, 360))
        self.assertTrue(any("low.jpg" in call.args[0] for call in request_get.call_args_list))

    def test_refreshes_storyboard_metadata_after_download_failures(self):
        stale = {
            "protocol": "mhtml",
            "width": 320,
            "height": 180,
            "rows": 1,
            "columns": 1,
            "fps": 0.1,
            "fragments": [{"url": "https://example.test/stale.jpg", "duration": 10.0}],
        }
        fresh = {
            **stale,
            "fragments": [{"url": "https://example.test/fresh.jpg", "duration": 10.0}],
        }
        bad_response = Mock()
        bad_response.content = b""
        bad_response.raise_for_status.side_effect = requests.HTTPError("403")
        good_response = self._response(self._sprite_bytes(320, 180))

        def get(url, **_kwargs):
            return good_response if "fresh.jpg" in url else bad_response

        with patch.object(
            youtube_frames,
            "_storyboard_info",
            side_effect=[
                {"duration": 10.0, "format": stale, "formats": [stale]},
                {"duration": 10.0, "format": fresh, "formats": [fresh]},
            ],
        ) as storyboard_info, patch.object(youtube_frames.requests, "get", side_effect=get):
            result = _fetch_youtube_storyboard_frame("9dXuhVJ-L5k", 4.0)

        self.assertEqual((result["width"], result["height"]), (640, 360))
        self.assertEqual(storyboard_info.call_count, 2)
        self.assertFalse(storyboard_info.call_args_list[0].kwargs["force_refresh"])
        self.assertTrue(storyboard_info.call_args_list[1].kwargs["force_refresh"])

    def test_precise_frame_seeks_to_requested_timestamp_and_forwards_headers(self):
        stream = {
            "url": "https://example.test/video.mp4",
            "protocol": "https",
            "height": 720,
            "vcodec": "avc1",
            "acodec": "mp4a",
            "ext": "mp4",
            "http_headers": {
                "User-Agent": "test-agent",
                "Referer": "https://www.youtube.com/",
                "Accept-Language": "zh-TW",
            },
        }
        completed = Mock(returncode=0, stdout=self._frame_bytes(), stderr=b"")
        with patch.object(
            youtube_frames,
            "_youtube_info",
            return_value={"duration": 500.0, "streams": [stream], "formats": []},
        ), patch.object(youtube_frames, "_ffmpeg_executable", return_value="/fake/ffmpeg"), patch.object(
            youtube_frames.subprocess, "run", return_value=completed
        ) as run:
            result = fetch_youtube_precise_frame("9dXuhVJ-L5k", 123.456)

        command = run.call_args.args[0]
        self.assertEqual(result["source"], "exact")
        self.assertAlmostEqual(result["frame_seconds"], 123.456, places=3)
        self.assertIn("-ss", command)
        self.assertEqual(command[command.index("-ss") + 1], "123.456")
        self.assertIn("https://example.test/video.mp4", command)
        self.assertIn("-user_agent", command)
        self.assertEqual(command[command.index("-user_agent") + 1], "test-agent")
        self.assertIn("-referer", command)
        self.assertIn("Accept-Language: zh-TW", command[command.index("-headers") + 1])

    def test_precise_frame_tries_another_stream_when_first_fails(self):
        streams = [
            {"url": "https://example.test/a.mp4", "protocol": "https", "height": 720, "vcodec": "avc1"},
            {"url": "https://example.test/b.mp4", "protocol": "https", "height": 480, "vcodec": "avc1"},
        ]
        failed = Mock(returncode=1, stdout=b"", stderr=b"403")
        success = Mock(returncode=0, stdout=self._frame_bytes(), stderr=b"")
        with patch.object(
            youtube_frames,
            "_youtube_info",
            return_value={"duration": 500.0, "streams": streams, "formats": []},
        ), patch.object(youtube_frames, "_ffmpeg_executable", return_value="/fake/ffmpeg"), patch.object(
            youtube_frames.subprocess, "run", side_effect=[failed, success]
        ) as run:
            result = fetch_youtube_precise_frame("9dXuhVJ-L5k", 45.0)

        self.assertEqual(result["source"], "exact")
        self.assertEqual(run.call_count, 2)
        self.assertIn("b.mp4", run.call_args_list[1].args[0][run.call_args_list[1].args[0].index("-i") + 1])

    def test_public_fetch_prefers_precise_frame(self):
        exact = {
            "bytes": self._frame_bytes(),
            "mime_type": "image/jpeg",
            "requested_seconds": 33.3,
            "frame_seconds": 33.3,
            "width": 960,
            "height": 540,
            "source": "exact",
        }
        with patch.object(youtube_frames, "fetch_youtube_precise_frame", return_value=exact) as precise, patch.object(
            youtube_frames, "_fetch_youtube_storyboard_frame"
        ) as storyboard:
            result = fetch_youtube_storyboard_frame("9dXuhVJ-L5k", 33.3)
        self.assertEqual(result["source"], "exact")
        precise.assert_called_once()
        storyboard.assert_not_called()

    def test_public_fetch_falls_back_to_storyboard(self):
        fallback = {
            "bytes": self._frame_bytes(),
            "mime_type": "image/jpeg",
            "requested_seconds": 33.3,
            "frame_seconds": 30.0,
            "width": 960,
            "height": 540,
            "source": "storyboard",
        }
        with patch.object(
            youtube_frames,
            "fetch_youtube_precise_frame",
            side_effect=YoutubeFrameError("precise unavailable"),
        ), patch.object(youtube_frames, "_fetch_youtube_storyboard_frame", return_value=fallback) as storyboard:
            result = fetch_youtube_storyboard_frame("9dXuhVJ-L5k", 33.3)
        self.assertEqual(result["source"], "storyboard")
        storyboard.assert_called_once()

    def test_rejects_invalid_video_id_without_network(self):
        with patch.object(youtube_frames, "_youtube_info") as youtube_info:
            with self.assertRaises(YoutubeFrameError):
                fetch_youtube_precise_frame("bad", 10)
        youtube_info.assert_not_called()

    def test_reports_missing_storyboard(self):
        fake_ydl = Mock()
        fake_ydl.__enter__ = Mock(return_value=fake_ydl)
        fake_ydl.__exit__ = Mock(return_value=False)
        fake_ydl.extract_info.return_value = {"duration": 100, "formats": []}
        with patch.object(youtube_frames.yt_dlp, "YoutubeDL", return_value=fake_ydl):
            with self.assertRaisesRegex(YoutubeFrameError, "沒有可用"):
                _fetch_youtube_storyboard_frame("9dXuhVJ-L5k", 10)


if __name__ == "__main__":
    unittest.main()
