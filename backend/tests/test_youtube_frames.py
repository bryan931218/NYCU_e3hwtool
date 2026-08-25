import io
import json
import unittest
from unittest.mock import Mock, patch

import requests
from PIL import Image, ImageDraw

from e3_tracker.services import youtube_frames
from e3_tracker.services.youtube_frames import (
    YoutubeFrameError,
    _fetch_youtube_storyboard_frame,
    fetch_youtube_cached_frame,
    fetch_youtube_precise_frame,
    fetch_youtube_storyboard_frame,
)


class YoutubeStoryboardFrameTests(unittest.TestCase):
    def setUp(self):
        youtube_frames._metadata_cache.clear()
        youtube_frames._metadata_video_locks.clear()
        youtube_frames._frame_cache.clear()
        youtube_frames._frame_video_locks.clear()
        youtube_frames._storyboard_catalog = None

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

    def test_public_fetch_prefers_reliable_storyboard_frame(self):
        storyboard_frame = {
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
            "_fetch_youtube_reliable_storyboard_frame",
            return_value=storyboard_frame,
        ) as storyboard, patch.object(youtube_frames, "fetch_youtube_precise_frame") as precise:
            result = fetch_youtube_storyboard_frame("9dXuhVJ-L5k", 33.3)
        self.assertEqual(result["source"], "storyboard")
        storyboard.assert_called_once()
        precise.assert_not_called()

    def test_public_fetch_falls_back_to_precise_frame(self):
        exact = {
            "bytes": self._frame_bytes(),
            "mime_type": "image/jpeg",
            "requested_seconds": 33.3,
            "frame_seconds": 33.3,
            "width": 960,
            "height": 540,
            "source": "exact",
        }
        with patch.object(
            youtube_frames,
            "_fetch_youtube_reliable_storyboard_frame",
            side_effect=YoutubeFrameError("storyboard unavailable"),
        ) as storyboard, patch.object(youtube_frames, "fetch_youtube_precise_frame", return_value=exact) as precise:
            result = fetch_youtube_storyboard_frame("9dXuhVJ-L5k", 33.3)
        self.assertEqual(result["source"], "exact")
        storyboard.assert_called_once()
        precise.assert_called_once()

    def test_reliable_storyboard_uses_bundled_metadata_without_extracting(self):
        frame = {"source": "storyboard", "frame_seconds": 30.0}
        bundled = {"duration": 100.0, "formats": [{"format_id": "bundled"}]}
        with patch.object(youtube_frames, "_bundled_storyboard_info", return_value=bundled), patch.object(
            youtube_frames, "_frame_from_storyboard_info", return_value=frame
        ) as render, patch.object(youtube_frames, "_extract_watch_page_storyboards") as watch_page, patch.object(
            youtube_frames, "_fetch_youtube_storyboard_frame"
        ) as slow_fallback:
            result = youtube_frames._fetch_youtube_reliable_storyboard_frame("9dXuhVJ-L5k", 33.3)

        self.assertIs(result, frame)
        render.assert_called_once()
        watch_page.assert_not_called()
        slow_fallback.assert_not_called()

    def test_reliable_storyboard_refreshes_an_expired_bundled_signature(self):
        frame = {"source": "storyboard", "frame_seconds": 30.0}
        bundled = {"duration": 100.0, "formats": [{"format_id": "bundled"}]}
        live = {"duration": 100.0, "formats": [{"format_id": "live"}]}
        with patch.object(youtube_frames, "_bundled_storyboard_info", return_value=bundled), patch.object(
            youtube_frames,
            "_frame_from_storyboard_info",
            side_effect=[YoutubeFrameError("expired signature"), frame],
        ) as render, patch.object(
            youtube_frames, "_extract_watch_page_storyboards", return_value=live
        ) as watch_page, patch.object(youtube_frames, "_fetch_youtube_storyboard_frame") as slow_fallback:
            result = youtube_frames._fetch_youtube_reliable_storyboard_frame("9dXuhVJ-L5k", 33.3)

        self.assertIs(result, frame)
        self.assertEqual(render.call_count, 2)
        watch_page.assert_called_once_with("9dXuhVJ-L5k")
        slow_fallback.assert_not_called()

    def test_reliable_storyboard_supports_video_missing_from_catalog(self):
        frame = {"source": "storyboard", "frame_seconds": 30.0}
        live = {"duration": 100.0, "formats": [{"format_id": "live"}]}
        with patch.object(youtube_frames, "_bundled_storyboard_info", return_value=None), patch.object(
            youtube_frames, "_extract_watch_page_storyboards", return_value=live
        ) as watch_page, patch.object(
            youtube_frames, "_frame_from_storyboard_info", return_value=frame
        ), patch.object(youtube_frames, "_fetch_youtube_storyboard_frame") as slow_fallback:
            result = youtube_frames._fetch_youtube_reliable_storyboard_frame("9dXuhVJ-L5k", 33.3)

        self.assertIs(result, frame)
        watch_page.assert_called_once_with("9dXuhVJ-L5k")
        slow_fallback.assert_not_called()

    def test_cached_frame_reuses_prefetched_timestamp(self):
        frame = {
            "bytes": self._frame_bytes(),
            "mime_type": "image/jpeg",
            "requested_seconds": 987.654,
            "frame_seconds": 987.654,
            "width": 960,
            "height": 540,
            "source": "exact",
        }
        with patch.object(youtube_frames, "fetch_youtube_storyboard_frame", return_value=frame) as fetch:
            first = fetch_youtube_cached_frame("9dXuhVJ-L5k", 987.654)
            second = fetch_youtube_cached_frame("9dXuhVJ-L5k", 987.654)
        self.assertIs(first, second)
        fetch.assert_called_once()

    def test_rejects_invalid_video_id_without_network(self):
        with patch.object(youtube_frames, "_youtube_info") as youtube_info:
            with self.assertRaises(YoutubeFrameError):
                fetch_youtube_precise_frame("bad", 10)
        youtube_info.assert_not_called()

    def test_extractor_allows_storyboard_only_responses(self):
        options = youtube_frames._youtube_dl_options(None)

        self.assertTrue(options["ignore_no_formats_error"])
        self.assertTrue(options["skip_download"])
        self.assertTrue(options["noplaylist"])

    def test_extractor_enables_ejs_remote_component_when_node_is_available(self):
        with patch.object(youtube_frames.shutil, "which", side_effect=lambda name: "/usr/bin/node" if name == "node" else None):
            options = youtube_frames._youtube_dl_options(None)

        self.assertEqual(options["js_runtimes"], {"node": {}})
        self.assertEqual(options["remote_components"], {"ejs:github"})

    def test_extracts_storyboard_metadata_directly_from_watch_page(self):
        player_response = {
            "videoDetails": {"lengthSeconds": "100"},
            "storyboards": {
                "playerStoryboardSpecRenderer": {
                    "spec": (
                        "https://i.ytimg.com/sb/9dXuhVJ-L5k/storyboard3_L$L/$N.jpg?x=1"
                        "|320#180#10#3#3#10000#M$M#signature"
                    )
                }
            },
        }
        html = f"<script>var ytInitialPlayerResponse = {json.dumps(player_response)};</script>"
        response = Mock(
            content=html.encode("utf-8"),
            text=html,
        )
        response.raise_for_status.return_value = None
        with patch.object(youtube_frames.requests, "get", return_value=response):
            result = youtube_frames._extract_watch_page_storyboards("9dXuhVJ-L5k")

        storyboard = result["formats"][0]
        self.assertEqual(result["duration"], 100.0)
        self.assertEqual((storyboard["width"], storyboard["height"]), (320, 180))
        self.assertEqual(len(storyboard["fragments"]), 2)
        self.assertIn("storyboard3_L0/M0.jpg", storyboard["fragments"][0]["url"])
        self.assertIn("sigh=signature", storyboard["fragments"][0]["url"])

    def test_metadata_uses_watch_page_when_all_yt_dlp_clients_fail(self):
        failed_ydl = Mock()
        failed_ydl.__enter__ = Mock(return_value=failed_ydl)
        failed_ydl.__exit__ = Mock(return_value=False)
        failed_ydl.extract_info.side_effect = RuntimeError("blocked")
        fallback = {
            "duration": 90,
            "formats": [
                {
                    "protocol": "mhtml",
                    "width": 320,
                    "height": 180,
                    "rows": 1,
                    "columns": 1,
                    "fragments": [{"url": "https://example.test/sprite", "duration": 10}],
                }
            ],
        }
        with patch.object(youtube_frames.yt_dlp, "YoutubeDL", return_value=failed_ydl), patch.object(
            youtube_frames, "_bundled_storyboard_info", return_value=None
        ), patch.object(
            youtube_frames, "_extract_watch_page_storyboards", return_value=fallback
        ) as watch_page, patch.object(youtube_frames.time, "sleep"):
            result = youtube_frames._extract_youtube_info("9dXuhVJ-L5k")

        self.assertEqual(result, fallback)
        watch_page.assert_called_once_with("9dXuhVJ-L5k")

    def test_metadata_uses_bundled_catalog_when_network_extractors_fail(self):
        failed_ydl = Mock()
        failed_ydl.__enter__ = Mock(return_value=failed_ydl)
        failed_ydl.__exit__ = Mock(return_value=False)
        failed_ydl.extract_info.side_effect = RuntimeError("blocked")
        bundled = {
            "duration": 90,
            "formats": [
                {
                    "protocol": "mhtml",
                    "width": 320,
                    "height": 180,
                    "rows": 1,
                    "columns": 1,
                    "fragments": [{"url": "https://example.test/sprite", "duration": 10}],
                }
            ],
            "_metadata_source": "bundled_catalog",
        }
        with patch.object(youtube_frames.yt_dlp, "YoutubeDL", return_value=failed_ydl), patch.object(
            youtube_frames,
            "_extract_watch_page_storyboards",
            side_effect=YoutubeFrameError("watch page unavailable"),
        ), patch.object(youtube_frames, "_bundled_storyboard_info", return_value=bundled), patch.object(
            youtube_frames.time, "sleep"
        ):
            result = youtube_frames._extract_youtube_info("9dXuhVJ-L5k")

        self.assertEqual(result, bundled)

    def test_bundled_catalog_expands_compact_storyboard_spec(self):
        youtube_frames._storyboard_catalog = {
            "9dXuhVJ-L5k": {
                "duration": 100,
                "spec": (
                    "https://i.ytimg.com/sb/9dXuhVJ-L5k/storyboard3_L$L/$N.jpg?x=1"
                    "|320#180#10#3#3#10000#M$M#signature"
                ),
            }
        }

        result = youtube_frames._bundled_storyboard_info("9dXuhVJ-L5k")

        self.assertIsNotNone(result)
        self.assertEqual(result["_metadata_source"], "bundled_catalog")
        self.assertIn("sigh=signature", result["formats"][0]["fragments"][0]["url"])

    def test_metadata_refresh_failure_reuses_recent_stale_storyboard(self):
        cached = {
            "duration": 90.0,
            "format": None,
            "formats": [{"protocol": "mhtml", "fragments": []}],
            "streams": [],
        }
        youtube_frames._metadata_cache["9dXuhVJ-L5k"] = (
            youtube_frames.time.monotonic() - youtube_frames._METADATA_CACHE_TTL_SECONDS - 1,
            cached,
        )
        with patch.object(
            youtube_frames,
            "_extract_youtube_info",
            side_effect=youtube_frames.YoutubeMetadataError("temporarily blocked"),
        ):
            result = youtube_frames._youtube_info("9dXuhVJ-L5k", force_refresh=True)

        self.assertIs(result, cached)

    def test_metadata_retries_with_an_alternate_youtube_client(self):
        failed_ydl = Mock()
        failed_ydl.__enter__ = Mock(return_value=failed_ydl)
        failed_ydl.__exit__ = Mock(return_value=False)
        failed_ydl.extract_info.side_effect = RuntimeError("temporary failure")
        working_ydl = Mock()
        working_ydl.__enter__ = Mock(return_value=working_ydl)
        working_ydl.__exit__ = Mock(return_value=False)
        working_ydl.extract_info.return_value = {
            "duration": 90,
            "formats": [
                {
                    "protocol": "mhtml",
                    "width": 320,
                    "height": 180,
                    "rows": 1,
                    "columns": 1,
                    "fragments": [{"url": "https://example.test/sprite", "duration": 10}],
                }
            ],
        }
        with patch.object(
            youtube_frames.yt_dlp,
            "YoutubeDL",
            side_effect=[failed_ydl, working_ydl],
        ) as youtube_dl, patch.object(
            youtube_frames, "_bundled_storyboard_info", return_value=None
        ), patch.object(youtube_frames.time, "sleep"):
            result = youtube_frames._extract_youtube_info("9dXuhVJ-L5k")

        self.assertEqual(result["duration"], 90)
        self.assertEqual(youtube_dl.call_count, 2)
        fallback_options = youtube_dl.call_args_list[1].args[0]
        self.assertEqual(
            fallback_options["extractor_args"]["youtube"]["player_client"],
            ["android_vr"],
        )

    def test_metadata_retries_when_first_response_has_no_frame_formats(self):
        empty_ydl = Mock()
        empty_ydl.__enter__ = Mock(return_value=empty_ydl)
        empty_ydl.__exit__ = Mock(return_value=False)
        empty_ydl.extract_info.return_value = {"duration": 90, "formats": []}
        working_ydl = Mock()
        working_ydl.__enter__ = Mock(return_value=working_ydl)
        working_ydl.__exit__ = Mock(return_value=False)
        working_ydl.extract_info.return_value = {
            "duration": 90,
            "formats": [
                {
                    "url": "https://example.test/video.mp4",
                    "protocol": "https",
                    "height": 720,
                    "vcodec": "avc1",
                }
            ],
        }
        with patch.object(
            youtube_frames.yt_dlp,
            "YoutubeDL",
            side_effect=[empty_ydl, working_ydl],
        ) as youtube_dl, patch.object(
            youtube_frames, "_bundled_storyboard_info", return_value=None
        ), patch.object(youtube_frames.time, "sleep"):
            result = youtube_frames._extract_youtube_info("9dXuhVJ-L5k")

        self.assertEqual(result["formats"][0]["height"], 720)
        self.assertEqual(youtube_dl.call_count, 2)

    def test_reports_missing_storyboard(self):
        fake_ydl = Mock()
        fake_ydl.__enter__ = Mock(return_value=fake_ydl)
        fake_ydl.__exit__ = Mock(return_value=False)
        fake_ydl.extract_info.return_value = {"duration": 100, "formats": []}
        with patch.object(youtube_frames.yt_dlp, "YoutubeDL", return_value=fake_ydl), patch.object(
            youtube_frames,
            "_bundled_storyboard_info",
            return_value=None,
        ), patch.object(
            youtube_frames,
            "_extract_watch_page_storyboards",
            side_effect=YoutubeFrameError("watch page unavailable"),
        ):
            with self.assertRaisesRegex(YoutubeFrameError, "YouTube 暫時無法提供"):
                _fetch_youtube_storyboard_frame("9dXuhVJ-L5k", 10)


if __name__ == "__main__":
    unittest.main()
