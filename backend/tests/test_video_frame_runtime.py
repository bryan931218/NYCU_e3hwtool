import unittest
from unittest.mock import Mock, patch

from e3_tracker.services.youtube_frames import YoutubeFrameError
from e3_tracker.shared import video_frame_runtime as runtime


class VideoFrameRuntimeTests(unittest.TestCase):
    def setUp(self):
        runtime._client_cache.clear()
        runtime._embed_cache.clear()

    def test_parses_embed_storyboard_spec_high_quality_first(self):
        spec = (
            "https://i.ytimg.com/sb/abcdefghijk/storyboard3_L$L/$N.jpg?sqp=test"
            "|48#27#100#10#10#0#default#sig0"
            "|160#90#102#5#5#10000#M$M#sig1"
        )
        formats = runtime._storyboards_from_spec(spec, 1002.0)
        self.assertEqual(len(formats), 2)
        self.assertEqual(formats[0]["width"], 160)
        self.assertEqual(formats[0]["height"], 90)
        self.assertEqual(formats[0]["columns"], 5)
        self.assertEqual(formats[0]["rows"], 5)
        self.assertIn("storyboard3_L1/M0.jpg", formats[0]["fragments"][0]["url"])
        self.assertIn("sigh=sig1", formats[0]["fragments"][0]["url"])

    def test_embed_metadata_parses_without_ytdlp(self):
        spec = (
            "https://i.ytimg.com/sb/abcdefghijk/storyboard3_L$L/$N.jpg?sqp=test"
            "|160#90#20#5#4#10000#M$M#sig"
        )
        page = (
            '<script>var x={"videoDetails":{"lengthSeconds":"200"},'
            '"storyboards":{"playerStoryboardSpecRenderer":{"spec":'
            + repr(spec).replace("'", '"')
            + '}}};</script>'
        )
        response = Mock()
        response.text = page
        response.raise_for_status.return_value = None
        with patch.object(runtime.requests, "get", return_value=response):
            info = runtime._embed_storyboard_info("abcdefghijk")
        self.assertEqual(info["duration"], 200.0)
        self.assertTrue(info["formats"])

    def test_web_embedded_client_is_forced_for_exact_metadata(self):
        fake_ydl = Mock()
        fake_ydl.__enter__ = Mock(return_value=fake_ydl)
        fake_ydl.__exit__ = Mock(return_value=False)
        fake_ydl.extract_info.return_value = {"formats": [], "duration": 10}
        with patch.object(runtime.yt_dlp, "YoutubeDL", return_value=fake_ydl) as factory:
            runtime._extract_with_player_client("abcdefghijk", "web_embedded")
        options = factory.call_args.args[0]
        self.assertEqual(
            options["extractor_args"]["youtube"]["player_client"],
            ["web_embedded"],
        )

    def test_public_runtime_uses_embed_storyboard_when_exact_clients_fail(self):
        expected = {
            "bytes": b"jpeg",
            "mime_type": "image/jpeg",
            "frame_seconds": 12.0,
            "requested_seconds": 12.3,
            "source": "storyboard:embed",
        }
        with patch.object(
            runtime,
            "_fetch_exact_with_client",
            side_effect=YoutubeFrameError("blocked"),
        ) as exact, patch.object(
            runtime,
            "_fetch_embed_storyboard_frame",
            return_value=expected,
        ) as embed, patch.object(
            runtime.base_frames,
            "fetch_youtube_storyboard_frame",
        ) as legacy:
            result = runtime.fetch_reliable_youtube_frame("abcdefghijk", 12.3)
        self.assertIs(result, expected)
        self.assertEqual(exact.call_count, len(runtime._PLAYER_CLIENTS))
        embed.assert_called_once()
        legacy.assert_not_called()

    def test_legacy_path_remains_last_fallback(self):
        expected = {
            "bytes": b"jpeg",
            "mime_type": "image/jpeg",
            "frame_seconds": 10.0,
            "requested_seconds": 12.3,
            "source": "storyboard",
        }
        with patch.object(
            runtime,
            "_fetch_exact_with_client",
            side_effect=YoutubeFrameError("blocked"),
        ), patch.object(
            runtime,
            "_fetch_embed_storyboard_frame",
            side_effect=YoutubeFrameError("embed blocked"),
        ), patch.object(
            runtime.base_frames,
            "fetch_youtube_storyboard_frame",
            return_value=expected,
        ):
            result = runtime.fetch_reliable_youtube_frame("abcdefghijk", 12.3)
        self.assertIs(result, expected)

    def test_install_patches_web_binding(self):
        web_module = Mock()
        web_module._RELIABLE_VIDEO_FRAME_RUNTIME_INSTALLED = False
        runtime.install_video_frame_runtime(web_module)
        self.assertIs(web_module.fetch_youtube_storyboard_frame, runtime.fetch_reliable_youtube_frame)
        self.assertTrue(web_module._RELIABLE_VIDEO_FRAME_RUNTIME_INSTALLED)


if __name__ == "__main__":
    unittest.main()
