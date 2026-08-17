import io
import unittest
from unittest.mock import Mock, patch

from PIL import Image, ImageDraw

from e3_tracker.services import youtube_frames
from e3_tracker.services.youtube_frames import YoutubeFrameError, fetch_youtube_storyboard_frame


class YoutubeStoryboardFrameTests(unittest.TestCase):
    def setUp(self):
        youtube_frames._metadata_cache.clear()

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

        response = Mock()
        response.content = sprite_bytes.getvalue()
        response.raise_for_status.return_value = None
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
            result = fetch_youtube_storyboard_frame("9dXuhVJ-L5k", 25.0)

        self.assertAlmostEqual(result["frame_seconds"], 20.0)
        self.assertEqual((result["width"], result["height"]), (640, 360))
        with Image.open(io.BytesIO(result["bytes"])) as frame:
            red, green, blue = frame.resize((1, 1)).getpixel((0, 0))
        self.assertGreater(blue, 180)
        self.assertLess(red, 70)
        self.assertLess(green, 100)

    def test_rejects_invalid_video_id_without_network(self):
        with patch.object(youtube_frames, "_storyboard_info") as storyboard_info:
            with self.assertRaises(YoutubeFrameError):
                fetch_youtube_storyboard_frame("bad", 10)
        storyboard_info.assert_not_called()

    def test_reports_missing_storyboard(self):
        fake_ydl = Mock()
        fake_ydl.__enter__ = Mock(return_value=fake_ydl)
        fake_ydl.__exit__ = Mock(return_value=False)
        fake_ydl.extract_info.return_value = {"duration": 100, "formats": []}
        with patch.object(youtube_frames.yt_dlp, "YoutubeDL", return_value=fake_ydl):
            with self.assertRaisesRegex(YoutubeFrameError, "沒有可用"):
                fetch_youtube_storyboard_frame("9dXuhVJ-L5k", 10)


if __name__ == "__main__":
    unittest.main()
