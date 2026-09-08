import os
import io
from pathlib import Path
import tempfile
import unittest
import subprocess
from unittest.mock import patch

from e3_tracker.services.local_video_media import find_local_video, local_audio_clip, local_frame
from e3_tracker.services.youtube_frames import YoutubeAudioError, YoutubeFrameError, _ffmpeg_executable
from PIL import Image, ImageStat


class LocalVideoMediaTests(unittest.TestCase):
    def test_only_exact_title_inside_configured_root(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'E3_VIDEO_MEDIA_ROOT': directory}):
            subject = Path(directory) / 'subject'
            subject.mkdir()
            path = subject / '001_Lesson.mp4'
            path.touch()
            self.assertEqual(find_local_video({'subject': 'subject', 'title': '001_Lesson'}), path)
            self.assertIsNone(find_local_video({'subject': '../subject', 'title': '001_Lesson'}))
            self.assertIsNone(find_local_video({'subject': 'subject', 'title': '../001_Lesson'}))
            self.assertIsNone(find_local_video({'subject': 'subject', 'title': '002_Lesson'}))

    def test_without_configuration_no_local_files_are_used(self):
        with patch.dict(os.environ, {'E3_VIDEO_MEDIA_ROOT': ''}):
            self.assertIsNone(find_local_video({'subject': 'subject', 'title': '001_Lesson'}))

    def test_decoder_failure_is_audio_failure(self):
        with patch('e3_tracker.services.local_video_media._decode', side_effect=YoutubeFrameError('missing audio')):
            with self.assertRaises(YoutubeAudioError):
                local_audio_clip(Path('missing.mp4'), 10, 30)

    def test_real_ffmpeg_audio_and_frame(self):
        executable = _ffmpeg_executable()
        if not executable:
            self.skipTest('ffmpeg unavailable')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'sample.mp4'
            subprocess.run([
                executable, '-hide_banner', '-loglevel', 'error', '-f', 'lavfi',
                '-i', 'testsrc=size=640x360:rate=10', '-f', 'lavfi',
                '-i', 'sine=frequency=440:sample_rate=16000', '-t', '2',
                '-pix_fmt', 'yuv420p', str(path),
            ], check=True, timeout=15, capture_output=True)
            clip = local_audio_clip(path, .2, 1.2)
            self.assertGreater(len(clip['bytes']), 30000)
            self.assertTrue(clip['bytes'].startswith(b'RIFF'))
            frame = local_frame(path, .5)
            image = Image.open(io.BytesIO(frame['bytes']))
            self.assertEqual(image.size, (640, 360))
            self.assertGreater(max(ImageStat.Stat(image).stddev), 10)
