"""Optional trusted, mounted originals for exact video-question evidence."""

import os
from pathlib import Path
import subprocess

from .youtube_frames import YoutubeAudioError, YoutubeFrameError, _ffmpeg_executable


def find_local_video(video):
    configured = os.getenv('E3_VIDEO_MEDIA_ROOT', '').strip()
    if not configured:
        return None
    root = Path(configured).resolve()
    subject = str(video.get('subject') or '')
    title = str(video.get('title') or '')
    if not subject or not title or any(char in subject + title for char in '/\\\x00'):
        return None
    for extension in ('.mp4', '.mkv', '.webm', '.mov'):
        path = (root / subject / (title + extension)).resolve()
        if path.is_relative_to(root) and path.is_file():
            return path
    return None


def _decode(path, seconds, options):
    executable = _ffmpeg_executable()
    if not executable:
        raise YoutubeFrameError('伺服器沒有可用的影片擷取器。')
    try:
        result = subprocess.run(
            [executable, '-hide_banner', '-loglevel', 'error', '-nostdin',
             '-ss', str(max(0, seconds)), '-i', str(path), *options, 'pipe:1'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=12, check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise YoutubeFrameError('原始影片擷取失敗。') from exc
    if result.returncode or len(result.stdout) < 128:
        raise YoutubeFrameError('原始影片沒有可用的畫面或音訊。')
    return result.stdout


def local_audio_clip(path, start, end):
    try:
        data = _decode(path, start, ['-t', str(end - start), '-vn', '-ac', '1', '-ar', '16000', '-f', 'wav'])
    except YoutubeFrameError as exc:
        raise YoutubeAudioError(str(exc)) from exc
    return {'bytes': data, 'filename': 'video-context.wav', 'mime_type': 'audio/wav',
            'start_seconds': start, 'end_seconds': end, 'duration_seconds': end - start, 'source': 'local_original'}


def local_frame(path, seconds):
    data = _decode(path, seconds, ['-frames:v', '1', '-vf', 'scale=min(1280\\,iw):-2', '-f', 'image2pipe', '-vcodec', 'mjpeg'])
    return {'bytes': data, 'mime_type': 'image/jpeg', 'frame_seconds': seconds,
            'requested_seconds': seconds, 'source': 'exact'}
