"""Fetch the closest available YouTube storyboard tile for a playback time."""

from __future__ import annotations

import io
import math
import re
import threading
import time
from typing import Any, Dict, Tuple

import requests
import yt_dlp
from PIL import Image


YOUTUBE_VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
_METADATA_CACHE_TTL_SECONDS = 10 * 60
_METADATA_CACHE_LIMIT = 32
_metadata_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_metadata_lock = threading.Lock()


class YoutubeFrameError(RuntimeError):
    """Raised when a usable YouTube storyboard frame cannot be obtained."""


def _storyboard_info(video_id: str) -> Dict[str, Any]:
    now = time.monotonic()
    with _metadata_lock:
        cached = _metadata_cache.get(video_id)
        if cached and now - cached[0] < _METADATA_CACHE_TTL_SECONDS:
            return cached[1]

    options = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "socket_timeout": 20,
        "retries": 2,
        "extractor_retries": 2,
    }
    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
    except Exception as exc:
        raise YoutubeFrameError("目前無法讀取這部影片的預覽影格。") from exc
    if not isinstance(info, dict):
        raise YoutubeFrameError("這部影片目前沒有可用的預覽影格。")

    formats = [
        item
        for item in (info.get("formats") or [])
        if isinstance(item, dict)
        and str(item.get("protocol") or "") == "mhtml"
        and item.get("fragments")
        and int(item.get("width") or 0) > 0
        and int(item.get("height") or 0) > 0
        and int(item.get("rows") or 0) > 0
        and int(item.get("columns") or 0) > 0
    ]
    if not formats:
        raise YoutubeFrameError("這部影片目前沒有可用的預覽影格。")
    storyboard = max(
        formats,
        key=lambda item: int(item.get("width") or 0) * int(item.get("height") or 0),
    )
    result = {
        "duration": float(info.get("duration") or 0),
        "format": storyboard,
    }
    with _metadata_lock:
        if len(_metadata_cache) >= _METADATA_CACHE_LIMIT:
            oldest = min(_metadata_cache, key=lambda key: _metadata_cache[key][0])
            _metadata_cache.pop(oldest, None)
        _metadata_cache[video_id] = (now, result)
    return result


def fetch_youtube_storyboard_frame(
    youtube_video_id: str,
    playback_seconds: float,
    *,
    timeout: int = 25,
) -> Dict[str, Any]:
    """Return a JPEG of the storyboard tile closest to ``playback_seconds``."""

    video_id = str(youtube_video_id or "").strip()
    if not YOUTUBE_VIDEO_ID_RE.fullmatch(video_id):
        raise YoutubeFrameError("影片代碼無效。")
    try:
        requested_seconds = float(playback_seconds)
    except (TypeError, ValueError) as exc:
        raise YoutubeFrameError("播放時間無效。") from exc
    if not math.isfinite(requested_seconds) or requested_seconds < 0:
        raise YoutubeFrameError("播放時間無效。")

    info = _storyboard_info(video_id)
    duration = max(0.0, float(info.get("duration") or 0))
    if duration > 0:
        requested_seconds = min(requested_seconds, duration)
    storyboard = info["format"]
    fragments = storyboard.get("fragments") or []
    rows = int(storyboard.get("rows") or 0)
    columns = int(storyboard.get("columns") or 0)
    tile_count = rows * columns

    sheet_start = 0.0
    selected_fragment = fragments[-1]
    fragment_duration = max(0.001, float(selected_fragment.get("duration") or 0))
    for fragment in fragments:
        candidate_duration = max(0.001, float(fragment.get("duration") or 0))
        if requested_seconds < sheet_start + candidate_duration or fragment is fragments[-1]:
            selected_fragment = fragment
            fragment_duration = candidate_duration
            break
        sheet_start += candidate_duration

    interval = fragment_duration / tile_count
    local_seconds = max(0.0, requested_seconds - sheet_start)
    tile_index = min(tile_count - 1, int(local_seconds // interval))
    frame_seconds = min(duration or requested_seconds, sheet_start + tile_index * interval)
    image_url = str(selected_fragment.get("url") or "").strip()
    if not image_url.startswith(("https://", "http://")):
        raise YoutubeFrameError("這部影片目前沒有可用的預覽影格。")

    try:
        response = requests.get(image_url, timeout=timeout)
        response.raise_for_status()
        if len(response.content) > 3 * 1024 * 1024:
            raise YoutubeFrameError("影片預覽影格過大，暫時無法分析。")
        with Image.open(io.BytesIO(response.content)) as sprite:
            sprite.load()
            tile_width = int(storyboard.get("width") or 0)
            tile_height = int(storyboard.get("height") or 0)
            column = tile_index % columns
            row = tile_index // columns
            left = column * tile_width
            top = row * tile_height
            if left + tile_width > sprite.width or top + tile_height > sprite.height:
                raise YoutubeFrameError("影片預覽影格格式異常。")
            frame = sprite.crop((left, top, left + tile_width, top + tile_height)).convert("RGB")
            if frame.width < 640:
                frame = frame.resize((640, round(frame.height * 640 / frame.width)), Image.Resampling.LANCZOS)
            output = io.BytesIO()
            frame.save(output, format="JPEG", quality=90, optimize=True)
    except YoutubeFrameError:
        raise
    except Exception as exc:
        raise YoutubeFrameError("目前無法下載這部影片的預覽影格。") from exc

    return {
        "bytes": output.getvalue(),
        "mime_type": "image/jpeg",
        "requested_seconds": requested_seconds,
        "frame_seconds": max(0.0, frame_seconds),
        "width": frame.width,
        "height": frame.height,
    }
