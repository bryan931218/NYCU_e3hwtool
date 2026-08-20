"""Fetch the closest available YouTube storyboard tile for a playback time."""

from __future__ import annotations

import io
import math
import re
import threading
import time
from typing import Any, Dict, List, Tuple

import requests
import yt_dlp
from PIL import Image


YOUTUBE_VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
_METADATA_CACHE_TTL_SECONDS = 10 * 60
_METADATA_CACHE_LIMIT = 32
_SPRITE_DOWNLOAD_ATTEMPTS = 2
_MAX_SPRITE_BYTES = 3 * 1024 * 1024
_metadata_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_metadata_lock = threading.Lock()


class YoutubeFrameError(RuntimeError):
    """Raised when a usable YouTube storyboard frame cannot be obtained."""


def _positive_int(value: Any) -> int:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return max(0, parsed)


def _positive_float(value: Any) -> float:
    try:
        parsed = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(parsed):
        return 0.0
    return max(0.0, parsed)


def _storyboard_info(video_id: str, *, force_refresh: bool = False) -> Dict[str, Any]:
    now = time.monotonic()
    if not force_refresh:
        with _metadata_lock:
            cached = _metadata_cache.get(video_id)
            if cached and now - cached[0] < _METADATA_CACHE_TTL_SECONDS:
                return cached[1]
    else:
        with _metadata_lock:
            _metadata_cache.pop(video_id, None)

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
        and _positive_int(item.get("width")) > 0
        and _positive_int(item.get("height")) > 0
        and _positive_int(item.get("rows")) > 0
        and _positive_int(item.get("columns")) > 0
    ]
    if not formats:
        raise YoutubeFrameError("這部影片目前沒有可用的預覽影格。")
    formats.sort(
        key=lambda item: _positive_int(item.get("width")) * _positive_int(item.get("height")),
        reverse=True,
    )
    result = {
        "duration": _positive_float(info.get("duration")),
        # Keep ``format`` for compatibility while retaining all storyboard
        # quality levels for fallback when one CDN sheet is temporarily bad.
        "format": formats[0],
        "formats": formats,
    }
    with _metadata_lock:
        if len(_metadata_cache) >= _METADATA_CACHE_LIMIT:
            oldest = min(_metadata_cache, key=lambda key: _metadata_cache[key][0])
            _metadata_cache.pop(oldest, None)
        _metadata_cache[video_id] = (now, result)
    return result


def _storyboard_formats(info: Dict[str, Any]) -> List[Dict[str, Any]]:
    formats = [item for item in (info.get("formats") or []) if isinstance(item, dict)]
    legacy = info.get("format")
    if isinstance(legacy, dict) and legacy not in formats:
        formats.append(legacy)
    if not formats and isinstance(legacy, dict):
        formats = [legacy]
    formats.sort(
        key=lambda item: _positive_int(item.get("width")) * _positive_int(item.get("height")),
        reverse=True,
    )
    return formats


def _storyboard_interval(
    storyboard: Dict[str, Any],
    fragments: List[Dict[str, Any]],
    tile_count: int,
) -> float:
    fps = _positive_float(storyboard.get("fps"))
    if fps > 0:
        return 1.0 / fps

    # yt-dlp normally exposes fps for YouTube storyboards. For older metadata,
    # derive the interval from a non-final sheet, which is normally full. Using
    # a short final fragment divided by rows*columns is incorrect because the
    # final sheet is often only partially populated.
    for fragment in fragments[:-1]:
        fragment_duration = _positive_float(fragment.get("duration"))
        if fragment_duration > 0:
            return max(0.001, fragment_duration / tile_count)
    if fragments:
        fragment_duration = _positive_float(fragments[0].get("duration"))
        if fragment_duration > 0:
            return max(0.001, fragment_duration / tile_count)
    return 1.0


def _storyboard_location(
    storyboard: Dict[str, Any],
    requested_seconds: float,
    duration: float,
) -> Tuple[Dict[str, Any], int, int, float]:
    fragments = [
        item
        for item in (storyboard.get("fragments") or [])
        if isinstance(item, dict)
    ]
    rows = _positive_int(storyboard.get("rows"))
    columns = _positive_int(storyboard.get("columns"))
    tile_count = rows * columns
    if not fragments or tile_count <= 0:
        raise YoutubeFrameError("這部影片目前沒有可用的預覽影格。")

    interval = _storyboard_interval(storyboard, fragments, tile_count)
    sheet_start = 0.0
    selected_fragment = fragments[-1]
    fragment_duration = max(0.001, _positive_float(selected_fragment.get("duration")))
    for index, fragment in enumerate(fragments):
        candidate_duration = max(0.001, _positive_float(fragment.get("duration")))
        if requested_seconds < sheet_start + candidate_duration or index == len(fragments) - 1:
            selected_fragment = fragment
            fragment_duration = candidate_duration
            break
        sheet_start += candidate_duration

    # The last storyboard sheet commonly has fewer real tiles than rows*columns.
    # Use fps (or a full-sheet-derived interval) to count actual frames so a
    # timestamp near the end never points into an empty/out-of-bounds cell.
    expected_tiles = min(
        tile_count,
        max(1, int(math.ceil(fragment_duration / interval - 1e-9))),
    )
    local_seconds = max(0.0, requested_seconds - sheet_start)
    tile_index = min(expected_tiles - 1, max(0, int(local_seconds // interval)))
    frame_seconds = sheet_start + tile_index * interval
    if duration > 0:
        frame_seconds = min(duration, frame_seconds)
    return selected_fragment, tile_index, expected_tiles, max(0.0, frame_seconds)


def _request_headers(storyboard: Dict[str, Any], fragment: Dict[str, Any]) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for source in (storyboard.get("http_headers"), fragment.get("http_headers")):
        if not isinstance(source, dict):
            continue
        for key, value in source.items():
            if value is not None and str(key).strip():
                headers[str(key)] = str(value)

    # yt-dlp can supply a storyboard-specific User-Agent and Referer. Preserve
    # them exactly; only add normal image-request defaults when absent.
    headers.setdefault(
        "User-Agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0 Safari/537.36",
    )
    headers.setdefault("Accept", "image/avif,image/webp,image/apng,image/*,*/*;q=0.8")
    headers.setdefault("Referer", "https://www.youtube.com/")
    return headers


def _download_storyboard_sprite(
    image_url: str,
    storyboard: Dict[str, Any],
    fragment: Dict[str, Any],
    *,
    timeout: int,
) -> bytes:
    last_error: Exception | None = None
    headers = _request_headers(storyboard, fragment)
    for attempt in range(_SPRITE_DOWNLOAD_ATTEMPTS):
        try:
            response = requests.get(image_url, timeout=timeout, headers=headers)
            response.raise_for_status()
            if len(response.content) > _MAX_SPRITE_BYTES:
                raise YoutubeFrameError("影片預覽影格過大，暫時無法分析。")
            if not response.content:
                raise YoutubeFrameError("影片預覽影格內容為空。")
            return response.content
        except YoutubeFrameError:
            raise
        except (requests.RequestException, OSError) as exc:
            last_error = exc
            if attempt + 1 < _SPRITE_DOWNLOAD_ATTEMPTS:
                time.sleep(0.12 * (attempt + 1))
    raise YoutubeFrameError("目前無法下載這部影片的預覽影格。") from last_error


def _frame_from_storyboard(
    storyboard: Dict[str, Any],
    requested_seconds: float,
    duration: float,
    *,
    timeout: int,
) -> Dict[str, Any]:
    rows = _positive_int(storyboard.get("rows"))
    columns = _positive_int(storyboard.get("columns"))
    tile_width = _positive_int(storyboard.get("width"))
    tile_height = _positive_int(storyboard.get("height"))
    if not all((rows, columns, tile_width, tile_height)):
        raise YoutubeFrameError("影片預覽影格格式異常。")

    fragment, tile_index, expected_tiles, frame_seconds = _storyboard_location(
        storyboard,
        requested_seconds,
        duration,
    )
    image_url = str(fragment.get("url") or "").strip()
    if not image_url.startswith(("https://", "http://")):
        raise YoutubeFrameError("這部影片目前沒有可用的預覽影格。")

    sprite_bytes = _download_storyboard_sprite(
        image_url,
        storyboard,
        fragment,
        timeout=timeout,
    )
    try:
        with Image.open(io.BytesIO(sprite_bytes)) as sprite_source:
            sprite_source.load()
            sprite = sprite_source.convert("RGB")

        actual_columns = min(columns, sprite.width // tile_width)
        actual_rows = min(rows, sprite.height // tile_height)
        if actual_columns <= 0 or actual_rows <= 0:
            raise YoutubeFrameError("影片預覽影格格式異常。")

        # A partial final sheet may physically contain fewer rows. Cap to what
        # both the metadata timing and the downloaded image can actually hold.
        geometry_tiles = actual_rows * actual_columns
        available_tiles = min(expected_tiles, geometry_tiles)
        if available_tiles <= 0:
            raise YoutubeFrameError("影片預覽影格格式異常。")
        tile_index = min(tile_index, available_tiles - 1)
        column = tile_index % columns
        row = tile_index // columns
        left = column * tile_width
        top = row * tile_height
        if (
            column >= actual_columns
            or left + tile_width > sprite.width
            or top + tile_height > sprite.height
        ):
            raise YoutubeFrameError("影片預覽影格格式異常。")

        frame = sprite.crop((left, top, left + tile_width, top + tile_height))
        if frame.width < 640:
            frame = frame.resize(
                (640, max(1, round(frame.height * 640 / frame.width))),
                Image.Resampling.LANCZOS,
            )
        output = io.BytesIO()
        frame.save(output, format="JPEG", quality=90, optimize=True)
    except YoutubeFrameError:
        raise
    except Exception as exc:
        raise YoutubeFrameError("影片預覽影格格式異常。") from exc

    return {
        "bytes": output.getvalue(),
        "mime_type": "image/jpeg",
        "requested_seconds": requested_seconds,
        "frame_seconds": max(0.0, frame_seconds),
        "width": frame.width,
        "height": frame.height,
    }


def fetch_youtube_storyboard_frame(
    youtube_video_id: str,
    playback_seconds: float,
    *,
    timeout: int = 25,
) -> Dict[str, Any]:
    """Return a JPEG of the storyboard tile closest to ``playback_seconds``.

    The highest-quality storyboard is attempted first. If its signed URL,
    geometry, or sprite is temporarily unusable, lower-resolution storyboard
    levels are tried before metadata is refreshed once and the sequence retried.
    """

    video_id = str(youtube_video_id or "").strip()
    if not YOUTUBE_VIDEO_ID_RE.fullmatch(video_id):
        raise YoutubeFrameError("影片代碼無效。")
    try:
        requested_seconds = float(playback_seconds)
    except (TypeError, ValueError) as exc:
        raise YoutubeFrameError("播放時間無效。") from exc
    if not math.isfinite(requested_seconds) or requested_seconds < 0:
        raise YoutubeFrameError("播放時間無效。")

    last_error: YoutubeFrameError | None = None
    for metadata_attempt in range(2):
        try:
            info = _storyboard_info(video_id, force_refresh=metadata_attempt > 0)
        except YoutubeFrameError as exc:
            last_error = exc
            continue

        duration = _positive_float(info.get("duration"))
        sample_seconds = min(requested_seconds, duration) if duration > 0 else requested_seconds
        formats = _storyboard_formats(info)
        if not formats:
            last_error = YoutubeFrameError("這部影片目前沒有可用的預覽影格。")
            continue

        for storyboard in formats:
            try:
                return _frame_from_storyboard(
                    storyboard,
                    sample_seconds,
                    duration,
                    timeout=timeout,
                )
            except YoutubeFrameError as exc:
                last_error = exc

    if last_error is not None:
        raise last_error
    raise YoutubeFrameError("目前無法讀取這部影片的預覽影格。")
