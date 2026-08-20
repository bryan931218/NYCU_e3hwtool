"""Fetch a YouTube frame at the requested playback time with storyboard fallback."""

from __future__ import annotations

import io
import logging
import math
import re
import shutil
import subprocess
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
_EXACT_FRAME_TIMEOUT_SECONDS = 18
_EXACT_FRAME_MAX_STREAMS = 4
_MAX_FRAME_WIDTH = 1280
_MIN_FRAME_WIDTH = 640
_EXTRACTOR_STRATEGIES = (
    None,
    ("web", "android_vr"),
    ("android_vr",),
)
_metadata_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_metadata_video_locks: Dict[str, threading.Lock] = {}
_metadata_lock = threading.Lock()
_logger = logging.getLogger(__name__)


class YoutubeFrameError(RuntimeError):
    """Raised when a usable YouTube frame cannot be obtained."""


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


def _validate_frame_request(youtube_video_id: str, playback_seconds: float) -> Tuple[str, float]:
    video_id = str(youtube_video_id or "").strip()
    if not YOUTUBE_VIDEO_ID_RE.fullmatch(video_id):
        raise YoutubeFrameError("影片代碼無效。")
    try:
        requested_seconds = float(playback_seconds)
    except (TypeError, ValueError) as exc:
        raise YoutubeFrameError("播放時間無效。") from exc
    if not math.isfinite(requested_seconds) or requested_seconds < 0:
        raise YoutubeFrameError("播放時間無效。")
    return video_id, requested_seconds


def _stream_rank(item: Dict[str, Any]) -> Tuple[int, int, int, int, float]:
    """Prefer direct 720p-ish streams that are cheap to seek and still readable."""
    height = _positive_int(item.get("height"))
    protocol = str(item.get("protocol") or "").lower()
    ext = str(item.get("ext") or "").lower()
    acodec = str(item.get("acodec") or "none").lower()
    direct_http = int(protocol in {"https", "http", "http_dash_segments"})
    progressive = int(acodec not in {"", "none"})
    mp4 = int(ext == "mp4")
    if 1 <= height <= 720:
        height_score = 10_000 + height
    elif height > 720:
        height_score = max(1, 10_000 - (height - 720))
    else:
        height_score = 0
    try:
        tbr = float(item.get("tbr") or 0)
    except (TypeError, ValueError):
        tbr = 0.0
    return direct_http, height_score, progressive, mp4, -max(0.0, tbr)


def _video_metadata_lock(video_id: str) -> threading.Lock:
    with _metadata_lock:
        lock = _metadata_video_locks.get(video_id)
        if lock is None:
            lock = threading.Lock()
            _metadata_video_locks[video_id] = lock
        return lock


def _youtube_dl_options(player_clients: Tuple[str, ...] | None) -> Dict[str, Any]:
    options: Dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        # A storyboard is an image-only format. YouTube may return it even
        # when the selected client exposes no playable A/V stream.
        "ignore_no_formats_error": True,
        "noplaylist": True,
        "socket_timeout": 20,
        "retries": 3,
        "extractor_retries": 3,
    }
    available_runtimes = {
        runtime: {}
        for runtime in ("node", "deno")
        if shutil.which(runtime)
    }
    if available_runtimes:
        options["js_runtimes"] = available_runtimes
    if player_clients:
        options["extractor_args"] = {
            "youtube": {"player_client": list(player_clients)}
        }
    return options


def _has_frame_formats(info: Dict[str, Any]) -> bool:
    for item in info.get("formats") or []:
        if not isinstance(item, dict):
            continue
        if (
            str(item.get("protocol") or "") == "mhtml"
            and item.get("fragments")
            and _positive_int(item.get("width")) > 0
            and _positive_int(item.get("height")) > 0
        ):
            return True
        if (
            str(item.get("vcodec") or "none").lower() not in {"", "none"}
            and str(item.get("url") or "").startswith(("https://", "http://"))
        ):
            return True
    return False


def _extract_youtube_info(video_id: str) -> Dict[str, Any]:
    url = f"https://www.youtube.com/watch?v={video_id}"
    last_error: BaseException | None = None
    for strategy_index, player_clients in enumerate(_EXTRACTOR_STRATEGIES):
        try:
            with yt_dlp.YoutubeDL(_youtube_dl_options(player_clients)) as ydl:
                info = ydl.extract_info(url, download=False)
            if isinstance(info, dict) and _has_frame_formats(info):
                return info
            last_error = ValueError("frame formats missing")
        except Exception as exc:
            last_error = exc
            _logger.warning(
                "YouTube frame metadata attempt %s failed for %s: %s",
                strategy_index + 1,
                video_id,
                type(exc).__name__,
            )
        if strategy_index + 1 < len(_EXTRACTOR_STRATEGIES):
            time.sleep(0.25 * (strategy_index + 1))
    raise YoutubeFrameError(
        "目前無法讀取這部影片的影格資訊，系統已自動重試，請稍後再試。"
    ) from last_error


def _youtube_info(video_id: str, *, force_refresh: bool = False) -> Dict[str, Any]:
    """Return cached storyboard and direct-stream metadata from one yt-dlp lookup."""
    now = time.monotonic()
    if not force_refresh:
        with _metadata_lock:
            cached = _metadata_cache.get(video_id)
            if cached and now - cached[0] < _METADATA_CACHE_TTL_SECONDS:
                return cached[1]
    with _video_metadata_lock(video_id):
        now = time.monotonic()
        if not force_refresh:
            with _metadata_lock:
                cached = _metadata_cache.get(video_id)
                if cached and now - cached[0] < _METADATA_CACHE_TTL_SECONDS:
                    return cached[1]
        else:
            with _metadata_lock:
                _metadata_cache.pop(video_id, None)

        info = _extract_youtube_info(video_id)
        raw_formats = [item for item in (info.get("formats") or []) if isinstance(item, dict)]
        storyboards = [
            item
            for item in raw_formats
            if str(item.get("protocol") or "") == "mhtml"
            and item.get("fragments")
            and _positive_int(item.get("width")) > 0
            and _positive_int(item.get("height")) > 0
            and _positive_int(item.get("rows")) > 0
            and _positive_int(item.get("columns")) > 0
        ]
        storyboards.sort(
            key=lambda item: _positive_int(item.get("width")) * _positive_int(item.get("height")),
            reverse=True,
        )

        streams = [
            item
            for item in raw_formats
            if str(item.get("vcodec") or "none").lower() not in {"", "none"}
            and str(item.get("url") or "").startswith(("https://", "http://"))
            and str(item.get("protocol") or "").lower() != "mhtml"
            and _positive_int(item.get("height")) > 0
        ]
        streams.sort(key=_stream_rank, reverse=True)

        result = {
            "duration": _positive_float(info.get("duration")),
            "format": storyboards[0] if storyboards else None,
            "formats": storyboards,
            "streams": streams,
        }
        with _metadata_lock:
            if len(_metadata_cache) >= _METADATA_CACHE_LIMIT:
                oldest = min(_metadata_cache, key=lambda key: _metadata_cache[key][0])
                _metadata_cache.pop(oldest, None)
            _metadata_cache[video_id] = (time.monotonic(), result)
        return result


def _storyboard_info(video_id: str, *, force_refresh: bool = False) -> Dict[str, Any]:
    """Compatibility wrapper returning the storyboard subset of YouTube metadata."""
    info = _youtube_info(video_id, force_refresh=force_refresh)
    formats = [item for item in (info.get("formats") or []) if isinstance(item, dict)]
    if not formats:
        raise YoutubeFrameError("這部影片目前沒有可用的預覽影格。")
    return {
        "duration": _positive_float(info.get("duration")),
        "format": formats[0],
        "formats": formats,
    }


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
    fragments = [item for item in (storyboard.get("fragments") or []) if isinstance(item, dict)]
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


def _request_headers(*sources: Any) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for source in sources:
        if not isinstance(source, dict):
            continue
        for key, value in source.items():
            if value is not None and str(key).strip():
                headers[str(key)] = str(value)
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
    headers = _request_headers(storyboard.get("http_headers"), fragment.get("http_headers"))
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


def _normalize_frame_image(image_bytes: bytes) -> Tuple[bytes, int, int]:
    try:
        with Image.open(io.BytesIO(image_bytes)) as source:
            source.load()
            frame = source.convert("RGB")
    except Exception as exc:
        raise YoutubeFrameError("影片影格格式異常。") from exc

    if frame.width > _MAX_FRAME_WIDTH:
        frame = frame.resize(
            (_MAX_FRAME_WIDTH, max(1, round(frame.height * _MAX_FRAME_WIDTH / frame.width))),
            Image.Resampling.LANCZOS,
        )
    elif frame.width < _MIN_FRAME_WIDTH:
        frame = frame.resize(
            (_MIN_FRAME_WIDTH, max(1, round(frame.height * _MIN_FRAME_WIDTH / frame.width))),
            Image.Resampling.LANCZOS,
        )
    output = io.BytesIO()
    frame.save(output, format="JPEG", quality=90, optimize=True)
    return output.getvalue(), frame.width, frame.height


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
        geometry_tiles = actual_rows * actual_columns
        available_tiles = min(expected_tiles, geometry_tiles)
        if available_tiles <= 0:
            raise YoutubeFrameError("影片預覽影格格式異常。")
        tile_index = min(tile_index, available_tiles - 1)
        column = tile_index % columns
        row = tile_index // columns
        left = column * tile_width
        top = row * tile_height
        if column >= actual_columns or left + tile_width > sprite.width or top + tile_height > sprite.height:
            raise YoutubeFrameError("影片預覽影格格式異常。")
        cropped = sprite.crop((left, top, left + tile_width, top + tile_height))
        temporary = io.BytesIO()
        cropped.save(temporary, format="PNG")
        frame_bytes, width, height = _normalize_frame_image(temporary.getvalue())
    except YoutubeFrameError:
        raise
    except Exception as exc:
        raise YoutubeFrameError("影片預覽影格格式異常。") from exc

    return {
        "bytes": frame_bytes,
        "mime_type": "image/jpeg",
        "requested_seconds": requested_seconds,
        "frame_seconds": max(0.0, frame_seconds),
        "width": width,
        "height": height,
        "source": "storyboard",
    }


def _ffmpeg_executable() -> str:
    try:
        import imageio_ffmpeg

        executable = str(imageio_ffmpeg.get_ffmpeg_exe() or "").strip()
        if executable:
            return executable
    except Exception:
        pass
    return str(shutil.which("ffmpeg") or "")


def _ffmpeg_http_args(headers: Dict[str, str]) -> List[str]:
    args: List[str] = []
    extra_lines: List[str] = []
    for key, value in headers.items():
        lowered = key.lower()
        if lowered == "user-agent":
            args.extend(["-user_agent", value])
        elif lowered == "referer":
            args.extend(["-referer", value])
        elif lowered not in {"accept-encoding", "content-length", "host"}:
            extra_lines.append(f"{key}: {value}")
    if extra_lines:
        args.extend(["-headers", "\r\n".join(extra_lines) + "\r\n"])
    return args


def _frame_from_stream(
    stream: Dict[str, Any],
    requested_seconds: float,
    duration: float,
    *,
    timeout: int,
) -> Dict[str, Any]:
    ffmpeg = _ffmpeg_executable()
    if not ffmpeg:
        raise YoutubeFrameError("伺服器目前沒有可用的影片影格擷取器。")
    stream_url = str(stream.get("url") or "").strip()
    if not stream_url.startswith(("https://", "http://")):
        raise YoutubeFrameError("影片串流網址無效。")

    sample_seconds = requested_seconds
    if duration > 0:
        sample_seconds = min(sample_seconds, max(0.0, duration - 0.05))

    headers = _request_headers(stream.get("http_headers"))
    command = [
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-rw_timeout",
        str(max(1, int(timeout)) * 1_000_000),
        *_ffmpeg_http_args(headers),
        "-ss",
        f"{sample_seconds:.3f}",
        "-accurate_seek",
        "-i",
        stream_url,
        "-map",
        "0:v:0",
        "-frames:v",
        "1",
        "-an",
        "-sn",
        "-f",
        "image2pipe",
        "-vcodec",
        "mjpeg",
        "pipe:1",
    ]
    try:
        completed = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=max(5, int(timeout) + 4),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise YoutubeFrameError("目前無法精確擷取這一幕。") from exc
    if completed.returncode != 0 or not completed.stdout:
        raise YoutubeFrameError("目前無法精確擷取這一幕。")

    frame_bytes, width, height = _normalize_frame_image(completed.stdout)
    return {
        "bytes": frame_bytes,
        "mime_type": "image/jpeg",
        "requested_seconds": requested_seconds,
        "frame_seconds": max(0.0, sample_seconds),
        "width": width,
        "height": height,
        "source": "exact",
    }


def fetch_youtube_precise_frame(
    youtube_video_id: str,
    playback_seconds: float,
    *,
    timeout: int = _EXACT_FRAME_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    """Decode the video stream at the requested time instead of using a thumbnail."""
    video_id, requested_seconds = _validate_frame_request(youtube_video_id, playback_seconds)
    last_error: YoutubeFrameError | None = None
    for metadata_attempt in range(2):
        try:
            info = _youtube_info(video_id, force_refresh=metadata_attempt > 0)
        except YoutubeFrameError as exc:
            last_error = exc
            continue
        duration = _positive_float(info.get("duration"))
        streams = [item for item in (info.get("streams") or []) if isinstance(item, dict)]
        if not streams:
            last_error = YoutubeFrameError("這部影片目前沒有可用的精確影格串流。")
            continue
        for stream in streams[:_EXACT_FRAME_MAX_STREAMS]:
            try:
                return _frame_from_stream(
                    stream,
                    requested_seconds,
                    duration,
                    timeout=timeout,
                )
            except YoutubeFrameError as exc:
                last_error = exc
    if last_error is not None:
        raise last_error
    raise YoutubeFrameError("目前無法精確擷取這一幕。")


def _fetch_youtube_storyboard_frame(
    youtube_video_id: str,
    playback_seconds: float,
    *,
    timeout: int = 25,
) -> Dict[str, Any]:
    """Return the closest available YouTube storyboard tile."""
    video_id, requested_seconds = _validate_frame_request(youtube_video_id, playback_seconds)
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


def fetch_youtube_storyboard_frame(
    youtube_video_id: str,
    playback_seconds: float,
    *,
    timeout: int = 25,
) -> Dict[str, Any]:
    """Fetch the actual playback frame first, falling back to a storyboard tile.

    The public function name is retained for compatibility with the existing
    study-plan route. Precise stream decoding is preferred; the robust
    storyboard path remains a transparent fallback when YouTube blocks or does
    not expose a seekable stream.
    """
    precise_timeout = min(max(6, int(timeout)), _EXACT_FRAME_TIMEOUT_SECONDS)
    try:
        return fetch_youtube_precise_frame(
            youtube_video_id,
            playback_seconds,
            timeout=precise_timeout,
        )
    except YoutubeFrameError:
        return _fetch_youtube_storyboard_frame(
            youtube_video_id,
            playback_seconds,
            timeout=timeout,
        )
