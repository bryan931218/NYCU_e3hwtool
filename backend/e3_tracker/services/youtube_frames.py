"""Fetch a YouTube frame at the requested playback time with storyboard fallback."""

from __future__ import annotations

import io
import json
import logging
import math
from pathlib import Path
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
_METADATA_STALE_TTL_SECONDS = 6 * 60 * 60
_METADATA_CACHE_LIMIT = 32
_FRAME_CACHE_TTL_SECONDS = 5 * 60
_FRAME_CACHE_LIMIT = 64
_SPRITE_DOWNLOAD_ATTEMPTS = 2
_MAX_SPRITE_BYTES = 3 * 1024 * 1024
_EXACT_FRAME_TIMEOUT_SECONDS = 18
_EXACT_FRAME_MAX_STREAMS = 4
_MAX_FRAME_WIDTH = 1280
_MIN_FRAME_WIDTH = 640
_STORYBOARD_CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "shared" / "youtube_storyboard_catalog.json"
)
_EXTRACTOR_STRATEGIES = (
    None,
    ("android_vr",),
    ("web_embedded", "web_safari"),
)
_metadata_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_metadata_video_locks: Dict[str, threading.Lock] = {}
_metadata_lock = threading.Lock()
_frame_cache: Dict[Tuple[str, float], Tuple[float, Dict[str, Any]]] = {}
_frame_video_locks: Dict[Tuple[str, float], threading.Lock] = {}
_frame_lock = threading.Lock()
_storyboard_catalog: Dict[str, Dict[str, Any]] | None = None
_storyboard_catalog_lock = threading.Lock()
_logger = logging.getLogger(__name__)


class YoutubeFrameError(RuntimeError):
    """Raised when a usable YouTube frame cannot be obtained."""


class YoutubeMetadataError(YoutubeFrameError):
    """Raised when YouTube blocks every metadata extraction strategy."""


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


def _validate_playback_seconds(playback_seconds: float) -> float:
    try:
        requested_seconds = float(playback_seconds)
    except (TypeError, ValueError) as exc:
        raise YoutubeFrameError("播放時間無效。") from exc
    if not math.isfinite(requested_seconds) or requested_seconds < 0:
        raise YoutubeFrameError("播放時間無效。")
    return requested_seconds


def _validate_frame_request(youtube_video_id: str, playback_seconds: float) -> Tuple[str, float]:
    video_id = str(youtube_video_id or "").strip()
    if not YOUTUBE_VIDEO_ID_RE.fullmatch(video_id):
        raise YoutubeFrameError("影片代碼無效。")
    requested_seconds = _validate_playback_seconds(playback_seconds)
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
        # Keep server-side retries bounded so a blocked data-centre IP does not
        # make the user wait through several minute-long retries.
        "socket_timeout": 8,
        "retries": 1,
        "extractor_retries": 1,
    }
    available_runtimes = {
        runtime: {}
        for runtime in ("node", "deno")
        if shutil.which(runtime)
    }
    if available_runtimes:
        options["js_runtimes"] = available_runtimes
        # Current YouTube playback responses can require an external JS
        # challenge solver. The Python API does not enable remote components
        # unless explicitly requested.
        options["remote_components"] = {"ejs:github"}
    if player_clients:
        options["extractor_args"] = {
            "youtube": {"player_client": list(player_clients)}
        }
    return options


def _watch_page_headers() -> Dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }


def _initial_player_response(html: str) -> Dict[str, Any]:
    marker = re.search(r"(?:var\s+)?ytInitialPlayerResponse\s*=\s*", html)
    if not marker:
        raise YoutubeMetadataError("YouTube 播放頁沒有可用的影格資訊。")
    try:
        payload, _ = json.JSONDecoder().raw_decode(html[marker.end() :].lstrip())
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise YoutubeMetadataError("YouTube 播放頁的影格資訊格式異常。") from exc
    if not isinstance(payload, dict):
        raise YoutubeMetadataError("YouTube 播放頁沒有可用的影格資訊。")
    return payload


def _storyboard_formats_from_spec(
    spec: str,
    *,
    duration: float,
) -> List[Dict[str, Any]]:
    parts = str(spec or "").split("|")
    if len(parts) < 2 or not parts[0].startswith(("https://", "http://")):
        return []
    template = parts[0]
    formats: List[Dict[str, Any]] = []
    for level, raw_level in enumerate(parts[1:]):
        fields = raw_level.split("#")
        if len(fields) < 8:
            continue
        width = _positive_int(fields[0])
        height = _positive_int(fields[1])
        frame_count = _positive_int(fields[2])
        columns = _positive_int(fields[3])
        rows = _positive_int(fields[4])
        interval_ms = _positive_float(fields[5])
        filename_template = str(fields[6] or "default")
        signature = str(fields[7] or "").strip()
        tile_count = columns * rows
        if not all((width, height, frame_count, tile_count)):
            continue
        interval = (
            duration / frame_count
            if duration > 0
            else max(0.001, interval_ms / 1000 if interval_ms > 0 else 1.0)
        )
        fragment_count = max(1, int(math.ceil(frame_count / tile_count)))
        fragment_duration = max(0.001, interval * tile_count)
        fragments: List[Dict[str, Any]] = []
        for fragment_index in range(fragment_count):
            filename = filename_template.replace("$M", str(fragment_index))
            image_url = (
                template.replace("$L", str(level)).replace("$N", filename)
            )
            if signature:
                separator = "&" if "?" in image_url else "?"
                image_url = f"{image_url}{separator}sigh={signature}"
            fragments.append(
                {
                    "url": image_url,
                    "duration": fragment_duration,
                    "http_headers": _watch_page_headers(),
                }
            )
        formats.append(
            {
                "format_id": f"watch-sb{level}",
                "protocol": "mhtml",
                "ext": "mhtml",
                "width": width,
                "height": height,
                "columns": columns,
                "rows": rows,
                "fps": 1.0 / interval,
                "fragments": fragments,
                "http_headers": _watch_page_headers(),
            }
        )
    return formats


def _load_storyboard_catalog() -> Dict[str, Dict[str, Any]]:
    """Load pre-resolved metadata for the course videos shipped with the app."""
    global _storyboard_catalog
    if _storyboard_catalog is not None:
        return _storyboard_catalog
    with _storyboard_catalog_lock:
        if _storyboard_catalog is not None:
            return _storyboard_catalog
        catalog: Dict[str, Dict[str, Any]] = {}
        try:
            raw = json.loads(_STORYBOARD_CATALOG_PATH.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                catalog = {
                    str(video_id): item
                    for video_id, item in raw.items()
                    if YOUTUBE_VIDEO_ID_RE.fullmatch(str(video_id))
                    and isinstance(item, dict)
                }
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            _logger.warning("Bundled YouTube storyboard catalog is unavailable")
        _storyboard_catalog = catalog
        return catalog


def _bundled_storyboard_info(video_id: str) -> Dict[str, Any] | None:
    item = _load_storyboard_catalog().get(video_id)
    if not item:
        return None
    duration = _positive_float(item.get("duration"))
    spec = str(item.get("spec") or "")
    formats = _storyboard_formats_from_spec(
        spec,
        duration=duration,
    )
    if not formats:
        return None
    return {
        "duration": duration,
        "formats": formats,
        "storyboard_spec": spec,
        "_metadata_source": "bundled_catalog",
    }


def _extract_watch_page_storyboards(video_id: str) -> Dict[str, Any]:
    """Extract public storyboard metadata without relying on yt-dlp clients."""
    last_error: BaseException | None = None
    for host in ("www.youtube.com", "m.youtube.com"):
        try:
            response = requests.get(
                f"https://{host}/watch?v={video_id}",
                params={"hl": "en", "bpctr": "9999999999", "has_verified": "1"},
                headers=_watch_page_headers(),
                timeout=8,
            )
            response.raise_for_status()
            if len(response.content) > 2_500_000:
                raise YoutubeMetadataError("YouTube 播放頁內容過大。")
            payload = _initial_player_response(response.text)
            duration = _positive_float(
                (payload.get("videoDetails") or {}).get("lengthSeconds")
            )
            storyboards = payload.get("storyboards") or {}
            renderer = (
                storyboards.get("playerStoryboardSpecRenderer")
                or storyboards.get("playerLiveStoryboardSpecRenderer")
                or {}
            )
            spec = str(renderer.get("spec") or "")
            formats = _storyboard_formats_from_spec(
                spec,
                duration=duration,
            )
            if formats:
                return {
                    "duration": duration,
                    "formats": formats,
                    "storyboard_spec": spec,
                    "_metadata_source": "watch_page",
                }
            last_error = YoutubeMetadataError("YouTube 播放頁沒有可用的預覽影格。")
        except (requests.RequestException, YoutubeMetadataError) as exc:
            last_error = exc
    raise YoutubeMetadataError("YouTube 播放頁沒有可用的預覽影格。") from last_error


def _storyboard_metadata_from_info(video_id: str, info: Dict[str, Any]) -> Dict[str, Any] | None:
    spec = str(info.get("storyboard_spec") or "").strip()
    if not spec:
        return None
    return {
        "youtube_video_id": video_id,
        "duration_seconds": _positive_float(info.get("duration")),
        "storyboard_spec": spec,
    }


def _storyboard_info_from_metadata(
    video_id: str,
    storyboard_metadata: Dict[str, Any] | None,
) -> Dict[str, Any] | None:
    if not isinstance(storyboard_metadata, dict):
        return None
    metadata_video_id = str(storyboard_metadata.get("youtube_video_id") or video_id).strip()
    if metadata_video_id != video_id:
        return None
    duration = _positive_float(storyboard_metadata.get("duration_seconds"))
    spec = str(storyboard_metadata.get("storyboard_spec") or "").strip()
    formats = _storyboard_formats_from_spec(spec, duration=duration)
    if not formats:
        return None
    return {
        "duration": duration,
        "formats": formats,
        "storyboard_spec": spec,
        "_metadata_source": "database",
    }


def fetch_youtube_storyboard_metadata(youtube_video_id: str) -> Dict[str, Any]:
    """Resolve reusable storyboard metadata for background persistence."""
    video_id, _ = _validate_frame_request(youtube_video_id, 0)
    info = _bundled_storyboard_info(video_id)
    if info is None:
        info = _extract_watch_page_storyboards(video_id)
    metadata_payload = _storyboard_metadata_from_info(video_id, info)
    if metadata_payload is None:
        raise YoutubeMetadataError("YouTube 沒有提供可保存的預覽影格索引。")
    return metadata_payload


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
        # Course video IDs are known ahead of time. If the normal extractor is
        # blocked by a data-centre IP, use the shipped metadata immediately
        # instead of repeating the same YouTube challenge for several seconds.
        if strategy_index == 0:
            bundled_info = _bundled_storyboard_info(video_id)
            if bundled_info and _has_frame_formats(bundled_info):
                _logger.info("Using bundled YouTube storyboard metadata for %s", video_id)
                return bundled_info
        if strategy_index + 1 < len(_EXTRACTOR_STRATEGIES):
            time.sleep(0.25 * (strategy_index + 1))
    try:
        watch_info = _extract_watch_page_storyboards(video_id)
        if _has_frame_formats(watch_info):
            _logger.info("Using direct YouTube watch-page storyboard metadata for %s", video_id)
            return watch_info
    except Exception as exc:
        last_error = exc
        _logger.warning(
            "YouTube watch-page storyboard fallback failed for %s: %s",
            video_id,
            type(exc).__name__,
        )
    raise YoutubeMetadataError("YouTube 暫時無法提供這部影片的影格資訊，請稍後再試。") from last_error


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
        stale: Tuple[float, Dict[str, Any]] | None = None
        with _metadata_lock:
            stale = _metadata_cache.get(video_id)
        if not force_refresh:
            with _metadata_lock:
                cached = _metadata_cache.get(video_id)
                if cached and now - cached[0] < _METADATA_CACHE_TTL_SECONDS:
                    return cached[1]
        else:
            with _metadata_lock:
                _metadata_cache.pop(video_id, None)

        try:
            info = _extract_youtube_info(video_id)
        except YoutubeMetadataError:
            if stale and now - stale[0] < _METADATA_STALE_TTL_SECONDS:
                _logger.info("Using stale YouTube frame metadata for %s", video_id)
                return stale[1]
            raise
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
            "metadata_source": str(info.get("_metadata_source") or "yt_dlp"),
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


def _frame_from_storyboard_info(
    info: Dict[str, Any],
    requested_seconds: float,
    *,
    timeout: int,
) -> Dict[str, Any]:
    """Try every resolution from one metadata response before changing source."""
    duration = _positive_float(info.get("duration"))
    sample_seconds = min(requested_seconds, duration) if duration > 0 else requested_seconds
    formats = _storyboard_formats(info)
    if not formats:
        raise YoutubeFrameError("這部影片目前沒有可用的預覽影格。")

    last_error: YoutubeFrameError | None = None
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
            timeout=max(3, int(timeout) + 2),
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
    deadline = time.monotonic() + max(6, int(timeout))
    last_error: YoutubeFrameError | None = None
    for metadata_attempt in range(2):
        if metadata_attempt > 0 and time.monotonic() >= deadline:
            break
        try:
            info = _youtube_info(video_id, force_refresh=metadata_attempt > 0)
        except YoutubeMetadataError:
            raise
        except YoutubeFrameError as exc:
            last_error = exc
            continue
        duration = _positive_float(info.get("duration"))
        streams = [item for item in (info.get("streams") or []) if isinstance(item, dict)]
        if not streams:
            last_error = YoutubeFrameError("這部影片目前沒有可用的精確影格串流。")
            if str(info.get("metadata_source") or "") in {"watch_page", "bundled_catalog"}:
                break
            continue
        for stream in streams[:_EXACT_FRAME_MAX_STREAMS]:
            remaining = deadline - time.monotonic()
            if remaining < 1:
                last_error = YoutubeFrameError("精準影格取樣逾時，已切換預覽影格。")
                break
            try:
                return _frame_from_stream(
                    stream,
                    requested_seconds,
                    duration,
                    timeout=max(1, min(6, int(math.ceil(remaining)))),
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
        except YoutubeMetadataError:
            raise
        except YoutubeFrameError as exc:
            last_error = exc
            continue

        try:
            return _frame_from_storyboard_info(
                info,
                requested_seconds,
                timeout=timeout,
            )
        except YoutubeFrameError as exc:
            last_error = exc

    if last_error is not None:
        raise last_error
    raise YoutubeFrameError("目前無法讀取這部影片的預覽影格。")


def _fetch_youtube_reliable_storyboard_frame(
    youtube_video_id: str,
    playback_seconds: float,
    *,
    timeout: int = 10,
    storyboard_metadata: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Use cheap storyboard sources before invoking the slower yt-dlp extractor."""
    video_id, requested_seconds = _validate_frame_request(youtube_video_id, playback_seconds)
    last_error: YoutubeFrameError | None = None

    persisted_info = _storyboard_info_from_metadata(video_id, storyboard_metadata)
    bundled_info = _bundled_storyboard_info(video_id)
    candidates = [item for item in (persisted_info, bundled_info) if item]
    seen_specs: set[str] = set()
    for candidate in candidates:
        candidate_spec = str(candidate.get("storyboard_spec") or "")
        if candidate_spec and candidate_spec in seen_specs:
            continue
        seen_specs.add(candidate_spec)
        try:
            frame = _frame_from_storyboard_info(
                candidate,
                requested_seconds,
                timeout=min(6, max(3, int(timeout))),
            )
            metadata_payload = _storyboard_metadata_from_info(video_id, candidate)
            if metadata_payload:
                frame = {**frame, "storyboard_metadata": metadata_payload}
            return frame
        except YoutubeFrameError as exc:
            last_error = exc

    try:
        live_info = _extract_watch_page_storyboards(video_id)
        frame = _frame_from_storyboard_info(
            live_info,
            requested_seconds,
            timeout=min(6, max(3, int(timeout))),
        )
        metadata_payload = _storyboard_metadata_from_info(video_id, live_info)
        if metadata_payload:
            frame = {**frame, "storyboard_metadata": metadata_payload}
        return frame
    except YoutubeFrameError as exc:
        last_error = exc

    try:
        return _fetch_youtube_storyboard_frame(
            video_id,
            requested_seconds,
            timeout=min(8, max(4, int(timeout))),
        )
    except YoutubeFrameError as exc:
        if last_error is not None:
            raise YoutubeFrameError("YouTube 暫時無法提供這一幕，請稍後再試。") from exc
        raise


def fetch_youtube_storyboard_frame(
    youtube_video_id: str,
    playback_seconds: float,
    *,
    timeout: int = 25,
    storyboard_metadata: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Fetch a reliable storyboard tile, falling back to exact stream decoding.

    The public function name is retained for compatibility with the existing
    study-plan route. Storyboards are available for more videos and avoid long
    extractor delays on data-centre hosts; exact decoding remains the fallback.
    """
    try:
        return _fetch_youtube_reliable_storyboard_frame(
            youtube_video_id,
            playback_seconds,
            timeout=min(10, max(4, int(timeout))),
            storyboard_metadata=storyboard_metadata,
        )
    except YoutubeFrameError:
        return fetch_youtube_precise_frame(
            youtube_video_id,
            playback_seconds,
            timeout=min(max(6, int(timeout)), _EXACT_FRAME_TIMEOUT_SECONDS),
        )


def fetch_youtube_cached_frame(
    youtube_video_id: str,
    playback_seconds: float,
    *,
    timeout: int = 18,
    storyboard_metadata: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Reuse a recently prefetched frame for the matching video timestamp."""
    video_id, requested_seconds = _validate_frame_request(youtube_video_id, playback_seconds)
    key = (video_id, round(requested_seconds, 3))
    now = time.monotonic()
    with _frame_lock:
        cached = _frame_cache.get(key)
        if cached and now - cached[0] < _FRAME_CACHE_TTL_SECONDS:
            return cached[1]
        lock = _frame_video_locks.setdefault(key, threading.Lock())

    with lock:
        now = time.monotonic()
        with _frame_lock:
            cached = _frame_cache.get(key)
            if cached and now - cached[0] < _FRAME_CACHE_TTL_SECONDS:
                return cached[1]
        frame = fetch_youtube_storyboard_frame(
            video_id,
            requested_seconds,
            timeout=timeout,
            storyboard_metadata=storyboard_metadata,
        )
        with _frame_lock:
            if len(_frame_cache) >= _FRAME_CACHE_LIMIT:
                oldest = min(_frame_cache, key=lambda item: _frame_cache[item][0])
                _frame_cache.pop(oldest, None)
                _frame_video_locks.pop(oldest, None)
            _frame_cache[key] = (time.monotonic(), frame)
        return frame
