from __future__ import annotations

import json
import logging
import math
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
import yt_dlp

from e3_tracker.services import youtube_frames as base_frames


LOGGER = logging.getLogger(__name__)
_PLAYER_CLIENTS = ("web_embedded", "android_vr")
_CLIENT_CACHE_TTL_SECONDS = 5 * 60
_EMBED_CACHE_TTL_SECONDS = 10 * 60
_EXACT_STREAM_LIMIT = 4
_client_cache: Dict[Tuple[str, str], Tuple[float, Dict[str, Any]]] = {}
_embed_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_cache_lock = threading.Lock()


def _client_cache_key(video_id: str, player_client: str) -> Tuple[str, str]:
    return video_id, player_client


def _extract_with_player_client(
    video_id: str,
    player_client: str,
    *,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    key = _client_cache_key(video_id, player_client)
    now = time.monotonic()
    if not force_refresh:
        with _cache_lock:
            cached = _client_cache.get(key)
            if cached and now - cached[0] < _CLIENT_CACHE_TTL_SECONDS:
                return cached[1]
    else:
        with _cache_lock:
            _client_cache.pop(key, None)

    options = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "socket_timeout": 10,
        "retries": 1,
        "extractor_retries": 1,
        "extractor_args": {"youtube": {"player_client": [player_client]}},
    }
    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            info = ydl.extract_info(
                f"https://www.youtube.com/watch?v={video_id}",
                download=False,
            )
    except Exception as exc:
        raise base_frames.YoutubeFrameError(
            f"YouTube {player_client} metadata unavailable"
        ) from exc
    if not isinstance(info, dict):
        raise base_frames.YoutubeFrameError(
            f"YouTube {player_client} metadata unavailable"
        )

    with _cache_lock:
        _client_cache[key] = (now, info)
    return info


def _stream_candidates(info: Dict[str, Any]) -> List[Dict[str, Any]]:
    streams = []
    for item in info.get("formats") or []:
        if not isinstance(item, dict):
            continue
        try:
            height = int(item.get("height") or 0)
        except (TypeError, ValueError):
            height = 0
        if (
            str(item.get("vcodec") or "none").lower() not in {"", "none"}
            and str(item.get("url") or "").startswith(("https://", "http://"))
            and str(item.get("protocol") or "").lower() != "mhtml"
            and height > 0
        ):
            streams.append(item)
    streams.sort(key=base_frames._stream_rank, reverse=True)
    return streams


def _fetch_exact_with_client(
    video_id: str,
    requested_seconds: float,
    player_client: str,
    *,
    timeout: int,
) -> Dict[str, Any]:
    last_error: Optional[Exception] = None
    for refresh in (False, True):
        try:
            info = _extract_with_player_client(
                video_id,
                player_client,
                force_refresh=refresh,
            )
        except base_frames.YoutubeFrameError as exc:
            last_error = exc
            continue

        duration = base_frames._positive_float(info.get("duration"))
        streams = _stream_candidates(info)
        if not streams:
            last_error = base_frames.YoutubeFrameError(
                f"YouTube {player_client} returned no seekable video stream"
            )
            continue

        for stream in streams[:_EXACT_STREAM_LIMIT]:
            try:
                result = base_frames._frame_from_stream(
                    stream,
                    requested_seconds,
                    duration,
                    timeout=timeout,
                )
                result["source"] = f"exact:{player_client}"
                return result
            except base_frames.YoutubeFrameError as exc:
                last_error = exc

    raise base_frames.YoutubeFrameError(
        f"Exact frame unavailable via {player_client}"
    ) from last_error


def _read_json_string(source: str, key: str, *, start: int = 0) -> str:
    marker = f'"{key}"'
    key_index = source.find(marker, start)
    if key_index < 0:
        return ""
    colon_index = source.find(":", key_index + len(marker))
    if colon_index < 0:
        return ""
    quote_index = source.find('"', colon_index + 1)
    if quote_index < 0:
        return ""

    escaped = False
    end_index = quote_index + 1
    while end_index < len(source):
        char = source[end_index]
        if escaped:
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == '"':
            try:
                return str(json.loads(source[quote_index : end_index + 1]) or "")
            except (json.JSONDecodeError, TypeError, ValueError):
                return ""
        end_index += 1
    return ""


def _embed_duration_seconds(page: str) -> float:
    match = re.search(r'"lengthSeconds"\s*:\s*"?(\d+(?:\.\d+)?)"?', page)
    if match:
        return max(0.0, float(match.group(1)))
    match = re.search(r'"approxDurationMs"\s*:\s*"?(\d+(?:\.\d+)?)"?', page)
    if match:
        return max(0.0, float(match.group(1)) / 1000.0)
    return 0.0


def _storyboards_from_spec(spec: str, duration: float) -> List[Dict[str, Any]]:
    parts = str(spec or "").split("|")
    if len(parts) < 2:
        return []

    base_url = urljoin("https://i.ytimg.com/", parts[0].strip())
    variants = parts[1:]
    if not base_url.startswith(("https://", "http://")):
        return []

    if duration <= 0:
        duration_candidates: List[float] = []
        for raw_variant in variants:
            args = raw_variant.split("#")
            if len(args) != 8:
                continue
            try:
                frame_count = int(args[2])
                interval_ms = int(args[5])
            except (TypeError, ValueError):
                continue
            if frame_count > 0 and interval_ms > 0:
                duration_candidates.append(frame_count * interval_ms / 1000.0)
        if duration_candidates:
            duration = max(duration_candidates)

    storyboards: List[Dict[str, Any]] = []
    for level in range(len(variants) - 1, -1, -1):
        args = variants[level].split("#")
        if len(args) != 8:
            continue
        try:
            width, height, frame_count, columns, rows = [int(value) for value in args[:5]]
            interval_ms = int(args[5] or 0)
        except (TypeError, ValueError):
            continue
        if min(width, height, frame_count, columns, rows) <= 0:
            continue

        name_template = args[6]
        signature = args[7]
        level_url = base_url.replace("$L", str(level)).replace("$N", name_template)
        separator = "&" if "?" in level_url else "?"
        level_url = f"{level_url}{separator}sigh={signature}"

        cells_per_sheet = columns * rows
        fragment_count = max(1, int(math.ceil(frame_count / cells_per_sheet)))
        if duration > 0:
            frame_rate = frame_count / duration
            sheet_duration = duration / max(frame_count / cells_per_sheet, 1e-9)
        elif interval_ms > 0:
            frame_rate = 1000.0 / interval_ms
            sheet_duration = interval_ms * cells_per_sheet / 1000.0
        else:
            continue

        fragments = []
        for fragment_index in range(fragment_count):
            fragment_start = fragment_index * sheet_duration
            fragment_duration = sheet_duration
            if duration > 0:
                fragment_duration = min(
                    sheet_duration,
                    max(0.001, duration - fragment_start),
                )
            fragments.append(
                {
                    "url": level_url.replace("$M", str(fragment_index)),
                    "duration": max(0.001, fragment_duration),
                }
            )

        storyboards.append(
            {
                "protocol": "mhtml",
                "width": width,
                "height": height,
                "rows": rows,
                "columns": columns,
                "fps": frame_rate,
                "fragments": fragments,
                "http_headers": {
                    "Referer": "https://www.youtube.com/",
                    "Accept-Language": "en-US,en;q=0.8",
                },
            }
        )

    storyboards.sort(
        key=lambda item: int(item.get("width") or 0) * int(item.get("height") or 0),
        reverse=True,
    )
    return storyboards


def _embed_storyboard_info(
    video_id: str,
    *,
    force_refresh: bool = False,
    timeout: int = 10,
) -> Dict[str, Any]:
    now = time.monotonic()
    if not force_refresh:
        with _cache_lock:
            cached = _embed_cache.get(video_id)
            if cached and now - cached[0] < _EMBED_CACHE_TTL_SECONDS:
                return cached[1]
    else:
        with _cache_lock:
            _embed_cache.pop(video_id, None)

    urls = (
        f"https://www.youtube.com/embed/{video_id}?hl=en&enablejsapi=1",
        f"https://www.youtube-nocookie.com/embed/{video_id}?hl=en&enablejsapi=1",
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.8",
    }
    last_error: Optional[Exception] = None

    for url in urls:
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            page = response.text
        except requests.RequestException as exc:
            last_error = exc
            continue

        renderer_index = page.find('"playerStoryboardSpecRenderer"')
        if renderer_index < 0:
            last_error = base_frames.YoutubeFrameError(
                "Embed page did not expose storyboard metadata"
            )
            continue
        spec = _read_json_string(page, "spec", start=renderer_index)
        duration = _embed_duration_seconds(page)
        storyboards = _storyboards_from_spec(spec, duration)
        if not storyboards:
            last_error = base_frames.YoutubeFrameError(
                "Embed storyboard metadata was incomplete"
            )
            continue

        result = {"duration": duration, "format": storyboards[0], "formats": storyboards}
        with _cache_lock:
            _embed_cache[video_id] = (now, result)
        return result

    raise base_frames.YoutubeFrameError(
        "YouTube embed storyboard metadata unavailable"
    ) from last_error


def _fetch_embed_storyboard_frame(
    video_id: str,
    requested_seconds: float,
    *,
    timeout: int,
) -> Dict[str, Any]:
    last_error: Optional[Exception] = None
    for refresh in (False, True):
        try:
            info = _embed_storyboard_info(
                video_id,
                force_refresh=refresh,
                timeout=min(max(4, timeout), 12),
            )
        except base_frames.YoutubeFrameError as exc:
            last_error = exc
            continue

        duration = base_frames._positive_float(info.get("duration"))
        sample_seconds = min(requested_seconds, duration) if duration > 0 else requested_seconds
        for storyboard in info.get("formats") or []:
            if not isinstance(storyboard, dict):
                continue
            try:
                result = base_frames._frame_from_storyboard(
                    storyboard,
                    sample_seconds,
                    duration,
                    timeout=timeout,
                )
                result["source"] = "storyboard:embed"
                return result
            except base_frames.YoutubeFrameError as exc:
                last_error = exc

    raise base_frames.YoutubeFrameError(
        "YouTube embed storyboard frame unavailable"
    ) from last_error


def fetch_reliable_youtube_frame(
    youtube_video_id: str,
    playback_seconds: float,
    *,
    timeout: int = 25,
) -> Dict[str, Any]:
    video_id, requested_seconds = base_frames._validate_frame_request(
        youtube_video_id,
        playback_seconds,
    )

    errors: List[str] = []
    exact_timeout = min(max(6, int(timeout)), 14)
    for player_client in _PLAYER_CLIENTS:
        try:
            return _fetch_exact_with_client(
                video_id,
                requested_seconds,
                player_client,
                timeout=exact_timeout,
            )
        except base_frames.YoutubeFrameError as exc:
            errors.append(f"{player_client}: {exc}")
            LOGGER.warning(
                "Video frame exact client %s failed for %s: %s",
                player_client,
                video_id,
                exc.__cause__ or exc,
            )

    try:
        return _fetch_embed_storyboard_frame(video_id, requested_seconds, timeout=timeout)
    except base_frames.YoutubeFrameError as exc:
        errors.append(f"embed_storyboard: {exc}")
        LOGGER.warning(
            "Video frame embed storyboard failed for %s: %s",
            video_id,
            exc.__cause__ or exc,
        )

    try:
        return base_frames.fetch_youtube_storyboard_frame(
            video_id,
            requested_seconds,
            timeout=timeout,
        )
    except base_frames.YoutubeFrameError as exc:
        errors.append(f"legacy: {exc}")
        LOGGER.error(
            "All video-frame paths failed for %s at %.3fs: %s",
            video_id,
            requested_seconds,
            " | ".join(errors),
        )
        raise base_frames.YoutubeFrameError(
            "目前無法取得這一幕的影片畫面，請稍後再試一次。"
        ) from exc


def install_video_frame_runtime(web_module: Any) -> None:
    if getattr(web_module, "_RELIABLE_VIDEO_FRAME_RUNTIME_INSTALLED", False):
        return
    web_module.fetch_youtube_storyboard_frame = fetch_reliable_youtube_frame
    web_module._RELIABLE_VIDEO_FRAME_RUNTIME_INSTALLED = True
