from __future__ import annotations

import re
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from .youtube_matching import match_playlist_entries


KNOWN_YOUTUBE_PLAYLISTS = (
    {
        "subject": "線性代數",
        "playlist_id": "PLCzFJSBZ0Y8k",
        "url": "https://www.youtube.com/playlist?list=PLCzFJSBZ0Y8k",
    },
    {
        "subject": "離散數學",
        "playlist_id": "PL_K5Hu3jjOvX7TBRl0BB2o2CIdRjdqtuU",
        "url": "https://www.youtube.com/playlist?list=PL_K5Hu3jjOvX7TBRl0BB2o2CIdRjdqtuU",
    },
    {
        "subject": "資料結構",
        "playlist_id": "PLVQPv-6fTfFU",
        "url": "https://www.youtube.com/playlist?list=PLVQPv-6fTfFU",
    },
    {
        "subject": "作業系統",
        "playlist_id": "PLSzCQwVDl-IQ",
        "url": "https://www.youtube.com/playlist?list=PLSzCQwVDl-IQ",
    },
    {
        "subject": "計算機組織",
        "playlist_id": "PLRO3eCCfBCO8",
        "url": "https://www.youtube.com/playlist?list=PLRO3eCCfBCO8",
    },
    {
        "subject": "演算法",
        "playlist_id": "PLCQ2ilCoQYRk",
        "url": "https://www.youtube.com/playlist?list=PLCQ2ilCoQYRk",
    },
)


class YoutubePlaylistSyncBusyError(RuntimeError):
    pass


_sync_lock = threading.Lock()
_auto_sync_start_lock = threading.Lock()
_auto_sync_started = False
_TAIPEI_TZ = timezone(timedelta(hours=8))
_AUTO_SYNC_HOUR = 13


def fetch_youtube_playlist(source: Dict[str, str]) -> List[Dict[str, Any]]:
    from yt_dlp import YoutubeDL  # type: ignore

    options = {
        "extract_flat": "in_playlist",
        "ignoreerrors": True,
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "socket_timeout": 25,
        "retries": 2,
    }
    with YoutubeDL(options) as ydl:
        info = ydl.extract_info(source["url"], download=False)

    playlist_id = str(source["playlist_id"] or "").strip()
    subject = str(source["subject"] or "").strip()
    links: List[Dict[str, Any]] = []
    for sequence, entry in enumerate((info or {}).get("entries") or [], start=1):
        if not isinstance(entry, dict):
            continue
        video_id = str(entry.get("id") or "").strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
            continue
        links.append(
            {
                "subject": subject,
                "playlist_position": entry.get("playlist_index") or sequence,
                "title": str(entry.get("title") or ""),
                "duration_seconds": entry.get("duration"),
                "youtube_video_id": video_id,
                "youtube_playlist_id": playlist_id,
                "youtube_url": f"https://www.youtube.com/watch?v={video_id}&list={playlist_id}",
            }
        )
    return links


def sync_known_youtube_playlists(
    storage: Any,
    sources: Optional[Iterable[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    if not _sync_lock.acquire(blocking=False):
        raise YoutubePlaylistSyncBusyError("已有 YouTube 同步正在進行")

    try:
        selected_sources = list(sources or KNOWN_YOUTUBE_PLAYLISTS)
        links: List[Dict[str, Any]] = []
        errors: List[Dict[str, str]] = []
        fetched_subjects: List[str] = []
        empty_subjects: List[str] = []
        worker_count = max(1, min(3, len(selected_sources)))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(fetch_youtube_playlist, source): source
                for source in selected_sources
            }
            for future in as_completed(futures):
                source = futures[future]
                subject = str(source.get("subject") or "").strip()
                try:
                    fetched = future.result()
                except Exception as exc:
                    errors.append({"subject": subject, "message": str(exc)[:180]})
                    continue
                links.extend(fetched)
                fetched_subjects.append(subject)
                if not fetched:
                    empty_subjects.append(subject)

        links, skipped = match_playlist_entries(links, storage.list_study_plan_videos_with_records())
        database_result = storage.sync_study_plan_youtube_links(links) if links else {
            "matched": 0,
            "updated": 0,
            "unchanged": 0,
            "unmatched": 0,
            "manual_overrides_preserved": 0,
        }
        return {
            **database_result,
            "needs_review": len(skipped),
            "skipped_matches": [
                {key: item.get(key) for key in ("subject", "title", "youtube_url", "reason")}
                for item in skipped
            ],
            "playlist_count": len(selected_sources),
            "fetched_subjects": sorted(fetched_subjects),
            "empty_subjects": sorted(empty_subjects),
            "errors": sorted(errors, key=lambda item: item["subject"]),
            "youtube_video_ids": sorted(
                {
                    str(item.get("youtube_video_id") or "").strip()
                    for item in links
                    if item.get("youtube_video_id")
                }
            ),
            "ok": bool(fetched_subjects),
        }
    finally:
        _sync_lock.release()


def _auto_sync_enabled() -> bool:
    configured = str(os.getenv("E3_YOUTUBE_AUTO_SYNC_ENABLED") or "").strip().lower()
    if configured:
        return configured not in {"0", "false", "no", "off"}
    return bool(str(os.getenv("RAILWAY_ENVIRONMENT") or "").strip())


def _seconds_until_next_auto_sync(now: Optional[datetime] = None) -> float:
    current = now.astimezone(_TAIPEI_TZ) if now else datetime.now(_TAIPEI_TZ)
    target = current.replace(
        hour=_AUTO_SYNC_HOUR,
        minute=0,
        second=0,
        microsecond=0,
    )
    if target <= current:
        target += timedelta(days=1)
    return max(1.0, (target - current).total_seconds())


def _run_auto_sync_once(storage: Any, logger: Any) -> Dict[str, Any]:
    try:
        result = sync_known_youtube_playlists(storage)
    except YoutubePlaylistSyncBusyError:
        logger.info("YouTube playlist auto sync skipped because another sync is running")
        return {"ok": False, "busy": True}
    except Exception:
        logger.exception("YouTube playlist auto sync failed")
        return {"ok": False, "error": "sync_failed"}

    logger.info(
        "YouTube playlist auto sync completed: fetched=%s matched=%s updated=%s review=%s errors=%s",
        len(result.get("fetched_subjects") or []),
        result.get("matched", 0),
        result.get("updated", 0),
        result.get("needs_review", 0),
        len(result.get("errors") or []),
    )
    return result


def start_youtube_playlist_auto_sync(storage: Any, logger: Any) -> bool:
    """Sync once after deployment startup, then every day at 13:00 Taipei time."""
    global _auto_sync_started
    if not _auto_sync_enabled():
        return False
    with _auto_sync_start_lock:
        if _auto_sync_started:
            return False
        _auto_sync_started = True

    def worker() -> None:
        _run_auto_sync_once(storage, logger)
        while True:
            threading.Event().wait(_seconds_until_next_auto_sync())
            _run_auto_sync_once(storage, logger)

    threading.Thread(
        target=worker,
        name="youtube-playlist-auto-sync",
        daemon=True,
    ).start()
    return True
