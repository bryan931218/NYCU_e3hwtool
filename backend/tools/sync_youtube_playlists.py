"""Sync known YouTube playlists into the study-plan video inventory.

The script updates backend/e3_tracker/shared/study_plan_videos.json.
It uses yt-dlp for public playlist extraction. If yt-dlp is not installed,
it installs the package into the system temp directory so the application
runtime does not gain a new project dependency.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[2]
INVENTORY_PATH = ROOT / "backend" / "e3_tracker" / "shared" / "study_plan_videos.json"
PLAYLISTS = [
    {
        "subject": "線性代數",
        "playlist_id": "PLCzFJSBZ0Y8k",
        "url": "https://www.youtube.com/playlist?list=PLCzFJSBZ0Y8k",
    },
]


def ensure_ytdlp() -> None:
    if importlib.util.find_spec("yt_dlp") is not None:
        return
    target = Path(tempfile.gettempdir()) / "codex-yt-dlp"
    target.mkdir(parents=True, exist_ok=True)
    if str(target) not in sys.path:
        sys.path.insert(0, str(target))
    if importlib.util.find_spec("yt_dlp") is not None:
        return
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--quiet", "--target", str(target), "yt-dlp"]
    )


def fetch_playlist(url: str) -> List[Dict[str, Any]]:
    ensure_ytdlp()
    from yt_dlp import YoutubeDL  # type: ignore

    options = {
        "extract_flat": "in_playlist",
        "ignoreerrors": True,
        "quiet": True,
        "skip_download": True,
    }
    with YoutubeDL(options) as ydl:
        info = ydl.extract_info(url, download=False)
    entries = (info or {}).get("entries") or []
    return [entry for entry in entries if entry and entry.get("id")]


def load_inventory() -> List[Dict[str, Any]]:
    return json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))


def write_inventory(items: Iterable[Dict[str, Any]]) -> None:
    INVENTORY_PATH.write_text(
        json.dumps(list(items), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def sync_inventory(dry_run: bool = False) -> Dict[str, int]:
    inventory = load_inventory()
    by_key = {
        (str(item.get("subject") or ""), int(item.get("sequence") or 0)): item
        for item in inventory
    }
    updated = 0
    added = 0
    for playlist in PLAYLISTS:
        subject = playlist["subject"]
        playlist_id = playlist["playlist_id"]
        for index, entry in enumerate(fetch_playlist(playlist["url"]), start=1):
            video_id = str(entry.get("id") or "").strip()
            if not video_id:
                continue
            youtube_url = f"https://www.youtube.com/watch?v={video_id}&list={playlist_id}"
            key = (subject, index)
            item = by_key.get(key)
            if item is None:
                item = {
                    "subject": subject,
                    "sequence": index,
                    "title": str(entry.get("title") or f"YouTube {video_id}").strip(),
                    "duration_seconds": float(entry.get("duration") or 0),
                }
                inventory.append(item)
                by_key[key] = item
                added += 1
            before = (
                item.get("youtube_video_id"),
                item.get("youtube_playlist_id"),
                item.get("youtube_url"),
            )
            item["youtube_video_id"] = video_id
            item["youtube_playlist_id"] = playlist_id
            item["youtube_url"] = youtube_url
            if before != (item["youtube_video_id"], item["youtube_playlist_id"], item["youtube_url"]):
                updated += 1
    if not dry_run:
        write_inventory(inventory)
    return {"updated": updated, "added": added, "total": len(inventory)}


def sync_database() -> None:
    backend_root = ROOT / "backend"
    if str(backend_root) not in sys.path:
        sys.path.insert(0, str(backend_root))
    from e3_tracker.shared.config import load_env_defaults
    from e3_tracker.shared.storage import PersistentStorage

    env_defaults = load_env_defaults()
    configured_cache_dir = env_defaults.get("cache_dir")
    data_root = Path(configured_cache_dir).expanduser() if configured_cache_dir else Path(tempfile.gettempdir()) / "e3_tracker_cache"
    database_url = env_defaults.get("database_url") or ""
    db_location = database_url or str((data_root / "e3_tracker.sqlite3").resolve())
    storage = PersistentStorage(db_location)
    storage.sync_study_plan_videos(load_inventory())
    storage._engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Fetch and report changes without writing JSON.")
    parser.add_argument("--sync-db", action="store_true", help="Also sync the updated inventory into the E3 database.")
    args = parser.parse_args()
    result = sync_inventory(dry_run=args.dry_run)
    if args.sync_db and not args.dry_run:
        sync_database()
    mode = "DRY RUN" if args.dry_run else "UPDATED"
    print(f"{mode}: updated={result['updated']} added={result['added']} total={result['total']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
