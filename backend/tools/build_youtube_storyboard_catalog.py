"""Build a compact storyboard metadata catalog for configured course videos."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path
import sys
from typing import Any

import requests


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from e3_tracker.services.youtube_frames import (  # noqa: E402
    YoutubeMetadataError,
    _initial_player_response,
    _positive_float,
    _watch_page_headers,
)


SOURCE_PATH = BACKEND_ROOT / "e3_tracker" / "shared" / "study_plan_videos.json"
OUTPUT_PATH = BACKEND_ROOT / "e3_tracker" / "shared" / "youtube_storyboard_catalog.json"


def _fetch_entry(video_id: str) -> tuple[str, dict[str, Any]]:
    last_error: BaseException | None = None
    for host in ("www.youtube.com", "m.youtube.com"):
        try:
            response = requests.get(
                f"https://{host}/watch?v={video_id}",
                params={"hl": "en", "bpctr": "9999999999", "has_verified": "1"},
                headers=_watch_page_headers(),
                timeout=12,
            )
            response.raise_for_status()
            payload = _initial_player_response(response.text)
            renderer = (
                (payload.get("storyboards") or {}).get("playerStoryboardSpecRenderer")
                or (payload.get("storyboards") or {}).get("playerLiveStoryboardSpecRenderer")
                or {}
            )
            spec = str(renderer.get("spec") or "")
            if spec:
                return video_id, {
                    "duration": _positive_float(
                        (payload.get("videoDetails") or {}).get("lengthSeconds")
                    ),
                    "spec": spec,
                }
            last_error = YoutubeMetadataError("storyboard spec missing")
        except (requests.RequestException, YoutubeMetadataError) as exc:
            last_error = exc
    raise RuntimeError(f"{video_id}: {type(last_error).__name__}") from last_error


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--extra-video-ids",
        default="",
        help="Comma-separated deployed override IDs to include with the seed data.",
    )
    args = parser.parse_args()
    source = json.loads(SOURCE_PATH.read_text(encoding="utf-8"))
    video_ids = sorted(
        {
            str(item.get("youtube_video_id") or "").strip()
            for item in source
            if str(item.get("youtube_video_id") or "").strip()
        }
        | {
            video_id.strip()
            for video_id in args.extra_video_ids.split(",")
            if video_id.strip()
        }
    )
    catalog: dict[str, dict[str, Any]] = {}
    failures: list[str] = []
    with ThreadPoolExecutor(max_workers=max(1, min(16, args.workers))) as executor:
        futures = {executor.submit(_fetch_entry, video_id): video_id for video_id in video_ids}
        for future in as_completed(futures):
            video_id = futures[future]
            try:
                resolved_id, entry = future.result()
                catalog[resolved_id] = entry
            except Exception as exc:
                failures.append(f"{video_id}: {exc}")
    ordered = {video_id: catalog[video_id] for video_id in sorted(catalog)}
    OUTPUT_PATH.write_text(
        json.dumps(ordered, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"resolved={len(ordered)} total={len(video_ids)} failures={len(failures)}")
    for failure in failures:
        print(failure)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
