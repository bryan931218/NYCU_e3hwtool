from __future__ import annotations

import os
import warnings
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import text

from .storage import PersistentStorage


_YOUTUBE_FIELDS = (
    "youtube_video_id",
    "youtube_playlist_id",
    "youtube_url",
)


class DeploymentSafeStorage(PersistentStorage):
    """Keep database-synced YouTube links authoritative across app restarts."""

    def sync_study_plan_videos(self, videos: List[Dict[str, Any]]) -> None:
        existing: Dict[tuple[str, int], Dict[str, str]] = {}
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT subject, sequence, youtube_video_id, "
                    "youtube_playlist_id, youtube_url FROM study_plan_videos"
                )
            ).mappings()
            for row in rows:
                key = (str(row.get("subject") or ""), int(row.get("sequence") or 0))
                existing[key] = {
                    field: str(row.get(field) or "").strip()
                    for field in _YOUTUBE_FIELDS
                }

        merged: List[Dict[str, Any]] = []
        for source in videos:
            item = dict(source)
            try:
                key = (
                    str(item.get("subject") or "").strip(),
                    int(item.get("sequence") or 0),
                )
            except (TypeError, ValueError):
                merged.append(item)
                continue
            saved = existing.get(key)
            if saved:
                for field, value in saved.items():
                    if value:
                        item[field] = value
            merged.append(item)

        super().sync_study_plan_videos(merged)


def install_deployment_runtime(web_module: Any) -> None:
    """Install production-safe storage and the player compatibility patch."""

    web_module.PersistentStorage = DeploymentSafeStorage

    root_dir = Path(__file__).resolve().parents[3]
    patch_path = root_dir / "frontend" / "templates" / "_player_shortcut_compat.html"
    if patch_path.exists():
        patch = patch_path.read_text(encoding="utf-8")
        marker = "__e3PlayerShortcutCompatibilityInstalled"
        if marker not in web_module.STUDY_UPLOAD_TRACKER_TEMPLATE:
            web_module.STUDY_UPLOAD_TRACKER_TEMPLATE += "\n" + patch

    has_persistent_target = any(
        str(os.getenv(name) or "").strip()
        for name in (
            "E3_DATABASE_URL",
            "DATABASE_URL",
            "RAILWAY_VOLUME_MOUNT_PATH",
        )
    )
    if os.getenv("RAILWAY_ENVIRONMENT") and not has_persistent_target:
        warnings.warn(
            "Railway is running without DATABASE_URL or a mounted volume; "
            "SQLite data will be ephemeral across deployments.",
            RuntimeWarning,
            stacklevel=2,
        )
