from __future__ import annotations

import os
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping

from flask import Response, flash, redirect, render_template_string, request, session, url_for
from sqlalchemy import inspect, text

from .storage import PersistentStorage


_YOUTUBE_FIELDS = (
    "youtube_video_id",
    "youtube_playlist_id",
    "youtube_url",
)

PLAYER_RATE_MIN = 1.05
PLAYER_RATE_MAX = 2.0
PLAYER_RATE_STEP = 0.05
PLAYER_SETTINGS_DEFAULTS: Dict[str, Any] = {
    "hold_space_rate": 2.0,
    "hold_delay_ms": 300,
    "seek_seconds": 10,
    "center_click_toggle": True,
    "show_shortcut_hint": True,
    "hint_duration_ms": 1400,
}


def _number(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed == parsed else default


def _integer(value: Any, default: int) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _boolean(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def normalize_player_settings(values: Mapping[str, Any] | None) -> Dict[str, Any]:
    source = dict(values or {})
    requested_rate = _number(
        source.get("hold_space_rate"),
        float(PLAYER_SETTINGS_DEFAULTS["hold_space_rate"]),
    )
    hold_space_rate = min(PLAYER_RATE_MAX, max(PLAYER_RATE_MIN, requested_rate))
    hold_space_rate = round(
        round((hold_space_rate - PLAYER_RATE_MIN) / PLAYER_RATE_STEP)
        * PLAYER_RATE_STEP
        + PLAYER_RATE_MIN,
        2,
    )
    hold_delay_ms = min(
        1200,
        max(150, _integer(source.get("hold_delay_ms"), 300)),
    )
    seek_seconds = min(
        120,
        max(1, _integer(source.get("seek_seconds"), 10)),
    )
    hint_duration_ms = min(
        5000,
        max(500, _integer(source.get("hint_duration_ms"), 1400)),
    )
    return {
        "hold_space_rate": float(hold_space_rate),
        "hold_delay_ms": hold_delay_ms,
        "seek_seconds": seek_seconds,
        "center_click_toggle": _boolean(
            source.get("center_click_toggle"),
            bool(PLAYER_SETTINGS_DEFAULTS["center_click_toggle"]),
        ),
        "show_shortcut_hint": _boolean(
            source.get("show_shortcut_hint"),
            bool(PLAYER_SETTINGS_DEFAULTS["show_shortcut_hint"]),
        ),
        "hint_duration_ms": hint_duration_ms,
    }


class DeploymentSafeStorage(PersistentStorage):
    """Keep deployment data and player preferences authoritative across restarts."""

    def __init__(self, database_url: str) -> None:
        super().__init__(database_url)
        self._ensure_player_settings_table()

    def _ensure_player_settings_table(self) -> None:
        with self._lock, self._engine.begin() as conn:
            conn.execute(
                text(
                    "CREATE TABLE IF NOT EXISTS study_player_settings ("
                    "id INTEGER PRIMARY KEY, "
                    "hold_space_rate FLOAT NOT NULL, "
                    "hold_delay_ms INTEGER NOT NULL, "
                    "seek_seconds INTEGER NOT NULL DEFAULT 10, "
                    "center_click_toggle INTEGER NOT NULL, "
                    "show_shortcut_hint INTEGER NOT NULL, "
                    "hint_duration_ms INTEGER NOT NULL, "
                    "updated_at VARCHAR(64) NOT NULL"
                    ")"
                )
            )
        columns = {
            column["name"]
            for column in inspect(self._engine).get_columns("study_player_settings")
        }
        if "seek_seconds" not in columns:
            with self._lock, self._engine.begin() as conn:
                conn.execute(
                    text(
                        "ALTER TABLE study_player_settings "
                        "ADD COLUMN seek_seconds INTEGER NOT NULL DEFAULT 10"
                    )
                )

    def load_study_player_settings(self) -> Dict[str, Any]:
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT hold_space_rate, hold_delay_ms, seek_seconds, "
                    "center_click_toggle, show_shortcut_hint, hint_duration_ms "
                    "FROM study_player_settings WHERE id = 1"
                )
            ).mappings().first()
        if not row:
            return dict(PLAYER_SETTINGS_DEFAULTS)
        return normalize_player_settings(dict(row))

    def save_study_player_settings(self, values: Mapping[str, Any]) -> Dict[str, Any]:
        settings = normalize_player_settings(values)
        params = {
            **settings,
            "center_click_toggle": 1 if settings["center_click_toggle"] else 0,
            "show_shortcut_hint": 1 if settings["show_shortcut_hint"] else 0,
            "updated_at": datetime.utcnow().isoformat(),
        }
        with self._lock, self._engine.begin() as conn:
            exists = conn.execute(
                text("SELECT id FROM study_player_settings WHERE id = 1")
            ).first()
            if exists:
                conn.execute(
                    text(
                        "UPDATE study_player_settings SET "
                        "hold_space_rate = :hold_space_rate, "
                        "hold_delay_ms = :hold_delay_ms, "
                        "seek_seconds = :seek_seconds, "
                        "center_click_toggle = :center_click_toggle, "
                        "show_shortcut_hint = :show_shortcut_hint, "
                        "hint_duration_ms = :hint_duration_ms, "
                        "updated_at = :updated_at WHERE id = 1"
                    ),
                    params,
                )
            else:
                conn.execute(
                    text(
                        "INSERT INTO study_player_settings ("
                        "id, hold_space_rate, hold_delay_ms, seek_seconds, "
                        "center_click_toggle, show_shortcut_hint, hint_duration_ms, updated_at"
                        ") VALUES ("
                        "1, :hold_space_rate, :hold_delay_ms, :seek_seconds, "
                        ":center_click_toggle, :show_shortcut_hint, "
                        ":hint_duration_ms, :updated_at"
                        ")"
                    ),
                    params,
                )
        return settings

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


def _admin_access(storage: DeploymentSafeStorage) -> tuple[bool, bool]:
    username = str(session.get("username") or "").strip()
    session_token = str(session.get("session_token") or "").strip()
    authenticated = bool(
        username
        and session_token
        and storage.is_valid_web_session(session_token, username)
    )
    return authenticated, bool(authenticated and session.get("is_admin"))


def _register_player_settings_routes(
    app: Any,
    storage: DeploymentSafeStorage,
    template: str,
) -> None:
    if "admin_study_player_settings" in app.view_functions:
        return

    def require_admin():
        authenticated, is_admin = _admin_access(storage)
        if not authenticated:
            return redirect(url_for("login"))
        if not is_admin:
            return Response("Forbidden", status=403, mimetype="text/plain")
        return None

    def admin_study_player_settings():
        denied = require_admin()
        if denied is not None:
            return denied
        if request.method == "POST":
            saved = storage.save_study_player_settings(
                {
                    "hold_space_rate": request.form.get("hold_space_rate"),
                    "hold_delay_ms": request.form.get("hold_delay_ms"),
                    "seek_seconds": request.form.get("seek_seconds"),
                    "center_click_toggle": request.form.get("center_click_toggle") == "1",
                    "show_shortcut_hint": request.form.get("show_shortcut_hint") == "1",
                    "hint_duration_ms": request.form.get("hint_duration_ms"),
                }
            )
            flash(
                "播放器設定已儲存："
                f"長按空白鍵 {saved['hold_space_rate']:g}×，"
                f"快進／後退 {saved['seek_seconds']} 秒。",
                "success",
            )
            return redirect(url_for("admin_study_player_settings"))
        return render_template_string(
            template,
            settings=storage.load_study_player_settings(),
            username=session.get("username") or "管理員",
        )

    def admin_study_player_settings_json():
        denied = require_admin()
        if denied is not None:
            return denied
        return {
            "ok": True,
            "settings": storage.load_study_player_settings(),
        }

    app.add_url_rule(
        "/admin/study-player-settings",
        endpoint="admin_study_player_settings",
        view_func=admin_study_player_settings,
        methods=["GET", "POST"],
    )
    app.add_url_rule(
        "/admin/study-player-settings.json",
        endpoint="admin_study_player_settings_json",
        view_func=admin_study_player_settings_json,
        methods=["GET"],
    )


def install_deployment_runtime(web_module: Any) -> None:
    """Install production-safe storage, player settings, and shortcut behavior."""

    if getattr(web_module, "__e3_deployment_runtime_installed", False):
        return
    web_module.__e3_deployment_runtime_installed = True
    web_module.PersistentStorage = DeploymentSafeStorage

    root_dir = Path(__file__).resolve().parents[3]
    patch_path = root_dir / "frontend" / "templates" / "_player_shortcut_compat.html"
    settings_template_path = (
        root_dir / "frontend" / "templates" / "admin_study_player_settings.html"
    )
    settings_template = (
        settings_template_path.read_text(encoding="utf-8")
        if settings_template_path.exists()
        else "<h1>播放器設定</h1>"
    )
    if patch_path.exists():
        patch = patch_path.read_text(encoding="utf-8")
        marker = "__e3PlayerShortcutCompatibilityInstalled"
        if marker not in web_module.STUDY_UPLOAD_TRACKER_TEMPLATE:
            web_module.STUDY_UPLOAD_TRACKER_TEMPLATE += "\n" + patch

    original_create_app = web_module.create_app

    def create_app_with_player_settings(*args: Any, **kwargs: Any):
        app = original_create_app(*args, **kwargs)
        storage = app.extensions.get("e3_storage")
        if isinstance(storage, DeploymentSafeStorage):
            _register_player_settings_routes(app, storage, settings_template)
        return app

    web_module.create_app = create_app_with_player_settings

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
