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
    "default_playback_rate": 1.0,
    "hold_space_rate": 2.0,
    "hold_delay_ms": 300,
    "seek_back_seconds": 10,
    "seek_forward_seconds": 10,
    "seek_repeat_ms": 150,
    "playback_rate_step": 0.05,
    "volume_step": 5,
    "controls_hide_ms": 2600,
    "center_click_toggle": True,
    "pause_on_marker": False,
    "show_speed_presets": True,
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
    default_playback_rate = min(
        2.0,
        max(0.25, _number(source.get("default_playback_rate"), 1.0)),
    )
    default_playback_rate = round(round(default_playback_rate * 20) / 20, 2)
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
    legacy_seek_seconds = source.get("seek_seconds", 10)
    seek_back_seconds = min(
        120,
        max(1, _integer(source.get("seek_back_seconds", legacy_seek_seconds), 10)),
    )
    seek_forward_seconds = min(
        120,
        max(1, _integer(source.get("seek_forward_seconds", legacy_seek_seconds), 10)),
    )
    seek_repeat_ms = min(
        500,
        max(75, _integer(source.get("seek_repeat_ms"), 150)),
    )
    requested_rate_step = _number(source.get("playback_rate_step"), 0.05)
    playback_rate_step = min(
        (0.05, 0.1, 0.25),
        key=lambda option: abs(option - requested_rate_step),
    )
    volume_step = min(
        20,
        max(1, _integer(source.get("volume_step"), 5)),
    )
    controls_hide_ms = min(
        8000,
        max(1200, _integer(source.get("controls_hide_ms"), 2600)),
    )
    hint_duration_ms = min(
        5000,
        max(500, _integer(source.get("hint_duration_ms"), 1400)),
    )
    return {
        "default_playback_rate": float(default_playback_rate),
        "hold_space_rate": float(hold_space_rate),
        "hold_delay_ms": hold_delay_ms,
        "seek_back_seconds": seek_back_seconds,
        "seek_forward_seconds": seek_forward_seconds,
        "seek_repeat_ms": seek_repeat_ms,
        "playback_rate_step": float(playback_rate_step),
        "volume_step": volume_step,
        "controls_hide_ms": controls_hide_ms,
        "center_click_toggle": _boolean(
            source.get("center_click_toggle"),
            bool(PLAYER_SETTINGS_DEFAULTS["center_click_toggle"]),
        ),
        "pause_on_marker": _boolean(
            source.get("pause_on_marker"),
            bool(PLAYER_SETTINGS_DEFAULTS["pause_on_marker"]),
        ),
        "show_speed_presets": _boolean(
            source.get("show_speed_presets"),
            bool(PLAYER_SETTINGS_DEFAULTS["show_speed_presets"]),
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
                    "default_playback_rate FLOAT NOT NULL DEFAULT 1.0, "
                    "hold_space_rate FLOAT NOT NULL, "
                    "hold_delay_ms INTEGER NOT NULL, "
                    "seek_back_seconds INTEGER NOT NULL DEFAULT 10, "
                    "seek_forward_seconds INTEGER NOT NULL DEFAULT 10, "
                    "seek_repeat_ms INTEGER NOT NULL DEFAULT 150, "
                    "playback_rate_step FLOAT NOT NULL DEFAULT 0.05, "
                    "volume_step INTEGER NOT NULL DEFAULT 5, "
                    "controls_hide_ms INTEGER NOT NULL DEFAULT 2600, "
                    "center_click_toggle INTEGER NOT NULL, "
                    "pause_on_marker INTEGER NOT NULL DEFAULT 0, "
                    "show_speed_presets INTEGER NOT NULL DEFAULT 1, "
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
        migrations = {
            "default_playback_rate": "FLOAT NOT NULL DEFAULT 1.0",
            "seek_back_seconds": "INTEGER NOT NULL DEFAULT 10",
            "seek_forward_seconds": "INTEGER NOT NULL DEFAULT 10",
            "seek_repeat_ms": "INTEGER NOT NULL DEFAULT 150",
            "playback_rate_step": "FLOAT NOT NULL DEFAULT 0.05",
            "volume_step": "INTEGER NOT NULL DEFAULT 5",
            "controls_hide_ms": "INTEGER NOT NULL DEFAULT 2600",
            "pause_on_marker": "INTEGER NOT NULL DEFAULT 0",
            "show_speed_presets": "INTEGER NOT NULL DEFAULT 1",
        }
        for column_name, definition in migrations.items():
            if column_name in columns:
                continue
            with self._lock, self._engine.begin() as conn:
                conn.execute(
                    text(
                        "ALTER TABLE study_player_settings "
                        f"ADD COLUMN {column_name} {definition}"
                    )
                )

    def load_study_player_settings(self) -> Dict[str, Any]:
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT default_playback_rate, hold_space_rate, hold_delay_ms, "
                    "seek_back_seconds, seek_forward_seconds, seek_repeat_ms, "
                    "playback_rate_step, volume_step, controls_hide_ms, "
                    "center_click_toggle, pause_on_marker, show_speed_presets, "
                    "show_shortcut_hint, hint_duration_ms "
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
            "pause_on_marker": 1 if settings["pause_on_marker"] else 0,
            "show_speed_presets": 1 if settings["show_speed_presets"] else 0,
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
                        "default_playback_rate = :default_playback_rate, "
                        "hold_space_rate = :hold_space_rate, "
                        "hold_delay_ms = :hold_delay_ms, "
                        "seek_back_seconds = :seek_back_seconds, "
                        "seek_forward_seconds = :seek_forward_seconds, "
                        "seek_repeat_ms = :seek_repeat_ms, "
                        "playback_rate_step = :playback_rate_step, "
                        "volume_step = :volume_step, "
                        "controls_hide_ms = :controls_hide_ms, "
                        "center_click_toggle = :center_click_toggle, "
                        "pause_on_marker = :pause_on_marker, "
                        "show_speed_presets = :show_speed_presets, "
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
                        "id, default_playback_rate, hold_space_rate, hold_delay_ms, "
                        "seek_back_seconds, seek_forward_seconds, seek_repeat_ms, "
                        "playback_rate_step, volume_step, controls_hide_ms, "
                        "center_click_toggle, pause_on_marker, show_speed_presets, "
                        "show_shortcut_hint, hint_duration_ms, updated_at"
                        ") VALUES ("
                        "1, :default_playback_rate, :hold_space_rate, :hold_delay_ms, "
                        ":seek_back_seconds, :seek_forward_seconds, :seek_repeat_ms, "
                        ":playback_rate_step, :volume_step, :controls_hide_ms, "
                        ":center_click_toggle, :pause_on_marker, :show_speed_presets, "
                        ":show_shortcut_hint, "
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
                    "default_playback_rate": request.form.get("default_playback_rate"),
                    "hold_space_rate": request.form.get("hold_space_rate"),
                    "hold_delay_ms": request.form.get("hold_delay_ms"),
                    "seek_back_seconds": request.form.get("seek_back_seconds"),
                    "seek_forward_seconds": request.form.get("seek_forward_seconds"),
                    "seek_repeat_ms": request.form.get("seek_repeat_ms"),
                    "playback_rate_step": request.form.get("playback_rate_step"),
                    "volume_step": request.form.get("volume_step"),
                    "controls_hide_ms": request.form.get("controls_hide_ms"),
                    "center_click_toggle": request.form.get("center_click_toggle") == "1",
                    "pause_on_marker": request.form.get("pause_on_marker") == "1",
                    "show_speed_presets": request.form.get("show_speed_presets") == "1",
                    "show_shortcut_hint": request.form.get("show_shortcut_hint") == "1",
                    "hint_duration_ms": request.form.get("hint_duration_ms"),
                }
            )
            flash(
                "播放器設定已儲存："
                f"預設 {saved['default_playback_rate']:g}×，"
                f"後退 {saved['seek_back_seconds']} 秒、"
                f"快進 {saved['seek_forward_seconds']} 秒。",
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
