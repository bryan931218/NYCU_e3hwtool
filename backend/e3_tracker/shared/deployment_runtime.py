from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping

from flask import Response, flash, redirect, render_template_string, request, send_file, session, url_for
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
DISCORD_PRESENCE_IDLE_SECONDS = 5 * 60 + 30
DISCORD_APPLICATION_ID_PATTERN = re.compile(r"^[0-9]{17,20}$")
DISCORD_TOKEN_NONCE_PATTERN = re.compile(r"^[A-Za-z0-9_-]{12,32}$")


def issue_discord_presence_token(application_id: str, signing_secret: Any) -> str:
    normalized_application_id = str(application_id or "").strip()
    if not DISCORD_APPLICATION_ID_PATTERN.fullmatch(normalized_application_id):
        raise ValueError("Invalid Discord Application ID")
    nonce = secrets.token_urlsafe(12)
    message = f"e3-discord-presence:{normalized_application_id}:{nonce}".encode("utf-8")
    key = signing_secret if isinstance(signing_secret, bytes) else str(signing_secret or "").encode("utf-8")
    signature = hmac.new(key, message, hashlib.sha256).hexdigest()
    return f"dp1.{normalized_application_id}.{nonce}.{signature}"


def verify_discord_presence_signed_token(token: str, signing_secret: Any) -> str:
    parts = str(token or "").strip().split(".")
    if len(parts) != 4 or parts[0] != "dp1":
        return ""
    _version, application_id, nonce, signature = parts
    if not DISCORD_APPLICATION_ID_PATTERN.fullmatch(application_id):
        return ""
    if not DISCORD_TOKEN_NONCE_PATTERN.fullmatch(nonce) or not re.fullmatch(r"[0-9a-f]{64}", signature):
        return ""
    message = f"e3-discord-presence:{application_id}:{nonce}".encode("utf-8")
    key = signing_secret if isinstance(signing_secret, bytes) else str(signing_secret or "").encode("utf-8")
    expected = hmac.new(key, message, hashlib.sha256).hexdigest()
    return application_id if hmac.compare_digest(expected, signature) else ""


def format_discord_study_duration(seconds: Any) -> str:
    total_minutes = max(0, int(float(seconds or 0) // 60))
    hours, minutes = divmod(total_minutes, 60)
    if hours and minutes:
        return f"{hours} 小時 {minutes} 分"
    if hours:
        return f"{hours} 小時"
    return f"{minutes} 分"


def _discord_utc_datetime(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_discord_presence_payload(
    storage: Any,
    *,
    now: datetime | None = None,
    public_url: str = "",
    cover_image_url: str = "",
) -> Dict[str, Any]:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    else:
        current = current.astimezone(timezone.utc)
    day = storage._study_plan_business_day_from_timestamp(current.isoformat())
    summary = storage.get_study_time_summary(day=day)
    sessions = storage.list_study_time_sessions(day=day, limit=100)
    active_session = None
    for candidate in sessions:
        if bool(candidate.get("completed")):
            continue
        updated_at = _discord_utc_datetime(candidate.get("updated_at"))
        if updated_at is None:
            continue
        age_seconds = max(0.0, (current - updated_at).total_seconds())
        if age_seconds <= DISCORD_PRESENCE_IDLE_SECONDS:
            active_session = candidate
            break

    total_seconds = max(0.0, float(summary.get("total_seconds") or 0))
    payload: Dict[str, Any] = {
        "ok": True,
        "day": day,
        "active": bool(active_session) or total_seconds > 0,
        "studying_now": bool(active_session),
        "today_total_seconds": round(total_seconds, 1),
        "today_video_seconds": round(max(0.0, float(summary.get("video_seconds") or 0)), 1),
        "today_practice_seconds": round(max(0.0, float(summary.get("practice_seconds") or 0)), 1),
        "today_label": format_discord_study_duration(total_seconds),
        "public_url": str(public_url or "").strip(),
        "cover_image_url": str(cover_image_url or "").strip(),
        "checked_at": current.isoformat(),
    }
    if active_session:
        kind = str(active_session.get("kind") or "")
        subject = str(active_session.get("subject") or "").strip()
        label = str(active_session.get("label") or "").strip()
        started_at = _discord_utc_datetime(active_session.get("started_at"))
        details = f"正在讀{subject}" if kind == "video" and subject else (
            "正在看課程影片" if kind == "video" else "正在刷題"
        )
        payload.update(
            {
                "kind": kind,
                "subject": subject,
                "label": label,
                "details": details[:128],
                "state": f"今日實際學習 {format_discord_study_duration(total_seconds)}"[:128],
                "session_started_at": int(started_at.timestamp()) if started_at else None,
                "session_updated_at": str(active_session.get("updated_at") or ""),
            }
        )
    elif total_seconds > 0:
        payload.update(
            {
                "kind": "daily_summary",
                "subject": "",
                "label": "",
                "details": "今日學習紀錄",
                "state": f"實際學習 {format_discord_study_duration(total_seconds)}"[:128],
                "session_started_at": None,
                "session_updated_at": "",
            }
        )
    return payload


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
        self._ensure_discord_presence_settings_table()
        self._ensure_data_repairs_table()

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

    def _ensure_discord_presence_settings_table(self) -> None:
        with self._lock, self._engine.begin() as conn:
            conn.execute(
                text(
                    "CREATE TABLE IF NOT EXISTS discord_presence_settings ("
                    "id INTEGER PRIMARY KEY, "
                    "application_id VARCHAR(32) NOT NULL, "
                    "token_hash VARCHAR(64) NOT NULL, "
                    "enabled INTEGER NOT NULL DEFAULT 0, "
                    "updated_at VARCHAR(64) NOT NULL"
                    ")"
                )
            )

    def _ensure_data_repairs_table(self) -> None:
        with self._lock, self._engine.begin() as conn:
            conn.execute(
                text(
                    "CREATE TABLE IF NOT EXISTS e3_data_repairs ("
                    "repair_key VARCHAR(120) PRIMARY KEY, "
                    "details TEXT NOT NULL, "
                    "applied_at VARCHAR(64) NOT NULL"
                    ")"
                )
            )

    def _repair_discrete_video_11_12_history(self) -> None:
        repair_key = "discrete-video-11-to-12-2026-08-26"
        repair_day = "2026-08-26"
        with self._lock, self._engine.begin() as conn:
            if conn.execute(
                text("SELECT repair_key FROM e3_data_repairs WHERE repair_key = :repair_key"),
                {"repair_key": repair_key},
            ).first():
                return

            videos = conn.execute(
                text(
                    "SELECT id, sequence, title FROM study_plan_videos "
                    "WHERE subject = :subject AND sequence IN (11, 12)"
                ),
                {"subject": "離散數學"},
            ).mappings().all()
            video_by_sequence = {int(row["sequence"]): row for row in videos}
            source = video_by_sequence.get(11)
            target = video_by_sequence.get(12)
            if source is None or target is None:
                return

            source_id = int(source["id"])
            target_id = int(target["id"])
            events = conn.execute(
                text(
                    "SELECT id, video_id, watched_seconds, updated_at "
                    "FROM study_plan_activity_events "
                    "WHERE video_id IN (:source_id, :target_id) "
                    "ORDER BY updated_at, id"
                ),
                {"source_id": source_id, "target_id": target_id},
            ).mappings().all()
            moved_event_ids = [
                int(row["id"])
                for row in events
                if int(row["video_id"]) == source_id
                and self._study_plan_business_day_from_timestamp(str(row["updated_at"] or "")) == repair_day
            ]
            if not moved_event_ids:
                return

            for event_id in moved_event_ids:
                conn.execute(
                    text(
                        "UPDATE study_plan_activity_events SET video_id = :target_id "
                        "WHERE id = :event_id"
                    ),
                    {"target_id": target_id, "event_id": event_id},
                )

            repaired_events = conn.execute(
                text(
                    "SELECT id, video_id, watched_seconds FROM study_plan_activity_events "
                    "WHERE video_id IN (:source_id, :target_id) "
                    "ORDER BY updated_at, id"
                ),
                {"source_id": source_id, "target_id": target_id},
            ).mappings().all()
            previous_by_video = {source_id: 0.0, target_id: 0.0}
            for row in repaired_events:
                video_id = int(row["video_id"])
                previous = previous_by_video[video_id]
                watched = max(0.0, float(row["watched_seconds"] or 0))
                conn.execute(
                    text(
                        "UPDATE study_plan_activity_events SET "
                        "previous_watched_seconds = :previous, delta_seconds = :delta "
                        "WHERE id = :event_id"
                    ),
                    {
                        "previous": previous,
                        "delta": watched - previous,
                        "event_id": int(row["id"]),
                    },
                )
                previous_by_video[video_id] = watched

            moved_sessions = conn.execute(
                text(
                    "UPDATE study_time_sessions SET video_id = :target_id, label = :target_title "
                    "WHERE video_id = :source_id AND day = :repair_day"
                ),
                {
                    "source_id": source_id,
                    "target_id": target_id,
                    "target_title": str(target["title"] or "離散數學第 12 支"),
                    "repair_day": repair_day,
                },
            ).rowcount
            details = json.dumps(
                {
                    "moved_events": len(moved_event_ids),
                    "moved_sessions": max(0, int(moved_sessions or 0)),
                },
                ensure_ascii=False,
            )
            conn.execute(
                text(
                    "INSERT INTO e3_data_repairs (repair_key, details, applied_at) "
                    "VALUES (:repair_key, :details, :applied_at)"
                ),
                {
                    "repair_key": repair_key,
                    "details": details,
                    "applied_at": datetime.utcnow().isoformat(),
                },
            )

    def _repair_confirmed_discrete_14_calendar_move(self) -> None:
        """Finish the confirmed 23-minute Sep 1 -> Sep 2 calendar correction."""
        repair_key = "confirmed-discrete-14-calendar-move-2026-09-01-to-02"
        source_day = "2026-09-01"
        target_day = "2026-09-02"
        moved_seconds = 23 * 60
        with self._lock, self._engine.connect() as conn:
            if conn.execute(
                text("SELECT repair_key FROM e3_data_repairs WHERE repair_key = :repair_key"),
                {"repair_key": repair_key},
            ).first():
                return
            video = conn.execute(
                text(
                    "SELECT id FROM study_plan_videos "
                    "WHERE subject = :subject AND sequence = :sequence"
                ),
                {"subject": "離散數學", "sequence": 14},
            ).mappings().first()
        if not video:
            return

        video_id = int(video["id"])
        source_seconds = sum(
            max(0.0, float(item.get("delta_seconds") or 0))
            for item in self.list_study_plan_activity_events(day=source_day)
            if int(item.get("video_id") or 0) == video_id
        )
        target_seconds = sum(
            max(0.0, float(item.get("delta_seconds") or 0))
            for item in self.list_study_plan_activity_events(day=target_day)
            if int(item.get("video_id") or 0) == video_id
        )
        # This exact ten-minute target is the production state the user showed.
        # If anything has changed since then, fail closed instead of guessing.
        if source_seconds + 0.5 < moved_seconds or abs(target_seconds - 10 * 60) > 0.5:
            return
        result = self.move_study_plan_activity_between_days(
            video_id=video_id,
            source_day=source_day,
            target_day=target_day,
            seconds=moved_seconds,
            expected_source_seconds=source_seconds,
            expected_target_seconds=target_seconds,
        )
        if not result or result.get("stale"):
            return

        verified_source = sum(
            max(0.0, float(item.get("delta_seconds") or 0))
            for item in self.list_study_plan_activity_events(day=source_day)
            if int(item.get("video_id") or 0) == video_id
        )
        verified_target = sum(
            max(0.0, float(item.get("delta_seconds") or 0))
            for item in self.list_study_plan_activity_events(day=target_day)
            if int(item.get("video_id") or 0) == video_id
        )
        if (
            abs(verified_source - (source_seconds - moved_seconds)) > 0.5
            or abs(verified_target - (target_seconds + moved_seconds)) > 0.5
        ):
            self.undo_move_study_plan_activity_between_days(
                original_rows=list(result.get("original_rows") or []),
                generated_ids=list(result.get("generated_ids") or []),
            )
            return
        with self._lock, self._engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO e3_data_repairs (repair_key, details, applied_at) "
                    "VALUES (:repair_key, :details, :applied_at)"
                ),
                {
                    "repair_key": repair_key,
                    "details": json.dumps({
                        "video_id": video_id,
                        "source_day": source_day,
                        "target_day": target_day,
                        "moved_seconds": moved_seconds,
                        "source_seconds": verified_source,
                        "target_seconds": verified_target,
                    }, ensure_ascii=False),
                    "applied_at": datetime.utcnow().isoformat(),
                },
            )

    def _repair_discrete_video_11_progress_offset(self) -> None:
        repair_key = "discrete-video-11-progress-offset-2026-08-27"
        repair_day = "2026-08-26"
        with self._lock, self._engine.begin() as conn:
            if conn.execute(
                text("SELECT repair_key FROM e3_data_repairs WHERE repair_key = :repair_key"),
                {"repair_key": repair_key},
            ).first():
                return

            videos = conn.execute(
                text(
                    "SELECT id, sequence FROM study_plan_videos "
                    "WHERE subject = :subject AND sequence IN (11, 12)"
                ),
                {"subject": "離散數學"},
            ).mappings().all()
            video_id_by_sequence = {
                int(row["sequence"]): int(row["id"])
                for row in videos
            }
            source_id = video_id_by_sequence.get(11)
            target_id = video_id_by_sequence.get(12)
            if source_id is None or target_id is None:
                return

            target_events = conn.execute(
                text(
                    "SELECT watched_seconds, updated_at FROM study_plan_activity_events "
                    "WHERE video_id = :target_id ORDER BY updated_at, id"
                ),
                {"target_id": target_id},
            ).mappings().all()
            repaired_day_positions = [
                max(0.0, float(row["watched_seconds"] or 0))
                for row in target_events
                if self._study_plan_business_day_from_timestamp(str(row["updated_at"] or "")) == repair_day
            ]
            baseline = repaired_day_positions[-1] if repaired_day_positions else 0.0
            if baseline <= 0:
                return

            source_events = conn.execute(
                text(
                    "SELECT id, watched_seconds, updated_at FROM study_plan_activity_events "
                    "WHERE video_id = :source_id ORDER BY updated_at, id"
                ),
                {"source_id": source_id},
            ).mappings().all()
            adjusted_event_count = 0
            for row in source_events:
                event_day = self._study_plan_business_day_from_timestamp(str(row["updated_at"] or ""))
                if event_day <= repair_day:
                    continue
                corrected_watched = max(0.0, float(row["watched_seconds"] or 0) - baseline)
                conn.execute(
                    text(
                        "UPDATE study_plan_activity_events SET watched_seconds = :watched "
                        "WHERE id = :event_id"
                    ),
                    {"watched": corrected_watched, "event_id": int(row["id"])},
                )
                adjusted_event_count += 1

            running_position = 0.0
            corrected_source_events = conn.execute(
                text(
                    "SELECT id, watched_seconds FROM study_plan_activity_events "
                    "WHERE video_id = :source_id ORDER BY updated_at, id"
                ),
                {"source_id": source_id},
            ).mappings().all()
            for row in corrected_source_events:
                watched = max(0.0, float(row["watched_seconds"] or 0))
                conn.execute(
                    text(
                        "UPDATE study_plan_activity_events SET "
                        "previous_watched_seconds = :previous, delta_seconds = :delta "
                        "WHERE id = :event_id"
                    ),
                    {
                        "previous": running_position,
                        "delta": watched - running_position,
                        "event_id": int(row["id"]),
                    },
                )
                running_position = watched

            record = conn.execute(
                text(
                    "SELECT watched_seconds, playback_seconds FROM study_plan_video_records "
                    "WHERE video_id = :source_id"
                ),
                {"source_id": source_id},
            ).mappings().first()
            corrected_record = None
            now = datetime.utcnow().isoformat()
            if record is not None:
                corrected_record = max(0.0, float(record["watched_seconds"] or 0) - baseline)
                corrected_playback = max(0.0, float(record["playback_seconds"] or 0) - baseline)
                conn.execute(
                    text(
                        "UPDATE study_plan_video_records SET watched_seconds = :watched, "
                        "playback_seconds = :playback, progress_version = progress_version + 1, "
                        "updated_at = :updated_at WHERE video_id = :source_id"
                    ),
                    {
                        "watched": corrected_record,
                        "playback": corrected_playback,
                        "updated_at": now,
                        "source_id": source_id,
                    },
                )
                self._record_study_plan_daily_snapshot_locked(conn, now=now)

            conn.execute(
                text(
                    "INSERT INTO e3_data_repairs (repair_key, details, applied_at) "
                    "VALUES (:repair_key, :details, :applied_at)"
                ),
                {
                    "repair_key": repair_key,
                    "details": json.dumps(
                        {
                            "baseline_seconds": baseline,
                            "adjusted_events": adjusted_event_count,
                            "corrected_record_seconds": corrected_record,
                        },
                        ensure_ascii=False,
                    ),
                    "applied_at": now,
                },
            )

    def _restore_discrete_video_11_fresh_progress(self) -> None:
        repair_key = "discrete-video-11-fresh-progress-2026-08-27"
        offset_repair_key = "discrete-video-11-progress-offset-2026-08-27"
        repair_day = "2026-08-26"
        with self._lock, self._engine.begin() as conn:
            if conn.execute(
                text("SELECT repair_key FROM e3_data_repairs WHERE repair_key = :repair_key"),
                {"repair_key": repair_key},
            ).first():
                return

            offset_repair = conn.execute(
                text("SELECT details FROM e3_data_repairs WHERE repair_key = :repair_key"),
                {"repair_key": offset_repair_key},
            ).mappings().first()
            if offset_repair is None:
                return

            try:
                baseline = max(
                    0.0,
                    float(json.loads(str(offset_repair["details"] or "{}"))["baseline_seconds"]),
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                return
            if baseline <= 0:
                return

            source = conn.execute(
                text(
                    "SELECT id FROM study_plan_videos "
                    "WHERE subject = :subject AND sequence = 11"
                ),
                {"subject": "離散數學"},
            ).mappings().first()
            if source is None:
                return
            source_id = int(source["id"])

            source_events = conn.execute(
                text(
                    "SELECT id, watched_seconds, updated_at FROM study_plan_activity_events "
                    "WHERE video_id = :source_id ORDER BY updated_at, id"
                ),
                {"source_id": source_id},
            ).mappings().all()
            restored_event_count = 0
            running_position = 0.0
            for row in source_events:
                watched = max(0.0, float(row["watched_seconds"] or 0))
                event_day = self._study_plan_business_day_from_timestamp(str(row["updated_at"] or ""))
                if event_day > repair_day:
                    watched += baseline
                    restored_event_count += 1
                conn.execute(
                    text(
                        "UPDATE study_plan_activity_events SET watched_seconds = :watched, "
                        "previous_watched_seconds = :previous, delta_seconds = :delta "
                        "WHERE id = :event_id"
                    ),
                    {
                        "watched": watched,
                        "previous": running_position,
                        "delta": watched - running_position,
                        "event_id": int(row["id"]),
                    },
                )
                running_position = watched

            record = conn.execute(
                text(
                    "SELECT watched_seconds, playback_seconds FROM study_plan_video_records "
                    "WHERE video_id = :source_id"
                ),
                {"source_id": source_id},
            ).mappings().first()
            restored_record = None
            now = datetime.utcnow().isoformat()
            if record is not None:
                restored_record = max(0.0, float(record["watched_seconds"] or 0)) + baseline
                restored_playback = max(0.0, float(record["playback_seconds"] or 0)) + baseline
                conn.execute(
                    text(
                        "UPDATE study_plan_video_records SET watched_seconds = :watched, "
                        "playback_seconds = :playback, progress_version = progress_version + 1, "
                        "updated_at = :updated_at WHERE video_id = :source_id"
                    ),
                    {
                        "watched": restored_record,
                        "playback": restored_playback,
                        "updated_at": now,
                        "source_id": source_id,
                    },
                )
                self._record_study_plan_daily_snapshot_locked(conn, now=now)

            conn.execute(
                text(
                    "INSERT INTO e3_data_repairs (repair_key, details, applied_at) "
                    "VALUES (:repair_key, :details, :applied_at)"
                ),
                {
                    "repair_key": repair_key,
                    "details": json.dumps(
                        {
                            "restored_seconds": baseline,
                            "restored_events": restored_event_count,
                            "restored_record_seconds": restored_record,
                        },
                        ensure_ascii=False,
                    ),
                    "applied_at": now,
                },
            )

    def load_discord_presence_settings(self) -> Dict[str, Any]:
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT application_id, token_hash, enabled, updated_at "
                    "FROM discord_presence_settings WHERE id = 1"
                )
            ).mappings().first()
        if not row:
            return {
                "application_id": "",
                "has_token": False,
                "enabled": False,
                "updated_at": "",
            }
        return {
            "application_id": str(row.get("application_id") or ""),
            "has_token": bool(str(row.get("token_hash") or "")),
            "enabled": bool(row.get("enabled")),
            "updated_at": str(row.get("updated_at") or ""),
        }

    def save_discord_presence_settings(
        self,
        *,
        application_id: str,
        token: str | None = None,
        enabled: bool,
    ) -> Dict[str, Any]:
        normalized_application_id = str(application_id or "").strip()
        token_hash = hashlib.sha256(str(token).encode("utf-8")).hexdigest() if token else None
        now = datetime.utcnow().isoformat()
        with self._lock, self._engine.begin() as conn:
            existing = conn.execute(
                text(
                    "SELECT token_hash FROM discord_presence_settings WHERE id = 1"
                )
            ).mappings().first()
            resolved_hash = token_hash if token_hash is not None else str((existing or {}).get("token_hash") or "")
            params = {
                "application_id": normalized_application_id,
                "token_hash": resolved_hash,
                "enabled": 1 if enabled and resolved_hash and normalized_application_id else 0,
                "updated_at": now,
            }
            if existing:
                conn.execute(
                    text(
                        "UPDATE discord_presence_settings SET "
                        "application_id = :application_id, token_hash = :token_hash, "
                        "enabled = :enabled, updated_at = :updated_at WHERE id = 1"
                    ),
                    params,
                )
            else:
                conn.execute(
                    text(
                        "INSERT INTO discord_presence_settings "
                        "(id, application_id, token_hash, enabled, updated_at) VALUES "
                        "(1, :application_id, :token_hash, :enabled, :updated_at)"
                    ),
                    params,
                )
        return self.load_discord_presence_settings()

    def revoke_discord_presence_token(self) -> None:
        current = self.load_discord_presence_settings()
        with self._lock, self._engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM discord_presence_settings WHERE id = 1"
                )
            )
        if current.get("application_id"):
            self.save_discord_presence_settings(
                application_id=str(current["application_id"]),
                enabled=False,
            )

    def verify_discord_presence_token(self, token: str) -> bool:
        candidate = str(token or "").strip()
        if not candidate:
            return False
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT token_hash, enabled FROM discord_presence_settings WHERE id = 1"
                )
            ).mappings().first()
        expected = str((row or {}).get("token_hash") or "")
        actual = hashlib.sha256(candidate.encode("utf-8")).hexdigest()
        return bool(row and row.get("enabled") and expected and hmac.compare_digest(expected, actual))

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
        self._repair_discrete_video_11_12_history()
        self._repair_confirmed_discrete_14_calendar_move()
        self._restore_discrete_video_11_fresh_progress()


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
    agent_path: Path,
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
        base_url = str(os.getenv("E3_APP_HOME_URL") or request.url_root).rstrip("/")

        def render_settings(*, revealed_token: str = ""):
            return render_template_string(
                template,
                settings=storage.load_study_player_settings(),
                discord=storage.load_discord_presence_settings(),
                discord_token_once=revealed_token,
                discord_api_url=f"{base_url}/api/discord-presence",
                discord_agent_url=f"{base_url}/downloads/e3-discord-presence.py",
                username=session.get("username") or "管理員",
            )

        if request.method == "POST":
            action = str(request.form.get("action") or "save_player").strip()
            if action == "discord_generate":
                application_id = str(request.form.get("discord_application_id") or "").strip()
                if not DISCORD_APPLICATION_ID_PATTERN.fullmatch(application_id):
                    flash("Discord Application ID 格式不正確。", "error")
                    return redirect(url_for("admin_study_player_settings", _anchor="discord-presence"))
                token = issue_discord_presence_token(application_id, app.secret_key)
                storage.save_discord_presence_settings(
                    application_id=application_id,
                    token=token,
                    enabled=True,
                )
                flash("Discord 連線權杖已更新，請下載新的 Windows 安裝檔。", "success")
                return render_settings(revealed_token=token)
            if action == "discord_revoke":
                storage.revoke_discord_presence_token()
                flash("Discord 學習狀態連線已停用。", "success")
                return redirect(url_for("admin_study_player_settings", _anchor="discord-presence"))
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
        return render_settings()

    def admin_study_player_settings_json():
        denied = require_admin()
        if denied is not None:
            return denied
        return {
            "ok": True,
            "settings": storage.load_study_player_settings(),
        }

    def discord_presence_api():
        authorization = str(request.headers.get("Authorization") or "")
        scheme, _separator, token = authorization.partition(" ")
        authorized = bool(
            scheme.lower() == "bearer"
            and storage.verify_discord_presence_token(token)
        )
        if not authorized and scheme.lower() == "bearer":
            recovered_application_id = verify_discord_presence_signed_token(
                token,
                app.secret_key,
            )
            current_settings = storage.load_discord_presence_settings()
            can_recover = bool(
                recovered_application_id
                and not current_settings.get("application_id")
                and not current_settings.get("has_token")
            )
            if can_recover:
                storage.save_discord_presence_settings(
                    application_id=recovered_application_id,
                    token=token,
                    enabled=True,
                )
                authorized = True
        if not authorized:
            return (
                {"ok": False, "error": "invalid_presence_token"},
                401,
                {
                    "Cache-Control": "no-store, max-age=0",
                    "WWW-Authenticate": 'Bearer realm="E3 Discord Presence"',
                },
            )
        base_url = str(os.getenv("E3_APP_HOME_URL") or request.url_root).rstrip("/")
        return (
            build_discord_presence_payload(
                storage,
                public_url=f"{base_url}/study-progress",
                cover_image_url=f"{base_url}/static/discord/e3-study-cover.png",
            ),
            200,
            {"Cache-Control": "no-store, max-age=0"},
        )

    def discord_presence_agent_download():
        if not agent_path.is_file():
            return Response("Not Found", status=404, mimetype="text/plain")
        return send_file(
            agent_path,
            mimetype="text/x-python",
            as_attachment=True,
            download_name="e3_discord_presence.py",
            conditional=True,
            max_age=3600,
        )

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
    app.add_url_rule(
        "/api/discord-presence",
        endpoint="discord_presence_api",
        view_func=discord_presence_api,
        methods=["GET"],
    )
    app.add_url_rule(
        "/downloads/e3-discord-presence.py",
        endpoint="discord_presence_agent_download",
        view_func=discord_presence_agent_download,
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
    discord_agent_path = root_dir / "backend" / "tools" / "e3_discord_presence.py"
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
            _register_player_settings_routes(
                app,
                storage,
                settings_template,
                discord_agent_path,
            )
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
