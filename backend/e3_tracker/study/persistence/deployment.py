"""Study-owned player persistence, synchronization and historical data repairs."""

import hashlib
import hmac
import json
from datetime import datetime
from typing import Any, Dict, List, Mapping
from sqlalchemy import text
from e3_tracker.study.domain.player_settings import (
    PLAYER_SETTINGS_DEFAULTS,
    normalize_player_settings,
)
from e3_tracker.study.services.discord_presence import DISCORD_APPLICATION_ID_PATTERN

_YOUTUBE_FIELDS = (
    "youtube_video_id",
    "youtube_playlist_id",
    "youtube_url",
)


class StudyDeploymentStorage:
    """Keep deployment data and player preferences authoritative across restarts."""

    def _repair_discrete_video_11_12_history(self) -> None:
        repair_key = "discrete-video-11-to-12-2026-08-26"
        repair_day = "2026-08-26"
        with self._lock, self._engine.begin() as conn:
            if conn.execute(
                text(
                    "SELECT repair_key FROM e3_data_repairs WHERE repair_key = :repair_key"
                ),
                {"repair_key": repair_key},
            ).first():
                return

            videos = (
                conn.execute(
                    text(
                        "SELECT id, sequence, title FROM study_plan_videos "
                        "WHERE subject = :subject AND sequence IN (11, 12)"
                    ),
                    {"subject": "離散數學"},
                )
                .mappings()
                .all()
            )
            video_by_sequence = {int(row["sequence"]): row for row in videos}
            source = video_by_sequence.get(11)
            target = video_by_sequence.get(12)
            if source is None or target is None:
                return

            source_id = int(source["id"])
            target_id = int(target["id"])
            events = (
                conn.execute(
                    text(
                        "SELECT id, video_id, watched_seconds, updated_at "
                        "FROM study_plan_activity_events "
                        "WHERE video_id IN (:source_id, :target_id) "
                        "ORDER BY updated_at, id"
                    ),
                    {"source_id": source_id, "target_id": target_id},
                )
                .mappings()
                .all()
            )
            moved_event_ids = [
                int(row["id"])
                for row in events
                if int(row["video_id"]) == source_id
                and self._study_plan_business_day_from_timestamp(
                    str(row["updated_at"] or "")
                )
                == repair_day
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

            repaired_events = (
                conn.execute(
                    text(
                        "SELECT id, video_id, watched_seconds FROM study_plan_activity_events "
                        "WHERE video_id IN (:source_id, :target_id) "
                        "ORDER BY updated_at, id"
                    ),
                    {"source_id": source_id, "target_id": target_id},
                )
                .mappings()
                .all()
            )
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
                text(
                    "SELECT repair_key FROM e3_data_repairs WHERE repair_key = :repair_key"
                ),
                {"repair_key": repair_key},
            ).first():
                return
            video = (
                conn.execute(
                    text(
                        "SELECT id FROM study_plan_videos "
                        "WHERE subject = :subject AND sequence = :sequence"
                    ),
                    {"subject": "離散數學", "sequence": 14},
                )
                .mappings()
                .first()
            )
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
        # The UI rounds the stored seconds to ten minutes (the actual value is
        # about 9m32s for this video). Match only values that render as 10m; if
        # anything has changed beyond that narrow interval, fail closed.
        renders_as_ten_minutes = 9.5 * 60 <= target_seconds < 10.5 * 60
        if source_seconds + 0.5 < moved_seconds or not renders_as_ten_minutes:
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
                    "details": json.dumps(
                        {
                            "video_id": video_id,
                            "source_day": source_day,
                            "target_day": target_day,
                            "moved_seconds": moved_seconds,
                            "source_seconds": verified_source,
                            "target_seconds": verified_target,
                        },
                        ensure_ascii=False,
                    ),
                    "applied_at": datetime.utcnow().isoformat(),
                },
            )

    def _repair_discrete_video_11_progress_offset(self) -> None:
        repair_key = "discrete-video-11-progress-offset-2026-08-27"
        repair_day = "2026-08-26"
        with self._lock, self._engine.begin() as conn:
            if conn.execute(
                text(
                    "SELECT repair_key FROM e3_data_repairs WHERE repair_key = :repair_key"
                ),
                {"repair_key": repair_key},
            ).first():
                return

            videos = (
                conn.execute(
                    text(
                        "SELECT id, sequence FROM study_plan_videos "
                        "WHERE subject = :subject AND sequence IN (11, 12)"
                    ),
                    {"subject": "離散數學"},
                )
                .mappings()
                .all()
            )
            video_id_by_sequence = {
                int(row["sequence"]): int(row["id"]) for row in videos
            }
            source_id = video_id_by_sequence.get(11)
            target_id = video_id_by_sequence.get(12)
            if source_id is None or target_id is None:
                return

            target_events = (
                conn.execute(
                    text(
                        "SELECT watched_seconds, updated_at FROM study_plan_activity_events "
                        "WHERE video_id = :target_id ORDER BY updated_at, id"
                    ),
                    {"target_id": target_id},
                )
                .mappings()
                .all()
            )
            repaired_day_positions = [
                max(0.0, float(row["watched_seconds"] or 0))
                for row in target_events
                if self._study_plan_business_day_from_timestamp(
                    str(row["updated_at"] or "")
                )
                == repair_day
            ]
            baseline = repaired_day_positions[-1] if repaired_day_positions else 0.0
            if baseline <= 0:
                return

            source_events = (
                conn.execute(
                    text(
                        "SELECT id, watched_seconds, updated_at FROM study_plan_activity_events "
                        "WHERE video_id = :source_id ORDER BY updated_at, id"
                    ),
                    {"source_id": source_id},
                )
                .mappings()
                .all()
            )
            adjusted_event_count = 0
            for row in source_events:
                event_day = self._study_plan_business_day_from_timestamp(
                    str(row["updated_at"] or "")
                )
                if event_day <= repair_day:
                    continue
                corrected_watched = max(
                    0.0, float(row["watched_seconds"] or 0) - baseline
                )
                conn.execute(
                    text(
                        "UPDATE study_plan_activity_events SET watched_seconds = :watched "
                        "WHERE id = :event_id"
                    ),
                    {"watched": corrected_watched, "event_id": int(row["id"])},
                )
                adjusted_event_count += 1

            running_position = 0.0
            corrected_source_events = (
                conn.execute(
                    text(
                        "SELECT id, watched_seconds FROM study_plan_activity_events "
                        "WHERE video_id = :source_id ORDER BY updated_at, id"
                    ),
                    {"source_id": source_id},
                )
                .mappings()
                .all()
            )
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

            record = (
                conn.execute(
                    text(
                        "SELECT watched_seconds, playback_seconds FROM study_plan_video_records "
                        "WHERE video_id = :source_id"
                    ),
                    {"source_id": source_id},
                )
                .mappings()
                .first()
            )
            corrected_record = None
            now = datetime.utcnow().isoformat()
            if record is not None:
                corrected_record = max(
                    0.0, float(record["watched_seconds"] or 0) - baseline
                )
                corrected_playback = max(
                    0.0, float(record["playback_seconds"] or 0) - baseline
                )
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
                text(
                    "SELECT repair_key FROM e3_data_repairs WHERE repair_key = :repair_key"
                ),
                {"repair_key": repair_key},
            ).first():
                return

            offset_repair = (
                conn.execute(
                    text(
                        "SELECT details FROM e3_data_repairs WHERE repair_key = :repair_key"
                    ),
                    {"repair_key": offset_repair_key},
                )
                .mappings()
                .first()
            )
            if offset_repair is None:
                return

            try:
                baseline = max(
                    0.0,
                    float(
                        json.loads(str(offset_repair["details"] or "{}"))[
                            "baseline_seconds"
                        ]
                    ),
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                return
            if baseline <= 0:
                return

            source = (
                conn.execute(
                    text(
                        "SELECT id FROM study_plan_videos "
                        "WHERE subject = :subject AND sequence = 11"
                    ),
                    {"subject": "離散數學"},
                )
                .mappings()
                .first()
            )
            if source is None:
                return
            source_id = int(source["id"])

            source_events = (
                conn.execute(
                    text(
                        "SELECT id, watched_seconds, updated_at FROM study_plan_activity_events "
                        "WHERE video_id = :source_id ORDER BY updated_at, id"
                    ),
                    {"source_id": source_id},
                )
                .mappings()
                .all()
            )
            restored_event_count = 0
            running_position = 0.0
            for row in source_events:
                watched = max(0.0, float(row["watched_seconds"] or 0))
                event_day = self._study_plan_business_day_from_timestamp(
                    str(row["updated_at"] or "")
                )
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

            record = (
                conn.execute(
                    text(
                        "SELECT watched_seconds, playback_seconds FROM study_plan_video_records "
                        "WHERE video_id = :source_id"
                    ),
                    {"source_id": source_id},
                )
                .mappings()
                .first()
            )
            restored_record = None
            now = datetime.utcnow().isoformat()
            if record is not None:
                restored_record = (
                    max(0.0, float(record["watched_seconds"] or 0)) + baseline
                )
                restored_playback = (
                    max(0.0, float(record["playback_seconds"] or 0)) + baseline
                )
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
            row = (
                conn.execute(
                    text(
                        "SELECT application_id, token_hash, enabled, updated_at "
                        "FROM discord_presence_settings WHERE id = 1"
                    )
                )
                .mappings()
                .first()
            )
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
        token_hash = (
            hashlib.sha256(str(token).encode("utf-8")).hexdigest() if token else None
        )
        now = datetime.utcnow().isoformat()
        with self._lock, self._engine.begin() as conn:
            existing = (
                conn.execute(
                    text(
                        "SELECT token_hash FROM discord_presence_settings WHERE id = 1"
                    )
                )
                .mappings()
                .first()
            )
            resolved_hash = (
                token_hash
                if token_hash is not None
                else str((existing or {}).get("token_hash") or "")
            )
            params = {
                "application_id": normalized_application_id,
                "token_hash": resolved_hash,
                "enabled": (
                    1 if enabled and resolved_hash and normalized_application_id else 0
                ),
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
            conn.execute(text("DELETE FROM discord_presence_settings WHERE id = 1"))
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
            row = (
                conn.execute(
                    text(
                        "SELECT token_hash, enabled FROM discord_presence_settings WHERE id = 1"
                    )
                )
                .mappings()
                .first()
            )
        expected = str((row or {}).get("token_hash") or "")
        actual = hashlib.sha256(candidate.encode("utf-8")).hexdigest()
        return bool(
            row
            and row.get("enabled")
            and expected
            and hmac.compare_digest(expected, actual)
        )

    def load_study_player_settings(self) -> Dict[str, Any]:
        with self._lock, self._engine.connect() as conn:
            row = (
                conn.execute(
                    text(
                        "SELECT default_playback_rate, hold_space_rate, hold_delay_ms, "
                        "seek_back_seconds, seek_forward_seconds, seek_repeat_ms, "
                        "playback_rate_step, volume_step, controls_hide_ms, "
                        "center_click_toggle, pause_on_marker, show_speed_presets, "
                        "show_shortcut_hint, hint_duration_ms "
                        "FROM study_player_settings WHERE id = 1"
                    )
                )
                .mappings()
                .first()
            )
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
