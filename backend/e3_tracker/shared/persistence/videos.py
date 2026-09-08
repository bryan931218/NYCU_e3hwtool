"""Persistence operations for videos."""
import json
import math
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import delete, insert, select, update
from sqlalchemy.exc import IntegrityError

from .schema import study_plan_videos_table, study_plan_video_overrides_table, youtube_storyboard_metadata_table, study_plan_video_records_table, study_plan_daily_snapshots_table, study_plan_activity_events_table, study_plan_video_markers_table, study_plan_replan_settings_table, study_plan_rest_days_table


class VideosStorage:
    def sync_study_plan_videos(self, videos: List[Dict[str, Any]]) -> None:
        """Upsert the source inventory while preserving each video's viewing record."""
        now = self._now_iso()
        normalized: List[Dict[str, Any]] = []
        for item in videos:
            try:
                subject = str(item.get("subject") or "").strip()
                sequence = int(item.get("sequence") or 0)
                title = str(item.get("title") or "").strip()
                duration_seconds = float(item.get("duration_seconds") or 0)
            except (AttributeError, TypeError, ValueError):
                continue
            if not subject or sequence <= 0 or not title or duration_seconds <= 0:
                continue
            normalized.append(
                {
                    "subject": subject,
                    "sequence": sequence,
                    "title": title,
                    "duration_seconds": duration_seconds,
                    "youtube_video_id": str(item.get("youtube_video_id") or "").strip() or None,
                    "youtube_playlist_id": str(item.get("youtube_playlist_id") or "").strip() or None,
                    "youtube_url": str(item.get("youtube_url") or "").strip() or None,
                }
            )
        if not normalized:
            return
        with self._lock, self._engine.begin() as conn:
            existing_rows = conn.execute(
                select(
                    study_plan_videos_table.c.id,
                    study_plan_videos_table.c.subject,
                    study_plan_videos_table.c.sequence,
                )
            ).fetchall()
            existing = {(row.subject, int(row.sequence)): int(row.id) for row in existing_rows}
            for item in normalized:
                key = (item["subject"], item["sequence"])
                values = {**item, "updated_at": now}
                video_id = existing.get(key)
                if video_id:
                    conn.execute(
                        update(study_plan_videos_table)
                        .where(study_plan_videos_table.c.id == video_id)
                        .values(**values)
                    )
                else:
                    conn.execute(insert(study_plan_videos_table).values(created_at=now, **values))

    def list_study_plan_videos_with_records(self) -> List[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                select(
                    study_plan_videos_table.c.id,
                    study_plan_videos_table.c.subject,
                    study_plan_videos_table.c.sequence,
                    study_plan_videos_table.c.title,
                    study_plan_videos_table.c.duration_seconds,
                    study_plan_videos_table.c.youtube_video_id,
                    study_plan_videos_table.c.youtube_playlist_id,
                    study_plan_videos_table.c.youtube_url,
                    study_plan_video_records_table.c.watched_seconds,
                    study_plan_video_records_table.c.playback_seconds,
                    study_plan_video_records_table.c.progress_version,
                    study_plan_video_records_table.c.notes,
                    study_plan_video_records_table.c.updated_at,
                    study_plan_video_overrides_table.c.video_id.label("override_video_id"),
                    study_plan_video_overrides_table.c.youtube_video_id.label("override_youtube_video_id"),
                    study_plan_video_overrides_table.c.youtube_playlist_id.label("override_youtube_playlist_id"),
                    study_plan_video_overrides_table.c.youtube_url.label("override_youtube_url"),
                )
                .select_from(
                    study_plan_videos_table.outerjoin(
                        study_plan_video_records_table,
                        study_plan_videos_table.c.id == study_plan_video_records_table.c.video_id,
                    ).outerjoin(
                        study_plan_video_overrides_table,
                        study_plan_videos_table.c.id == study_plan_video_overrides_table.c.video_id,
                    )
                )
                .order_by(study_plan_videos_table.c.subject, study_plan_videos_table.c.sequence)
            ).fetchall()

        def _to_taipei(dt_str: Optional[str]) -> Optional[str]:
            if not dt_str:
                return None
            try:
                return (datetime.fromisoformat(dt_str) + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M")
            except (ValueError, TypeError):
                return dt_str[:16].replace("T", " ")

        return [
            {
                "id": int(row.id),
                "subject": row.subject,
                "sequence": int(row.sequence),
                "title": row.title,
                "duration_seconds": float(row.duration_seconds or 0),
                "youtube_video_id": (
                    (row.override_youtube_video_id or "")
                    if row.override_video_id is not None
                    else (row.youtube_video_id or "")
                ),
                "youtube_playlist_id": (
                    (row.override_youtube_playlist_id or "")
                    if row.override_video_id is not None
                    else (row.youtube_playlist_id or "")
                ),
                "youtube_url": (
                    (row.override_youtube_url or "")
                    if row.override_video_id is not None
                    else (row.youtube_url or "")
                ),
                "watched_seconds": max(0.0, float(row.watched_seconds or 0)),
                "playback_seconds": min(
                    max(0.0, float(row.playback_seconds or 0)),
                    max(0.0, float(row.duration_seconds or 0)),
                ),
                "progress_version": max(0, int(row.progress_version or 0)),
                "notes": row.notes or "",
                "updated_at": _to_taipei(row.updated_at),
                "updated_at_iso": row.updated_at or "",
            }
            for row in rows
        ]

    def upsert_study_plan_video_record(
        self,
        *,
        video_id: int,
        watched_seconds: float,
        notes: str,
    ) -> bool:
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            video = conn.execute(
                select(study_plan_videos_table.c.duration_seconds).where(
                    study_plan_videos_table.c.id == video_id
                )
            ).fetchone()
            if not video:
                return False
            raw_seconds = float(watched_seconds or 0)
            if not math.isfinite(raw_seconds):
                return False
            normalized_seconds = max(0.0, min(raw_seconds, float(video.duration_seconds or 0)))
            existing = conn.execute(
                select(
                    study_plan_video_records_table.c.video_id,
                    study_plan_video_records_table.c.watched_seconds,
                    study_plan_video_records_table.c.progress_version,
                ).where(
                    study_plan_video_records_table.c.video_id == video_id
                )
            ).fetchone()
            previous_watched_seconds = float(existing.watched_seconds or 0) if existing else 0.0
            progress_version = max(0, int(existing.progress_version or 0)) + 1 if existing else 1
            values = {
                "watched_seconds": normalized_seconds,
                "playback_seconds": normalized_seconds,
                "progress_version": progress_version,
                "notes": notes,
                "updated_at": now,
            }
            if existing:
                conn.execute(
                    update(study_plan_video_records_table)
                    .where(study_plan_video_records_table.c.video_id == video_id)
                    .values(**values)
                )
            else:
                conn.execute(
                    insert(study_plan_video_records_table).values(video_id=video_id, **values)
                )
            self._record_study_plan_activity_locked(
                conn,
                video_id=video_id,
                previous_watched_seconds=previous_watched_seconds,
                watched_seconds=normalized_seconds,
                now=now,
            )
            self._record_study_plan_daily_snapshot_locked(conn, now=now)
        return True

    def update_study_plan_video_progress(
        self,
        *,
        video_id: int,
        watched_seconds: float,
        expected_version: int,
    ) -> Optional[Dict[str, Any]]:
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            video = conn.execute(
                select(
                    study_plan_videos_table.c.id,
                    study_plan_videos_table.c.duration_seconds,
                    study_plan_videos_table.c.youtube_video_id,
                ).where(study_plan_videos_table.c.id == video_id)
            ).fetchone()
            if not video:
                return None
            existing = conn.execute(
                select(
                    study_plan_video_records_table.c.watched_seconds,
                    study_plan_video_records_table.c.playback_seconds,
                    study_plan_video_records_table.c.progress_version,
                    study_plan_video_records_table.c.notes,
                ).where(study_plan_video_records_table.c.video_id == video_id)
            ).fetchone()
            duration_seconds = max(0.0, float(video.duration_seconds or 0))
            raw_seconds = float(watched_seconds or 0)
            if not math.isfinite(raw_seconds):
                return None
            current_seconds = max(0.0, min(raw_seconds, duration_seconds))
            notes = existing.notes if existing else ""
            previous_watched_seconds = float(existing.watched_seconds or 0) if existing else 0.0
            previous_playback_seconds = float(existing.playback_seconds or 0) if existing else 0.0
            current_version = max(0, int(existing.progress_version or 0)) if existing else 0

            def _progress_result(
                saved_seconds: float,
                playback_seconds: float,
                progress_version: int,
                *,
                stale: bool,
            ) -> Dict[str, Any]:
                return {
                    "video_id": int(video.id),
                    "duration_seconds": duration_seconds,
                    "watched_seconds": saved_seconds,
                    "playback_seconds": playback_seconds,
                    "progress_version": progress_version,
                    "stale": stale,
                    "completion": min(
                        100.0,
                        (saved_seconds / duration_seconds * 100) if duration_seconds else 0.0,
                    ),
                    "youtube_video_id": video.youtube_video_id or "",
                }

            # Reject delayed requests and stale tabs instead of letting them replace the
            # latest saved position. The caller adopts this authoritative version.
            if max(0, int(expected_version)) != current_version:
                return _progress_result(
                    previous_watched_seconds,
                    previous_playback_seconds,
                    current_version,
                    stale=True,
                )

            # Progress follows the last saved player position. Seeking backward is a
            # correction, not a replay event that should preserve a historical maximum.
            normalized_seconds = current_seconds
            if existing and abs(normalized_seconds - previous_watched_seconds) < 0.01:
                return _progress_result(
                    previous_watched_seconds,
                    current_seconds,
                    current_version,
                    stale=False,
                )

            next_version = current_version + 1
            values = {
                "watched_seconds": normalized_seconds,
                "playback_seconds": current_seconds,
                "progress_version": next_version,
                "notes": notes or "",
                "updated_at": now,
            }
            if existing:
                conn.execute(
                    update(study_plan_video_records_table)
                    .where(study_plan_video_records_table.c.video_id == video_id)
                    .values(**values)
                )
            else:
                conn.execute(insert(study_plan_video_records_table).values(video_id=video_id, **values))
            self._record_study_plan_activity_locked(
                conn,
                video_id=int(video.id),
                previous_watched_seconds=previous_watched_seconds,
                watched_seconds=normalized_seconds,
                now=now,
            )
            self._record_study_plan_daily_snapshot_locked(conn, now=now)
            return _progress_result(
                normalized_seconds,
                current_seconds,
                next_version,
                stale=False,
            )

    def delete_study_plan_video_record(self, video_id: int) -> bool:
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            existing = conn.execute(
                select(study_plan_video_records_table.c.watched_seconds).where(
                    study_plan_video_records_table.c.video_id == video_id
                )
            ).fetchone()
            previous_watched_seconds = float(existing.watched_seconds or 0) if existing else 0.0
            result = conn.execute(
                delete(study_plan_video_records_table).where(study_plan_video_records_table.c.video_id == video_id)
            )
            if result.rowcount:
                self._record_study_plan_activity_locked(
                    conn,
                    video_id=video_id,
                    previous_watched_seconds=previous_watched_seconds,
                    watched_seconds=0.0,
                    now=now,
                )
                self._record_study_plan_daily_snapshot_locked(conn, now=now)
        return bool(result.rowcount)

    def list_study_plan_daily_snapshots(
        self,
        *,
        start_day: Optional[str] = None,
        end_day: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        stmt = select(
            study_plan_daily_snapshots_table.c.day,
            study_plan_daily_snapshots_table.c.total_watched_seconds,
            study_plan_daily_snapshots_table.c.updated_at,
        ).order_by(study_plan_daily_snapshots_table.c.day)
        if start_day:
            stmt = stmt.where(study_plan_daily_snapshots_table.c.day >= start_day)
        if end_day:
            stmt = stmt.where(study_plan_daily_snapshots_table.c.day <= end_day)
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
        return [
            {
                "day": str(row.day),
                "total_watched_seconds": max(0.0, float(row.total_watched_seconds or 0)),
                "updated_at": row.updated_at or "",
            }
            for row in rows
        ]

    def update_study_plan_video_youtube(
        self,
        *,
        video_id: int,
        youtube_video_id: str,
        youtube_playlist_id: str,
        youtube_url: str,
    ) -> bool:
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            exists = conn.execute(
                select(study_plan_videos_table.c.id)
                .where(study_plan_videos_table.c.id == int(video_id))
            ).fetchone()
            if not exists:
                return False
            values = {
                "youtube_video_id": str(youtube_video_id or "").strip() or None,
                "youtube_playlist_id": str(youtube_playlist_id or "").strip() or None,
                "youtube_url": str(youtube_url or "").strip() or None,
                "updated_at": now,
            }
            conn.execute(
                update(study_plan_video_overrides_table)
                .where(study_plan_video_overrides_table.c.video_id == int(video_id))
                .values(**values)
            )
            if not conn.execute(
                select(study_plan_video_overrides_table.c.video_id)
                .where(study_plan_video_overrides_table.c.video_id == int(video_id))
            ).fetchone():
                conn.execute(
                    insert(study_plan_video_overrides_table).values(
                        video_id=int(video_id),
                        **values,
                    )
                )
            return True

    def get_youtube_storyboard_metadata(self, youtube_video_id: str) -> Optional[Dict[str, Any]]:
        video_id = str(youtube_video_id or "").strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
            return None
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                select(youtube_storyboard_metadata_table).where(
                    youtube_storyboard_metadata_table.c.youtube_video_id == video_id
                )
            ).fetchone()
        if row is None:
            return None
        return {
            "youtube_video_id": str(row.youtube_video_id),
            "duration_seconds": max(0.0, float(row.duration_seconds or 0)),
            "storyboard_spec": str(row.storyboard_spec or ""),
            "updated_at": str(row.updated_at or ""),
        }

    def upsert_youtube_storyboard_metadata(
        self,
        *,
        youtube_video_id: str,
        duration_seconds: float,
        storyboard_spec: str,
    ) -> Optional[Dict[str, Any]]:
        video_id = str(youtube_video_id or "").strip()
        spec = str(storyboard_spec or "").strip()
        if (
            not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id)
            or not spec
            or len(spec) > 250_000
        ):
            return None
        try:
            duration = max(0.0, float(duration_seconds or 0))
        except (TypeError, ValueError):
            duration = 0.0
        if not math.isfinite(duration):
            duration = 0.0
        now = self._now_iso()
        values = {
            "duration_seconds": duration,
            "storyboard_spec": spec,
            "updated_at": now,
        }
        try:
            with self._lock, self._engine.begin() as conn:
                result = conn.execute(
                    update(youtube_storyboard_metadata_table)
                    .where(youtube_storyboard_metadata_table.c.youtube_video_id == video_id)
                    .values(**values)
                )
                if not result.rowcount:
                    conn.execute(
                        insert(youtube_storyboard_metadata_table).values(
                            youtube_video_id=video_id,
                            **values,
                        )
                    )
        except IntegrityError:
            # Separate web workers can discover the same new video concurrently.
            with self._lock, self._engine.begin() as conn:
                conn.execute(
                    update(youtube_storyboard_metadata_table)
                    .where(youtube_storyboard_metadata_table.c.youtube_video_id == video_id)
                    .values(**values)
                )
        return {
            "youtube_video_id": video_id,
            **values,
        }

    def sync_study_plan_youtube_links(self, links: List[Dict[str, Any]]) -> Dict[str, int]:
        """Update playlist-derived links without replacing manual overrides or progress."""
        normalized: Dict[Tuple[str, int], Dict[str, str]] = {}
        for item in links:
            try:
                subject = str(item.get("subject") or "").strip()
                sequence = int(item.get("sequence") or 0)
            except (AttributeError, TypeError, ValueError):
                continue
            video_id = str(item.get("youtube_video_id") or "").strip()
            playlist_id = str(item.get("youtube_playlist_id") or "").strip()
            youtube_url = str(item.get("youtube_url") or "").strip()
            if not subject or sequence <= 0 or not video_id or not youtube_url:
                continue
            normalized[(subject, sequence)] = {
                "youtube_video_id": video_id,
                "youtube_playlist_id": playlist_id,
                "youtube_url": youtube_url,
            }

        result = {
            "matched": 0,
            "updated": 0,
            "unchanged": 0,
            "unmatched": 0,
            "manual_overrides_preserved": 0,
        }
        if not normalized:
            return result

        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            rows = conn.execute(
                select(
                    study_plan_videos_table.c.id,
                    study_plan_videos_table.c.subject,
                    study_plan_videos_table.c.sequence,
                    study_plan_videos_table.c.youtube_video_id,
                    study_plan_videos_table.c.youtube_playlist_id,
                    study_plan_videos_table.c.youtube_url,
                )
            ).fetchall()
            existing = {
                (str(row.subject), int(row.sequence)): row
                for row in rows
            }
            override_ids = {
                int(row.video_id)
                for row in conn.execute(
                    select(study_plan_video_overrides_table.c.video_id)
                ).fetchall()
            }
            for key, values in normalized.items():
                row = existing.get(key)
                if row is None:
                    result["unmatched"] += 1
                    continue
                result["matched"] += 1
                if int(row.id) in override_ids:
                    result["manual_overrides_preserved"] += 1
                before = (
                    str(row.youtube_video_id or ""),
                    str(row.youtube_playlist_id or ""),
                    str(row.youtube_url or ""),
                )
                after = (
                    values["youtube_video_id"],
                    values["youtube_playlist_id"],
                    values["youtube_url"],
                )
                if before == after:
                    result["unchanged"] += 1
                    continue
                conn.execute(
                    update(study_plan_videos_table)
                    .where(study_plan_videos_table.c.id == int(row.id))
                    .values(**values, updated_at=now)
                )
                result["updated"] += 1
        return result

    def list_study_plan_activity_events(
        self,
        *,
        day: Optional[str] = None,
        start_day: Optional[str] = None,
        end_day: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if day:
            start_day = day
            end_day = day
        if not start_day and not end_day:
            return []
        stmt = (
            select(
                study_plan_activity_events_table.c.id,
                study_plan_activity_events_table.c.video_id,
                study_plan_activity_events_table.c.previous_watched_seconds,
                study_plan_activity_events_table.c.watched_seconds,
                study_plan_activity_events_table.c.delta_seconds,
                study_plan_activity_events_table.c.updated_at,
                study_plan_videos_table.c.subject,
                study_plan_videos_table.c.sequence,
                study_plan_videos_table.c.title,
                study_plan_videos_table.c.duration_seconds,
            )
            .select_from(
                study_plan_activity_events_table.join(
                    study_plan_videos_table,
                    study_plan_activity_events_table.c.video_id == study_plan_videos_table.c.id,
                )
            )
        )
        if start_day:
            stmt = stmt.where(study_plan_activity_events_table.c.day >= start_day)
        if end_day:
            stmt = stmt.where(study_plan_activity_events_table.c.day <= end_day)
        stmt = stmt.order_by(
            study_plan_activity_events_table.c.day,
            study_plan_activity_events_table.c.updated_at,
            study_plan_activity_events_table.c.id,
        )
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        by_video: Dict[tuple[str, int], Dict[str, Any]] = {}
        for row in rows:
            video_id = int(row.video_id)
            activity_day = self._study_plan_business_day_from_timestamp(str(row.updated_at or ""))
            if start_day and activity_day < start_day:
                continue
            if end_day and activity_day > end_day:
                continue
            activity_key = (activity_day, video_id)
            existing = by_video.get(activity_key)
            if existing is None:
                by_video[activity_key] = {
                    "day": activity_day,
                    "video_id": video_id,
                    "subject": row.subject,
                    "sequence": int(row.sequence or 0),
                    "title": row.title,
                    "duration_seconds": max(0.0, float(row.duration_seconds or 0)),
                    "previous_watched_seconds": max(0.0, float(row.previous_watched_seconds or 0)),
                    "watched_seconds": max(0.0, float(row.watched_seconds or 0)),
                    "delta_seconds": float(row.delta_seconds or 0),
                    "updated_at": row.updated_at or "",
                }
            else:
                existing["watched_seconds"] = max(0.0, float(row.watched_seconds or 0))
                # Keep each video's final position for the day, including a backward seek.
                existing["delta_seconds"] = float(existing["watched_seconds"]) - float(existing["previous_watched_seconds"])
                existing["updated_at"] = row.updated_at or existing["updated_at"]

        return sorted(
            by_video.values(),
            key=lambda item: (str(item["day"]), str(item["updated_at"]), int(item["sequence"])),
        )

    def move_study_plan_activity_between_days(
        self,
        *,
        video_id: int,
        source_day: str,
        target_day: str,
        seconds: float,
        expected_source_seconds: float,
        expected_target_seconds: float,
    ) -> Optional[Dict[str, Any]]:
        """Reattribute new-progress activity without changing the video's final position."""
        try:
            normalized_video_id = int(video_id)
            moved_seconds = float(seconds)
        except (TypeError, ValueError):
            return None
        if normalized_video_id <= 0 or moved_seconds <= 0 or target_day == source_day:
            return None

        def grouped_credit(rows: List[Any]) -> Dict[str, Dict[str, Any]]:
            grouped: Dict[str, Dict[str, Any]] = {}
            for row in rows:
                day_value = self._study_plan_business_day_from_timestamp(str(row.updated_at or ""))
                entry = grouped.get(day_value)
                if entry is None:
                    grouped[day_value] = {
                        "previous": max(0.0, float(row.previous_watched_seconds or 0)),
                        "watched": max(0.0, float(row.watched_seconds or 0)),
                        "row_ids": [int(row.id)],
                    }
                else:
                    entry["watched"] = max(0.0, float(row.watched_seconds or 0))
                    entry["row_ids"].append(int(row.id))
            high_water = 0.0
            for day_value in sorted(grouped):
                entry = grouped[day_value]
                baseline = max(high_water, float(entry["previous"]))
                entry["credited"] = max(0.0, float(entry["watched"]) - baseline)
                entry["baseline"] = baseline
                high_water = max(high_water, float(entry["previous"]), float(entry["watched"]))
            return grouped

        with self._lock, self._engine.begin() as conn:
            rows = conn.execute(
                select(study_plan_activity_events_table)
                .where(study_plan_activity_events_table.c.video_id == normalized_video_id)
                .order_by(
                    study_plan_activity_events_table.c.day,
                    study_plan_activity_events_table.c.updated_at,
                    study_plan_activity_events_table.c.id,
                )
            ).fetchall()
            grouped = grouped_credit(list(rows))
            source = grouped.get(str(source_day or ""))
            target = grouped.get(str(target_day or ""), {"credited": 0.0})
            source_credited = float((source or {}).get("credited") or 0)
            target_credited = float(target.get("credited") or 0)
            if (
                not source
                or abs(source_credited - float(expected_source_seconds or 0)) > 0.5
                or abs(target_credited - float(expected_target_seconds or 0)) > 0.5
                or source_credited + 0.5 < moved_seconds
            ):
                return {"stale": True}

            range_start = min(str(source_day), str(target_day))
            range_end = max(str(source_day), str(target_day))
            affected_days = sorted({
                str(day_value)
                for day_value in grouped
                if range_start <= str(day_value) <= range_end
            } | {str(source_day), str(target_day)})
            affected_ids = {
                int(row.id)
                for row in rows
                if range_start <= self._study_plan_business_day_from_timestamp(
                    str(row.updated_at or "")
                ) <= range_end
            }
            original_rows = [{
                "id": int(row.id),
                "day": str(row.day or ""),
                "video_id": int(row.video_id),
                "previous_watched_seconds": float(row.previous_watched_seconds or 0),
                "watched_seconds": float(row.watched_seconds or 0),
                "delta_seconds": float(row.delta_seconds or 0),
                "updated_at": str(row.updated_at or ""),
            } for row in rows if int(row.id) in affected_ids]
            conn.execute(
                delete(study_plan_activity_events_table).where(
                    study_plan_activity_events_table.c.id.in_(affected_ids)
                )
            )

            generated_ids = []
            credited_by_day = {
                day_value: float((grouped.get(day_value) or {}).get("credited") or 0)
                for day_value in affected_days
            }
            credited_by_day[str(source_day)] = max(
                0.0, credited_by_day.get(str(source_day), 0.0) - moved_seconds
            )
            credited_by_day[str(target_day)] = (
                credited_by_day.get(str(target_day), 0.0) + moved_seconds
            )

            running_position = 0.0
            for day_value in sorted(grouped):
                if day_value >= range_start:
                    break
                entry = grouped[day_value]
                running_position = max(
                    running_position,
                    float(entry.get("previous") or 0),
                    float(entry.get("watched") or 0),
                )
            first_existing_day = next(
                (day_value for day_value in affected_days if day_value in grouped),
                None,
            )
            if first_existing_day:
                running_position = max(
                    running_position,
                    float(grouped[first_existing_day].get("baseline") or 0),
                )

            timestamps_by_day: Dict[str, str] = {}
            for row in rows:
                row_day = self._study_plan_business_day_from_timestamp(str(row.updated_at or ""))
                if row_day not in affected_days:
                    continue
                timestamp = str(row.updated_at or "")
                if timestamp and (
                    row_day not in timestamps_by_day or timestamp < timestamps_by_day[row_day]
                ):
                    timestamps_by_day[row_day] = timestamp
            for day_value in affected_days:
                day_credit = max(0.0, float(credited_by_day.get(day_value) or 0))
                if day_credit <= 0.01:
                    continue
                next_position = running_position + day_credit
                inserted = conn.execute(insert(study_plan_activity_events_table).values(
                    day=day_value,
                    video_id=normalized_video_id,
                    previous_watched_seconds=running_position,
                    watched_seconds=next_position,
                    delta_seconds=day_credit,
                    updated_at=timestamps_by_day.get(
                        day_value, f"{day_value}T12:00:00.000000"
                    ),
                ))
                generated_ids.append(int(inserted.inserted_primary_key[0]))
                running_position = next_position
        return {
            "stale": False,
            "video_id": normalized_video_id,
            "original_rows": original_rows,
            "generated_ids": generated_ids,
            "before_source_seconds": source_credited,
            "before_target_seconds": target_credited,
            "source_seconds": source_credited - moved_seconds,
            "target_seconds": target_credited + moved_seconds,
        }

    def undo_move_study_plan_activity_between_days(
        self,
        *,
        original_rows: List[Dict[str, Any]],
        generated_ids: List[int],
    ) -> bool:
        try:
            normalized_ids = [int(value) for value in generated_ids]
        except (TypeError, ValueError):
            return False
        if not original_rows or not normalized_ids:
            return False
        with self._lock, self._engine.begin() as conn:
            existing = conn.execute(
                select(study_plan_activity_events_table.c.id).where(
                    study_plan_activity_events_table.c.id.in_(normalized_ids)
                )
            ).fetchall()
            if {int(row.id) for row in existing} != set(normalized_ids):
                return False
            conn.execute(
                delete(study_plan_activity_events_table).where(
                    study_plan_activity_events_table.c.id.in_(normalized_ids)
                )
            )
            for row in original_rows:
                conn.execute(insert(study_plan_activity_events_table).values(
                    id=int(row.get("id") or 0),
                    day=str(row.get("day") or ""),
                    video_id=int(row.get("video_id") or 0),
                    previous_watched_seconds=float(row.get("previous_watched_seconds") or 0),
                    watched_seconds=float(row.get("watched_seconds") or 0),
                    delta_seconds=float(row.get("delta_seconds") or 0),
                    updated_at=str(row.get("updated_at") or ""),
                ))
        return True

    def list_study_plan_video_markers(
        self,
        *,
        video_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        stmt = select(
            study_plan_video_markers_table.c.id,
            study_plan_video_markers_table.c.video_id,
            study_plan_video_markers_table.c.playback_seconds,
            study_plan_video_markers_table.c.note,
            study_plan_video_markers_table.c.summary,
            study_plan_video_markers_table.c.summary_status,
            study_plan_video_markers_table.c.summary_generated_at,
            study_plan_video_markers_table.c.created_at,
            study_plan_video_markers_table.c.updated_at,
        )
        normalized_ids = sorted({int(video_id) for video_id in (video_ids or []) if int(video_id) > 0})
        if video_ids is not None:
            if not normalized_ids:
                return []
            stmt = stmt.where(study_plan_video_markers_table.c.video_id.in_(normalized_ids))
        stmt = stmt.order_by(
            study_plan_video_markers_table.c.video_id,
            study_plan_video_markers_table.c.playback_seconds,
            study_plan_video_markers_table.c.id,
        )
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
        return [
            {
                "id": int(row.id),
                "video_id": int(row.video_id),
                "playback_seconds": max(0.0, float(row.playback_seconds or 0)),
                "note": str(row.note or ""),
                "summary": str(row.summary or ""),
                "summary_status": str(row.summary_status or ""),
                "summary_generated_at": str(row.summary_generated_at or ""),
                "created_at": str(row.created_at or ""),
                "updated_at": str(row.updated_at or ""),
            }
            for row in rows
        ]

    def create_study_plan_video_marker(
        self,
        *,
        video_id: int,
        playback_seconds: float,
        note: str,
    ) -> Optional[Dict[str, Any]]:
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            video = conn.execute(
                select(study_plan_videos_table.c.duration_seconds).where(
                    study_plan_videos_table.c.id == int(video_id)
                )
            ).fetchone()
            if not video:
                return None
            raw_seconds = float(playback_seconds or 0)
            if not math.isfinite(raw_seconds):
                return None
            normalized_seconds = max(0.0, min(raw_seconds, float(video.duration_seconds or 0)))
            normalized_note = str(note or "").strip()[:280] or "關鍵片段"
            result = conn.execute(
                insert(study_plan_video_markers_table).values(
                    video_id=int(video_id),
                    playback_seconds=normalized_seconds,
                    note=normalized_note,
                    summary="",
                    summary_status="",
                    summary_generated_at=None,
                    created_at=now,
                    updated_at=now,
                )
            )
            marker_id = int(result.inserted_primary_key[0])
        return {
            "id": marker_id,
            "video_id": int(video_id),
            "playback_seconds": normalized_seconds,
            "note": normalized_note,
            "summary": "",
            "summary_status": "",
            "summary_generated_at": "",
            "created_at": now,
            "updated_at": now,
        }

    def delete_study_plan_video_marker(self, marker_id: int) -> bool:
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(
                delete(study_plan_video_markers_table).where(
                    study_plan_video_markers_table.c.id == int(marker_id)
                )
            )
        return bool(result.rowcount)

    def update_study_plan_video_marker(self, marker_id: int, *, note: str) -> Optional[Dict[str, Any]]:
        normalized_note = str(note or "").strip()[:280] or "關鍵片段"
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(
                update(study_plan_video_markers_table)
                .where(study_plan_video_markers_table.c.id == int(marker_id))
                .values(
                    note=normalized_note,
                    summary="",
                    summary_status="pending",
                    summary_generated_at=None,
                    updated_at=now,
                )
            )
            if not result.rowcount:
                return None
            row = conn.execute(
                select(
                    study_plan_video_markers_table.c.id,
                    study_plan_video_markers_table.c.video_id,
                    study_plan_video_markers_table.c.playback_seconds,
                    study_plan_video_markers_table.c.note,
                    study_plan_video_markers_table.c.summary,
                    study_plan_video_markers_table.c.summary_status,
                    study_plan_video_markers_table.c.summary_generated_at,
                    study_plan_video_markers_table.c.created_at,
                    study_plan_video_markers_table.c.updated_at,
                ).where(study_plan_video_markers_table.c.id == int(marker_id))
            ).fetchone()
        if not row:
            return None
        return {
            "id": int(row.id),
            "video_id": int(row.video_id),
            "playback_seconds": max(0.0, float(row.playback_seconds or 0)),
            "note": str(row.note or ""),
            "summary": str(row.summary or ""),
            "summary_status": str(row.summary_status or ""),
            "summary_generated_at": str(row.summary_generated_at or ""),
            "created_at": str(row.created_at or ""),
            "updated_at": str(row.updated_at or ""),
        }

    def get_study_plan_video_marker(self, marker_id: int) -> Optional[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                select(
                    study_plan_video_markers_table.c.id,
                    study_plan_video_markers_table.c.video_id,
                    study_plan_video_markers_table.c.playback_seconds,
                    study_plan_video_markers_table.c.note,
                    study_plan_video_markers_table.c.summary,
                    study_plan_video_markers_table.c.summary_status,
                    study_plan_video_markers_table.c.summary_generated_at,
                    study_plan_video_markers_table.c.created_at,
                    study_plan_video_markers_table.c.updated_at,
                ).where(study_plan_video_markers_table.c.id == int(marker_id))
            ).fetchone()
        if not row:
            return None
        return {
            "id": int(row.id),
            "video_id": int(row.video_id),
            "playback_seconds": max(0.0, float(row.playback_seconds or 0)),
            "note": str(row.note or ""),
            "summary": str(row.summary or ""),
            "summary_status": str(row.summary_status or ""),
            "summary_generated_at": str(row.summary_generated_at or ""),
            "created_at": str(row.created_at or ""),
            "updated_at": str(row.updated_at or ""),
        }

    def update_study_plan_video_marker_summary(
        self,
        marker_id: int,
        *,
        summary: str,
        status: str,
    ) -> Optional[Dict[str, Any]]:
        normalized_status = str(status or "").strip()[:16]
        normalized_summary = str(summary or "").strip()
        now = self._now_iso()
        generated_at = now if normalized_status == "ready" else None
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(
                update(study_plan_video_markers_table)
                .where(study_plan_video_markers_table.c.id == int(marker_id))
                .values(
                    summary=normalized_summary,
                    summary_status=normalized_status,
                    summary_generated_at=generated_at,
                    updated_at=now,
                )
            )
        if not result.rowcount:
            return None
        return self.get_study_plan_video_marker(marker_id)

    def get_study_plan_replan_settings(self) -> Optional[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                select(study_plan_replan_settings_table).where(
                    study_plan_replan_settings_table.c.id == 1
                )
            ).fetchone()
        if not row:
            return None

        def _mapping(value: Any) -> Dict[str, float]:
            try:
                payload = json.loads(str(value or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                return {}
            if not isinstance(payload, dict):
                return {}
            result: Dict[str, float] = {}
            for key, amount in payload.items():
                try:
                    normalized = max(0.0, float(amount or 0))
                except (TypeError, ValueError):
                    continue
                if math.isfinite(normalized):
                    result[str(key)] = normalized
            return result

        return {
            "start_date": str(row.start_date),
            "end_date": str(row.end_date),
            "weekday_minutes": max(0.0, float(row.weekday_minutes or 0)),
            "weekend_minutes": max(0.0, float(row.weekend_minutes or 0)),
            "baseline_by_subject": _mapping(row.baseline_by_subject),
            "subject_targets": _mapping(row.subject_targets),
            "created_at": str(row.created_at or ""),
            "updated_at": str(row.updated_at or ""),
        }

    def save_study_plan_replan_settings(
        self,
        *,
        start_date: str,
        end_date: str,
        weekday_minutes: float,
        weekend_minutes: float,
        baseline_by_subject: Dict[str, float],
        subject_targets: Dict[str, float],
    ) -> None:
        now = self._now_iso()
        values = {
            "start_date": str(start_date),
            "end_date": str(end_date),
            "weekday_minutes": max(0.0, float(weekday_minutes or 0)),
            "weekend_minutes": max(0.0, float(weekend_minutes or 0)),
            "baseline_by_subject": json.dumps(baseline_by_subject, ensure_ascii=False, sort_keys=True),
            "subject_targets": json.dumps(subject_targets, ensure_ascii=False, sort_keys=True),
            "updated_at": now,
        }
        with self._lock, self._engine.begin() as conn:
            existing = conn.execute(
                select(study_plan_replan_settings_table.c.id).where(
                    study_plan_replan_settings_table.c.id == 1
                )
            ).fetchone()
            if existing:
                conn.execute(
                    update(study_plan_replan_settings_table)
                    .where(study_plan_replan_settings_table.c.id == 1)
                    .values(**values)
                )
            else:
                conn.execute(
                    insert(study_plan_replan_settings_table).values(
                        id=1,
                        created_at=now,
                        **values,
                    )
                )

    def delete_study_plan_replan_settings(self) -> bool:
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(
                delete(study_plan_replan_settings_table).where(
                    study_plan_replan_settings_table.c.id == 1
                )
            )
        return bool(result.rowcount)

    def list_study_plan_rest_days(self) -> List[str]:
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                select(study_plan_rest_days_table.c.study_date).order_by(
                    study_plan_rest_days_table.c.study_date.asc()
                )
            ).fetchall()
        return [str(row.study_date) for row in rows]

    def add_study_plan_rest_day(self, study_date: str) -> bool:
        try:
            normalized = datetime.strptime(str(study_date or ""), "%Y-%m-%d").date().isoformat()
        except (TypeError, ValueError):
            return False
        try:
            with self._lock, self._engine.begin() as conn:
                existing = conn.execute(
                    select(study_plan_rest_days_table.c.study_date).where(
                        study_plan_rest_days_table.c.study_date == normalized
                    )
                ).fetchone()
                if existing:
                    return False
                conn.execute(
                    insert(study_plan_rest_days_table).values(
                        study_date=normalized,
                        created_at=self._now_iso(),
                    )
                )
        except IntegrityError:
            return False
        return True

    def delete_study_plan_rest_day(self, study_date: str) -> bool:
        try:
            normalized = datetime.strptime(str(study_date or ""), "%Y-%m-%d").date().isoformat()
        except (TypeError, ValueError):
            return False
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(
                delete(study_plan_rest_days_table).where(
                    study_plan_rest_days_table.c.study_date == normalized
                )
            )
        return bool(result.rowcount)
