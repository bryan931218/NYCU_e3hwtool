"""Persistence operations for study time."""
import math
from typing import Any, Dict, List, Optional
from sqlalchemy import delete, insert, func, select, update

from .schema import study_plan_videos_table, study_time_sessions_table


class StudyTimeStorage:
    def record_study_time_session(
        self,
        *,
        session_key: str,
        kind: str,
        elapsed_seconds: float,
        video_id: Optional[int] = None,
        label: str = "",
        completed: bool = False,
    ) -> Optional[Dict[str, Any]]:
        key = str(session_key or "").strip()[:80]
        session_kind = str(kind or "").strip().lower()
        elapsed = max(0.0, min(float(elapsed_seconds or 0), 24 * 60 * 60))
        if not key or session_kind not in {"video", "practice"} or not math.isfinite(elapsed):
            return None
        now = self._now_iso()
        day = self._study_plan_business_day_from_timestamp(now)
        with self._lock, self._engine.begin() as conn:
            resolved_video_id: Optional[int] = None
            resolved_label = str(label or "").strip()[:191]
            if session_kind == "video":
                resolved_video_id = int(video_id or 0)
                video = conn.execute(
                    select(study_plan_videos_table.c.title).where(
                        study_plan_videos_table.c.id == resolved_video_id
                    )
                ).fetchone()
                if not video:
                    return None
                resolved_label = str(video.title or "影片學習")[:191]
            elif not resolved_label:
                resolved_label = "刷題"

            existing = conn.execute(
                select(
                    study_time_sessions_table.c.kind,
                    study_time_sessions_table.c.elapsed_seconds,
                    study_time_sessions_table.c.completed,
                ).where(study_time_sessions_table.c.session_key == key)
            ).fetchone()
            if existing and str(existing.kind) != session_kind:
                return None
            values = {
                "day": day,
                "kind": session_kind,
                "video_id": resolved_video_id,
                "label": resolved_label,
                "elapsed_seconds": max(elapsed, float(existing.elapsed_seconds or 0)) if existing else elapsed,
                "completed": 1 if completed or (existing and existing.completed) else 0,
                "updated_at": now,
            }
            if existing:
                conn.execute(
                    update(study_time_sessions_table)
                    .where(study_time_sessions_table.c.session_key == key)
                    .values(**values)
                )
            else:
                conn.execute(
                    insert(study_time_sessions_table).values(
                        session_key=key,
                        started_at=now,
                        **values,
                    )
                )
        return self.get_study_time_summary(day=day)

    def get_study_time_summary(self, *, day: str) -> Dict[str, Any]:
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                select(
                    study_time_sessions_table.c.kind,
                    study_time_sessions_table.c.elapsed_seconds,
                ).where(study_time_sessions_table.c.day == str(day or ""))
            ).fetchall()
        video_seconds = sum(
            max(0.0, float(row.elapsed_seconds or 0)) for row in rows if row.kind == "video"
        )
        practice_seconds = sum(
            max(0.0, float(row.elapsed_seconds or 0)) for row in rows if row.kind == "practice"
        )
        total_seconds = video_seconds + practice_seconds
        return {
            "day": str(day or ""),
            "video_seconds": video_seconds,
            "practice_seconds": practice_seconds,
            "total_seconds": total_seconds,
            "session_count": len(rows),
        }

    def list_study_time_daily_totals(
        self,
        *,
        start_day: str,
        end_day: str,
    ) -> List[Dict[str, Any]]:
        stmt = (
            select(
                study_time_sessions_table.c.day,
                func.sum(study_time_sessions_table.c.elapsed_seconds).label("total_seconds"),
                func.count().label("session_count"),
            )
            .where(study_time_sessions_table.c.day >= str(start_day or ""))
            .where(study_time_sessions_table.c.day <= str(end_day or ""))
            .group_by(study_time_sessions_table.c.day)
            .order_by(study_time_sessions_table.c.day)
        )
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
        return [
            {
                "date": str(row.day or ""),
                "total_seconds": max(0.0, float(row.total_seconds or 0)),
                "session_count": max(0, int(row.session_count or 0)),
            }
            for row in rows
        ]

    def list_study_time_sessions(self, *, day: str, limit: int = 20) -> List[Dict[str, Any]]:
        stmt = (
            select(
                study_time_sessions_table.c.session_key,
                study_time_sessions_table.c.kind,
                study_time_sessions_table.c.video_id,
                study_time_sessions_table.c.label,
                study_time_sessions_table.c.elapsed_seconds,
                study_time_sessions_table.c.completed,
                study_time_sessions_table.c.started_at,
                study_time_sessions_table.c.updated_at,
                study_plan_videos_table.c.subject,
                study_plan_videos_table.c.sequence,
            )
            .select_from(
                study_time_sessions_table.outerjoin(
                    study_plan_videos_table,
                    study_time_sessions_table.c.video_id == study_plan_videos_table.c.id,
                )
            )
            .where(study_time_sessions_table.c.day == str(day or ""))
            .order_by(study_time_sessions_table.c.updated_at.desc())
            .limit(max(1, min(int(limit or 20), 100)))
        )
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
        return [
            {
                "session_id": str(row.session_key),
                "kind": str(row.kind),
                "video_id": int(row.video_id) if row.video_id is not None else None,
                "label": str(row.label or ""),
                "elapsed_seconds": max(0.0, float(row.elapsed_seconds or 0)),
                "completed": bool(row.completed),
                "started_at": str(row.started_at or ""),
                "updated_at": str(row.updated_at or ""),
                "subject": str(row.subject or ""),
                "sequence": int(row.sequence or 0),
            }
            for row in rows
        ]

    def delete_study_time_session(self, session_key: str) -> bool:
        key = str(session_key or "").strip()[:80]
        if not key:
            return False
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(
                delete(study_time_sessions_table).where(
                    study_time_sessions_table.c.session_key == key
                )
            )
        return bool(result.rowcount)

    def get_study_time_session(self, session_key: str) -> Optional[Dict[str, Any]]:
        key = str(session_key or "").strip()[:80]
        if not key:
            return None
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                select(study_time_sessions_table).where(
                    study_time_sessions_table.c.session_key == key
                )
            ).fetchone()
        if not row:
            return None
        return {
            "session_id": str(row.session_key),
            "day": str(row.day),
            "kind": str(row.kind),
            "video_id": int(row.video_id) if row.video_id is not None else None,
            "label": str(row.label or ""),
            "elapsed_seconds": max(0.0, float(row.elapsed_seconds or 0)),
            "completed": bool(row.completed),
            "started_at": str(row.started_at or ""),
            "updated_at": str(row.updated_at or ""),
        }

    def update_study_time_session_elapsed(
        self,
        *,
        session_key: str,
        elapsed_seconds: float,
        expected_updated_at: str,
    ) -> Optional[Dict[str, Any]]:
        key = str(session_key or "").strip()[:80]
        try:
            elapsed = float(elapsed_seconds)
        except (TypeError, ValueError):
            return None
        if not key or not math.isfinite(elapsed) or elapsed < 0 or elapsed > 24 * 60 * 60:
            return None
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            row = conn.execute(
                select(study_time_sessions_table).where(
                    study_time_sessions_table.c.session_key == key
                )
            ).fetchone()
            if not row:
                return None
            current_updated_at = str(row.updated_at or "")
            if current_updated_at != str(expected_updated_at or ""):
                return {
                    "stale": True,
                    "session_id": key,
                    "elapsed_seconds": max(0.0, float(row.elapsed_seconds or 0)),
                    "updated_at": current_updated_at,
                }
            conn.execute(
                update(study_time_sessions_table)
                .where(study_time_sessions_table.c.session_key == key)
                .values(elapsed_seconds=elapsed, updated_at=now)
            )
        return {
            "stale": False,
            "session_id": key,
            "elapsed_seconds": elapsed,
            "updated_at": now,
        }

    def move_study_time_between_days(
        self,
        *,
        moves: List[Dict[str, Any]],
        source_day: str,
        target_day: str,
        target_session_key: str,
    ) -> Optional[Dict[str, Any]]:
        """Atomically move selected session seconds to another business day."""
        key = str(target_session_key or "").strip()[:80]
        try:
            normalized_moves = [
                {
                    "session_id": str(item.get("session_id") or "").strip()[:80],
                    "seconds": float(item.get("seconds") or 0),
                    "expected_updated_at": str(item.get("expected_updated_at") or ""),
                }
                for item in moves
            ]
        except (TypeError, ValueError):
            return None
        if (
            not key
            or not normalized_moves
            or source_day == target_day
            or not all(item["session_id"] and math.isfinite(item["seconds"]) and item["seconds"] > 0 for item in normalized_moves)
        ):
            return None
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            if conn.execute(
                select(study_time_sessions_table.c.session_key).where(
                    study_time_sessions_table.c.session_key == key
                )
            ).fetchone():
                return {"stale": True}
            source_rows = []
            for move in normalized_moves:
                row = conn.execute(
                    select(study_time_sessions_table).where(
                        study_time_sessions_table.c.session_key == move["session_id"]
                    )
                ).fetchone()
                if (
                    not row
                    or str(row.day or "") != str(source_day or "")
                    or str(row.updated_at or "") != move["expected_updated_at"]
                    or float(row.elapsed_seconds or 0) + 0.05 < move["seconds"]
                ):
                    return {"stale": True}
                source_rows.append((row, move))
            video_ids = {int(row.video_id) if row.video_id is not None else None for row, _ in source_rows}
            kinds = {str(row.kind or "") for row, _ in source_rows}
            if len(video_ids) != 1 or len(kinds) != 1:
                return None
            updated_sources = []
            for row, move in source_rows:
                remaining = max(0.0, float(row.elapsed_seconds or 0) - move["seconds"])
                conn.execute(
                    update(study_time_sessions_table)
                    .where(study_time_sessions_table.c.session_key == move["session_id"])
                    .values(elapsed_seconds=remaining, updated_at=now)
                )
                updated_sources.append({
                    "session_id": move["session_id"],
                    "elapsed_seconds": remaining,
                    "updated_at": now,
                })
            first = source_rows[0][0]
            moved_seconds = sum(move["seconds"] for _, move in source_rows)
            conn.execute(insert(study_time_sessions_table).values(
                session_key=key,
                day=str(target_day or ""),
                kind=str(first.kind or ""),
                video_id=int(first.video_id) if first.video_id is not None else None,
                label=str(first.label or "")[:191],
                elapsed_seconds=moved_seconds,
                completed=0,
                started_at=now,
                updated_at=now,
            ))
        return {
            "stale": False,
            "source_sessions": updated_sources,
            "target_session": {
                "session_id": key,
                "day": str(target_day or ""),
                "elapsed_seconds": moved_seconds,
                "updated_at": now,
            },
        }

    def undo_move_study_time_between_days(
        self,
        *,
        source_sessions: List[Dict[str, Any]],
        target_session_key: str,
        expected_target_updated_at: str,
    ) -> bool:
        """Atomically undo a prior move when none of its rows has changed since."""
        key = str(target_session_key or "").strip()[:80]
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            target = conn.execute(
                select(study_time_sessions_table).where(
                    study_time_sessions_table.c.session_key == key
                )
            ).fetchone()
            if not target or str(target.updated_at or "") != str(expected_target_updated_at or ""):
                return False
            current_sources = []
            for source in source_sessions:
                session_id = str(source.get("session_id") or "").strip()[:80]
                row = conn.execute(
                    select(study_time_sessions_table).where(
                        study_time_sessions_table.c.session_key == session_id
                    )
                ).fetchone()
                if not row or str(row.updated_at or "") != str(source.get("expected_updated_at") or ""):
                    return False
                current_sources.append((row, source))
            for row, source in current_sources:
                conn.execute(
                    update(study_time_sessions_table)
                    .where(study_time_sessions_table.c.session_key == str(source.get("session_id") or ""))
                    .values(elapsed_seconds=float(source.get("restore_seconds") or 0), updated_at=now)
                )
            conn.execute(
                delete(study_time_sessions_table).where(
                    study_time_sessions_table.c.session_key == key
                )
            )
        return True
