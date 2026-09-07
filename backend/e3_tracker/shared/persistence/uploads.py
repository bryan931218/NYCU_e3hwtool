"""Persistence operations for uploads."""
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from sqlalchemy import insert, select, update

from .schema import study_note_upload_jobs_table


class UploadsStorage:
    def save_study_note_upload_job(
        self,
        *,
        job_id: str,
        username: str,
        status: str,
        progress: int,
        message: str,
        created_at: float,
        updated_at: float,
        session_id: Optional[int] = None,
    ) -> None:
        values = {
            "username": str(username or "")[:191],
            "status": str(status or "running")[:24],
            "progress": max(0, min(int(progress or 0), 100)),
            "message": str(message or "正在處理筆記。")[:1000],
            "session_id": int(session_id) if session_id is not None else None,
            "created_at": float(created_at),
            "updated_at": float(updated_at),
        }
        with self._lock, self._engine.begin() as conn:
            existing = conn.execute(
                select(study_note_upload_jobs_table.c.job_id).where(
                    study_note_upload_jobs_table.c.job_id == job_id
                )
            ).first()
            if existing:
                conn.execute(
                    update(study_note_upload_jobs_table)
                    .where(study_note_upload_jobs_table.c.job_id == job_id)
                    .values(**values)
                )
            else:
                conn.execute(
                    insert(study_note_upload_jobs_table).values(
                        job_id=str(job_id or "")[:96],
                        **values,
                    )
                )

    def get_study_note_upload_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._engine.connect() as conn:
            row = conn.execute(
                select(study_note_upload_jobs_table).where(
                    study_note_upload_jobs_table.c.job_id == job_id
                )
            ).first()
        return dict(row._mapping) if row else None

    def get_current_study_note_upload_job(
        self,
        username: str,
        *,
        terminal_window_seconds: int = 600,
    ) -> Optional[Dict[str, Any]]:
        if not username:
            return None
        now = datetime.now(timezone.utc).timestamp()
        with self._engine.connect() as conn:
            running = conn.execute(
                select(study_note_upload_jobs_table)
                .where(
                    study_note_upload_jobs_table.c.username == username,
                    study_note_upload_jobs_table.c.status == "running",
                    study_note_upload_jobs_table.c.updated_at >= now - 24 * 60 * 60,
                )
                .order_by(study_note_upload_jobs_table.c.updated_at.desc())
                .limit(1)
            ).first()
            if running:
                return dict(running._mapping)
            cutoff = now - max(0, int(terminal_window_seconds))
            recent = conn.execute(
                select(study_note_upload_jobs_table)
                .where(
                    study_note_upload_jobs_table.c.username == username,
                    study_note_upload_jobs_table.c.updated_at >= cutoff,
                )
                .order_by(study_note_upload_jobs_table.c.updated_at.desc())
                .limit(1)
            ).first()
        return dict(recent._mapping) if recent else None
