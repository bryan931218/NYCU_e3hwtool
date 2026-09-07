"""Public storage facade; domain operations live in shared.persistence."""
import json
import math
import os
import re
import threading
import unicodedata
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from fsrs import Card as FSRSCard
from fsrs import Rating as FSRSRating
from fsrs import Scheduler as FSRSScheduler
from sqlalchemy import (
    Column,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    delete,
    inspect,
    insert,
    func,
    select,
    text,
    update,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from .source_localization import canonicalize_source_text, literal_source_evidence
from .persistence.schema import (
    metadata,
    users_table,
    user_preferences_table,
    courses_table,
    assignments_table,
    assignment_views_table,
    user_fetch_state_table,
    fetch_errors_table,
    google_tokens_table,
    web_sessions_table,
    announcements_table,
    announcement_votes_table,
    feedback_table,
    traffic_state_table,
    traffic_events_table,
    study_plan_videos_table,
    study_plan_video_overrides_table,
    youtube_storyboard_metadata_table,
    study_plan_video_records_table,
    study_plan_daily_snapshots_table,
    study_plan_activity_events_table,
    study_time_sessions_table,
    study_plan_video_markers_table,
    study_plan_replan_settings_table,
    study_plan_rest_days_table,
    study_assistant_actions_table,
    study_recall_sessions_table,
    study_recall_glossaries_table,
    study_note_upload_jobs_table,
    study_recall_attempts_table,
    study_recall_card_reviews_table,
)
from .persistence.recall_support import (
    _recall_search_compact,
    _recall_search_query_core,
    _recall_search_bigrams,
    _recall_search_similarity,
    _recall_search_formula_signature,
    _recall_search_formula_similarity,
    _recall_search_contains_formula,
    _recall_search_excerpt,
    _recall_search_resolved_page,
    RECALL_DAILY_CAPACITY,
    RECALL_FSRS_SCHEDULER,
    _RECALL_SEARCH_QUESTION_PHRASES,
)
from .persistence.assignments import AssignmentsStorage
from .persistence.videos import VideosStorage
from .persistence.study_time import StudyTimeStorage
from .persistence.recall import RecallStorage
from .persistence.uploads import UploadsStorage
from .persistence.assistant import AssistantStorage
from .persistence.community import CommunityStorage
from .persistence.accounts import AccountsStorage


class PersistentStorage(AssignmentsStorage, VideosStorage, StudyTimeStorage, RecallStorage, UploadsStorage, AssistantStorage, CommunityStorage, AccountsStorage):
    """Database-backed persistence with normalized storage."""

    def __init__(self, database_url: str) -> None:
        if not database_url:
            raise ValueError("Database URL is required (set E3_DATABASE_URL).")
        normalized = self._normalize_url(database_url)
        self._engine: Engine = create_engine(
            normalized,
            future=True,
            pool_pre_ping=True,
        )
        self._lock = threading.Lock()
        self._recall_search_cache_lock = threading.Lock()
        self._recall_search_cache_signature: tuple[tuple[int, str], ...] = ()
        self._recall_search_cache_documents: List[Dict[str, Any]] = []
        metadata.create_all(self._engine)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        inspector = inspect(self._engine)
        if inspector.has_table("user_preferences"):
            pref_columns = {col["name"] for col in inspector.get_columns("user_preferences")}
            if "status_filter" not in pref_columns:
                with self._lock, self._engine.begin() as conn:
                    conn.execute(text("ALTER TABLE user_preferences ADD COLUMN status_filter TEXT"))
            if "include_ignored_overdue" not in pref_columns:
                with self._lock, self._engine.begin() as conn:
                    conn.execute(text("ALTER TABLE user_preferences ADD COLUMN include_ignored_overdue INTEGER"))
            if "show_graded" not in pref_columns:
                with self._lock, self._engine.begin() as conn:
                    conn.execute(text("ALTER TABLE user_preferences ADD COLUMN show_graded INTEGER"))
            if "ignored_overdue_uids" not in pref_columns:
                with self._lock, self._engine.begin() as conn:
                    conn.execute(text("ALTER TABLE user_preferences ADD COLUMN ignored_overdue_uids TEXT"))
            if "semester_filter" not in pref_columns:
                with self._lock, self._engine.begin() as conn:
                    conn.execute(text("ALTER TABLE user_preferences ADD COLUMN semester_filter TEXT"))
        if inspector.has_table("user_fetch_state"):
            fetch_state_columns = {col["name"] for col in inspector.get_columns("user_fetch_state")}
            missing_fetch_state_columns = []
            if "semester_catalog" not in fetch_state_columns:
                missing_fetch_state_columns.append(("semester_catalog", "TEXT"))
            if "selected_semesters" not in fetch_state_columns:
                missing_fetch_state_columns.append(("selected_semesters", "TEXT"))
            if missing_fetch_state_columns:
                with self._lock, self._engine.begin() as conn:
                    for column_name, column_type in missing_fetch_state_columns:
                        conn.execute(text(f"ALTER TABLE user_fetch_state ADD COLUMN {column_name} {column_type}"))
        if not inspector.has_table("web_sessions"):
            metadata.tables["web_sessions"].create(self._engine, checkfirst=True)
        if not inspector.has_table("assignment_views"):
            metadata.tables["assignment_views"].create(self._engine, checkfirst=True)
        if not inspector.has_table("study_plan_daily_snapshots"):
            metadata.tables["study_plan_daily_snapshots"].create(self._engine, checkfirst=True)
        if not inspector.has_table("study_plan_activity_events"):
            metadata.tables["study_plan_activity_events"].create(self._engine, checkfirst=True)
        if not inspector.has_table("study_time_sessions"):
            metadata.tables["study_time_sessions"].create(self._engine, checkfirst=True)
        if inspector.has_table("study_plan_video_markers"):
            marker_columns = {
                col["name"] for col in inspector.get_columns("study_plan_video_markers")
            }
            missing_marker_columns = []
            if "summary" not in marker_columns:
                missing_marker_columns.append(("summary", "TEXT NOT NULL DEFAULT ''"))
            if "summary_status" not in marker_columns:
                missing_marker_columns.append(("summary_status", "VARCHAR(16) NOT NULL DEFAULT ''"))
            if "summary_generated_at" not in marker_columns:
                missing_marker_columns.append(("summary_generated_at", "VARCHAR(64)"))
            if missing_marker_columns:
                with self._lock, self._engine.begin() as conn:
                    for column_name, column_type in missing_marker_columns:
                        conn.execute(
                            text(
                                "ALTER TABLE study_plan_video_markers "
                                f"ADD COLUMN {column_name} {column_type}"
                            )
                        )
        if inspector.has_table("study_plan_video_records"):
            video_record_columns = {col["name"] for col in inspector.get_columns("study_plan_video_records")}
            if "playback_seconds" not in video_record_columns:
                with self._lock, self._engine.begin() as conn:
                    conn.execute(text("ALTER TABLE study_plan_video_records ADD COLUMN playback_seconds FLOAT"))
                    # Existing records only stored accumulated progress. Use it as the
                    # initial resume point once, then persist real player positions.
                    conn.execute(
                        text(
                            "UPDATE study_plan_video_records "
                            "SET playback_seconds = watched_seconds "
                            "WHERE playback_seconds IS NULL"
                        )
                    )
            if "progress_version" not in video_record_columns:
                with self._lock, self._engine.begin() as conn:
                    conn.execute(
                        text(
                            "ALTER TABLE study_plan_video_records "
                            "ADD COLUMN progress_version INTEGER NOT NULL DEFAULT 0"
                        )
                    )
        if inspector.has_table("study_recall_card_reviews"):
            card_review_columns = {col["name"] for col in inspector.get_columns("study_recall_card_reviews")}
            if "ideal_review_at" not in card_review_columns:
                with self._lock, self._engine.begin() as conn:
                    conn.execute(text("ALTER TABLE study_recall_card_reviews ADD COLUMN ideal_review_at VARCHAR(10)"))
        if inspector.has_table("study_recall_sessions"):
            recall_columns = {col["name"] for col in inspector.get_columns("study_recall_sessions")}
            missing_recall_columns = []
            if "source_transcription" not in recall_columns:
                missing_recall_columns.append(("source_transcription", "TEXT"))
            if "uncertain_fragments" not in recall_columns:
                missing_recall_columns.append(("uncertain_fragments", "TEXT"))
            if "correction_records" not in recall_columns:
                missing_recall_columns.append(("correction_records", "TEXT"))
            if "organization_mode" not in recall_columns:
                missing_recall_columns.append(("organization_mode", "VARCHAR(32)"))
            if missing_recall_columns:
                with self._lock, self._engine.begin() as conn:
                    for column_name, column_type in missing_recall_columns:
                        conn.execute(text(f"ALTER TABLE study_recall_sessions ADD COLUMN {column_name} {column_type}"))
        if inspector.has_table("study_plan_videos"):
            study_video_columns = {col["name"] for col in inspector.get_columns("study_plan_videos")}
            missing_study_video_columns = []
            if "youtube_video_id" not in study_video_columns:
                missing_study_video_columns.append(("youtube_video_id", "VARCHAR(64)"))
            if "youtube_playlist_id" not in study_video_columns:
                missing_study_video_columns.append(("youtube_playlist_id", "VARCHAR(128)"))
            if "youtube_url" not in study_video_columns:
                missing_study_video_columns.append(("youtube_url", "TEXT"))
            if missing_study_video_columns:
                with self._lock, self._engine.begin() as conn:
                    for column_name, column_type in missing_study_video_columns:
                        conn.execute(text(f"ALTER TABLE study_plan_videos ADD COLUMN {column_name} {column_type}"))
        if not inspector.has_table("assignments"):
            return
        existing_columns = {col["name"] for col in inspector.get_columns("assignments")}
        missing_columns = []
        if "submitted_count" not in existing_columns:
            missing_columns.append(("submitted_count", "INTEGER"))
        if "participant_count" not in existing_columns:
            missing_columns.append(("participant_count", "INTEGER"))
        if "grade_text" not in existing_columns:
            missing_columns.append(("grade_text", "TEXT"))
        if "submitted_at" not in existing_columns:
            missing_columns.append(("submitted_at", "TEXT"))
        if "submitted_ts" not in existing_columns:
            missing_columns.append(("submitted_ts", "INTEGER"))
        if "remaining_text" not in existing_columns:
            missing_columns.append(("remaining_text", "TEXT"))
        if missing_columns:
            with self._lock, self._engine.begin() as conn:
                for column_name, column_type in missing_columns:
                    conn.execute(text(f"ALTER TABLE assignments ADD COLUMN {column_name} {column_type}"))
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        for table in metadata.sorted_tables:
            for index in table.indexes:
                try:
                    index.create(self._engine, checkfirst=True)
                except Exception:
                    pass

    def _normalize_url(self, raw: str) -> str:
        raw = self._normalize_filesystem_path(raw)
        if raw.startswith("postgres://"):
            return "postgresql+psycopg://" + raw[len("postgres://") :]
        if raw.startswith("postgresql://"):
            return "postgresql+psycopg://" + raw[len("postgresql://") :]
        if raw.startswith("sqlite:///") or raw.startswith("mysql://") or raw.startswith("mysql+pymysql://") or raw.startswith("postgresql+"):
            return raw
        if "://" in raw:
            return raw
        path = Path(raw).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite:///{path.as_posix()}"

    def _normalize_filesystem_path(self, raw: str) -> str:
        value = str(raw or "").strip()
        if not value:
            return value
        if os.name != "nt":
            return value
        if value.startswith("\\\\?\\UNC\\"):
            return "\\" + value[7:]
        if value.startswith("\\\\?\\"):
            return value[4:]
        return value

    def _now_iso(self) -> str:
        return datetime.utcnow().isoformat()

    @staticmethod
    def _study_plan_business_day_from_timestamp(value: str) -> str:
        """Return the study day for a UTC timestamp (the day changes at 08:00 Taipei)."""
        try:
            parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        except ValueError:
            return datetime.utcnow().date().isoformat()
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        # Taipei is UTC+8, so subtracting the 08:00 learning-day cutoff is
        # equivalent to taking the UTC calendar date for stored UTC timestamps.
        return parsed.date().isoformat()

    def _study_plan_business_today_iso(self) -> str:
        return self._study_plan_business_day_from_timestamp(self._now_iso())

    def _record_study_plan_daily_snapshot_locked(self, conn, *, now: str) -> None:
        day = self._study_plan_business_today_iso()
        rows = conn.execute(
            select(
                study_plan_video_records_table.c.watched_seconds,
                study_plan_videos_table.c.duration_seconds,
            ).select_from(
                study_plan_video_records_table.join(
                    study_plan_videos_table,
                    study_plan_video_records_table.c.video_id == study_plan_videos_table.c.id,
                )
            )
        ).fetchall()
        total_watched_seconds = sum(
            min(max(float(row.watched_seconds or 0), 0.0), max(float(row.duration_seconds or 0), 0.0))
            for row in rows
        )
        existing = conn.execute(
            select(study_plan_daily_snapshots_table.c.day).where(study_plan_daily_snapshots_table.c.day == day)
        ).fetchone()
        values = {
            "total_watched_seconds": total_watched_seconds,
            "updated_at": now,
        }
        if existing:
            conn.execute(
                update(study_plan_daily_snapshots_table)
                .where(study_plan_daily_snapshots_table.c.day == day)
                .values(**values)
            )
        else:
            conn.execute(insert(study_plan_daily_snapshots_table).values(day=day, **values))

    def _record_study_plan_activity_locked(
        self,
        conn,
        *,
        video_id: int,
        previous_watched_seconds: float,
        watched_seconds: float,
        now: str,
    ) -> None:
        previous = max(0.0, float(previous_watched_seconds or 0))
        current = max(0.0, float(watched_seconds or 0))
        delta = current - previous
        if abs(delta) < 0.01:
            return
        conn.execute(
            insert(study_plan_activity_events_table).values(
                day=self._study_plan_business_today_iso(),
                video_id=video_id,
                previous_watched_seconds=previous,
                watched_seconds=current,
                delta_seconds=delta,
                updated_at=now,
            )
        )

    def _ensure_user(self, conn, username: str, *, is_guest: Optional[bool] = None, is_admin: Optional[bool] = None) -> int:
        row = conn.execute(
            select(users_table.c.id, users_table.c.is_guest, users_table.c.is_admin)
            .where(users_table.c.username == username)
        ).fetchone()
        now = self._now_iso()
        if row:
            updates = {"last_seen": now}
            if is_guest is not None:
                updates["is_guest"] = 1 if is_guest else 0
            if is_admin is not None:
                updates["is_admin"] = 1 if is_admin else 0
            conn.execute(update(users_table).where(users_table.c.id == row.id).values(**updates))
            return int(row.id)
        try:
            result = conn.execute(
                insert(users_table).values(
                    username=username,
                    is_guest=1 if is_guest else 0,
                    is_admin=1 if is_admin else 0,
                    created_at=now,
                    last_seen=now,
                )
            )
            user_id = result.inserted_primary_key[0]
            return int(user_id)
        except IntegrityError:
            row = conn.execute(select(users_table.c.id).where(users_table.c.username == username)).fetchone()
            if not row:
                raise
            conn.execute(update(users_table).where(users_table.c.id == row.id).values(last_seen=now))
            return int(row.id)

    def _coerce_bool_int(self, value: Any) -> int:
        return 1 if bool(value) else 0

    def _course_sort_key(self, item: Dict[str, Any]):
        due_ts = item.get("due_ts")
        if due_ts is None:
            return (1, float("inf"))
        return (0, due_ts)

    def _global_sort_key(self, item: Dict[str, Any]):
        due_ts = item.get("due_ts")
        if due_ts is None:
            return (item.get("course_title", ""), 1, float("inf"))
        return (item.get("course_title", ""), 0, due_ts)

    def _assignment_uid(self, course_code: Optional[int], title: str, url: Optional[str]) -> str:
        return f"{course_code}|{title}|{url or ''}"

    def assignment_uid(self, course_code: Optional[int], title: str, url: Optional[str]) -> str:
        return self._assignment_uid(course_code, title, url)
