import json
import os
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

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


metadata = MetaData()

users_table = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("username", String(191), nullable=False, unique=True),
    Column("is_guest", Integer, nullable=False, default=0),
    Column("is_admin", Integer, nullable=False, default=0),
    Column("created_at", String(64), nullable=False),
    Column("last_seen", String(64)),
)

user_preferences_table = Table(
    "user_preferences",
    metadata,
    Column("user_id", Integer, primary_key=True),
    Column("view_mode", String(32)),
    Column("status_filter", String(32)),
    Column("include_ignored_overdue", Integer, nullable=False, default=0),
    Column("show_overdue", Integer, nullable=False, default=0),
    Column("show_completed", Integer, nullable=False, default=0),
    Column("show_graded", Integer, nullable=False, default=0),
    Column("ignored_overdue_uids", Text),
    Column("updated_at", String(64)),
)

courses_table = Table(
    "courses",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, nullable=False),
    Column("course_code", Integer, nullable=False),
    Column("title", Text, nullable=False),
    Column("url", Text),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
    UniqueConstraint("user_id", "course_code", name="uq_courses_user_course"),
)
Index("ix_courses_user_id", courses_table.c.user_id)

assignments_table = Table(
    "assignments",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("course_id", Integer, nullable=False),
    Column("uid", String(255), nullable=False),
    Column("title", Text, nullable=False),
    Column("url", Text),
    Column("due_at", String(64)),
    Column("due_ts", Integer),
    Column("overdue", Integer, nullable=False, default=0),
    Column("completed", Integer, nullable=False, default=0),
    Column("raw_status_text", Text),
    Column("grade_text", Text),
    Column("submitted_at", String(64)),
    Column("submitted_ts", Integer),
    Column("remaining_text", Text),
    Column("submitted_count", Integer),
    Column("participant_count", Integer),
    Column("updated_at", String(64), nullable=False),
    UniqueConstraint("course_id", "uid", name="uq_assignments_course_uid"),
)
Index("ix_assignments_course_id", assignments_table.c.course_id)
Index("ix_assignments_due_ts", assignments_table.c.due_ts)

assignment_views_table = Table(
    "assignment_views",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, nullable=False),
    Column("assignment_uid", String(255), nullable=False),
    Column("first_seen_at", String(64), nullable=False),
    Column("first_seen_ts", Integer, nullable=False),
    UniqueConstraint("user_id", "assignment_uid", name="uq_assignment_views_user_uid"),
)
Index("ix_assignment_views_user_id", assignment_views_table.c.user_id)

user_fetch_state_table = Table(
    "user_fetch_state",
    metadata,
    Column("user_id", Integer, primary_key=True),
    Column("fetched_at", String(64)),
    Column("fetched_ts", Integer),
    Column("excel_data", Text),
    Column("error_count", Integer, nullable=False, default=0),
)

fetch_errors_table = Table(
    "fetch_errors",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, nullable=False),
    Column("course_code", Integer),
    Column("course_title", Text),
    Column("assignment_title", Text),
    Column("message", Text, nullable=False),
)

google_tokens_table = Table(
    "google_tokens",
    metadata,
    Column("user_id", Integer, primary_key=True),
    Column("access_token", Text),
    Column("refresh_token", Text),
    Column("scope", Text),
    Column("token_type", String(32)),
    Column("expires_at", Float),
    Column("updated_at", String(64)),
)
Index("ix_fetch_errors_user_id", fetch_errors_table.c.user_id)

web_sessions_table = Table(
    "web_sessions",
    metadata,
    Column("session_token", String(191), primary_key=True),
    Column("username", String(191), nullable=False),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
)
Index("ix_web_sessions_username", web_sessions_table.c.username)

announcements_table = Table(
    "announcements",
    metadata,
    Column("id", String(191), primary_key=True),
    Column("title", Text, nullable=False),
    Column("content", Text, nullable=False),
    Column("author", String(191)),
    Column("created_at", String(64)),
    Column("created_label", String(64)),
)

announcement_votes_table = Table(
    "announcement_votes",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("announcement_id", String(191), nullable=False),
    Column("user_id", Integer, nullable=False),
    Column("vote_type", String(16), nullable=False),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
    UniqueConstraint("announcement_id", "user_id", name="uq_announcement_votes_announcement_user"),
)
Index("ix_announcement_votes_user_id", announcement_votes_table.c.user_id)

feedback_table = Table(
    "feedback",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer),
    Column("username", String(191)),
    Column("email", String(191)),
    Column("message", Text, nullable=False),
    Column("status", String(32)),
    Column("created_at", String(64)),
)
Index("ix_feedback_user_id", feedback_table.c.user_id)
Index("ix_feedback_status", feedback_table.c.status)

traffic_state_table = Table(
    "traffic_state",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("payload", Text, nullable=False),
    Column("updated_at", String(64)),
)

traffic_events_table = Table(
    "traffic_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ts", Float),
    Column("ip", String(128)),
    Column("action", String(128)),
    Column("status", String(32)),
    Column("username", String(191)),
    Column("is_guest", Integer),
    Column("is_admin", Integer),
    Column("meta", Text),
)
Index("ix_traffic_events_username", traffic_events_table.c.username)
Index("ix_traffic_events_ts", traffic_events_table.c.ts)
Index("ix_traffic_events_action", traffic_events_table.c.action)

study_plan_videos_table = Table(
    "study_plan_videos",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("subject", String(64), nullable=False),
    Column("sequence", Integer, nullable=False),
    Column("title", Text, nullable=False),
    Column("duration_seconds", Float, nullable=False),
    Column("youtube_video_id", String(64)),
    Column("youtube_playlist_id", String(128)),
    Column("youtube_url", Text),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
    UniqueConstraint("subject", "sequence", name="uq_study_plan_video_subject_sequence"),
)
Index("ix_study_plan_videos_subject_sequence", study_plan_videos_table.c.subject, study_plan_videos_table.c.sequence)

study_plan_video_records_table = Table(
    "study_plan_video_records",
    metadata,
    Column("video_id", Integer, primary_key=True),
    Column("watched_seconds", Float, nullable=False, default=0),
    Column("notes", Text),
    Column("updated_at", String(64), nullable=False),
)

study_plan_daily_snapshots_table = Table(
    "study_plan_daily_snapshots",
    metadata,
    Column("day", String(10), primary_key=True),
    Column("total_watched_seconds", Float, nullable=False, default=0),
    Column("updated_at", String(64), nullable=False),
)
Index("ix_study_plan_daily_snapshots_day", study_plan_daily_snapshots_table.c.day)

study_plan_activity_events_table = Table(
    "study_plan_activity_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("day", String(10), nullable=False),
    Column("video_id", Integer, nullable=False),
    Column("previous_watched_seconds", Float, nullable=False, default=0),
    Column("watched_seconds", Float, nullable=False, default=0),
    Column("delta_seconds", Float, nullable=False, default=0),
    Column("updated_at", String(64), nullable=False),
)
Index("ix_study_plan_activity_events_day", study_plan_activity_events_table.c.day)
Index("ix_study_plan_activity_events_video_id", study_plan_activity_events_table.c.video_id)

study_recall_sessions_table = Table(
    "study_recall_sessions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("study_date", String(10), nullable=False),
    Column("subject", String(64), nullable=False),
    Column("title", String(191), nullable=False),
    Column("image_filenames", Text, nullable=False),
    Column("summary", Text, nullable=False),
    Column("key_concepts", Text, nullable=False),
    Column("quiz_data", Text, nullable=False),
    Column("last_score_percent", Float),
    Column("last_self_rating", Integer),
    Column("next_review_at", String(10)),
    Column("review_count", Integer, nullable=False, default=0),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
)
Index("ix_study_recall_sessions_next_review", study_recall_sessions_table.c.next_review_at)

study_recall_attempts_table = Table(
    "study_recall_attempts",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("session_id", Integer, nullable=False),
    Column("score_percent", Float, nullable=False),
    Column("self_rating", Integer, nullable=False),
    Column("answers", Text, nullable=False),
    Column("next_review_at", String(10), nullable=False),
    Column("created_at", String(64), nullable=False),
)
Index("ix_study_recall_attempts_session", study_recall_attempts_table.c.session_id)

study_recall_card_reviews_table = Table(
    "study_recall_card_reviews",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("session_id", Integer, nullable=False),
    Column("concept_index", Integer, nullable=False),
    Column("rating", Integer, nullable=False),
    Column("interval_days", Integer, nullable=False),
    Column("ideal_review_at", String(10), nullable=False),
    Column("next_review_at", String(10), nullable=False),
    Column("created_at", String(64), nullable=False),
)
Index("ix_study_recall_card_reviews_session", study_recall_card_reviews_table.c.session_id)
Index("ix_study_recall_card_reviews_next_review", study_recall_card_reviews_table.c.next_review_at)

class PersistentStorage:
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
        if not inspector.has_table("web_sessions"):
            metadata.tables["web_sessions"].create(self._engine, checkfirst=True)
        if not inspector.has_table("assignment_views"):
            metadata.tables["assignment_views"].create(self._engine, checkfirst=True)
        if not inspector.has_table("study_plan_daily_snapshots"):
            metadata.tables["study_plan_daily_snapshots"].create(self._engine, checkfirst=True)
        if not inspector.has_table("study_plan_activity_events"):
            metadata.tables["study_plan_activity_events"].create(self._engine, checkfirst=True)
        if inspector.has_table("study_recall_card_reviews"):
            card_review_columns = {col["name"] for col in inspector.get_columns("study_recall_card_reviews")}
            if "ideal_review_at" not in card_review_columns:
                with self._lock, self._engine.begin() as conn:
                    conn.execute(text("ALTER TABLE study_recall_card_reviews ADD COLUMN ideal_review_at VARCHAR(10)"))
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

    def _taipei_today_iso(self) -> str:
        return datetime.utcnow().date().isoformat()

    def _record_study_plan_daily_snapshot_locked(self, conn, *, now: str) -> None:
        day = self._taipei_today_iso()
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
        delta = max(0.0, current - previous)
        if delta <= 0:
            return
        conn.execute(
            insert(study_plan_activity_events_table).values(
                day=self._taipei_today_iso(),
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

    # -- user preferences -------------------------------------------------
    def load_user_preferences(self, username: str) -> Dict[str, Any]:
        if not username:
            return {}
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                select(
                    user_preferences_table.c.view_mode,
                    user_preferences_table.c.status_filter,
                    user_preferences_table.c.include_ignored_overdue,
                    user_preferences_table.c.show_overdue,
                    user_preferences_table.c.show_completed,
                    user_preferences_table.c.show_graded,
                    user_preferences_table.c.ignored_overdue_uids,
                )
                .select_from(user_preferences_table.join(users_table, user_preferences_table.c.user_id == users_table.c.id))
                .where(users_table.c.username == username)
            ).fetchone()
        if not row:
            return {}
        ignored_overdue_uids: List[str] = []
        if row.ignored_overdue_uids:
            try:
                parsed = json.loads(row.ignored_overdue_uids)
                if isinstance(parsed, list):
                    ignored_overdue_uids = [str(item).strip() for item in parsed if str(item).strip()]
            except Exception:
                ignored_overdue_uids = []
        return {
            "view_mode": row.view_mode,
            "status_filter": row.status_filter,
            "include_ignored_overdue": bool(row.include_ignored_overdue),
            "show_overdue": bool(row.show_overdue),
            "show_completed": bool(row.show_completed),
            "show_graded": bool(row.show_graded),
            "ignored_overdue_uids": ignored_overdue_uids,
        }

    def save_user_preferences(self, username: str, prefs: Dict[str, Any]) -> None:
        if not username:
            return
        if not isinstance(prefs, dict):
            return
        view_mode = prefs.get("view_mode")
        status_filter = prefs.get("status_filter")
        if isinstance(status_filter, list):
            status_filter = json.dumps(
                [str(item).strip() for item in status_filter if str(item).strip()],
                ensure_ascii=False,
            )
        include_ignored_overdue = self._coerce_bool_int(prefs.get("include_ignored_overdue"))
        show_overdue = self._coerce_bool_int(prefs.get("show_overdue"))
        show_completed = self._coerce_bool_int(prefs.get("show_completed"))
        show_graded = self._coerce_bool_int(prefs.get("show_graded"))
        ignored_overdue_uids = prefs.get("ignored_overdue_uids")
        if not isinstance(ignored_overdue_uids, list):
            ignored_overdue_uids = []
        ignored_overdue_uids = [str(item).strip() for item in ignored_overdue_uids if str(item).strip()]
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            user_id = self._ensure_user(conn, username)
            conn.execute(delete(user_preferences_table).where(user_preferences_table.c.user_id == user_id))
            conn.execute(
                insert(user_preferences_table).values(
                    user_id=user_id,
                    view_mode=view_mode,
                    status_filter=status_filter,
                    include_ignored_overdue=include_ignored_overdue,
                    show_overdue=show_overdue,
                    show_completed=show_completed,
                    show_graded=show_graded,
                    ignored_overdue_uids=json.dumps(ignored_overdue_uids, ensure_ascii=False),
                    updated_at=now,
                )
            )

    # -- administrator study plan ---------------------------------------
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
                    study_plan_video_records_table.c.notes,
                    study_plan_video_records_table.c.updated_at,
                )
                .select_from(
                    study_plan_videos_table.outerjoin(
                        study_plan_video_records_table,
                        study_plan_videos_table.c.id == study_plan_video_records_table.c.video_id,
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
                "youtube_video_id": row.youtube_video_id or "",
                "youtube_playlist_id": row.youtube_playlist_id or "",
                "youtube_url": row.youtube_url or "",
                "watched_seconds": max(0.0, float(row.watched_seconds or 0)),
                "notes": row.notes or "",
                "updated_at": _to_taipei(row.updated_at),
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
            normalized_seconds = max(0.0, min(float(watched_seconds or 0), float(video.duration_seconds or 0)))
            existing = conn.execute(
                select(
                    study_plan_video_records_table.c.video_id,
                    study_plan_video_records_table.c.watched_seconds,
                ).where(
                    study_plan_video_records_table.c.video_id == video_id
                )
            ).fetchone()
            previous_watched_seconds = float(existing.watched_seconds or 0) if existing else 0.0
            values = {
                "watched_seconds": normalized_seconds,
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

    def update_study_plan_video_progress(self, *, video_id: int, watched_seconds: float) -> Optional[Dict[str, Any]]:
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
                    study_plan_video_records_table.c.notes,
                ).where(study_plan_video_records_table.c.video_id == video_id)
            ).fetchone()
            duration_seconds = max(0.0, float(video.duration_seconds or 0))
            current_seconds = max(0.0, min(float(watched_seconds or 0), duration_seconds))
            normalized_seconds = current_seconds
            notes = existing.notes if existing else ""
            previous_watched_seconds = float(existing.watched_seconds or 0) if existing else 0.0
            values = {
                "watched_seconds": normalized_seconds,
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
            return {
                "video_id": int(video.id),
                "duration_seconds": duration_seconds,
                "watched_seconds": normalized_seconds,
                "completion": min(100.0, (normalized_seconds / duration_seconds * 100) if duration_seconds else 0.0),
                "youtube_video_id": video.youtube_video_id or "",
            }

    def delete_study_plan_video_record(self, video_id: int) -> bool:
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(
                delete(study_plan_video_records_table).where(study_plan_video_records_table.c.video_id == video_id)
            )
            if result.rowcount:
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

    def list_study_plan_activity_events(self, *, day: str) -> List[Dict[str, Any]]:
        if not day:
            return []
        stmt = (
            select(
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
            .where(study_plan_activity_events_table.c.day == day)
            .order_by(study_plan_activity_events_table.c.updated_at)
        )
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        by_video: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            video_id = int(row.video_id)
            existing = by_video.get(video_id)
            delta_seconds = max(0.0, float(row.delta_seconds or 0))
            if existing is None:
                by_video[video_id] = {
                    "video_id": video_id,
                    "subject": row.subject,
                    "sequence": int(row.sequence or 0),
                    "title": row.title,
                    "duration_seconds": max(0.0, float(row.duration_seconds or 0)),
                    "previous_watched_seconds": max(0.0, float(row.previous_watched_seconds or 0)),
                    "watched_seconds": max(0.0, float(row.watched_seconds or 0)),
                    "delta_seconds": delta_seconds,
                    "updated_at": row.updated_at or "",
                }
            else:
                existing["delta_seconds"] = float(existing["delta_seconds"]) + delta_seconds
                existing["watched_seconds"] = max(float(existing["watched_seconds"]), max(0.0, float(row.watched_seconds or 0)))
                existing["updated_at"] = row.updated_at or existing["updated_at"]

        return sorted(by_video.values(), key=lambda item: (str(item["updated_at"]), int(item["sequence"])))

    # -- study recall loop -----------------------------------------------
    @staticmethod
    def _decode_json_list(value: Any) -> List[Any]:
        try:
            parsed = json.loads(value) if value else []
        except (TypeError, ValueError):
            return []
        return parsed if isinstance(parsed, list) else []

    @staticmethod
    def _decode_json_dict(value: Any) -> Dict[str, Any]:
        try:
            parsed = json.loads(value) if value else {}
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @staticmethod
    def _recall_interval_days(rating: int, previous_rating: Optional[int], previous_interval: int) -> int:
        """A confidence-based spaced-repetition interval with a short reset for weak recall."""
        rating = max(1, min(int(rating), 5))
        if rating == 1:
            return 1
        if rating == 2:
            return 2
        if previous_rating is None or previous_rating <= 2:
            return {3: 3, 4: 5, 5: 7}[rating]
        multiplier = {3: 1.5, 4: 2.2, 5: 2.8}[rating]
        minimum = {3: 3, 4: 5, 5: 7}[rating]
        return min(120, max(minimum, int(round(max(1, previous_interval) * multiplier))))

    def create_study_recall_session(
        self,
        *,
        study_date: str,
        subject: str,
        title: str,
        image_filenames: List[str],
        summary: str,
        key_concepts: List[Dict[str, Any]],
    ) -> int:
        now = self._now_iso()
        values = {
            "study_date": study_date,
            "subject": subject,
            "title": title,
            "image_filenames": json.dumps(image_filenames, ensure_ascii=False),
            "summary": summary,
            "key_concepts": json.dumps(key_concepts, ensure_ascii=False),
            "quiz_data": "[]",
            "review_count": 0,
            "created_at": now,
            "updated_at": now,
        }
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(insert(study_recall_sessions_table).values(**values))
            return int(result.inserted_primary_key[0])

    def get_study_recall_session(self, session_id: int) -> Optional[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                select(study_recall_sessions_table).where(study_recall_sessions_table.c.id == session_id)
            ).fetchone()
            if not row:
                return None
            attempts = conn.execute(
                select(study_recall_attempts_table)
                .where(study_recall_attempts_table.c.session_id == session_id)
                .order_by(study_recall_attempts_table.c.id.desc())
            ).fetchall()
            card_review_rows = conn.execute(
                select(study_recall_card_reviews_table)
                .where(study_recall_card_reviews_table.c.session_id == session_id)
                .order_by(study_recall_card_reviews_table.c.id.asc())
            ).fetchall()
        item = dict(row._mapping)
        item["image_filenames"] = self._decode_json_list(item.get("image_filenames"))
        item["key_concepts"] = self._decode_json_list(item.get("key_concepts"))
        item["questions"] = self._decode_json_list(item.get("quiz_data"))
        card_history: Dict[int, List[Dict[str, Any]]] = {}
        for review in card_review_rows:
            card_history.setdefault(int(review.concept_index), []).append(
                {
                    "rating": int(review.rating),
                    "interval_days": int(review.interval_days),
                    "ideal_review_at": review.ideal_review_at or review.next_review_at,
                    "next_review_at": review.next_review_at,
                    "created_at": review.created_at,
                }
            )
        for index, concept in enumerate(item["key_concepts"]):
            if not isinstance(concept, dict):
                continue
            history = card_history.get(index, [])
            latest = history[-1] if history else None
            concept["review"] = {
                "history": history[-12:],
                "last_rating": latest["rating"] if latest else None,
                "interval_days": latest["interval_days"] if latest else None,
                "ideal_review_at": latest["ideal_review_at"] if latest else None,
                "next_review_at": latest["next_review_at"] if latest else None,
            }
        item["attempts"] = [
            {
                "score_percent": round(float(attempt.score_percent or 0), 1),
                "self_rating": int(attempt.self_rating or 0),
                "next_review_at": attempt.next_review_at,
                "created_at": attempt.created_at,
                "answers": self._decode_json_dict(attempt.answers),
            }
            for attempt in attempts
        ]
        return item

    def list_due_study_recall_cards(self, *, today: str, limit: int = 18) -> List[Dict[str, Any]]:
        due_cards: List[Dict[str, Any]] = []
        for session in self.list_study_recall_sessions(limit=36):
            full_session = self.get_study_recall_session(int(session["id"]))
            if not full_session or str(full_session.get("study_date") or "") > today:
                continue
            for index, concept in enumerate(full_session.get("key_concepts") or []):
                if not isinstance(concept, dict):
                    continue
                review = concept.get("review") or {}
                next_review_at = review.get("next_review_at")
                if next_review_at and next_review_at > today:
                    continue
                due_cards.append(
                    {
                        "session_id": int(full_session["id"]),
                        "concept_index": index,
                        "session_title": str(full_session.get("title") or ""),
                        "subject": str(full_session.get("subject") or ""),
                        "concept": str(concept.get("concept") or ""),
                        "last_rating": review.get("last_rating"),
                        "next_review_at": next_review_at,
                    }
                )
        due_cards.sort(key=lambda item: (item["next_review_at"] or "0000-00-00", item["session_title"], item["concept_index"]))
        return due_cards[: max(1, min(int(limit), 60))]

    def list_study_recall_schedule(self, *, start_date: str, days: int = 7, daily_capacity: int = 18) -> List[Dict[str, Any]]:
        start = datetime.fromisoformat(start_date).date()
        schedule_days = [start + timedelta(days=offset) for offset in range(max(1, min(int(days), 31)))]
        loads = {day.isoformat(): 0 for day in schedule_days}
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                select(study_recall_card_reviews_table).order_by(study_recall_card_reviews_table.c.id.desc())
            ).fetchall()
        latest_by_card: Dict[tuple[int, int], Any] = {}
        for row in rows:
            latest_by_card.setdefault((int(row.session_id), int(row.concept_index)), row)
        for row in latest_by_card.values():
            if row.next_review_at in loads:
                loads[row.next_review_at] += 1
        return [
            {
                "date": day.isoformat(),
                "count": loads[day.isoformat()],
                "capacity": daily_capacity,
                "is_today": day == start,
            }
            for day in schedule_days
        ]

    def record_study_recall_card_ratings(self, *, session_id: int, ratings: Dict[int, int], review_date: str) -> bool:
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            session_row = conn.execute(
                select(
                    study_recall_sessions_table.c.id,
                    study_recall_sessions_table.c.key_concepts,
                    study_recall_sessions_table.c.review_count,
                ).where(study_recall_sessions_table.c.id == session_id)
            ).fetchone()
            if not session_row:
                return False
            concepts = self._decode_json_list(session_row.key_concepts)
            expected_indexes = set(range(len(concepts)))
            if not ratings or not set(ratings).issubset(expected_indexes):
                return False
            previous_rows = conn.execute(
                select(study_recall_card_reviews_table).order_by(study_recall_card_reviews_table.c.id.desc())
            ).fetchall()
            latest_by_index: Dict[int, Any] = {}
            scheduled_loads: Dict[str, int] = {}
            for row in previous_rows:
                card_key = (int(row.session_id), int(row.concept_index))
                if card_key in latest_by_index:
                    continue
                latest_by_index[card_key] = row
                if int(row.session_id) != session_id or int(row.concept_index) not in ratings:
                    scheduled_loads[row.next_review_at] = scheduled_loads.get(row.next_review_at, 0) + 1
            next_dates: List[str] = []
            normalized_ratings: List[int] = []
            review_day = datetime.fromisoformat(review_date).date()
            ordered_indexes = sorted(ratings, key=lambda index: (int(ratings[index]), index))
            assignments: Dict[int, tuple[int, str, str]] = {}
            for index in ordered_indexes:
                rating = max(1, min(int(ratings[index]), 5))
                previous = latest_by_index.get((session_id, index))
                interval_days = self._recall_interval_days(
                    rating,
                    int(previous.rating) if previous else None,
                    int(previous.interval_days) if previous else 0,
                )
                ideal_review_at = (review_day + timedelta(days=interval_days)).isoformat()
                candidate_day = datetime.fromisoformat(ideal_review_at).date()
                for _offset in range(61):
                    candidate = candidate_day.isoformat()
                    if scheduled_loads.get(candidate, 0) < 18:
                        break
                    candidate_day += timedelta(days=1)
                next_review_at = candidate_day.isoformat()
                scheduled_loads[next_review_at] = scheduled_loads.get(next_review_at, 0) + 1
                assignments[index] = (interval_days, ideal_review_at, next_review_at)
            for index in ordered_indexes:
                rating = max(1, min(int(ratings[index]), 5))
                interval_days, ideal_review_at, next_review_at = assignments[index]
                conn.execute(
                    insert(study_recall_card_reviews_table).values(
                        session_id=session_id,
                        concept_index=index,
                        rating=rating,
                        interval_days=interval_days,
                        ideal_review_at=ideal_review_at,
                        next_review_at=next_review_at,
                        created_at=now,
                    )
                )
                next_dates.append(next_review_at)
                normalized_ratings.append(rating)
            for (row_session_id, _concept_index), row in latest_by_index.items():
                if row_session_id == session_id and int(row.concept_index) not in ratings:
                    next_dates.append(row.next_review_at)
            conn.execute(
                update(study_recall_sessions_table)
                .where(study_recall_sessions_table.c.id == session_id)
                .values(
                    last_score_percent=None,
                    last_self_rating=int(round(sum(normalized_ratings) / len(normalized_ratings))),
                    next_review_at=min(next_dates),
                    review_count=int(session_row.review_count or 0) + 1,
                    updated_at=now,
                )
            )
        return True

    def list_study_recall_sessions(self, *, limit: int = 24) -> List[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                select(study_recall_sessions_table)
                .order_by(study_recall_sessions_table.c.created_at.desc())
                .limit(max(1, min(int(limit), 100)))
            ).fetchall()
        return [
            {
                "id": int(row.id),
                "study_date": row.study_date,
                "subject": row.subject,
                "title": row.title,
                "summary": row.summary,
                "key_concepts": self._decode_json_list(row.key_concepts),
                "last_score_percent": round(float(row.last_score_percent or 0), 1) if row.last_score_percent is not None else None,
                "last_self_rating": int(row.last_self_rating or 0) if row.last_self_rating is not None else None,
                "next_review_at": row.next_review_at,
                "review_count": int(row.review_count or 0),
                "created_at": row.created_at,
            }
            for row in rows
        ]

    def record_study_recall_attempt(
        self,
        *,
        session_id: int,
        score_percent: float,
        self_rating: int,
        answers: Dict[str, Any],
        next_review_at: str,
    ) -> bool:
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            session_row = conn.execute(
                select(study_recall_sessions_table.c.id, study_recall_sessions_table.c.review_count)
                .where(study_recall_sessions_table.c.id == session_id)
            ).fetchone()
            if not session_row:
                return False
            conn.execute(
                insert(study_recall_attempts_table).values(
                    session_id=session_id,
                    score_percent=max(0.0, min(float(score_percent), 100.0)),
                    self_rating=max(1, min(int(self_rating), 5)),
                    answers=json.dumps(answers, ensure_ascii=False),
                    next_review_at=next_review_at,
                    created_at=now,
                )
            )
            conn.execute(
                update(study_recall_sessions_table)
                .where(study_recall_sessions_table.c.id == session_id)
                .values(
                    last_score_percent=max(0.0, min(float(score_percent), 100.0)),
                    last_self_rating=max(1, min(int(self_rating), 5)),
                    next_review_at=next_review_at,
                    review_count=int(session_row.review_count or 0) + 1,
                    updated_at=now,
                )
            )
        return True

    # -- assignments ------------------------------------------------------
    def save_user_cache(self, username: str, payload: Dict[str, Any]) -> None:
        if not username:
            return
        if not isinstance(payload, dict):
            return
        prefs = payload.get("preferences")
        if isinstance(prefs, dict):
            self.save_user_preferences(username, prefs)
        result = payload.get("result")
        if not isinstance(result, dict):
            return
        courses = result.get("courses") or []
        errors = result.get("errors") or []
        excel_data = payload.get("excel_data")
        ts_raw = payload.get("ts")
        try:
            fetched_ts = int(ts_raw)
        except (TypeError, ValueError):
            fetched_ts = int(datetime.utcnow().timestamp())
        fetched_at = datetime.utcfromtimestamp(fetched_ts).isoformat()
        with self._lock, self._engine.begin() as conn:
            user_id = self._ensure_user(conn, username)
            conn.execute(delete(user_fetch_state_table).where(user_fetch_state_table.c.user_id == user_id))
            conn.execute(
                insert(user_fetch_state_table).values(
                    user_id=user_id,
                    fetched_at=fetched_at,
                    fetched_ts=fetched_ts,
                    excel_data=excel_data,
                    error_count=len(errors),
                )
            )
            conn.execute(delete(fetch_errors_table).where(fetch_errors_table.c.user_id == user_id))

            course_ids = conn.execute(
                select(courses_table.c.id).where(courses_table.c.user_id == user_id)
            ).scalars().all()
            if course_ids:
                conn.execute(delete(assignments_table).where(assignments_table.c.course_id.in_(course_ids)))
            conn.execute(delete(courses_table).where(courses_table.c.user_id == user_id))

            now = self._now_iso()
            assignment_rows: List[Dict[str, Any]] = []
            for course in courses:
                try:
                    course_code = int(course.get("id"))
                except (TypeError, ValueError):
                    continue
                title = str(course.get("title") or "").strip()
                if not title:
                    title = f"Course {course_code}"
                url = course.get("url")
                insert_result = conn.execute(
                    insert(courses_table).values(
                        user_id=user_id,
                        course_code=course_code,
                        title=title,
                        url=url,
                        created_at=now,
                        updated_at=now,
                    )
                )
                course_pk = int(insert_result.inserted_primary_key[0])
                for item in course.get("assignments") or []:
                    title_val = str(item.get("title") or "").strip()
                    if not title_val:
                        continue
                    due_ts = item.get("due_ts")
                    if due_ts in ("", None):
                        due_ts_val = None
                    else:
                        try:
                            due_ts_val = int(due_ts)
                        except (TypeError, ValueError):
                            due_ts_val = None
                    submitted_ts = item.get("submitted_ts")
                    if submitted_ts in ("", None):
                        submitted_ts_val = None
                    else:
                        try:
                            submitted_ts_val = int(submitted_ts)
                        except (TypeError, ValueError):
                            submitted_ts_val = None
                    assignment_rows.append(
                        {
                            "course_id": course_pk,
                            "uid": self._assignment_uid(course_code, title_val, item.get("url")),
                            "title": title_val,
                            "url": item.get("url"),
                            "due_at": item.get("due_at"),
                            "due_ts": due_ts_val,
                            "overdue": self._coerce_bool_int(item.get("overdue")),
                            "completed": self._coerce_bool_int(item.get("completed")),
                            "raw_status_text": item.get("raw_status_text"),
                            "grade_text": item.get("grade_text"),
                            "submitted_at": item.get("submitted_at"),
                            "submitted_ts": submitted_ts_val,
                            "remaining_text": item.get("remaining_text"),
                            "submitted_count": item.get("submitted_count"),
                            "participant_count": item.get("participant_count"),
                            "updated_at": now,
                        }
                    )
            if assignment_rows:
                conn.execute(insert(assignments_table), assignment_rows)

            error_rows: List[Dict[str, Any]] = []
            for err in errors:
                if not isinstance(err, dict):
                    continue
                course_code = err.get("course_id")
                try:
                    course_code_val = int(course_code) if course_code is not None else None
                except (TypeError, ValueError):
                    course_code_val = None
                error_rows.append(
                    {
                        "user_id": user_id,
                        "course_code": course_code_val,
                        "course_title": err.get("course_title"),
                        "assignment_title": err.get("assignment_title"),
                        "message": str(err.get("message") or ""),
                    }
                )
            if error_rows:
                conn.execute(insert(fetch_errors_table), error_rows)

    def mark_assignment_views(
        self,
        username: str,
        assignment_uids: List[str],
        *,
        seen_ts: Optional[int] = None,
    ) -> Dict[str, int]:
        if not username:
            return {}
        normalized_uids = [str(item or "").strip() for item in assignment_uids if str(item or "").strip()]
        if not normalized_uids:
            return {}
        unique_uids = list(dict.fromkeys(normalized_uids))
        if seen_ts is None:
            seen_ts = int(datetime.utcnow().timestamp())
        seen_at = datetime.utcfromtimestamp(int(seen_ts)).isoformat()
        with self._lock, self._engine.begin() as conn:
            user_id = self._ensure_user(conn, username)
            existing_rows = conn.execute(
                select(
                    assignment_views_table.c.assignment_uid,
                    assignment_views_table.c.first_seen_ts,
                ).where(
                    assignment_views_table.c.user_id == user_id,
                    assignment_views_table.c.assignment_uid.in_(unique_uids),
                )
            ).fetchall()
            first_seen_map = {str(row.assignment_uid): int(row.first_seen_ts) for row in existing_rows}
            missing_uids = [uid for uid in unique_uids if uid not in first_seen_map]
            if missing_uids:
                conn.execute(
                    insert(assignment_views_table),
                    [
                        {
                            "user_id": user_id,
                            "assignment_uid": uid,
                            "first_seen_at": seen_at,
                            "first_seen_ts": int(seen_ts),
                        }
                        for uid in missing_uids
                    ],
                )
                for uid in missing_uids:
                    first_seen_map[uid] = int(seen_ts)
        return first_seen_map

    def load_assignment_view_map(self, username: str, assignment_uids: List[str]) -> Dict[str, int]:
        if not username:
            return {}
        normalized_uids = [str(item or "").strip() for item in assignment_uids if str(item or "").strip()]
        if not normalized_uids:
            return {}
        unique_uids = list(dict.fromkeys(normalized_uids))
        with self._lock, self._engine.connect() as conn:
            user_row = conn.execute(
                select(users_table.c.id).where(users_table.c.username == username)
            ).fetchone()
            if not user_row:
                return {}
            rows = conn.execute(
                select(
                    assignment_views_table.c.assignment_uid,
                    assignment_views_table.c.first_seen_ts,
                ).where(
                    assignment_views_table.c.user_id == int(user_row.id),
                    assignment_views_table.c.assignment_uid.in_(unique_uids),
                )
            ).fetchall()
        return {str(row.assignment_uid): int(row.first_seen_ts) for row in rows}

    def load_user_cache(self, username: str) -> Optional[Dict[str, Any]]:
        if not username:
            return None
        with self._lock, self._engine.connect() as conn:
            user_row = conn.execute(
                select(users_table.c.id).where(users_table.c.username == username)
            ).fetchone()
            if not user_row:
                return None
            state_row = conn.execute(
                select(
                    user_fetch_state_table.c.fetched_ts,
                    user_fetch_state_table.c.excel_data,
                )
                .where(user_fetch_state_table.c.user_id == user_row.id)
                .limit(1)
            ).fetchone()
            if not state_row:
                return None

            course_rows = conn.execute(
                select(
                    courses_table.c.id,
                    courses_table.c.course_code,
                    courses_table.c.title,
                    courses_table.c.url,
                )
                .where(courses_table.c.user_id == user_row.id)
            ).fetchall()

            courses: List[Dict[str, Any]] = []
            course_map: Dict[int, Dict[str, Any]] = {}
            for row in course_rows:
                entry = {
                    "id": row.course_code,
                    "title": row.title,
                    "url": row.url,
                    "assignments": [],
                    "detected_assign_links": 0,
                }
                courses.append(entry)
                course_map[int(row.id)] = entry

            course_ids = [row.id for row in course_rows]
            all_assignments: List[Dict[str, Any]] = []
            if course_ids:
                assignment_rows = conn.execute(
                    select(
                        assignments_table.c.course_id,
                        assignments_table.c.title,
                        assignments_table.c.url,
                        assignments_table.c.due_at,
                        assignments_table.c.due_ts,
                        assignments_table.c.overdue,
                        assignments_table.c.completed,
                        assignments_table.c.raw_status_text,
                        assignments_table.c.grade_text,
                        assignments_table.c.submitted_at,
                        assignments_table.c.submitted_ts,
                        assignments_table.c.remaining_text,
                        assignments_table.c.submitted_count,
                        assignments_table.c.participant_count,
                    )
                    .where(assignments_table.c.course_id.in_(course_ids))
                ).fetchall()
                for row in assignment_rows:
                    course_entry = course_map.get(int(row.course_id))
                    if not course_entry:
                        continue
                    course_title = course_entry["title"]
                    item = {
                        "course_id": course_entry["id"],
                        "course_title": course_title,
                        "title": row.title,
                        "url": row.url,
                        "due_at": row.due_at,
                        "due_ts": row.due_ts,
                        "overdue": bool(row.overdue),
                        "completed": bool(row.completed),
                        "raw_status_text": row.raw_status_text,
                        "grade_text": row.grade_text,
                        "submitted_at": row.submitted_at,
                        "submitted_ts": row.submitted_ts,
                        "remaining_text": row.remaining_text,
                        "submitted_count": row.submitted_count,
                        "participant_count": row.participant_count,
                    }
                    course_entry["assignments"].append(item)
                    course_entry["detected_assign_links"] += 1
                    all_assignments.append(item)

            for course_entry in courses:
                course_entry["assignments"].sort(key=self._course_sort_key)
            all_assignments.sort(key=self._global_sort_key)

            error_rows = conn.execute(
                select(
                    fetch_errors_table.c.course_code,
                    fetch_errors_table.c.course_title,
                    fetch_errors_table.c.assignment_title,
                    fetch_errors_table.c.message,
                ).where(fetch_errors_table.c.user_id == user_row.id)
            ).fetchall()
            errors = [
                {
                    "course_id": row.course_code,
                    "course_title": row.course_title,
                    "assignment_title": row.assignment_title,
                    "message": row.message,
                }
                for row in error_rows
            ]

        cache: Dict[str, Any] = {
            "result": {
                "courses": courses,
                "all_assignments": all_assignments,
                "errors": errors,
            },
            "excel_data": state_row.excel_data,
            "ts": state_row.fetched_ts,
        }
        prefs = self.load_user_preferences(username)
        if prefs:
            cache["preferences"] = prefs
        return cache

    def list_cached_users(self, limit: int = 500) -> List[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                select(
                    users_table.c.username,
                    users_table.c.is_admin,
                    users_table.c.is_guest,
                    users_table.c.last_seen,
                    user_fetch_state_table.c.fetched_ts,
                    func.count(func.distinct(courses_table.c.id)).label("course_count"),
                    func.count(func.distinct(assignments_table.c.id)).label("assignment_count"),
                )
                .select_from(
                    users_table.join(user_fetch_state_table, user_fetch_state_table.c.user_id == users_table.c.id)
                    .outerjoin(courses_table, courses_table.c.user_id == users_table.c.id)
                    .outerjoin(assignments_table, assignments_table.c.course_id == courses_table.c.id)
                )
                .where(users_table.c.is_guest == 0)
                .group_by(
                    users_table.c.id,
                    users_table.c.username,
                    users_table.c.is_admin,
                    users_table.c.is_guest,
                    users_table.c.last_seen,
                    user_fetch_state_table.c.fetched_ts,
                )
                .order_by(
                    func.coalesce(user_fetch_state_table.c.fetched_ts, 0).desc(),
                    func.coalesce(users_table.c.last_seen, "").desc(),
                    users_table.c.username.asc(),
                )
                .limit(limit)
            ).fetchall()
        return [dict(row._mapping) for row in rows]

    def delete_user_cache(self, username: str) -> None:
        if not username:
            return
        with self._lock, self._engine.begin() as conn:
            user_row = conn.execute(
                select(users_table.c.id).where(users_table.c.username == username)
            ).fetchone()
            if not user_row:
                return
            user_id = int(user_row.id)
            course_ids = conn.execute(
                select(courses_table.c.id).where(courses_table.c.user_id == user_id)
            ).scalars().all()
            if course_ids:
                conn.execute(delete(assignments_table).where(assignments_table.c.course_id.in_(course_ids)))
            conn.execute(delete(courses_table).where(courses_table.c.user_id == user_id))
            conn.execute(delete(assignment_views_table).where(assignment_views_table.c.user_id == user_id))
            conn.execute(delete(user_fetch_state_table).where(user_fetch_state_table.c.user_id == user_id))
            conn.execute(delete(fetch_errors_table).where(fetch_errors_table.c.user_id == user_id))
            conn.execute(delete(user_preferences_table).where(user_preferences_table.c.user_id == user_id))
            conn.execute(delete(google_tokens_table).where(google_tokens_table.c.user_id == user_id))
            conn.execute(delete(web_sessions_table).where(web_sessions_table.c.username == username))

    # -- google tokens ----------------------------------------------------
    def save_google_tokens(self, username: str, payload: Dict[str, Any]) -> None:
        if not username:
            return
        if not isinstance(payload, dict):
            return
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            user_id = self._ensure_user(conn, username)
            conn.execute(delete(google_tokens_table).where(google_tokens_table.c.user_id == user_id))
            conn.execute(
                insert(google_tokens_table).values(
                    user_id=user_id,
                    access_token=payload.get("access_token"),
                    refresh_token=payload.get("refresh_token"),
                    scope=payload.get("scope"),
                    token_type=payload.get("token_type"),
                    expires_at=payload.get("expires_at"),
                    updated_at=now,
                )
            )

    def load_google_tokens(self, username: str) -> Optional[Dict[str, Any]]:
        if not username:
            return None
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                select(
                    google_tokens_table.c.access_token,
                    google_tokens_table.c.refresh_token,
                    google_tokens_table.c.scope,
                    google_tokens_table.c.token_type,
                    google_tokens_table.c.expires_at,
                )
                .select_from(google_tokens_table.join(users_table, google_tokens_table.c.user_id == users_table.c.id))
                .where(users_table.c.username == username)
            ).fetchone()
        if not row:
            return None
        return {
            "access_token": row.access_token,
            "refresh_token": row.refresh_token,
            "scope": row.scope,
            "token_type": row.token_type,
            "expires_at": row.expires_at,
        }

    def clear_google_tokens(self, username: str) -> None:
        if not username:
            return
        with self._lock, self._engine.begin() as conn:
            user_row = conn.execute(
                select(users_table.c.id).where(users_table.c.username == username)
            ).fetchone()
            if not user_row:
                return
            conn.execute(delete(google_tokens_table).where(google_tokens_table.c.user_id == user_row.id))

    # -- web sessions -----------------------------------------------------
    def save_web_session(self, session_token: str, username: str) -> None:
        if not session_token or not username:
            return
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            conn.execute(delete(web_sessions_table).where(web_sessions_table.c.session_token == session_token))
            conn.execute(
                insert(web_sessions_table).values(
                    session_token=session_token,
                    username=username,
                    created_at=now,
                    updated_at=now,
                )
            )

    def is_valid_web_session(self, session_token: str, username: str) -> bool:
        if not session_token or not username:
            return False
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                select(web_sessions_table.c.session_token)
                .where(web_sessions_table.c.session_token == session_token)
                .where(web_sessions_table.c.username == username)
                .limit(1)
            ).fetchone()
        return bool(row)

    def clear_web_session(self, session_token: str) -> None:
        if not session_token:
            return
        with self._lock, self._engine.begin() as conn:
            conn.execute(delete(web_sessions_table).where(web_sessions_table.c.session_token == session_token))

    # -- announcements ----------------------------------------------------
    def insert_announcement(self, entry: Dict[str, Any], limit: int) -> None:
        record = dict(entry)
        with self._lock, self._engine.begin() as conn:
            conn.execute(delete(announcements_table).where(announcements_table.c.id == record["id"]))
            conn.execute(announcements_table.insert().values(**record))
            ids_to_keep = conn.execute(
                select(announcements_table.c.id)
                .order_by(announcements_table.c.created_at.desc(), announcements_table.c.id.desc())
                .limit(limit)
            ).scalars().all()
            if ids_to_keep:
                conn.execute(delete(announcements_table).where(~announcements_table.c.id.in_(ids_to_keep)))
                conn.execute(delete(announcement_votes_table).where(~announcement_votes_table.c.announcement_id.in_(ids_to_keep)))

    def delete_announcement(self, announcement_id: str) -> bool:
        with self._lock, self._engine.begin() as conn:
            conn.execute(delete(announcement_votes_table).where(announcement_votes_table.c.announcement_id == announcement_id))
            result = conn.execute(delete(announcements_table).where(announcements_table.c.id == announcement_id))
            return result.rowcount > 0

    def list_announcements(self, limit: int) -> List[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                select(announcements_table)
                .order_by(announcements_table.c.created_at.desc(), announcements_table.c.id.desc())
                .limit(limit)
            ).fetchall()
        return [dict(row._mapping) for row in rows]

    def list_announcements_with_votes(self, limit: int, username: Optional[str] = None) -> List[Dict[str, Any]]:
        announcements = self.list_announcements(limit)
        if not announcements:
            return []
        announcement_ids = [str(item.get("id") or "").strip() for item in announcements if item.get("id")]
        if not announcement_ids:
            return announcements
        summary_map = {
            announcement_id: {"like_count": 0, "dislike_count": 0, "user_vote": None}
            for announcement_id in announcement_ids
        }
        with self._lock, self._engine.connect() as conn:
            vote_rows = conn.execute(
                select(
                    announcement_votes_table.c.announcement_id,
                    announcement_votes_table.c.vote_type,
                    func.count().label("count"),
                )
                .where(announcement_votes_table.c.announcement_id.in_(announcement_ids))
                .group_by(announcement_votes_table.c.announcement_id, announcement_votes_table.c.vote_type)
            ).fetchall()
            for row in vote_rows:
                announcement_id = str(row.announcement_id)
                if announcement_id not in summary_map:
                    continue
                if row.vote_type == "up":
                    summary_map[announcement_id]["like_count"] = int(row.count or 0)
                elif row.vote_type == "down":
                    summary_map[announcement_id]["dislike_count"] = int(row.count or 0)
            if username:
                user_row = conn.execute(
                    select(users_table.c.id).where(users_table.c.username == username)
                ).fetchone()
                if user_row:
                    user_votes = conn.execute(
                        select(
                            announcement_votes_table.c.announcement_id,
                            announcement_votes_table.c.vote_type,
                        )
                        .where(announcement_votes_table.c.announcement_id.in_(announcement_ids))
                        .where(announcement_votes_table.c.user_id == int(user_row.id))
                    ).fetchall()
                    for row in user_votes:
                        announcement_id = str(row.announcement_id)
                        if announcement_id in summary_map:
                            summary_map[announcement_id]["user_vote"] = row.vote_type
        merged: List[Dict[str, Any]] = []
        for item in announcements:
            announcement_id = str(item.get("id") or "").strip()
            summary = summary_map.get(announcement_id, {"like_count": 0, "dislike_count": 0, "user_vote": None})
            merged_item = dict(item)
            merged_item.update(summary)
            merged.append(merged_item)
        return merged

    def set_announcement_vote(self, announcement_id: str, username: str, vote_type: Optional[str]) -> Optional[Dict[str, Any]]:
        announcement_id = (announcement_id or "").strip()
        username = (username or "").strip()
        normalized_vote = (vote_type or "").strip().lower() or None
        if not announcement_id or not username:
            return None
        if normalized_vote not in {None, "up", "down"}:
            return None
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            announcement_exists = conn.execute(
                select(announcements_table.c.id).where(announcements_table.c.id == announcement_id)
            ).fetchone()
            if not announcement_exists:
                return None
            user_id = self._ensure_user(conn, username)
            if normalized_vote is None:
                conn.execute(
                    delete(announcement_votes_table)
                    .where(announcement_votes_table.c.announcement_id == announcement_id)
                    .where(announcement_votes_table.c.user_id == user_id)
                )
            else:
                existing = conn.execute(
                    select(announcement_votes_table.c.id)
                    .where(announcement_votes_table.c.announcement_id == announcement_id)
                    .where(announcement_votes_table.c.user_id == user_id)
                ).fetchone()
                if existing:
                    conn.execute(
                        update(announcement_votes_table)
                        .where(announcement_votes_table.c.id == int(existing.id))
                        .values(vote_type=normalized_vote, updated_at=now)
                    )
                else:
                    conn.execute(
                        insert(announcement_votes_table).values(
                            announcement_id=announcement_id,
                            user_id=user_id,
                            vote_type=normalized_vote,
                            created_at=now,
                            updated_at=now,
                        )
                    )
        announcement = self.list_announcements_with_votes(limit=500, username=username)
        for item in announcement:
            if str(item.get("id") or "") == announcement_id:
                return item
        return None

    # -- traffic state/events ---------------------------------------------
    def load_traffic_state(self) -> Optional[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(select(traffic_state_table.c.payload).where(traffic_state_table.c.id == 1)).fetchone()
        if not row:
            return None
        try:
            return json.loads(row[0])
        except Exception:
            return None

    def save_traffic_state(self, payload: Dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False)
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            conn.execute(delete(traffic_state_table).where(traffic_state_table.c.id == 1))
            conn.execute(traffic_state_table.insert().values(id=1, payload=data, updated_at=now))

    def append_traffic_event(self, event: Dict[str, Any], max_events: int) -> None:
        record = dict(event)
        meta = record.get("meta") or {}
        meta_json = json.dumps(meta, ensure_ascii=False)
        username = meta.get("username") if isinstance(meta, dict) else None
        is_guest = meta.get("is_guest") if isinstance(meta, dict) else None
        is_admin = meta.get("is_admin") if isinstance(meta, dict) else None
        with self._lock, self._engine.begin() as conn:
            conn.execute(
                traffic_events_table.insert().values(
                    ts=record.get("ts"),
                    ip=record.get("ip"),
                    action=record.get("action"),
                    status=record.get("status"),
                    username=username,
                    is_guest=self._coerce_bool_int(is_guest) if is_guest is not None else None,
                    is_admin=self._coerce_bool_int(is_admin) if is_admin is not None else None,
                    meta=meta_json,
                )
            )
            ids = conn.execute(
                select(traffic_events_table.c.id)
                .order_by(traffic_events_table.c.id.desc())
                .limit(max_events)
            ).scalars().all()
            if ids:
                min_keep = ids[-1]
                conn.execute(delete(traffic_events_table).where(traffic_events_table.c.id < min_keep))

    def recent_traffic_events(self, limit: int) -> List[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                select(
                    traffic_events_table.c.ts,
                    traffic_events_table.c.ip,
                    traffic_events_table.c.action,
                    traffic_events_table.c.status,
                    traffic_events_table.c.meta,
                )
                .order_by(traffic_events_table.c.id.desc())
                .limit(limit)
            ).fetchall()
        events: List[Dict[str, Any]] = []
        for row in reversed(rows):
            meta_raw = row.meta
            try:
                meta = json.loads(meta_raw) if meta_raw else {}
            except Exception:
                meta = {}
            events.append(
                {
                    "ts": row.ts,
                    "ip": row.ip,
                    "action": row.action,
                    "status": row.status,
                    "meta": meta,
                }
            )
        return events

    def clear_traffic_events(self) -> None:
        with self._lock, self._engine.begin() as conn:
            conn.execute(delete(traffic_events_table))

    def delete_traffic_events_for_user(self, username: str) -> int:
        if not username:
            return 0
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(delete(traffic_events_table).where(traffic_events_table.c.username == username))
            if result.rowcount:
                return result.rowcount
            pattern = f'%"username": "{username}"%'
            result = conn.execute(delete(traffic_events_table).where(traffic_events_table.c.meta.like(pattern)))
            return result.rowcount or 0

    # -- feedback ----------------------------------------------------------
    def add_feedback(self, payload: Dict[str, Any]) -> int:
        record = dict(payload)
        username = record.get("username")
        user_id = None
        with self._lock, self._engine.begin() as conn:
            if username:
                user_id = self._ensure_user(conn, str(username))
            result = conn.execute(
                feedback_table.insert().values(
                    user_id=user_id,
                    username=username,
                    email=record.get("email"),
                    message=record.get("message"),
                    status=record.get("status"),
                    created_at=record.get("created_at"),
                )
            )
            inserted = result.inserted_primary_key
            if inserted:
                try:
                    return int(inserted[0])
                except Exception:
                    return 0
            return 0

    def list_feedback(self, limit: int) -> List[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                select(feedback_table)
                .order_by(feedback_table.c.id.desc())
                .limit(limit)
            ).fetchall()
        return [dict(row._mapping) for row in rows]

    def update_feedback_status(self, feedback_id: int, status: str) -> bool:
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(
                update(feedback_table)
                .where(feedback_table.c.id == feedback_id)
                .values(status=status)
            )
            return result.rowcount > 0
