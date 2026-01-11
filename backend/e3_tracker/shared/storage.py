import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    delete,
    insert,
    select,
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
    Column("show_overdue", Integer, nullable=False, default=0),
    Column("show_completed", Integer, nullable=False, default=0),
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
fetch_runs_table = Table(
    "fetch_runs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, nullable=False),
    Column("fetched_at", String(64), nullable=False),
    Column("fetched_ts", Integer, nullable=False),
    Column("base_url", Text),
    Column("scope", String(32)),
    Column("login_method", String(32)),
    Column("excel_data", Text),
    Column("error_count", Integer, nullable=False, default=0),
)

course_snapshots_table = Table(
    "course_snapshots",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, nullable=False),
    Column("course_id", Integer, nullable=False),
    Column("title", Text, nullable=False),
    Column("url", Text),
    Column("detected_assign_links", Integer, nullable=False, default=0),
)

assignment_snapshots_table = Table(
    "assignment_snapshots",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, nullable=False),
    Column("course_id", Integer, nullable=False),
    Column("title", Text, nullable=False),
    Column("url", Text),
    Column("due_at", String(64)),
    Column("due_ts", Integer),
    Column("overdue", Integer, nullable=False, default=0),
    Column("completed", Integer, nullable=False, default=0),
    Column("raw_status_text", Text),
)

fetch_errors_table = Table(
    "fetch_errors",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, nullable=False),
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

class PersistentStorage:
    """Database-backed persistence with normalized storage and snapshots."""

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

    def _normalize_url(self, raw: str) -> str:
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

    def _now_iso(self) -> str:
        return datetime.utcnow().isoformat()

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

    # -- user preferences -------------------------------------------------
    def load_user_preferences(self, username: str) -> Dict[str, Any]:
        if not username:
            return {}
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                select(
                    user_preferences_table.c.view_mode,
                    user_preferences_table.c.show_overdue,
                    user_preferences_table.c.show_completed,
                )
                .select_from(user_preferences_table.join(users_table, user_preferences_table.c.user_id == users_table.c.id))
                .where(users_table.c.username == username)
            ).fetchone()
        if not row:
            return {}
        return {
            "view_mode": row.view_mode,
            "show_overdue": bool(row.show_overdue),
            "show_completed": bool(row.show_completed),
        }

    def save_user_preferences(self, username: str, prefs: Dict[str, Any]) -> None:
        if not username:
            return
        if not isinstance(prefs, dict):
            return
        view_mode = prefs.get("view_mode")
        show_overdue = self._coerce_bool_int(prefs.get("show_overdue"))
        show_completed = self._coerce_bool_int(prefs.get("show_completed"))
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            user_id = self._ensure_user(conn, username)
            conn.execute(delete(user_preferences_table).where(user_preferences_table.c.user_id == user_id))
            conn.execute(
                insert(user_preferences_table).values(
                    user_id=user_id,
                    view_mode=view_mode,
                    show_overdue=show_overdue,
                    show_completed=show_completed,
                    updated_at=now,
                )
            )

    # -- assignments snapshots -------------------------------------------
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
            run_result = conn.execute(
                insert(fetch_runs_table).values(
                    user_id=user_id,
                    fetched_at=fetched_at,
                    fetched_ts=fetched_ts,
                    base_url=None,
                    scope=None,
                    login_method=None,
                    excel_data=excel_data,
                    error_count=len(errors),
                )
            )
            run_id = int(run_result.inserted_primary_key[0])

            now = self._now_iso()
            for course in courses:
                try:
                    course_code = int(course.get("id"))
                except (TypeError, ValueError):
                    continue
                title = str(course.get("title") or "").strip()
                if not title:
                    title = f"Course {course_code}"
                url = course.get("url")
                row = conn.execute(
                    select(courses_table.c.id)
                    .where(courses_table.c.user_id == user_id)
                    .where(courses_table.c.course_code == course_code)
                ).fetchone()
                if row:
                    course_pk = int(row.id)
                    conn.execute(
                        update(courses_table)
                        .where(courses_table.c.id == course_pk)
                        .values(title=title, url=url, updated_at=now)
                    )
                else:
                    try:
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
                    except IntegrityError:
                        row = conn.execute(
                            select(courses_table.c.id)
                            .where(courses_table.c.user_id == user_id)
                            .where(courses_table.c.course_code == course_code)
                        ).fetchone()
                        if not row:
                            continue
                        course_pk = int(row.id)
                detected_links = course.get("detected_assign_links")
                if detected_links is None:
                    detected_links = len(course.get("assignments") or [])
                conn.execute(
                    insert(course_snapshots_table).values(
                        run_id=run_id,
                        course_id=course_pk,
                        title=title,
                        url=url,
                        detected_assign_links=int(detected_links),
                    )
                )
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
                    conn.execute(
                        insert(assignment_snapshots_table).values(
                            run_id=run_id,
                            course_id=course_pk,
                            title=title_val,
                            url=item.get("url"),
                            due_at=item.get("due_at"),
                            due_ts=due_ts_val,
                            overdue=self._coerce_bool_int(item.get("overdue")),
                            completed=self._coerce_bool_int(item.get("completed")),
                            raw_status_text=item.get("raw_status_text"),
                        )
                    )

            for err in errors:
                if not isinstance(err, dict):
                    continue
                course_code = err.get("course_id")
                try:
                    course_code_val = int(course_code) if course_code is not None else None
                except (TypeError, ValueError):
                    course_code_val = None
                conn.execute(
                    insert(fetch_errors_table).values(
                        run_id=run_id,
                        course_code=course_code_val,
                        course_title=err.get("course_title"),
                        assignment_title=err.get("assignment_title"),
                        message=str(err.get("message") or ""),
                    )
                )

    def load_user_cache(self, username: str) -> Optional[Dict[str, Any]]:
        if not username:
            return None
        with self._lock, self._engine.connect() as conn:
            user_row = conn.execute(
                select(users_table.c.id).where(users_table.c.username == username)
            ).fetchone()
            if not user_row:
                return None
            run_row = conn.execute(
                select(
                    fetch_runs_table.c.id,
                    fetch_runs_table.c.fetched_ts,
                    fetch_runs_table.c.excel_data,
                )
                .where(fetch_runs_table.c.user_id == user_row.id)
                .order_by(fetch_runs_table.c.fetched_ts.desc(), fetch_runs_table.c.id.desc())
                .limit(1)
            ).fetchone()
            if not run_row:
                return None
            run_id = int(run_row.id)

            course_rows = conn.execute(
                select(
                    course_snapshots_table.c.course_id,
                    course_snapshots_table.c.title,
                    course_snapshots_table.c.url,
                    course_snapshots_table.c.detected_assign_links,
                    courses_table.c.course_code,
                )
                .select_from(
                    course_snapshots_table.join(courses_table, course_snapshots_table.c.course_id == courses_table.c.id)
                )
                .where(course_snapshots_table.c.run_id == run_id)
            ).fetchall()

            courses: List[Dict[str, Any]] = []
            course_map: Dict[int, Dict[str, Any]] = {}
            for row in course_rows:
                entry = {
                    "id": row.course_code,
                    "title": row.title,
                    "url": row.url,
                    "assignments": [],
                    "detected_assign_links": row.detected_assign_links or 0,
                }
                courses.append(entry)
                course_map[int(row.course_id)] = entry

            assignment_rows = conn.execute(
                select(
                    assignment_snapshots_table.c.course_id,
                    assignment_snapshots_table.c.title,
                    assignment_snapshots_table.c.url,
                    assignment_snapshots_table.c.due_at,
                    assignment_snapshots_table.c.due_ts,
                    assignment_snapshots_table.c.overdue,
                    assignment_snapshots_table.c.completed,
                    assignment_snapshots_table.c.raw_status_text,
                    courses_table.c.course_code,
                )
                .select_from(
                    assignment_snapshots_table.join(courses_table, assignment_snapshots_table.c.course_id == courses_table.c.id)
                )
                .where(assignment_snapshots_table.c.run_id == run_id)
            ).fetchall()

            all_assignments: List[Dict[str, Any]] = []
            for row in assignment_rows:
                course_entry = course_map.get(int(row.course_id))
                course_title = course_entry["title"] if course_entry else f"Course {row.course_code}"
                item = {
                    "course_id": row.course_code,
                    "course_title": course_title,
                    "title": row.title,
                    "url": row.url,
                    "due_at": row.due_at,
                    "due_ts": row.due_ts,
                    "overdue": bool(row.overdue),
                    "completed": bool(row.completed),
                    "raw_status_text": row.raw_status_text,
                }
                if course_entry is None:
                    fallback = {
                        "id": row.course_code,
                        "title": course_title,
                        "url": None,
                        "assignments": [],
                        "detected_assign_links": 0,
                    }
                    courses.append(fallback)
                    course_map[int(row.course_id)] = fallback
                    course_entry = fallback
                course_entry["assignments"].append(item)
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
                ).where(fetch_errors_table.c.run_id == run_id)
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
            "excel_data": run_row.excel_data,
            "ts": run_row.fetched_ts,
        }
        prefs = self.load_user_preferences(username)
        if prefs:
            cache["preferences"] = prefs
        return cache

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
            run_ids = conn.execute(
                select(fetch_runs_table.c.id).where(fetch_runs_table.c.user_id == user_id)
            ).scalars().all()
            if run_ids:
                conn.execute(delete(assignment_snapshots_table).where(assignment_snapshots_table.c.run_id.in_(run_ids)))
                conn.execute(delete(course_snapshots_table).where(course_snapshots_table.c.run_id.in_(run_ids)))
                conn.execute(delete(fetch_errors_table).where(fetch_errors_table.c.run_id.in_(run_ids)))
            conn.execute(delete(fetch_runs_table).where(fetch_runs_table.c.user_id == user_id))
            conn.execute(delete(courses_table).where(courses_table.c.user_id == user_id))
            conn.execute(delete(user_preferences_table).where(user_preferences_table.c.user_id == user_id))
            conn.execute(delete(google_tokens_table).where(google_tokens_table.c.user_id == user_id))

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

    def delete_announcement(self, announcement_id: str) -> bool:
        with self._lock, self._engine.begin() as conn:
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
