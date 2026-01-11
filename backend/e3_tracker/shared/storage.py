import json
import threading
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
    create_engine,
    delete,
    update,
    select,
    text,
)
from sqlalchemy.engine import Engine


metadata = MetaData()

user_cache_table = Table(
    "user_cache",
    metadata,
    Column("username", String(191), primary_key=True),
    Column("payload", Text, nullable=False),
)

google_tokens_table = Table(
    "google_tokens",
    metadata,
    Column("username", String(191), primary_key=True),
    Column("payload", Text, nullable=False),
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

traffic_state_table = Table(
    "traffic_state",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("payload", Text, nullable=False),
)

traffic_events_table = Table(
    "traffic_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ts", Float),
    Column("ip", String(128)),
    Column("action", String(128)),
    Column("status", String(32)),
    Column("meta", Text),
)

feedback_table = Table(
    "feedback",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("username", String(191)),
    Column("email", String(191)),
    Column("message", Text, nullable=False),
    Column("status", String(32)),
    Column("created_at", String(64)),
)


class PersistentStorage:
    """Database-backed persistence layer for caches, tokens, announcements, and metrics."""

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
        if raw.startswith("sqlite:///") or raw.startswith("mysql://") or raw.startswith("mysql+pymysql://") or raw.startswith("postgresql://") or raw.startswith("postgresql+"):
            return raw
        if "://" in raw:
            return raw
        path = Path(raw).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite:///{path.as_posix()}"

    # -- user cache ---------------------------------------------------------
    def save_user_cache(self, username: str, payload: Dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False)
        with self._lock, self._engine.begin() as conn:
            conn.execute(delete(user_cache_table).where(user_cache_table.c.username == username))
            conn.execute(user_cache_table.insert().values(username=username, payload=data))

    def load_user_cache(self, username: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                select(user_cache_table.c.payload).where(user_cache_table.c.username == username)
            ).fetchone()
        if not row:
            return None
        try:
            return json.loads(row[0])
        except Exception:
            return None

    def delete_user_cache(self, username: str) -> None:
        with self._lock, self._engine.begin() as conn:
            conn.execute(delete(user_cache_table).where(user_cache_table.c.username == username))

    # -- google tokens ------------------------------------------------------
    def save_google_tokens(self, username: str, payload: Dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False)
        with self._lock, self._engine.begin() as conn:
            conn.execute(delete(google_tokens_table).where(google_tokens_table.c.username == username))
            conn.execute(google_tokens_table.insert().values(username=username, payload=data))

    def load_google_tokens(self, username: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                select(google_tokens_table.c.payload).where(google_tokens_table.c.username == username)
            ).fetchone()
        if not row:
            return None
        try:
            return json.loads(row[0])
        except Exception:
            return None

    def clear_google_tokens(self, username: str) -> None:
        with self._lock, self._engine.begin() as conn:
            conn.execute(delete(google_tokens_table).where(google_tokens_table.c.username == username))

    # -- announcements ------------------------------------------------------
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
            result = conn.execute(
                delete(announcements_table).where(announcements_table.c.id == announcement_id)
            )
            return result.rowcount > 0

    def list_announcements(self, limit: int) -> List[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                select(announcements_table)
                .order_by(announcements_table.c.created_at.desc(), announcements_table.c.id.desc())
                .limit(limit)
            ).fetchall()
        return [dict(row._mapping) for row in rows]

    # -- traffic state/events -----------------------------------------------
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
        with self._lock, self._engine.begin() as conn:
            conn.execute(delete(traffic_state_table).where(traffic_state_table.c.id == 1))
            conn.execute(traffic_state_table.insert().values(id=1, payload=data))

    def append_traffic_event(self, event: Dict[str, Any], max_events: int) -> None:
        record = dict(event)
        meta = json.dumps(record.get("meta") or {}, ensure_ascii=False)
        with self._lock, self._engine.begin() as conn:
            conn.execute(
                traffic_events_table.insert().values(
                    ts=record.get("ts"),
                    ip=record.get("ip"),
                    action=record.get("action"),
                    status=record.get("status"),
                    meta=meta,
                )
            )
            # keep latest max_events rows
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
        """Best-effort removal of traffic events belonging to a specific username (matching meta JSON)."""
        if not username:
            return 0
        pattern = f'%\"username\": \"{username}\"%'
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(
                delete(traffic_events_table).where(traffic_events_table.c.meta.like(pattern))
            )
            return result.rowcount or 0

    # -- feedback ------------------------------------------------------------
    def add_feedback(self, payload: Dict[str, Any]) -> int:
        record = dict(payload)
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(
                feedback_table.insert().values(
                    username=record.get("username"),
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
