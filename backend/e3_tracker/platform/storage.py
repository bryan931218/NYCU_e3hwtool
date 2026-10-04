"""Combined deployment storage; repositories live under their owning site."""

import os

import threading

from datetime import datetime

from pathlib import Path
from typing import Any


from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


from e3_tracker.assignments.persistence.assignments import AssignmentsStorage
from e3_tracker.assignments.persistence.custom_todos import CustomTodosStorage
from e3_tracker.assignments.persistence.usage import AssignmentUsageStorage
from e3_tracker.assignments.persistence.membership import AssignmentMembershipStorage
from e3_tracker.assignments.persistence.notifications import NotificationStorage
from e3_tracker.assignments.persistence.custom_todo_notifications import (
    CustomTodoNotificationStorage,
)
from e3_tracker.study.persistence.videos import VideosStorage
from e3_tracker.study.persistence.study_time import StudyTimeStorage
from e3_tracker.study.persistence.recall import RecallStorage
from e3_tracker.study.persistence.uploads import UploadsStorage
from e3_tracker.study.persistence.assistant import AssistantStorage
from e3_tracker.platform.persistence.community import CommunityStorage
from e3_tracker.platform.persistence.accounts import AccountsStorage
from e3_tracker.assignments.persistence.google_tokens import GoogleTokensStorage
from e3_tracker.platform.persistence.migrations import run_migrations
from e3_tracker.platform.security import CredentialCipher


class PersistentStorage(
    AssignmentMembershipStorage,
    AssignmentUsageStorage,
    CustomTodoNotificationStorage,
    CustomTodosStorage,
    NotificationStorage,
    AssignmentsStorage,
    VideosStorage,
    StudyTimeStorage,
    RecallStorage,
    UploadsStorage,
    AssistantStorage,
    CommunityStorage,
    AccountsStorage,
    GoogleTokensStorage,
):
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
        key_root = Path(self._engine.url.database).resolve().parent if self._engine.dialect.name == "sqlite" and self._engine.url.database != ":memory:" else Path(os.getenv("E3_CACHE_DIR", ".localdata"))
        self._credential_cipher = CredentialCipher(key_root)
        self._initialize_recall_cache()
        try:
            self._ensure_schema()
            self.purge_expired_guest_data(force=True)
            self.migrate_google_credentials()
        except Exception:
            self._engine.dispose()
            raise

    def _ensure_schema(self) -> None:
        run_migrations(self._engine)

    @staticmethod
    def _normalize_url(raw: str) -> str:
        raw = PersistentStorage._normalize_filesystem_path(raw)
        if raw.startswith("postgres://"):
            return "postgresql+psycopg://" + raw[len("postgres://") :]
        if raw.startswith("postgresql://"):
            return "postgresql+psycopg://" + raw[len("postgresql://") :]
        if (
            raw.startswith("sqlite:///")
            or raw.startswith("mysql://")
            or raw.startswith("mysql+pymysql://")
            or raw.startswith("postgresql+")
        ):
            return raw
        if "://" in raw:
            return raw
        path = Path(raw).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite:///{path.as_posix()}"

    @staticmethod
    def _normalize_filesystem_path(raw: str) -> str:
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

    def _coerce_bool_int(self, value: Any) -> int:
        return 1 if bool(value) else 0
