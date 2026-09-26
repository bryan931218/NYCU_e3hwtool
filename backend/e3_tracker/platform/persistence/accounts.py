"""Persistence operations for accounts."""

from typing import Optional
import hashlib
import time
from cryptography.fernet import InvalidToken
from sqlalchemy import delete, insert, select, update
from sqlalchemy.exc import IntegrityError

from e3_tracker.platform.persistence.core_schema import (
    users_table,
    web_sessions_table,
    security_limits_table,
)


class AccountsStorage:
    def load_user_profile(self, username: str) -> dict:
        with self._lock, self._engine.connect() as conn:
            row = (
                conn.execute(
                    select(users_table.c.profile_name, users_table.c.profile_surname)
                    .where(users_table.c.username == username)
                ).mappings().first()
            )
        return {
            "name": (row["profile_name"] or "") if row else "",
            "surname": (row["profile_surname"] or "") if row else "",
        }

    def save_user_profile(self, username: str, name: str, surname: str) -> None:
        name, surname = str(name or "").strip(), str(surname or "").strip()
        if (
            not username or not name or len(name) > 128
            or not surname or len(surname) > 16
        ):
            return
        with self._lock, self._engine.begin() as conn:
            user_id = self._ensure_user(conn, username)
            conn.execute(
                update(users_table)
                .where(users_table.c.id == user_id)
                .values(profile_name=name, profile_surname=surname)
            )

    def list_user_profiles(self) -> list:
        with self._lock, self._engine.connect() as conn:
            rows = (
                conn.execute(
                    select(users_table.c.username, users_table.c.profile_name)
                    .where(users_table.c.is_guest == 0)
                    .order_by(users_table.c.username)
                ).mappings().all()
            )
        return [dict(row) for row in rows]

    def load_user_surname(self, username: str) -> str:
        with self._lock, self._engine.connect() as conn:
            return (
                conn.execute(
                    select(users_table.c.profile_surname).where(
                        users_table.c.username == username
                    )
                ).scalar()
                or ""
            )

    def save_user_surname(self, username: str, surname: str) -> None:
        surname = str(surname or "").strip()
        if not username or not surname or len(surname) > 16:
            return
        with self._lock, self._engine.begin() as conn:
            user_id = self._ensure_user(conn, username)
            conn.execute(
                update(users_table)
                .where(users_table.c.id == user_id)
                .values(profile_surname=surname)
            )

    def save_web_session(
        self,
        session_token: str,
        username: str,
        *,
        is_guest=False,
        is_admin=False,
        moodle_session=None,
        lifetime=86400,
    ) -> None:
        if not session_token or not username:
            return
        now = self._now_iso()
        session_token = hashlib.sha256(session_token.encode()).hexdigest()
        with self._lock, self._engine.begin() as conn:
            conn.execute(
                delete(web_sessions_table).where(
                    web_sessions_table.c.expires_at <= time.time()
                )
            )
            conn.execute(
                delete(web_sessions_table).where(
                    web_sessions_table.c.session_token == session_token
                )
            )
            conn.execute(
                insert(web_sessions_table).values(
                    session_token=session_token,
                    username=username,
                    created_at=now,
                    updated_at=now,
                    is_guest=int(bool(is_guest)),
                    is_admin=int(bool(is_admin) and not is_guest),
                    moodle_credential=self._credential_cipher.encrypt(
                        moodle_session, f"moodle:{username}"
                    ),
                    expires_at=time.time() + lifetime,
                )
            )

    def is_valid_web_session(self, session_token: str, username: str) -> bool:
        return bool(self.load_web_session(session_token, username))

    def load_web_session(self, session_token: str, username: str = None):
        if not session_token:
            return None
        digest = hashlib.sha256(session_token.encode()).hexdigest()
        with self._lock, self._engine.connect() as conn:
            row = (
                conn.execute(
                    select(web_sessions_table)
                    .where(web_sessions_table.c.session_token == digest)
                    .where(web_sessions_table.c.expires_at > time.time())
                    .limit(1)
                )
                .mappings()
                .first()
            )
        if not row or (username and row["username"] != username):
            return None
        try:
            credential = self._credential_cipher.decrypt(
                row["moodle_credential"], f"moodle:{row['username']}"
            )
        except (ValueError, InvalidToken):
            return None
        return {
            "username": row["username"],
            "is_guest": bool(row["is_guest"]),
            "is_admin": bool(row["is_admin"] and not row["is_guest"]),
            "moodle_session": credential,
        }

    def clear_web_session(self, session_token: str) -> None:
        if not session_token:
            return
        session_token = hashlib.sha256(session_token.encode()).hexdigest()
        with self._lock, self._engine.begin() as conn:
            conn.execute(
                delete(web_sessions_table).where(
                    web_sessions_table.c.session_token == session_token
                )
            )

    def consume_security_limit(self, key: str, limit: int, window: int) -> bool:
        now = time.time()
        digest = hashlib.sha256(key.encode()).hexdigest()
        with self._lock, self._engine.begin() as conn:
            conn.execute(
                delete(security_limits_table).where(
                    security_limits_table.c.expires_at <= now
                )
            )
            changed = conn.execute(
                update(security_limits_table)
                .where(security_limits_table.c.key == digest)
                .values(count=security_limits_table.c.count + 1)
            ).rowcount
            if not changed:
                try:
                    with conn.begin_nested():
                        conn.execute(
                            insert(security_limits_table).values(
                                key=digest, count=1, expires_at=now + window
                            )
                        )
                except IntegrityError:
                    conn.execute(
                        update(security_limits_table)
                        .where(security_limits_table.c.key == digest)
                        .values(count=security_limits_table.c.count + 1)
                    )
            count = conn.execute(
                select(security_limits_table.c.count).where(
                    security_limits_table.c.key == digest
                )
            ).scalar_one()
        return count <= limit

    def _ensure_user(
        self,
        conn,
        username: str,
        *,
        is_guest: Optional[bool] = None,
        is_admin: Optional[bool] = None,
    ) -> int:
        row = conn.execute(
            select(
                users_table.c.id, users_table.c.is_guest, users_table.c.is_admin
            ).where(users_table.c.username == username)
        ).fetchone()
        now = self._now_iso()
        if row:
            updates = {"last_seen": now}
            if is_guest is not None:
                updates["is_guest"] = 1 if is_guest else 0
            if is_admin is not None:
                updates["is_admin"] = 1 if is_admin else 0
            conn.execute(
                update(users_table).where(users_table.c.id == row.id).values(**updates)
            )
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
            row = conn.execute(
                select(users_table.c.id).where(users_table.c.username == username)
            ).fetchone()
            if not row:
                raise
            conn.execute(
                update(users_table)
                .where(users_table.c.id == row.id)
                .values(last_seen=now)
            )
            return int(row.id)
