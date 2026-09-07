"""Persistence operations for accounts."""
from typing import Any, Dict, Optional
from sqlalchemy import delete, insert, select

from .schema import users_table, google_tokens_table, web_sessions_table


class AccountsStorage:
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
