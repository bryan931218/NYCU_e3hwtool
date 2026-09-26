"""E3 Google Calendar token persistence."""

from typing import Any, Dict, Optional
from sqlalchemy import delete, insert, select, update
from e3_tracker.platform.persistence.core_schema import users_table
from .schema import google_tokens_table


class GoogleTokensStorage:
    def save_google_tokens(self, username: str, payload: Dict[str, Any]) -> None:
        if not username:
            return
        if not isinstance(payload, dict):
            return
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            user_id = self._ensure_user(conn, username)
            conn.execute(
                delete(google_tokens_table).where(
                    google_tokens_table.c.user_id == user_id
                )
            )
            conn.execute(
                insert(google_tokens_table).values(
                    user_id=user_id,
                    access_token=self._credential_cipher.encrypt(
                        payload.get("access_token"), f"google:access:{user_id}"
                    ),
                    refresh_token=self._credential_cipher.encrypt(
                        payload.get("refresh_token"), f"google:refresh:{user_id}"
                    ),
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
                    google_tokens_table.c.user_id,
                    google_tokens_table.c.access_token,
                    google_tokens_table.c.refresh_token,
                    google_tokens_table.c.scope,
                    google_tokens_table.c.token_type,
                    google_tokens_table.c.expires_at,
                )
                .select_from(
                    google_tokens_table.join(
                        users_table, google_tokens_table.c.user_id == users_table.c.id
                    )
                )
                .where(users_table.c.username == username)
            ).fetchone()
        if not row:
            return None
        return {
            "access_token": self._credential_cipher.decrypt(
                row.access_token, f"google:access:{row.user_id}"
            ),
            "refresh_token": self._credential_cipher.decrypt(
                row.refresh_token, f"google:refresh:{row.user_id}"
            ),
            "scope": row.scope,
            "token_type": row.token_type,
            "expires_at": row.expires_at,
        }

    def migrate_google_credentials(self):
        with self._lock, self._engine.begin() as conn:
            for row in conn.execute(select(google_tokens_table)).mappings().all():
                values = {}
                for field, label in (
                    ("access_token", "access"),
                    ("refresh_token", "refresh"),
                ):
                    value = row[field]
                    context = f"google:{label}:{row['user_id']}"
                    if value and not value.startswith(self._credential_cipher.PREFIX):
                        values[field] = self._credential_cipher.encrypt(value, context)
                    elif value:
                        # A missing/wrong key fails startup instead of overwriting credentials.
                        self._credential_cipher.decrypt(value, context)
                if values:
                    conn.execute(
                        update(google_tokens_table)
                        .where(google_tokens_table.c.user_id == row["user_id"])
                        .values(**values)
                    )

    def clear_google_tokens(self, username: str) -> None:
        if not username:
            return
        with self._lock, self._engine.begin() as conn:
            user_row = conn.execute(
                select(users_table.c.id).where(users_table.c.username == username)
            ).fetchone()
            if not user_row:
                return
            conn.execute(
                delete(google_tokens_table).where(
                    google_tokens_table.c.user_id == user_row.id
                )
            )
