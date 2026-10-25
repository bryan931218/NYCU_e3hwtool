"""Persistence operations for accounts."""

from typing import Optional
import hashlib
import json
import math
import re
import time
from cryptography.fernet import InvalidToken
from sqlalchemy import delete, func, insert, or_, select, update
from sqlalchemy.exc import IntegrityError
from e3_tracker.platform.guest_privacy import is_guest_identity
from e3_tracker.platform.services.profile_names import is_academic_unit_name
from .guest_cleanup import (
    delete_guest_accounts,
    guest_account_condition,
    purge_inactive_guests,
)

from e3_tracker.platform.persistence.core_schema import (
    users_table,
    web_sessions_table,
    security_limits_table,
)


class AccountsStorage:
    @staticmethod
    def _missing_identity():
        return or_(users_table.c.profile_name.is_(None), users_table.c.profile_name == "",
            users_table.c.username.like("Session-%") & or_(
                users_table.c.student_number.is_(None), users_table.c.student_number == ""))

    def load_student_number(self, username: str) -> str:
        with self._lock, self._engine.connect() as conn:
            return conn.execute(
                select(users_table.c.student_number)
                .where(users_table.c.username == username)
            ).scalar() or ""

    def save_student_number(self, username: str, student_number: str) -> bool:
        if not username.startswith("Session-") or not re.fullmatch(r"[0-9]{9}", student_number):
            return False
        with self._lock, self._engine.begin() as conn:
            uid = self._ensure_user(conn, username)
            # A later response must not silently replace an established identity.
            return bool(conn.execute(
                update(users_table).where(
                    users_table.c.id == uid,
                    or_(users_table.c.student_number.is_(None),
                        users_table.c.student_number == "",
                        users_table.c.student_number == student_number),
                ).values(student_number=student_number)
            ).rowcount)

    def claim_student_number_sync(self, username: str, now: float, *, force=False) -> bool:
        if not username or is_guest_identity(username):
            return False
        with self._lock, self._engine.begin() as conn:
            exists = conn.execute(select(users_table.c.id).where(users_table.c.username == username)).scalar()
            if not exists:
                authenticated = conn.execute(select(web_sessions_table.c.username).where(
                    web_sessions_table.c.username == username, web_sessions_table.c.is_guest == 0,
                    web_sessions_table.c.expires_at > now, web_sessions_table.c.moodle_credential.is_not(None),
                ).limit(1)).scalar()
                if not authenticated:
                    return False
                self._ensure_user(conn, username)
            claim = update(users_table).where(
                users_table.c.username == username,
                users_table.c.is_guest == 0,
            )
            if not force:
                # Replace legacy hour-long leases without bypassing current five-minute claims.
                claim = claim.where(self._missing_identity(), or_(users_table.c.student_number_sync_after <= now,
                                        users_table.c.student_number_sync_after > now + 300))
            return bool(conn.execute(
                claim.values(student_number_sync_after=now + 300)
            ).rowcount)

    def save_profile_metadata(self, username, details):
        """Retain only display metadata and fixed diagnostics, never upstream HTML."""
        allowed_reasons = {'success', 'name_missing', 'login_required', 'access_denied', 'dashboard_unavailable',
            'profile_unavailable', 'edit_unavailable', 'profile_link_missing', 'edit_link_missing',
            'profile_owner_missing', 'edit_owner_mismatch', 'student_number_field_missing',
            'student_number_unlocked', 'student_number_invalid', 'upstream_timeout', 'upstream_request',
            'internal_error', 'identity_conflict', 'identity_missing'}
        clean = {}
        if (isinstance(details.get('checked_at'), (int, float)) and not isinstance(details['checked_at'], bool)
                and math.isfinite(details['checked_at']) and details['checked_at'] >= 0):
            clean['checked_at'] = details['checked_at']
        if isinstance(details.get('grade'), int) and not isinstance(details['grade'], bool) and 1 <= details['grade'] <= 15:
            if isinstance(details.get('academic_year'), int) and 100 <= details['academic_year'] <= 199:
                clean.update(grade=details['grade'], academic_year=details['academic_year'])
                clean['grade_checked_at'] = clean.get('checked_at', time.time())
                if details.get('degree_code') in {'1', '2', '3', '4', '5'}:
                    clean['degree_code'] = details['degree_code']
        department = str(details.get('department') or '').strip()
        if department and len(department) <= 128 and not any(char in department for char in '<>@'):
            clean['department'] = department
            clean['department_checked_at'] = clean.get('checked_at', time.time())
        reason = details.get('reason')
        clean['reason'] = reason if reason in allowed_reasons else 'identity_missing'
        with self._lock, self._engine.begin() as conn:
            row = conn.execute(select(users_table.c.id, users_table.c.profile_metadata).where(
                users_table.c.username == username, ~guest_account_condition())).first()
            if not row:
                return
            try:
                prior = json.loads(row.profile_metadata or '{}')
            except (ValueError, TypeError):
                prior = {}
            # Failures don't erase earlier useful fields; stale grades are checked
            # against their academic year by the presentation layer.
            merged = {**(prior if isinstance(prior, dict) else {}), **clean}
            conn.execute(update(users_table).where(users_table.c.id == row.id).values(
                profile_metadata=json.dumps(merged, ensure_ascii=False)))

    def save_identity_enrichment(self, username, name, surname, student_number=""):
        """Save independently available fields without recreating deleted accounts."""
        with self._lock, self._engine.begin() as conn:
            row = conn.execute(select(users_table.c.id, users_table.c.student_number).where(
                users_table.c.username == username, ~guest_account_condition())).first()
            if not row:
                return False
            expected = username if re.fullmatch(r"[0-9]{9}", username) else row.student_number
            if student_number and (not re.fullmatch(r"[0-9]{9}", student_number)
                                   or expected and student_number != expected):
                return False
            values = {}
            if student_number and username.startswith("Session-"):
                values['student_number'] = student_number
            if name and len(name) <= 128 and surname and len(surname) <= 16 and not is_academic_unit_name(name):
                values.update(profile_name=name, profile_surname=surname)
            if not values:
                return False
            return bool(conn.execute(update(users_table).where(users_table.c.id == row.id,
                or_(users_table.c.student_number.is_(None), users_table.c.student_number == "",
                    users_table.c.student_number == (student_number or row.student_number)),
            ).values(**values)).rowcount)

    def list_session_identity_users(self, now: float, limit=10) -> list:
        with self._lock, self._engine.connect() as conn:
            return list(conn.execute(
                select(web_sessions_table.c.username).distinct()
                .outerjoin(users_table, users_table.c.username == web_sessions_table.c.username)
                .where(
                    web_sessions_table.c.is_guest == 0,
                    or_(users_table.c.id.is_(None), ~guest_account_condition()),
                    web_sessions_table.c.expires_at > now,
                    web_sessions_table.c.moodle_credential.is_not(None),
                    self._missing_identity(),
                    or_(func.coalesce(users_table.c.student_number_sync_after, 0) <= now,
                        users_table.c.student_number_sync_after > now + 300),
                ).order_by(web_sessions_table.c.username).limit(limit)
            ).scalars())

    def load_session_identity_user(self, username: str, now: float):
        if not username or is_guest_identity(username):
            return None
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                select(web_sessions_table).where(
                    web_sessions_table.c.username == username,
                    web_sessions_table.c.is_guest == 0,
                    web_sessions_table.c.expires_at > now,
                    web_sessions_table.c.moodle_credential.is_not(None),
                ).order_by(web_sessions_table.c.updated_at.desc()).limit(1)
            ).mappings().first()
        if not row:
            return None
        try:
            credential = self._credential_cipher.decrypt(
                row["moodle_credential"], f"moodle:{username}"
            )
        except (ValueError, InvalidToken):
            return None
        return {"username": username, "moodle_session": credential} if credential else None

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
            or is_academic_unit_name(name)
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
                    select(users_table.c.username, users_table.c.profile_name, users_table.c.student_number, users_table.c.profile_metadata)
                    .where(~guest_account_condition())
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
        is_guest = bool(is_guest or is_guest_identity(username))
        now = self._now_iso()
        session_token = hashlib.sha256(session_token.encode()).hexdigest()
        with self._lock, self._engine.begin() as conn:
            purge_inactive_guests(conn)
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
                    .limit(1)
                )
                .mappings()
                .first()
            )
        if not row or (username and row["username"] != username):
            return None
        if row["expires_at"] <= time.time():
            self.clear_web_session(session_token)
            return None
        try:
            credential = self._credential_cipher.decrypt(
                row["moodle_credential"], f"moodle:{row['username']}"
            )
        except (ValueError, InvalidToken):
            return None
        is_guest = bool(row["is_guest"] or is_guest_identity(row["username"]))
        return {
            "username": row["username"],
            "is_guest": is_guest,
            "is_admin": bool(row["is_admin"] and not is_guest),
            "moodle_session": credential,
        }

    def clear_web_session(self, session_token: str) -> None:
        if not session_token:
            return
        session_token = hashlib.sha256(session_token.encode()).hexdigest()
        with self._lock, self._engine.begin() as conn:
            row = (
                conn.execute(
                    select(web_sessions_table).where(
                        web_sessions_table.c.session_token == session_token
                    )
                ).mappings().first()
            )
            conn.execute(
                delete(web_sessions_table).where(
                    web_sessions_table.c.session_token == session_token
                )
            )
            if row and (row["is_guest"] or is_guest_identity(row["username"])):
                active = conn.execute(
                    select(web_sessions_table.c.session_token)
                    .where(web_sessions_table.c.username == row["username"])
                    .where(web_sessions_table.c.expires_at > time.time())
                ).first()
                if not active:
                    delete_guest_accounts(conn, [row["username"]])

    def purge_expired_guest_data(self, *, force=False) -> None:
        with self._lock:
            now = time.monotonic()
            previous = getattr(self, "_guest_cleanup_at", 0)
            if not force and previous and now - previous < 60:
                return
            with self._engine.begin() as conn:
                purge_inactive_guests(conn)
            self._guest_cleanup_at = now

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
        if is_guest_identity(username):
            is_guest, is_admin = True, False
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
