"""First successful assignment login, independent of resettable traffic history."""

import hashlib
import re
import time
from datetime import datetime, timezone

from sqlalchemy import Column, Float, Integer, String, Table, select
from sqlalchemy.exc import IntegrityError

from e3_tracker.platform.guest_privacy import is_guest_identity
from e3_tracker.platform.persistence.core_schema import users_table, web_sessions_table
from e3_tracker.platform.persistence.guest_cleanup import guest_account_condition
from e3_tracker.platform.persistence.metadata import metadata


assignment_memberships = Table(
    "assignment_memberships", metadata,
    Column("identity_key", String(80), primary_key=True),
    Column("joined_at", Float, nullable=False),
    Column("is_new", Integer, nullable=False, default=0),
)


def membership_keys(username, student_number=""):
    account = "account:" + hashlib.sha256(username.encode()).hexdigest()
    number = username if re.fullmatch(r"[0-9]{9}", username) else student_number
    return (["student:" + number] if re.fullmatch(r"[0-9]{9}", number or "") else []) + [account]


def insert_membership(conn, key, joined_at, is_new):
    try:
        with conn.begin_nested():
            conn.execute(assignment_memberships.insert().values(
                identity_key=key, joined_at=joined_at, is_new=int(is_new),
            ))
        return True
    except IntegrityError:
        # The unique identity key also protects claims made by separate workers.
        if not conn.execute(select(assignment_memberships.c.identity_key).where(
            assignment_memberships.c.identity_key == key,
        )).first():
            raise
        return False


def migrate_memberships(conn):
    assignment_memberships.create(conn, checkfirst=True)
    existing = {}
    accounts = list(conn.execute(select(users_table).where(~guest_account_condition())).mappings())
    accounts.extend(conn.execute(select(
        web_sessions_table.c.username, web_sessions_table.c.created_at,
    ).where(web_sessions_table.c.is_guest == 0)).mappings())
    for row in accounts:
        if is_guest_identity(row["username"]):
            continue
        try:
            created = datetime.fromisoformat(row["created_at"])
            joined_at = (created if created.tzinfo else created.replace(tzinfo=timezone.utc)).timestamp()
        except (TypeError, ValueError, OverflowError, OSError):
            joined_at = time.time()
        for key in membership_keys(row["username"], row.get("student_number") or ""):
            existing[key] = min(existing.get(key, joined_at), joined_at)
    for key, joined_at in existing.items():
        insert_membership(conn, key, joined_at, False)


class AssignmentMembershipStorage:
    def claim_assignment_membership(self, username, *, now=None):
        if not username or is_guest_identity(username):
            return False
        with self._lock, self._engine.begin() as conn:
            if conn.dialect.name == "sqlite":
                conn.exec_driver_sql("BEGIN IMMEDIATE")
            row = conn.execute(select(users_table.c.student_number, users_table.c.is_guest).where(
                users_table.c.username == username,
            )).first()
            if row and row.is_guest:
                return False
            keys = membership_keys(username, row.student_number if row else "")
            previous = conn.execute(select(assignment_memberships).where(
                assignment_memberships.c.identity_key.in_(keys),
            ).order_by(assignment_memberships.c.joined_at)).mappings().first()
            joined_at = previous["joined_at"] if previous else (time.time() if now is None else now)
            is_new = bool(previous["is_new"]) if previous else True
            inserted = insert_membership(conn, keys[0], joined_at, is_new)
            canonical = conn.execute(select(assignment_memberships).where(
                assignment_memberships.c.identity_key == keys[0],
            )).mappings().one()
            for key in keys[1:]:
                insert_membership(conn, key, canonical["joined_at"], canonical["is_new"])
            return bool(inserted and not previous)

    def assignment_membership_snapshot(self, profiles):
        with self._lock, self._engine.connect() as conn:
            members = {row["identity_key"]: dict(row) for row in conn.execute(
                select(assignment_memberships),
            ).mappings()}
        result = {}
        for profile in profiles:
            matches = [members[key] for key in membership_keys(
                profile["username"], profile.get("student_number") or "",
            ) if key in members]
            if matches:
                result[profile["username"]] = min(matches, key=lambda item: item["joined_at"])
        return result
