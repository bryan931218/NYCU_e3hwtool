"""Persistent assignment-only feature usage and credential-free adoption snapshots."""

import json
import time
from datetime import datetime, timedelta
from sqlalchemy import delete, select, func
from sqlalchemy.exc import IntegrityError
from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.platform.persistence.core_schema import users_table, traffic_events_table, web_sessions_table
from e3_tracker.platform.persistence.guest_cleanup import guest_account_condition
from e3_tracker.platform.guest_privacy import is_guest_identity
from e3_tracker.assignments.domain.usage import feature_for_event, student_identity
from .usage_schema import feature_usage, usage_state
from .notification_schema import line_bindings, push_subscriptions, notification_settings
from .schema import google_tokens_table


def increment_usage(conn, user_id, feature, day, *, once=False):
    values = {"user_id": user_id, "feature": feature, "day": day, "count": 1}
    if conn.dialect.name == "mysql":
        from sqlalchemy.dialects.mysql import insert
        statement = insert(feature_usage).values(**values)
        statement = statement.on_duplicate_key_update(count=feature_usage.c.count if once else feature_usage.c.count + 1)
    else:
        if conn.dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        else:
            from sqlalchemy.dialects.sqlite import insert
        statement = insert(feature_usage).values(**values)
        if once:
            statement = statement.on_conflict_do_nothing(index_elements=["user_id", "day", "feature"])
        else:
            statement = statement.on_conflict_do_update(
                index_elements=["user_id", "day", "feature"],
                set_={"count": feature_usage.c.count + 1},
            )
    conn.execute(statement)


def migrate_usage(conn):
    feature_usage.create(conn, checkfirst=True)
    usage_state.create(conn, checkfirst=True)
    now = time.time()
    cutoff = (datetime.fromtimestamp(now, TAIPEI_TZ).date() - timedelta(days=729)).isoformat()
    accounts = {row.username: row.id for row in conn.execute(
        select(users_table.c.id, users_table.c.username).where(~guest_account_condition())
    )}
    samples = 0
    for event in conn.execute(select(traffic_events_table)).mappings():
        try:
            meta = json.loads(event["meta"] or "{}")
            if not isinstance(meta, dict):
                continue
            username = event["username"] or meta.get("username")
            if event["is_guest"] or is_guest_identity(username):
                continue
            feature = feature_for_event(event["action"], event["status"], meta)
            day = datetime.fromtimestamp(float(event["ts"]), TAIPEI_TZ).date().isoformat()
        except (ValueError, TypeError, OverflowError, OSError):
            continue
        if feature and username in accounts and cutoff <= day:
            increment_usage(conn, accounts[username], feature, day)
            samples += 1
    conn.execute(usage_state.insert().values(id=1, started_at=now, legacy_samples=samples))


class AssignmentUsageStorage:
    def record_assignment_usage(self, username, action, status="success", meta=None, *, now=None):
        meta = meta or {}
        feature = feature_for_event(action, status, meta)
        if (not username or is_guest_identity(username) or meta.get("is_guest")
                or meta.get("site") not in {None, "", "assignments"}
                or str(action or "").strip().lower().startswith(("study_", "public_study_"))
                or meta.get("activity_only") or status not in {"success", "info"}):
            return
        day = datetime.fromtimestamp(time.time() if now is None else now, TAIPEI_TZ).date().isoformat()
        with self._lock, self._engine.begin() as conn:
            account = conn.execute(select(users_table.c.id, users_table.c.is_guest).where(
                users_table.c.username == username,
            )).first()
            if account and account.is_guest:
                return
            user_id = account.id if account else None
            if user_id is None and conn.execute(select(web_sessions_table.c.username).where(
                web_sessions_table.c.username == username, web_sessions_table.c.is_guest == 0,
                web_sessions_table.c.expires_at > time.time(),
            ).limit(1)).first():
                # A newly logged-in account can emit usage before its first cache/profile.
                # Use a savepoint so competing workers cannot abort the outer transaction.
                try:
                    with conn.begin_nested():
                        conn.execute(users_table.insert().values(
                            username=username, is_guest=0, is_admin=0,
                            created_at=self._now_iso(), last_seen=self._now_iso(),
                        ))
                except IntegrityError:
                    pass
                user_id = conn.execute(select(users_table.c.id).where(
                    users_table.c.username == username, ~guest_account_condition(),
                )).scalar()
            if user_id is not None:
                if feature:
                    increment_usage(conn, user_id, feature, day)
                else:
                    # Presence survives event rotation and is separate from feature counters.
                    increment_usage(conn, user_id, "__presence", day, once=True)

    def assignment_daily_user_count(self, *, now=None):
        day = datetime.fromtimestamp(time.time() if now is None else now, TAIPEI_TZ).date().isoformat()
        return self.assignment_daily_user_counts(start=day, end=day, now=now).get(day, 0)

    def assignment_daily_user_counts(self, *, start=None, end=None, now=None):
        today = datetime.fromtimestamp(time.time() if now is None else now, TAIPEI_TZ).date()
        start = start or (today - timedelta(days=729)).isoformat()
        end = end or today.isoformat()
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(select(feature_usage.c.day, users_table.c.username, users_table.c.student_number).join(
                users_table, users_table.c.id == feature_usage.c.user_id,
            ).where(
                ~guest_account_condition(),
                feature_usage.c.day >= start, feature_usage.c.day <= end,
            ).distinct()).mappings().all()
        members = {}
        for row in rows:
            members.setdefault(row["day"], set()).add(student_identity(row) or row["username"])
        counts = {day: len(identities) for day, identities in members.items()}
        if start <= today.isoformat() <= end:
            counts.setdefault(today.isoformat(), 0)
        return counts

    def assignment_usage_snapshot(self, start, end):
        cutoff = (datetime.now(TAIPEI_TZ).date() - timedelta(days=729)).isoformat()
        observation_end = (datetime.fromisoformat(end).date() + timedelta(days=7)).isoformat()
        with self._lock, self._engine.begin() as conn:
            conn.execute(delete(feature_usage).where(feature_usage.c.day < cutoff))
            accounts = [dict(row) for row in conn.execute(select(
                users_table.c.id, users_table.c.username, users_table.c.student_number,
            ).where(~guest_account_condition())).mappings()]
            usage = [dict(row) for row in conn.execute(select(
                feature_usage.c.user_id, feature_usage.c.feature,
                func.sum(feature_usage.c.count).label("count"),
            ).where(
                feature_usage.c.day >= start, feature_usage.c.day <= end,
                feature_usage.c.feature != "__presence",
            ).group_by(feature_usage.c.user_id, feature_usage.c.feature)).mappings()]
            state = dict(conn.execute(select(usage_state)).mappings().first())
            daily = [dict(row) for row in conn.execute(select(
                feature_usage.c.user_id, feature_usage.c.day,
            ).where(feature_usage.c.day >= start, feature_usage.c.day <= observation_end).distinct()).mappings()]
            # Select only ownership IDs; never load/decrypt notification targets or tokens.
            linked = {
                "line": set(conn.execute(select(line_bindings.c.user_id)).scalars()),
                "browser": set(conn.execute(select(push_subscriptions.c.user_id)).scalars()),
                "google": set(conn.execute(select(google_tokens_table.c.user_id).where(
                    google_tokens_table.c.refresh_token.is_not(None),
                    google_tokens_table.c.refresh_token != "",
                )).scalars()),
            }
            settings = [dict(row) for row in conn.execute(select(
                notification_settings.c.user_id, notification_settings.c.preferences,
            )).mappings()]
        return {"accounts": accounts, "usage": usage, "daily": daily, "state": state, "linked": linked, "settings": settings}

    def clear_assignment_usage(self, username=None):
        with self._lock, self._engine.begin() as conn:
            statement = delete(feature_usage)
            if username is not None:
                statement = statement.where(feature_usage.c.user_id.in_(
                    select(users_table.c.id).where(users_table.c.username == username)
                ))
            removed = conn.execute(statement).rowcount
            if username is None:
                conn.execute(usage_state.update().values(started_at=time.time(), legacy_samples=0))
            return removed
