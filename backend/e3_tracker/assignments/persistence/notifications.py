"""Durable notification state; every operation is scoped to an account."""

import json
import secrets
import time
from sqlalchemy import delete, insert, select, update, or_
from sqlalchemy.exc import IntegrityError
from urllib.parse import urlencode
from e3_tracker.platform.persistence.core_schema import users_table, web_sessions_table
from e3_tracker.platform.guest_privacy import is_guest_identity
from e3_tracker.assignments.domain.notifications import (
    DEFAULT_NOTIFICATION_PREFERENCES,
    active_assignments,
    digest,
    notification_payload,
    course_message_notification_payload,
)
from .notification_schema import (
    notification_settings as settings,
    notification_seen as seen,
    push_subscriptions as subscriptions,
    line_bindings as bindings,
    line_link_codes as codes,
    notification_jobs as jobs,
)


class NotificationStorage:
    def _notification_user(self, conn, username):
        return (
            conn.execute(
                select(users_table.c.id).where(
                    users_table.c.username == username, users_table.c.is_guest == 0
                )
            ).scalar()
            if not is_guest_identity(username)
            else None
        )

    def notification_preferences(self, username):
        with self._lock, self._engine.connect() as conn:
            uid = self._notification_user(conn, username)
            row = (
                conn.execute(select(settings).where(settings.c.user_id == uid))
                .mappings()
                .first()
            )
            linked = bool(
                conn.execute(
                    select(bindings.c.user_id).where(bindings.c.user_id == uid)
                ).first()
            )
            endpoints = list(
                conn.execute(
                    select(subscriptions.c.endpoint_hash).where(
                        subscriptions.c.user_id == uid
                    )
                ).scalars()
            )
        return {
            "preferences": (
                {**DEFAULT_NOTIFICATION_PREFERENCES, **json.loads(row["preferences"])}
                if row
                else dict(DEFAULT_NOTIFICATION_PREFERENCES)
            ),
            "line_linked": linked,
            "browser_devices": len(endpoints),
            "browser_endpoint_hashes": endpoints,
            "sync_error": row["sync_error"] if row else None,
        }

    def save_notification_preferences(self, username, preferences):
        with self._lock, self._engine.begin() as conn:
            uid = self._ensure_user(conn, username)
            values = {"preferences": json.dumps(preferences), "sync_after": 0}
            if not conn.execute(
                update(settings).where(settings.c.user_id == uid).values(**values)
            ).rowcount:
                conn.execute(
                    insert(settings).values(user_id=uid, initialized=0, **values)
                )
            conn.execute(
                update(jobs)
                .where(jobs.c.user_id == uid, jobs.c.state == "pending")
                .values(state="cancelled")
            )

    def store_push_subscription(self, username, subscription):
        endpoint_hash = digest(subscription["endpoint"])
        with self._lock, self._engine.begin() as conn:
            uid = self._ensure_user(conn, username)
            old = (
                conn.execute(
                    select(subscriptions).where(
                        subscriptions.c.endpoint_hash == endpoint_hash
                    )
                )
                .mappings()
                .first()
            )
            if (
                not old
                and len(
                    conn.execute(
                        select(subscriptions.c.endpoint_hash).where(
                            subscriptions.c.user_id == uid
                        )
                    ).all()
                )
                >= 10
            ):
                raise ValueError("最多啟用 10 個裝置，請先移除不使用的裝置")
            if old:
                old_data = json.loads(
                    self._credential_cipher.decrypt(
                        old["subscription"], f"push:{endpoint_hash}"
                    )
                )
                if old_data["keys"] != subscription["keys"]:
                    raise ValueError("此推播訂閱無法驗證，請重新啟用")
                conn.execute(
                    delete(subscriptions).where(
                        subscriptions.c.endpoint_hash == endpoint_hash
                    )
                )
                conn.execute(
                    update(jobs)
                    .where(
                        jobs.c.target_hash == endpoint_hash, jobs.c.state == "pending"
                    )
                    .values(state="cancelled")
                )
            conn.execute(
                insert(subscriptions).values(
                    user_id=uid,
                    endpoint_hash=endpoint_hash,
                    subscription=self._credential_cipher.encrypt(
                        json.dumps(subscription), f"push:{endpoint_hash}"
                    ),
                )
            )
        return endpoint_hash

    def remove_push_subscription(self, username, endpoint_hash):
        with self._lock, self._engine.begin() as conn:
            uid = self._notification_user(conn, username)
            removed = conn.execute(
                delete(subscriptions).where(
                    subscriptions.c.user_id == uid,
                    subscriptions.c.endpoint_hash == endpoint_hash,
                )
            )
            conn.execute(
                update(jobs)
                .where(
                    jobs.c.user_id == uid,
                    jobs.c.target_hash == endpoint_hash,
                    jobs.c.state == "pending",
                )
                .values(state="cancelled")
            )
        return bool(removed.rowcount)

    def create_line_link_code(self, username):
        code = secrets.token_urlsafe(18)
        with self._lock, self._engine.begin() as conn:
            uid = self._ensure_user(conn, username)
            conn.execute(
                delete(codes).where(
                    or_(codes.c.user_id == uid, codes.c.expires_at <= time.time())
                )
            )
            conn.execute(
                insert(codes).values(
                    user_id=uid, code_hash=digest(code), expires_at=time.time() + 600
                )
            )
        return code

    def consume_line_link_code(self, code, target, *, return_username=False):
        target_hash = digest(target)
        with self._lock, self._engine.begin() as conn:
            # The delete is the atomic claim, including across web workers.
            row = (
                conn.execute(
                    select(codes).where(
                        codes.c.code_hash == digest(code),
                        codes.c.expires_at > time.time(),
                    )
                )
                .mappings()
                .first()
            )
            if not row:
                return False
            username = conn.execute(select(users_table.c.username).where(
                users_table.c.id == row["user_id"], users_table.c.is_guest == 0,
            )).scalar()
            if not username or is_guest_identity(username):
                return False
            owner = conn.execute(
                select(bindings.c.user_id).where(bindings.c.target_hash == target_hash)
            ).scalar()
            if owner is not None and owner != row["user_id"]:
                return False
            if not conn.execute(
                delete(codes).where(
                    codes.c.code_hash == digest(code), codes.c.expires_at > time.time()
                )
            ).rowcount:
                return False
            try:
                with conn.begin_nested():
                    conn.execute(
                        delete(bindings).where(bindings.c.user_id == row["user_id"])
                    )
                    conn.execute(
                        insert(bindings).values(
                            user_id=row["user_id"],
                            target_hash=target_hash,
                            target=self._credential_cipher.encrypt(
                                target, f"line:{row['user_id']}"
                            ),
                        )
                    )
            except IntegrityError:
                return False
        return username if return_username else True

    def unlink_line(self, username=None, target_hash=None):
        with self._lock, self._engine.begin() as conn:
            condition = (
                bindings.c.target_hash == target_hash
                if target_hash
                else bindings.c.user_id == self._notification_user(conn, username)
            )
            uid = (
                self._notification_user(conn, username)
                if username
                else conn.execute(select(bindings.c.user_id).where(condition)).scalar()
            )
            owner = conn.execute(select(users_table.c.username).join(
                bindings, bindings.c.user_id == users_table.c.id,
            ).where(condition)).scalar()
            removed = conn.execute(delete(bindings).where(condition))
            if uid is not None:
                conn.execute(delete(codes).where(codes.c.user_id == uid))
                conn.execute(
                    update(jobs)
                    .where(
                        jobs.c.user_id == uid,
                        jobs.c.channel == "line",
                        jobs.c.state == "pending",
                    )
                    .values(state="cancelled")
                )
        return owner if removed.rowcount else None

    def observe_notification_assignments(
        self, username, result, semester_key, *, now=None, baseline=False
    ):
        now = time.time() if now is None else now
        ignored = self.load_user_preferences(username).get("ignored_assignment_uids", [])
        items = active_assignments(result, semester_key, self.assignment_uid, ignored)
        with self._lock, self._engine.begin() as conn:
            uid = self._notification_user(conn, username)
            # Also takes a write lock in SQLite before reading initialization state.
            conn.execute(
                update(settings)
                .where(settings.c.user_id == uid)
                .values(initialized=settings.c.initialized)
            )
            row = (
                conn.execute(
                    select(settings).where(settings.c.user_id == uid).with_for_update()
                )
                .mappings()
                .first()
            )
            if not row:
                return
            prefs = json.loads(row["preferences"])
            known = set(
                conn.execute(
                    select(seen.c.uid_hash).where(seen.c.user_id == uid)
                ).scalars()
            )
            targets = []
            if prefs["browser_enabled"]:
                targets.extend(
                    ("browser", key)
                    for key in conn.execute(
                        select(subscriptions.c.endpoint_hash).where(
                            subscriptions.c.user_id == uid
                        )
                    ).scalars()
                )
            if prefs["line_enabled"]:
                targets.extend(
                    ("line", key)
                    for key in conn.execute(
                        select(bindings.c.target_hash).where(bindings.c.user_id == uid)
                    ).scalars()
                )
            for key, item in items.items():
                if key not in known:
                    conn.execute(insert(seen).values(user_id=uid, uid_hash=key))
                    if (
                        row["initialized"]
                        and not baseline
                        and prefs["new_assignment"]
                        and (not item.get("due_ts") or item["due_ts"] > now)
                    ):
                        self._queue_notification(
                            conn,
                            uid,
                            f"new:{key}",
                            targets,
                            {
                                **notification_payload(item, "new"),
                                "kind": "new",
                                "uid_hash": key,
                            },
                            now,
                            now + 86400,
                        )
                due = item.get("due_ts")
                if baseline or not prefs["due_reminder"] or not due or due <= now:
                    continue
                # Catch up only the nearest threshold when first enabled or synced late.
                thresholds = [
                    day for day in prefs["days_before"] if now >= due - day * 86400
                ]
                if thresholds:
                    day = min(thresholds)
                    self._queue_notification(
                        conn,
                        uid,
                        f"due:{key}:{int(due)}:{day}",
                        targets,
                        {
                            **notification_payload(item, "due", day),
                            "kind": "due",
                            "uid_hash": key,
                            "due_ts": int(due),
                            "days": day,
                        },
                        now,
                        min(due, now + 86400),
                    )
            conn.execute(
                update(settings).where(settings.c.user_id == uid).values(initialized=1)
            )

    def _queue_notification(self, conn, uid, event_key, targets, payload, now, expires):
        for channel, target in targets:
            job_id = digest(f"{uid}:{event_key}:{channel}:{target}")
            if conn.execute(select(jobs.c.id).where(jobs.c.id == job_id)).first():
                continue
            with conn.begin_nested():
                conn.execute(
                    insert(jobs).values(
                        id=job_id,
                        user_id=uid,
                        event_key=event_key,
                        channel=channel,
                        target_hash=target,
                        payload=self._credential_cipher.encrypt(json.dumps(payload, ensure_ascii=False), f'notification:{job_id}')
                        if payload.get('kind') in {'new_announcement', 'new_mail'} else json.dumps(payload, ensure_ascii=False),
                        state="pending",
                        attempts=0,
                        retry_at=now,
                        expires_at=expires,
                        created_at=now,
                    )
                )

    def _observe_course_message_notifications(self, conn, username, uid, kind, semester, items, successful_courses, *, baseline=False):
        from e3_tracker.assignments.services.collector import current_semester_key
        if semester != current_semester_key() or kind not in {'announcements', 'mail'}:
            return
        # The caller owns the account row write lock and commits cache + outbox together.
        row = conn.execute(select(settings).where(settings.c.user_id == uid).with_for_update()).mappings().first()
        if not row:
            return
        prefs = {**DEFAULT_NOTIFICATION_PREFERENCES, **json.loads(row['preferences'])}
        known = set(conn.execute(select(seen.c.uid_hash).where(seen.c.user_id == uid)).scalars())
        targets = []
        if prefs['browser_enabled']:
            targets.extend(('browser', key) for key in conn.execute(select(subscriptions.c.endpoint_hash).where(subscriptions.c.user_id == uid)).scalars())
        if prefs['line_enabled']:
            targets.extend(('line', key) for key in conn.execute(select(bindings.c.target_hash).where(bindings.c.user_id == uid)).scalars())
        preference = 'new_mail' if kind == 'mail' else 'new_announcement'
        now = time.time()
        for course_id in successful_courses:
            marker = digest(f'course-message-baseline:{kind}:{semester}:{course_id}')
            initial = baseline or marker not in known
            for item in items:
                if int(item['course_id']) != int(course_id):
                    continue
                key = digest(f'course-message:{kind}:{semester}:{item["key"]}')
                if key in known:
                    continue
                conn.execute(insert(seen).values(user_id=uid, uid_hash=key))
                known.add(key)
                # Older messages revealed by pagination/deletions must not become new alerts.
                timestamp = item.get('updated_ts')
                if initial or not prefs[preference] or not timestamp or not now - 86400 <= timestamp <= now + 300:
                    continue
                self._queue_notification(conn, uid, f'{preference}:{key}', targets, {
                    'kind': preference, 'message_kind': kind, 'message_key': item['key'], 'semester': semester,
                    **course_message_notification_payload(item, kind),
                    'url': '/courses/messages?' + urlencode({'tab':kind, 'semester':semester, 'item':item['key']}),
                }, now, now + 86400)
            if marker not in known:
                conn.execute(insert(seen).values(user_id=uid, uid_hash=marker))
                known.add(marker)

    def baseline_course_message_notifications(self, username, kind, semester, items):
        with self._lock, self._engine.begin() as conn:
            uid = self._announcement_user(conn, username, write=True)
            if uid:
                self._observe_course_message_notifications(conn, username, uid, kind, semester, items,
                    {item['course_id'] for item in items}, baseline=True)

    def notification_users(self):
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                select(users_table.c.username, settings.c.preferences)
                .join(settings, settings.c.user_id == users_table.c.id)
                .where(users_table.c.is_guest == 0)
            ).all()
        return [
            row.username
            for row in rows
            if any(
                json.loads(row.preferences).get(key)
                for key in ("browser_enabled", "line_enabled")
            )
        ]

    def notification_sync_user(self, username, now):
        with self._lock, self._engine.begin() as conn:
            uid = self._notification_user(conn, username)
            claimed = conn.execute(
                update(settings)
                .where(settings.c.user_id == uid, settings.c.sync_after <= now)
                .values(sync_after=now + 900)
            ).rowcount
            if not claimed:
                return None
            row = (
                conn.execute(
                    select(web_sessions_table)
                    .where(
                        web_sessions_table.c.username == username,
                        web_sessions_table.c.is_guest == 0,
                        web_sessions_table.c.expires_at > now,
                    )
                    .order_by(web_sessions_table.c.updated_at.desc())
                    .limit(1)
                )
                .mappings()
                .first()
            )
            conn.execute(
                update(settings)
                .where(settings.c.user_id == uid)
                .values(sync_error=None if row else "session_expired")
            )
        if not row:
            return None
        credential = self._credential_cipher.decrypt(
            row["moodle_credential"], f"moodle:{username}"
        )
        return (
            {"username": username, "moodle_session": credential} if credential else None
        )

    def notification_sync_error(self, username):
        with self._lock, self._engine.begin() as conn:
            uid = self._notification_user(conn, username)
            conn.execute(
                update(settings)
                .where(settings.c.user_id == uid)
                .values(sync_error="refresh_failed")
            )

    def claim_notification_jobs(self, now, limit=50):
        claimed = []
        with self._lock, self._engine.begin() as conn:
            conn.execute(
                update(jobs)
                .where(
                    jobs.c.state.in_(["pending", "sending"]), jobs.c.expires_at <= now
                )
                .values(state="cancelled")
            )
            conn.execute(delete(jobs).where(jobs.c.expires_at < now - 30 * 86400))
            conn.execute(delete(codes).where(codes.c.expires_at <= now))
            eligible = (
                jobs.c.state.in_(["pending", "sending"])
                & (jobs.c.retry_at <= now)
                & (jobs.c.attempts < 5)
            )
            rows = (
                conn.execute(
                    select(jobs)
                    .where(eligible)
                    .order_by(jobs.c.created_at)
                    .limit(limit)
                )
                .mappings()
                .all()
            )
            for row in rows:
                lease = secrets.token_hex(16)
                if conn.execute(
                    update(jobs)
                    .where(jobs.c.id == row["id"], eligible)
                    .values(
                        state="sending",
                        lease=lease,
                        retry_at=now + 300,
                        attempts=jobs.c.attempts + 1,
                    )
                ).rowcount:
                    claimed.append(
                        {**row, "lease": lease, "attempts": row["attempts"] + 1}
                    )
        return claimed

    def notification_delivery(self, job):
        with self._lock, self._engine.connect() as conn:
            username = conn.execute(
                select(users_table.c.username).where(users_table.c.id == job["user_id"])
            ).scalar()
            row = conn.execute(
                select(settings.c.preferences).where(
                    settings.c.user_id == job["user_id"]
                )
            ).scalar()
            if not username or not row:
                return None
            prefs = json.loads(row)
            raw = job['payload']
            if raw.startswith(self._credential_cipher.PREFIX):
                raw = self._credential_cipher.decrypt(raw, f'notification:{job["id"]}')
            payload = json.loads(raw)
            preference = {'new':'new_assignment', 'due':'due_reminder',
                          'new_announcement':'new_announcement', 'new_mail':'new_mail'}.get(payload.get('kind'))
            if (
                not preference or not prefs.get(f"{job['channel']}_enabled", False)
                or not prefs.get(preference, False)
            ):
                return None
            if payload["kind"] == "due" and payload["days"] not in prefs["days_before"]:
                return None
            table = subscriptions if job["channel"] == "browser" else bindings
            key = (
                table.c.endpoint_hash
                if job["channel"] == "browser"
                else table.c.target_hash
            )
            target = (
                conn.execute(
                    select(table).where(
                        table.c.user_id == job["user_id"], key == job["target_hash"]
                    )
                )
                .mappings()
                .first()
            )
        if not target:
            return None
        if job["channel"] == "browser":
            value = json.loads(
                self._credential_cipher.decrypt(
                    target["subscription"], f"push:{job['target_hash']}"
                )
            )
        else:
            value = self._credential_cipher.decrypt(
                target["target"], f"line:{job['user_id']}"
            )
        return username, prefs, payload, value

    def notification_test_target(self, username, channel, endpoint_hash=""):
        with self._lock, self._engine.connect() as conn:
            uid = self._notification_user(conn, username)
            if channel == "browser":
                row = conn.execute(
                    select(subscriptions.c.subscription).where(
                        subscriptions.c.user_id == uid,
                        subscriptions.c.endpoint_hash == endpoint_hash,
                    )
                ).scalar()
                return (
                    json.loads(
                        self._credential_cipher.decrypt(row, f"push:{endpoint_hash}")
                    )
                    if row
                    else None
                )
            if channel == "line":
                row = conn.execute(
                    select(bindings.c.target).where(bindings.c.user_id == uid)
                ).scalar()
                return (
                    self._credential_cipher.decrypt(row, f"line:{uid}") if row else None
                )
            return None

    def finish_notification_job(self, job, state, error=None, *, now=None):
        now = time.time() if now is None else now
        if state == "pending" and job["attempts"] >= 5:
            state = "failed"
        with self._lock, self._engine.begin() as conn:
            conn.execute(
                update(jobs)
                .where(
                    jobs.c.id == job["id"],
                    jobs.c.lease == job["lease"],
                    jobs.c.state == "sending",
                )
                .values(
                    state=state,
                    error=error,
                    retry_at=now + min(3600, 60 * 2 ** job["attempts"]),
                )
            )
