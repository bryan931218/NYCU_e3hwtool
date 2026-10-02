"""Durable notification scheduling for account-synced custom todo items."""

import json
import time
from datetime import datetime
from typing import Optional

from sqlalchemy import insert, select, update

from e3_tracker.assignments.domain.notifications import digest
from e3_tracker.assignments.persistence.notification_schema import (
    line_bindings as bindings,
    notification_jobs as jobs,
    notification_settings as settings,
    push_subscriptions as subscriptions,
)
from e3_tracker.platform.constants import TAIPEI_TZ


class CustomTodoNotificationStorage:
    """Schedule/cancel custom todo reminders using the existing notification queue."""

    def _custom_todo_event_prefix(self, uid: str) -> str:
        return f"custom-due:{digest(uid)}:"

    def cancel_custom_todo_notifications(self, username: str, uid: str) -> int:
        prefix = self._custom_todo_event_prefix(uid)
        with self._lock, self._engine.begin() as conn:
            user_ids = self.custom_todo_account_user_ids(conn, username)
            if not user_ids:
                return 0
            result = conn.execute(
                update(jobs)
                .where(
                    jobs.c.user_id.in_(user_ids),
                    jobs.c.event_key.like(prefix + "%"),
                    jobs.c.state == "pending",
                )
                .values(state="cancelled")
            )
            return int(result.rowcount or 0)

    def schedule_custom_todo_notifications(
        self,
        username: str,
        item: dict,
        *,
        now: Optional[float] = None,
    ) -> int:
        now = time.time() if now is None else float(now)
        uid_value = str(item.get("uid") or "").strip()
        title = str(item.get("title") or "").strip()
        course = str(item.get("course") or "").strip() or "自訂代辦"
        due_ts = int(item.get("due_ts") or 0)
        uid_hash = digest(uid_value)
        prefix = self._custom_todo_event_prefix(uid_value)

        with self._lock, self._engine.begin() as conn:
            user_ids = self.custom_todo_account_user_ids(conn, username)
            if not user_ids:
                return 0

            conn.execute(
                update(jobs)
                .where(
                    jobs.c.user_id.in_(user_ids),
                    jobs.c.event_key.like(prefix + "%"),
                    jobs.c.state == "pending",
                )
                .values(state="cancelled")
            )
            if due_ts <= now:
                return 0

            due_text = datetime.fromtimestamp(due_ts, TAIPEI_TZ).strftime("%m/%d %H:%M")
            expires_at = float(due_ts)
            created = 0

            for user_id in user_ids:
                raw_preferences = conn.execute(
                    select(settings.c.preferences).where(settings.c.user_id == user_id)
                ).scalar()
                if not raw_preferences:
                    continue
                try:
                    preferences = json.loads(raw_preferences)
                except (TypeError, ValueError):
                    continue
                if not preferences.get("due_reminder"):
                    continue

                raw_days = preferences.get("days_before", [1])
                days = sorted(
                    {
                        int(day)
                        for day in raw_days
                        if type(day) is int and 1 <= int(day) <= 30
                    },
                    reverse=True,
                ) or [1]

                targets = []
                if preferences.get("browser_enabled"):
                    targets.extend(
                        ("browser", endpoint_hash)
                        for endpoint_hash in conn.execute(
                            select(subscriptions.c.endpoint_hash).where(
                                subscriptions.c.user_id == user_id
                            )
                        ).scalars()
                    )
                if preferences.get("line_enabled"):
                    targets.extend(
                        ("line", target_hash)
                        for target_hash in conn.execute(
                            select(bindings.c.target_hash).where(
                                bindings.c.user_id == user_id
                            )
                        ).scalars()
                    )
                if not targets:
                    continue

                scheduled_thresholds = []
                crossed = []
                for day in days:
                    trigger_at = due_ts - day * 86400
                    if trigger_at <= now:
                        crossed.append(day)
                    else:
                        scheduled_thresholds.append((day, float(trigger_at)))
                if crossed:
                    scheduled_thresholds.append((min(crossed), now))

                for day, trigger_at in scheduled_thresholds:
                    payload = {
                        "title": f"自訂待辦到期提醒 · {day} 天前",
                        "body": f"{course[:160]}\n{title[:160]}\n截止：{due_text}",
                        "url": "/",
                        "kind": "due",
                        "custom_todo": True,
                        "custom_uid": uid_value,
                        "uid_hash": uid_hash,
                        "due_ts": due_ts,
                        "days": day,
                    }
                    event_key = f"{prefix}{due_ts}:{day}"
                    for channel, target_hash in targets:
                        job_id = digest(
                            f"{user_id}:{event_key}:{channel}:{target_hash}"
                        )
                        existing = (
                            conn.execute(select(jobs).where(jobs.c.id == job_id))
                            .mappings()
                            .first()
                        )
                        values = {
                            "payload": json.dumps(payload, ensure_ascii=False),
                            "retry_at": trigger_at,
                            "expires_at": expires_at,
                            "error": None,
                        }
                        if existing:
                            if existing["state"] in {"sent", "sending"}:
                                continue
                            conn.execute(
                                update(jobs)
                                .where(jobs.c.id == job_id)
                                .values(
                                    state="pending",
                                    attempts=0,
                                    lease=None,
                                    **values,
                                )
                            )
                        else:
                            conn.execute(
                                insert(jobs).values(
                                    id=job_id,
                                    user_id=user_id,
                                    event_key=event_key,
                                    channel=channel,
                                    target_hash=target_hash,
                                    state="pending",
                                    attempts=0,
                                    created_at=now,
                                    lease=None,
                                    **values,
                                )
                            )
                        created += 1
            return created
