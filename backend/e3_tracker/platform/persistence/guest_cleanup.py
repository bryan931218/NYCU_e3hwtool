"""Guest cache is temporary; prune abandoned accounts and legacy guest telemetry."""

import json
import time

from sqlalchemy import delete, or_, select, update

from e3_tracker.assignments.persistence.cleanup import delete_assignment_account_data
from e3_tracker.platform.guest_privacy import (
    is_guest_event,
    is_guest_identity,
    sanitize_traffic_event,
    without_guest_traffic,
)
from .core_schema import (
    announcement_votes_table,
    feedback_table,
    traffic_events_table,
    traffic_state_table,
    users_table,
    web_sessions_table,
)


def guest_account_condition():
    # startswith escapes the underscore so ordinary account names cannot match it.
    return or_(
        users_table.c.is_guest == 1,
        users_table.c.username.startswith("\u8a2a\u5ba2_", autoescape=True),
    )


def delete_guest_accounts(conn, usernames):
    if not usernames:
        return
    user_ids = list(
        conn.execute(
            select(users_table.c.id).where(users_table.c.username.in_(usernames))
        ).scalars()
    )
    if user_ids:
        delete_assignment_account_data(conn, user_ids)
        conn.execute(
            delete(announcement_votes_table).where(
                announcement_votes_table.c.user_id.in_(user_ids)
            )
        )
    conn.execute(
        delete(feedback_table).where(
            or_(
                feedback_table.c.user_id.in_(user_ids),
                feedback_table.c.username.in_(usernames),
            )
        )
    )
    conn.execute(
        delete(web_sessions_table).where(web_sessions_table.c.username.in_(usernames))
    )
    conn.execute(delete(users_table).where(users_table.c.username.in_(usernames)))


def purge_inactive_guests(conn, now=None):
    now = time.time() if now is None else now
    guests = set(
        conn.execute(
            select(users_table.c.username).where(guest_account_condition())
        ).scalars()
    )
    guests.update(
        conn.execute(
            select(web_sessions_table.c.username).where(
                web_sessions_table.c.is_guest == 1
            )
        ).scalars()
    )
    active = set(
        conn.execute(
            select(web_sessions_table.c.username).where(
                web_sessions_table.c.expires_at > now
            )
        ).scalars()
    )
    inactive = guests - active
    delete_guest_accounts(conn, inactive)
    conn.execute(
        delete(web_sessions_table).where(web_sessions_table.c.expires_at <= now)
    )
    if guests & active:
        conn.execute(
            update(users_table)
            .where(users_table.c.username.in_(guests & active))
            .values(is_guest=1, is_admin=0)
        )
    return guests


def remove_legacy_guest_traffic(conn, guest_names):
    for row in conn.execute(select(traffic_events_table)).mappings().all():
        try:
            meta = json.loads(row["meta"] or "{}")
        except (TypeError, ValueError):
            meta = {}
        event = {
            "ts": row["ts"], "ip": row["ip"], "action": row["action"],
            "status": row["status"], "meta": meta,
        }
        if (
            row["is_guest"]
            or row["username"] in guest_names
            or is_guest_identity(row["username"])
            or is_guest_event(event)
            or (isinstance(meta, dict) and meta.get("username") in guest_names)
        ):
            cleaned = (
                sanitize_traffic_event(event)
                if str(row["action"] or "").strip().lower() == "guest_login"
                else None
            )
            if cleaned is not None:
                conn.execute(
                    update(traffic_events_table)
                    .where(traffic_events_table.c.id == row["id"])
                    .values(
                        ip=None, username=None, is_guest=1, is_admin=0,
                        action=cleaned["action"], status=cleaned["status"],
                        meta=json.dumps(cleaned["meta"], ensure_ascii=False),
                    )
                )
            else:
                conn.execute(
                    delete(traffic_events_table).where(
                        traffic_events_table.c.id == row["id"]
                    )
                )
    for row in conn.execute(select(traffic_state_table)).mappings():
        try:
            payload = json.loads(row["payload"])
        except (TypeError, ValueError):
            continue
        cleaned = without_guest_traffic(payload, guest_names)
        if cleaned != payload:
            conn.execute(
                update(traffic_state_table)
                .where(traffic_state_table.c.id == row["id"])
                .values(payload=json.dumps(cleaned, ensure_ascii=False))
            )
