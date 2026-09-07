"""Persistence operations for community."""
import json
from typing import Any, Dict, List, Optional
from sqlalchemy import delete, insert, func, select, update

from .schema import users_table, announcements_table, announcement_votes_table, feedback_table, traffic_state_table, traffic_events_table


class CommunityStorage:
    def insert_announcement(self, entry: Dict[str, Any], limit: int) -> None:
        record = dict(entry)
        with self._lock, self._engine.begin() as conn:
            conn.execute(delete(announcements_table).where(announcements_table.c.id == record["id"]))
            conn.execute(announcements_table.insert().values(**record))
            ids_to_keep = conn.execute(
                select(announcements_table.c.id)
                .order_by(announcements_table.c.created_at.desc(), announcements_table.c.id.desc())
                .limit(limit)
            ).scalars().all()
            if ids_to_keep:
                conn.execute(delete(announcements_table).where(~announcements_table.c.id.in_(ids_to_keep)))
                conn.execute(delete(announcement_votes_table).where(~announcement_votes_table.c.announcement_id.in_(ids_to_keep)))

    def delete_announcement(self, announcement_id: str) -> bool:
        with self._lock, self._engine.begin() as conn:
            conn.execute(delete(announcement_votes_table).where(announcement_votes_table.c.announcement_id == announcement_id))
            result = conn.execute(delete(announcements_table).where(announcements_table.c.id == announcement_id))
            return result.rowcount > 0

    def list_announcements(self, limit: int) -> List[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                select(announcements_table)
                .order_by(announcements_table.c.created_at.desc(), announcements_table.c.id.desc())
                .limit(limit)
            ).fetchall()
        return [dict(row._mapping) for row in rows]

    def list_announcements_with_votes(self, limit: int, username: Optional[str] = None) -> List[Dict[str, Any]]:
        announcements = self.list_announcements(limit)
        if not announcements:
            return []
        announcement_ids = [str(item.get("id") or "").strip() for item in announcements if item.get("id")]
        if not announcement_ids:
            return announcements
        summary_map = {
            announcement_id: {"like_count": 0, "dislike_count": 0, "user_vote": None}
            for announcement_id in announcement_ids
        }
        with self._lock, self._engine.connect() as conn:
            vote_rows = conn.execute(
                select(
                    announcement_votes_table.c.announcement_id,
                    announcement_votes_table.c.vote_type,
                    func.count().label("count"),
                )
                .where(announcement_votes_table.c.announcement_id.in_(announcement_ids))
                .group_by(announcement_votes_table.c.announcement_id, announcement_votes_table.c.vote_type)
            ).fetchall()
            for row in vote_rows:
                announcement_id = str(row.announcement_id)
                if announcement_id not in summary_map:
                    continue
                if row.vote_type == "up":
                    summary_map[announcement_id]["like_count"] = int(row.count or 0)
                elif row.vote_type == "down":
                    summary_map[announcement_id]["dislike_count"] = int(row.count or 0)
            if username:
                user_row = conn.execute(
                    select(users_table.c.id).where(users_table.c.username == username)
                ).fetchone()
                if user_row:
                    user_votes = conn.execute(
                        select(
                            announcement_votes_table.c.announcement_id,
                            announcement_votes_table.c.vote_type,
                        )
                        .where(announcement_votes_table.c.announcement_id.in_(announcement_ids))
                        .where(announcement_votes_table.c.user_id == int(user_row.id))
                    ).fetchall()
                    for row in user_votes:
                        announcement_id = str(row.announcement_id)
                        if announcement_id in summary_map:
                            summary_map[announcement_id]["user_vote"] = row.vote_type
        merged: List[Dict[str, Any]] = []
        for item in announcements:
            announcement_id = str(item.get("id") or "").strip()
            summary = summary_map.get(announcement_id, {"like_count": 0, "dislike_count": 0, "user_vote": None})
            merged_item = dict(item)
            merged_item.update(summary)
            merged.append(merged_item)
        return merged

    def set_announcement_vote(self, announcement_id: str, username: str, vote_type: Optional[str]) -> Optional[Dict[str, Any]]:
        announcement_id = (announcement_id or "").strip()
        username = (username or "").strip()
        normalized_vote = (vote_type or "").strip().lower() or None
        if not announcement_id or not username:
            return None
        if normalized_vote not in {None, "up", "down"}:
            return None
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            announcement_exists = conn.execute(
                select(announcements_table.c.id).where(announcements_table.c.id == announcement_id)
            ).fetchone()
            if not announcement_exists:
                return None
            user_id = self._ensure_user(conn, username)
            if normalized_vote is None:
                conn.execute(
                    delete(announcement_votes_table)
                    .where(announcement_votes_table.c.announcement_id == announcement_id)
                    .where(announcement_votes_table.c.user_id == user_id)
                )
            else:
                existing = conn.execute(
                    select(announcement_votes_table.c.id)
                    .where(announcement_votes_table.c.announcement_id == announcement_id)
                    .where(announcement_votes_table.c.user_id == user_id)
                ).fetchone()
                if existing:
                    conn.execute(
                        update(announcement_votes_table)
                        .where(announcement_votes_table.c.id == int(existing.id))
                        .values(vote_type=normalized_vote, updated_at=now)
                    )
                else:
                    conn.execute(
                        insert(announcement_votes_table).values(
                            announcement_id=announcement_id,
                            user_id=user_id,
                            vote_type=normalized_vote,
                            created_at=now,
                            updated_at=now,
                        )
                    )
        announcement = self.list_announcements_with_votes(limit=500, username=username)
        for item in announcement:
            if str(item.get("id") or "") == announcement_id:
                return item
        return None

    def load_traffic_state(self) -> Optional[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(select(traffic_state_table.c.payload).where(traffic_state_table.c.id == 1)).fetchone()
        if not row:
            return None
        try:
            return json.loads(row[0])
        except Exception:
            return None

    def save_traffic_state(self, payload: Dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False)
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(
                update(traffic_state_table)
                .where(traffic_state_table.c.id == 1)
                .values(payload=data, updated_at=now)
            )
            if not result.rowcount:
                conn.execute(traffic_state_table.insert().values(id=1, payload=data, updated_at=now))

    def append_traffic_event(self, event: Dict[str, Any], max_events: int) -> None:
        record = dict(event)
        meta = record.get("meta") or {}
        meta_json = json.dumps(meta, ensure_ascii=False)
        username = meta.get("username") if isinstance(meta, dict) else None
        is_guest = meta.get("is_guest") if isinstance(meta, dict) else None
        is_admin = meta.get("is_admin") if isinstance(meta, dict) else None
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(
                traffic_events_table.insert().values(
                    ts=record.get("ts"),
                    ip=record.get("ip"),
                    action=record.get("action"),
                    status=record.get("status"),
                    username=username,
                    is_guest=self._coerce_bool_int(is_guest) if is_guest is not None else None,
                    is_admin=self._coerce_bool_int(is_admin) if is_admin is not None else None,
                    meta=meta_json,
                )
            )
            event_id = result.inserted_primary_key[0] if result.inserted_primary_key else None
            if event_id is not None:
                conn.execute(
                    delete(traffic_events_table).where(
                        traffic_events_table.c.id <= max(0, int(event_id) - max(1, int(max_events)))
                    )
                )

    def recent_traffic_events(self, limit: int) -> List[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                select(
                    traffic_events_table.c.ts,
                    traffic_events_table.c.ip,
                    traffic_events_table.c.action,
                    traffic_events_table.c.status,
                    traffic_events_table.c.meta,
                )
                .order_by(traffic_events_table.c.id.desc())
                .limit(limit)
            ).fetchall()
        events: List[Dict[str, Any]] = []
        for row in reversed(rows):
            meta_raw = row.meta
            try:
                meta = json.loads(meta_raw) if meta_raw else {}
            except Exception:
                meta = {}
            events.append(
                {
                    "ts": row.ts,
                    "ip": row.ip,
                    "action": row.action,
                    "status": row.status,
                    "meta": meta,
                }
            )
        return events

    def clear_traffic_events(self) -> None:
        with self._lock, self._engine.begin() as conn:
            conn.execute(delete(traffic_events_table))

    def delete_traffic_events_for_user(self, username: str) -> int:
        if not username:
            return 0
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(delete(traffic_events_table).where(traffic_events_table.c.username == username))
            if result.rowcount:
                return result.rowcount
            pattern = f'%"username": "{username}"%'
            result = conn.execute(delete(traffic_events_table).where(traffic_events_table.c.meta.like(pattern)))
            return result.rowcount or 0

    def add_feedback(self, payload: Dict[str, Any]) -> int:
        record = dict(payload)
        username = record.get("username")
        user_id = None
        with self._lock, self._engine.begin() as conn:
            if username:
                user_id = self._ensure_user(conn, str(username))
            result = conn.execute(
                feedback_table.insert().values(
                    user_id=user_id,
                    username=username,
                    email=record.get("email"),
                    message=record.get("message"),
                    status=record.get("status"),
                    created_at=record.get("created_at"),
                )
            )
            inserted = result.inserted_primary_key
            if inserted:
                try:
                    return int(inserted[0])
                except Exception:
                    return 0
            return 0

    def list_feedback(self, limit: int) -> List[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                select(feedback_table)
                .order_by(feedback_table.c.id.desc())
                .limit(limit)
            ).fetchall()
        return [dict(row._mapping) for row in rows]

    def update_feedback_status(self, feedback_id: int, status: str) -> bool:
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(
                update(feedback_table)
                .where(feedback_table.c.id == feedback_id)
                .values(status=status)
            )
            return result.rowcount > 0
