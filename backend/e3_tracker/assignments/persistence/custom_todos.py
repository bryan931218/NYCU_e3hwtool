"""Account-scoped persistence for user-created custom todo items."""

from sqlalchemy import delete, insert, select, update

from e3_tracker.assignments.persistence.schema import custom_todos_table
from e3_tracker.platform.persistence.core_schema import users_table


def _todo_payload(row):
    if not row:
        return None
    return {
        "uid": row["uid"],
        "course": row["course"],
        "title": row["title"],
        "due_ts": int(row["due_ts"]),
    }


class CustomTodosStorage:
    def list_custom_todos(self, username):
        with self._lock, self._engine.connect() as conn:
            user_id = conn.execute(
                select(users_table.c.id).where(users_table.c.username == username)
            ).scalar()
            if user_id is None:
                return []
            rows = (
                conn.execute(
                    select(custom_todos_table)
                    .where(custom_todos_table.c.user_id == user_id)
                    .order_by(custom_todos_table.c.due_ts, custom_todos_table.c.id)
                )
                .mappings()
                .all()
            )
        return [_todo_payload(row) for row in rows]

    def get_custom_todo(self, username, uid):
        with self._lock, self._engine.connect() as conn:
            user_id = conn.execute(
                select(users_table.c.id).where(users_table.c.username == username)
            ).scalar()
            if user_id is None:
                return None
            row = (
                conn.execute(
                    select(custom_todos_table).where(
                        custom_todos_table.c.user_id == user_id,
                        custom_todos_table.c.uid == str(uid),
                    )
                )
                .mappings()
                .first()
            )
        return _todo_payload(row)

    def upsert_custom_todo(self, username, item):
        uid = str(item["uid"])
        course = str(item["course"])
        title = str(item["title"])
        due_ts = int(item["due_ts"])
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            user_id = self._ensure_user(conn, username)
            existing = conn.execute(
                select(custom_todos_table.c.id).where(
                    custom_todos_table.c.user_id == user_id,
                    custom_todos_table.c.uid == uid,
                )
            ).scalar()
            if existing is None:
                conn.execute(
                    insert(custom_todos_table).values(
                        user_id=user_id,
                        uid=uid,
                        course=course,
                        title=title,
                        due_ts=due_ts,
                        created_at=now,
                        updated_at=now,
                    )
                )
            else:
                conn.execute(
                    update(custom_todos_table)
                    .where(custom_todos_table.c.id == existing)
                    .values(
                        course=course,
                        title=title,
                        due_ts=due_ts,
                        updated_at=now,
                    )
                )
        return {"uid": uid, "course": course, "title": title, "due_ts": due_ts}

    def delete_custom_todo(self, username, uid):
        with self._lock, self._engine.begin() as conn:
            user_id = conn.execute(
                select(users_table.c.id).where(users_table.c.username == username)
            ).scalar()
            if user_id is None:
                return False
            result = conn.execute(
                delete(custom_todos_table).where(
                    custom_todos_table.c.user_id == user_id,
                    custom_todos_table.c.uid == str(uid),
                )
            )
            return bool(result.rowcount)
