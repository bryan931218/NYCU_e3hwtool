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
    def custom_todo_account_user_ids(self, conn, username):
        row = (
            conn.execute(
                select(users_table.c.id, users_table.c.student_number).where(
                    users_table.c.username == username
                )
            )
            .mappings()
            .first()
        )
        if not row:
            return []
        student_number = str(row.get("student_number") or "").strip()
        if not student_number:
            return [int(row["id"])]
        return list(
            conn.execute(
                select(users_table.c.id).where(
                    users_table.c.student_number == student_number,
                    users_table.c.is_guest == 0,
                )
            ).scalars()
        ) or [int(row["id"])]

    def list_custom_todos(self, username):
        with self._lock, self._engine.connect() as conn:
            user_ids = self.custom_todo_account_user_ids(conn, username)
            if not user_ids:
                return []
            rows = (
                conn.execute(
                    select(custom_todos_table)
                    .where(custom_todos_table.c.user_id.in_(user_ids))
                    .order_by(custom_todos_table.c.due_ts, custom_todos_table.c.id)
                )
                .mappings()
                .all()
            )
        # A legacy duplicate can exist only if identities were enriched after separate
        # writes. Collapse by uid, preferring the most recently updated row.
        unique = {}
        for row in rows:
            current = unique.get(row["uid"])
            if current is None or str(row["updated_at"]) > str(current["updated_at"]):
                unique[row["uid"]] = row
        return sorted(
            (_todo_payload(row) for row in unique.values()),
            key=lambda item: (item["due_ts"], item["uid"]),
        )

    def get_custom_todo(self, username, uid):
        with self._lock, self._engine.connect() as conn:
            user_ids = self.custom_todo_account_user_ids(conn, username)
            if not user_ids:
                return None
            rows = (
                conn.execute(
                    select(custom_todos_table).where(
                        custom_todos_table.c.user_id.in_(user_ids),
                        custom_todos_table.c.uid == str(uid),
                    )
                )
                .mappings()
                .all()
            )
        if not rows:
            return None
        row = max(rows, key=lambda item: str(item["updated_at"]))
        return _todo_payload(row)

    def upsert_custom_todo(self, username, item):
        uid = str(item["uid"])
        course = str(item["course"])
        title = str(item["title"])
        due_ts = int(item["due_ts"])
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            current_user_id = self._ensure_user(conn, username)
            user_ids = self.custom_todo_account_user_ids(conn, username) or [current_user_id]
            existing_ids = list(
                conn.execute(
                    select(custom_todos_table.c.id).where(
                        custom_todos_table.c.user_id.in_(user_ids),
                        custom_todos_table.c.uid == uid,
                    )
                ).scalars()
            )
            if not existing_ids:
                conn.execute(
                    insert(custom_todos_table).values(
                        user_id=current_user_id,
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
                    .where(custom_todos_table.c.id.in_(existing_ids))
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
            user_ids = self.custom_todo_account_user_ids(conn, username)
            if not user_ids:
                return False
            result = conn.execute(
                delete(custom_todos_table).where(
                    custom_todos_table.c.user_id.in_(user_ids),
                    custom_todos_table.c.uid == str(uid),
                )
            )
            return bool(result.rowcount)
