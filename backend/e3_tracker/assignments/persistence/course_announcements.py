"""Per-account, per-semester news cache and read state, independent of assignment refreshes."""

import json
import time

from sqlalchemy import Column, Float, Integer, String, Table, Text, delete, insert, select, update

from e3_tracker.platform.persistence.metadata import metadata
from e3_tracker.platform.persistence.core_schema import users_table


course_announcement_cache = Table(
    'course_announcement_cache', metadata,
    Column('user_id', Integer, primary_key=True),
    Column('semester_key', String(16), primary_key=True),
    Column('payload', Text, nullable=False),
    Column('fetched_at', Float, nullable=False, default=0),
    Column('attempt', Float, nullable=False, default=0),
    Column('status', String(16), nullable=False, default='idle'),
    Column('error', String(300), nullable=False, default=''),
)


class CourseAnnouncementStorage:
    def _announcement_user(self, conn, username, *, write=False):
        if write and conn.dialect.name == 'sqlite':
            conn.exec_driver_sql('BEGIN IMMEDIATE')
        query = select(users_table.c.id).where(users_table.c.username == username, users_table.c.is_guest == 0)
        if write:
            query = query.with_for_update()
        return conn.execute(query).scalar()

    def load_course_announcements(self, username, semester):
        with self._lock, self._engine.connect() as conn:
            user_id = self._announcement_user(conn, username)
            row = conn.execute(select(course_announcement_cache).where(
                course_announcement_cache.c.user_id == user_id,
                course_announcement_cache.c.semester_key == semester,
            )).mappings().first() if user_id else None
        if not row:
            return {'items': [], 'fetched_at': 0, 'attempt': 0, 'status': 'idle', 'error': ''}
        return {**dict(row), 'items': json.loads(row['payload'])}

    def claim_course_announcement_refresh(self, username, semester, *, now=None):
        now = time.time() if now is None else now
        with self._lock, self._engine.begin() as conn:
            user_id = self._announcement_user(conn, username, write=True)
            if not user_id:
                return None
            condition = (course_announcement_cache.c.user_id == user_id, course_announcement_cache.c.semester_key == semester)
            row = conn.execute(select(course_announcement_cache).where(*condition)).mappings().first()
            if row and ((row['status'] == 'running' and now - row['attempt'] < 90) or now - row['attempt'] < 60):
                return None
            if row:
                conn.execute(update(course_announcement_cache).where(*condition).values(attempt=now, status='running', error=''))
            else:
                conn.execute(insert(course_announcement_cache).values(user_id=user_id, semester_key=semester,
                    payload='[]', fetched_at=0, attempt=now, status='running', error=''))
            return now

    def finish_course_announcement_refresh(self, username, semester, attempt, items, successful_courses, error=''):
        with self._lock, self._engine.begin() as conn:
            user_id = self._announcement_user(conn, username, write=True)
            if not user_id:
                return False
            condition = (course_announcement_cache.c.user_id == user_id, course_announcement_cache.c.semester_key == semester)
            row = conn.execute(select(course_announcement_cache).where(*condition)).mappings().first()
            if not row or row['attempt'] != attempt:
                return False
            old = {item['key']: item for item in json.loads(row['payload'])}
            merged = [item for item in old.values() if item['course_id'] not in successful_courses]
            for item in items:
                previous = old.get(item['key'], {})
                saved = {**item, 'read_at': previous.get('read_at', 0)}
                if previous.get('updated_ts') == item.get('updated_ts') and previous.get('title') == item.get('title'):
                    for field in ('content', 'links'):
                        if field in previous:
                            saved[field] = previous[field]
                merged.append(saved)
            merged.sort(key=lambda item: item.get('updated_ts') or 0, reverse=True)
            conn.execute(update(course_announcement_cache).where(*condition).values(payload=json.dumps(merged[:900], ensure_ascii=False),
                fetched_at=time.time() if successful_courses else row['fetched_at'], status='partial' if error and successful_courses else 'error' if error else 'success', error=error[:300]))
            return True

    def update_course_announcement(self, username, semester, key, *, content=None, read=None):
        with self._lock, self._engine.begin() as conn:
            user_id = self._announcement_user(conn, username, write=True)
            condition = (course_announcement_cache.c.user_id == user_id, course_announcement_cache.c.semester_key == semester)
            row = conn.execute(select(course_announcement_cache.c.payload).where(*condition)).first() if user_id else None
            if not row:
                return None
            items = json.loads(row.payload)
            item = next((item for item in items if item['key'] == key), None)
            if not item:
                return None
            if content is not None:
                item.update(content)
            if read is not None:
                item['read_at'] = time.time() if read else 0
            conn.execute(update(course_announcement_cache).where(*condition).values(payload=json.dumps(items, ensure_ascii=False)))
            return item
