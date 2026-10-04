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
    message_cache_table = course_announcement_cache

    def _encode_message_payload(self, items, username, semester):
        return json.dumps(items, ensure_ascii=False)

    def _decode_message_payload(self, payload, username, semester):
        return json.loads(payload)

    def _after_message_refresh(self, conn, username, user_id, semester, items, successful_courses):
        self._observe_course_message_notifications(conn, username, user_id, 'announcements', semester, items, successful_courses)

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
            row = conn.execute(select(self.message_cache_table).where(
                self.message_cache_table.c.user_id == user_id,
                self.message_cache_table.c.semester_key == semester,
            )).mappings().first() if user_id else None
        if not row:
            return {'items': [], 'fetched_at': 0, 'attempt': 0, 'status': 'idle', 'error': ''}
        result = dict(row)
        result.pop('payload')
        return {**result, 'items': self._decode_message_payload(row['payload'], username, semester)}

    def claim_course_announcement_refresh(self, username, semester, *, now=None):
        now = time.time() if now is None else now
        with self._lock, self._engine.begin() as conn:
            user_id = self._announcement_user(conn, username, write=True)
            if not user_id:
                return None
            condition = (self.message_cache_table.c.user_id == user_id, self.message_cache_table.c.semester_key == semester)
            row = conn.execute(select(self.message_cache_table).where(*condition)).mappings().first()
            if row and ((row['status'] == 'running' and now - row['attempt'] < 90) or now - row['attempt'] < 60):
                return None
            if row:
                conn.execute(update(self.message_cache_table).where(*condition).values(attempt=now, status='running', error=''))
            else:
                conn.execute(insert(self.message_cache_table).values(user_id=user_id, semester_key=semester,
                    payload=self._encode_message_payload([], username, semester), fetched_at=0, attempt=now, status='running', error=''))
            return now

    def finish_course_announcement_refresh(self, username, semester, attempt, items, successful_courses, error=''):
        with self._lock, self._engine.begin() as conn:
            user_id = self._announcement_user(conn, username, write=True)
            if not user_id:
                return False
            condition = (self.message_cache_table.c.user_id == user_id, self.message_cache_table.c.semester_key == semester)
            row = conn.execute(select(self.message_cache_table).where(*condition)).mappings().first()
            if not row or row['attempt'] != attempt:
                return False
            old = {item['key']: item for item in self._decode_message_payload(row['payload'], username, semester)}
            merged = [item for item in old.values() if item['course_id'] not in successful_courses]
            for item in items:
                previous = old.get(item['key'], {})
                saved = {**item, 'read_at': previous.get('read_at', time.time() if item.get('e3_read') else 0),
                         'seen_at': previous.get('seen_at', previous.get('read_at', 0))}
                if previous.get('updated_ts') == item.get('updated_ts') and previous.get('title') == item.get('title'):
                    for field in ('content', 'links'):
                        if field in previous:
                            saved[field] = previous[field]
                merged.append(saved)
            merged.sort(key=lambda item: item.get('updated_ts') or 0, reverse=True)
            conn.execute(update(self.message_cache_table).where(*condition).values(payload=self._encode_message_payload(merged[:900], username, semester),
                fetched_at=time.time() if successful_courses else row['fetched_at'], status='partial' if error and successful_courses else 'error' if error else 'success', error=error[:300]))
            self._after_message_refresh(conn, username, user_id, semester, merged, successful_courses)
            return True

    def acknowledge_course_messages(self, username, semester, course_ids):
        with self._lock, self._engine.begin() as conn:
            user_id = self._announcement_user(conn, username, write=True)
            condition = (self.message_cache_table.c.user_id == user_id, self.message_cache_table.c.semester_key == semester)
            row = conn.execute(select(self.message_cache_table.c.payload).where(*condition)).first() if user_id else None
            if not row:
                return
            items = self._decode_message_payload(row.payload, username, semester)
            changed = False
            for item in items:
                if item['course_id'] in course_ids and not item.get('seen_at'):
                    item['seen_at'] = time.time()
                    changed = True
            if changed:
                conn.execute(update(self.message_cache_table).where(*condition).values(payload=self._encode_message_payload(items, username, semester)))

    def update_course_announcement(self, username, semester, key, *, content=None, read=None, expected_version=None):
        with self._lock, self._engine.begin() as conn:
            user_id = self._announcement_user(conn, username, write=True)
            condition = (self.message_cache_table.c.user_id == user_id, self.message_cache_table.c.semester_key == semester)
            row = conn.execute(select(self.message_cache_table.c.payload).where(*condition)).first() if user_id else None
            if not row:
                return None
            items = self._decode_message_payload(row.payload, username, semester)
            item = next((item for item in items if item['key'] == key), None)
            if not item:
                return None
            if expected_version is not None and (item.get('title'), item.get('updated_ts')) != expected_version:
                return None
            if content is not None:
                item.update(content)
            if read is not None:
                item.setdefault('seen_at', item.get('read_at', 0))
                item['read_at'] = time.time() if read else 0
            conn.execute(update(self.message_cache_table).where(*condition).values(payload=self._encode_message_payload(items, username, semester)))
            return item
