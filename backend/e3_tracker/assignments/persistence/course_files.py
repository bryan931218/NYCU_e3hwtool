"""Encrypted, account/semester-scoped attachment metadata. No binary files are stored."""

import json
import time

from sqlalchemy import Column, Float, Integer, String, Table, Text, insert, select, update

from e3_tracker.platform.persistence.metadata import metadata
from e3_tracker.platform.persistence.core_schema import users_table


course_file_cache = Table(
    'course_file_cache', metadata,
    Column('user_id', Integer, primary_key=True),
    Column('semester_key', String(16), primary_key=True),
    Column('payload', Text, nullable=False),
    Column('fetched_at', Float, nullable=False, default=0),
    Column('attempt', Float, nullable=False, default=0),
    Column('status', String(16), nullable=False, default='idle'),
    Column('error', String(300), nullable=False, default=''),
)


class CourseFileStorage:
    def __init__(self, owner):
        self.owner = owner

    def _encode(self, value, username, semester):
        return self.owner._credential_cipher.encrypt(json.dumps(value, ensure_ascii=False), f'course-files:{username}:{semester}')

    def _decode(self, value, username, semester):
        return self.owner._credential_cipher.decrypt(value, f'course-files:{username}:{semester}')

    def _user(self, conn, username, write=False):
        if write and conn.dialect.name == 'sqlite':
            conn.exec_driver_sql('BEGIN IMMEDIATE')
        query = select(users_table.c.id).where(users_table.c.username == username, users_table.c.is_guest == 0)
        return conn.execute(query.with_for_update() if write else query).scalar()

    def load(self, username, semester):
        with self.owner._lock, self.owner._engine.connect() as conn:
            user_id = self._user(conn, username)
            row = conn.execute(select(course_file_cache).where(course_file_cache.c.user_id == user_id,
                course_file_cache.c.semester_key == semester)).mappings().first() if user_id else None
        if not row:
            return {'sources': {}, 'pending': [], 'fetched_at': 0, 'attempt': 0, 'status': 'idle', 'error': ''}
        return {**{key: row[key] for key in ('fetched_at', 'attempt', 'status', 'error')},
                **json.loads(self._decode(row['payload'], username, semester))}

    def claim(self, username, semester, *, now=None):
        now = time.time() if now is None else now
        with self.owner._lock, self.owner._engine.begin() as conn:
            user_id = self._user(conn, username, True)
            if not user_id:
                return None
            where = (course_file_cache.c.user_id == user_id, course_file_cache.c.semester_key == semester)
            row = conn.execute(select(course_file_cache).where(*where)).mappings().first()
            if row and ((row['status'] == 'running' and now - row['attempt'] < 90)
                        or now - row['attempt'] < (5 if row['status'] == 'partial' else 60)):
                return None
            values = {'attempt': now, 'status': 'running', 'error': ''}
            if row:
                conn.execute(update(course_file_cache).where(*where).values(**values))
            else:
                conn.execute(insert(course_file_cache).values(user_id=user_id, semester_key=semester,
                    payload=self._encode({'sources': {}, 'pending': []}, username, semester), fetched_at=0, **values))
            return now

    def finish(self, username, semester, attempt, sources, pending, error='', *, fetched=True):
        with self.owner._lock, self.owner._engine.begin() as conn:
            user_id = self._user(conn, username, True)
            if not user_id:
                return False
            where = (course_file_cache.c.user_id == user_id, course_file_cache.c.semester_key == semester,
                     course_file_cache.c.attempt == attempt)
            values = {'payload': self._encode({'sources': sources, 'pending': pending}, username, semester),
                      'status': 'partial' if pending else 'error' if error else 'success', 'error': error[:300]}
            if fetched:
                values['fetched_at'] = time.time()
            return conn.execute(update(course_file_cache).where(*where).values(**values)).rowcount == 1
