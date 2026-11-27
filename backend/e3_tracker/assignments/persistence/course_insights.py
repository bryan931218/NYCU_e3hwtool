"""Encrypted per-account settings and course analysis snapshots."""

import json
import time
from uuid import uuid4

from sqlalchemy import Column, Float, ForeignKey, Integer, String, Table, Text, case, delete, insert, select, update
from sqlalchemy.dialects.mysql import MEDIUMTEXT

from e3_tracker.platform.persistence.core_schema import users_table, web_sessions_table
from e3_tracker.platform.persistence.metadata import metadata
from .course_files import CourseFileStorage


insight_settings = Table(
    'course_insight_settings', metadata,
    Column('user_id', Integer, ForeignKey('users.id', ondelete='CASCADE'), primary_key=True),
    Column('secret', Text, nullable=False),
    Column('revision', String(32), nullable=False),
    Column('lease', String(32), nullable=False, default=''),
    Column('lease_until', Float, nullable=False, default=0),
    Column('retry_at', Float, nullable=False, default=0),
    Column('error', String(32), nullable=False, default=''),
    Column('requested_semester', String(16), nullable=False, default=''),
    Column('working_semester', String(16), nullable=False, default=''),
)
insight_courses = Table(
    'course_insight_courses', metadata,
    Column('user_id', Integer, ForeignKey('users.id', ondelete='CASCADE'), primary_key=True),
    Column('semester', String(16), primary_key=True),
    Column('course_id', Integer, primary_key=True),
    Column('checked', Float, nullable=False),
    Column('payload', Text().with_variant(MEDIUMTEXT(), 'mysql', 'mariadb'), nullable=False),
    Column('sources', Text().with_variant(MEDIUMTEXT(), 'mysql', 'mariadb'), nullable=False),
)


class CourseInsightStorage(CourseFileStorage):
    def has_configured_accounts(self):
        with self.owner._lock, self.owner._engine.connect() as conn:
            return bool(conn.execute(select(select(insight_settings.c.user_id).exists())).scalar())

    def settings(self, username):
        with self.owner._lock, self.owner._engine.connect() as conn:
            row = conn.execute(select(insight_settings.c.lease_until, insight_settings.c.retry_at,
                insight_settings.c.error, insight_settings.c.requested_semester, insight_settings.c.working_semester
                ).where(insight_settings.c.user_id == self._user(conn, username))).mappings().first()
        return dict(row) if row else None

    def configure(self, username, key):
        with self.owner._lock, self.owner._engine.begin() as conn:
            uid = self._user(conn, username, True)
            if uid is None:
                return False
            conn.execute(delete(insight_settings).where(insight_settings.c.user_id == uid))
            if key:
                secret = self.owner._credential_cipher.encrypt(key, f'course-ai-key:{uid}')
                conn.execute(insert(insight_settings).values(user_id=uid, secret=secret, revision=uuid4().hex))
            else:
                conn.execute(delete(insight_courses).where(insight_courses.c.user_id == uid))
            return True

    def due_accounts(self, now):
        session = select(web_sessions_table.c.session_token).where(
            web_sessions_table.c.username == users_table.c.username,
            web_sessions_table.c.expires_at > now, web_sessions_table.c.is_guest == 0,
            web_sessions_table.c.moodle_credential.is_not(None)).exists()
        with self.owner._lock, self.owner._engine.connect() as conn:
            return list(conn.execute(select(users_table.c.username)
                .join(insight_settings, insight_settings.c.user_id == users_table.c.id)
                .where(users_table.c.is_guest == 0, insight_settings.c.retry_at <= now,
                       insight_settings.c.lease_until <= now, session)
                .order_by((insight_settings.c.requested_semester != '').desc(),
                          insight_settings.c.retry_at, insight_settings.c.user_id).limit(12)).scalars())

    def request_semester(self, username, semester):
        now = time.time()
        with self.owner._lock, self.owner._engine.begin() as conn:
            uid = self._user(conn, username, True)
            conn.execute(update(insight_settings).where(insight_settings.c.user_id == uid,
                insight_settings.c.requested_semester != semester).values(requested_semester=semester,
                retry_at=case((~insight_settings.c.error.in_(('key', 'quota', 'session_expired', 'provider')), now),
                              else_=insight_settings.c.retry_at)))

    def discard_semester(self, username, semester):
        with self.owner._lock, self.owner._engine.begin() as conn:
            uid = self._user(conn, username, True)
            conn.execute(update(insight_settings).where(insight_settings.c.user_id == uid,
                insight_settings.c.requested_semester == semester).values(requested_semester=''))

    def claim(self, username, semester=''):
        now = time.time()
        with self.owner._lock, self.owner._engine.begin() as conn:
            uid = self._user(conn, username, True)
            row = conn.execute(select(insight_settings).where(insight_settings.c.user_id == uid)).mappings().first()
            if not row or max(row['lease_until'], row['retry_at']) > now or (
                    semester and row['requested_semester'] not in ('', semester)):
                return None
            lease = uuid4().hex
            key = self.owner._credential_cipher.decrypt(row['secret'], f'course-ai-key:{uid}')
            conn.execute(update(insight_settings).where(insight_settings.c.user_id == uid)
                .values(lease=lease, lease_until=now + 180, working_semester=semester, error=''))
            return uid, row['revision'], lease, key

    def _claim_filter(self, claim):
        uid, revision, lease, _ = claim
        return (insight_settings.c.user_id == uid, insight_settings.c.revision == revision,
                insight_settings.c.lease == lease, insight_settings.c.lease_until > time.time())

    def active(self, claim):
        with self.owner._lock, self.owner._engine.connect() as conn:
            return conn.execute(select(insight_settings.c.user_id).where(*self._claim_filter(claim))).scalar() is not None

    def finish(self, claim, *, error='', pause=45, completed_semester=''):
        now = time.time()
        retry_at = now + pause
        if error not in {'key', 'quota', 'session_expired', 'provider'}:
            retry_at = case(((insight_settings.c.requested_semester != '') &
                             (insight_settings.c.requested_semester != completed_semester), now + min(5, pause)),
                            else_=retry_at)
        with self.owner._lock, self.owner._engine.begin() as conn:
            conn.execute(update(insight_settings).where(*self._claim_filter(claim))
                .values(lease='', lease_until=0, retry_at=retry_at, error=error, working_semester='',
                        requested_semester=case(
                            (insight_settings.c.requested_semester == completed_semester, ''),
                            else_=insight_settings.c.requested_semester)))

    def snapshots(self, username, semester, course_ids, *, include_sources=False):
        if not course_ids:
            return {}
        with self.owner._lock, self.owner._engine.connect() as conn:
            uid = self._user(conn, username)
            columns = [insight_courses.c.course_id, insight_courses.c.checked, insight_courses.c.payload]
            if include_sources:
                columns.append(insight_courses.c.sources)
            rows = conn.execute(select(*columns).where(insight_courses.c.user_id == uid,
                insight_courses.c.semester == semester, insight_courses.c.course_id.in_(course_ids))).mappings().all()
        results = {}
        for row in rows:
            context = f"course-ai:{uid}:{semester}:{row['course_id']}"
            value = json.loads(self.owner._credential_cipher.decrypt(row['payload'], context))
            if include_sources:
                value['sources'] = json.loads(self.owner._credential_cipher.decrypt(row['sources'], context + ':sources'))
            results[row['course_id']] = {**value, 'checked': row['checked']}
        return results

    def save(self, username, semester, course_id, claim, snapshot):
        uid = claim[0]
        with self.owner._lock, self.owner._engine.begin() as conn:
            if self._user(conn, username, True) != uid or not conn.execute(
                select(insight_settings.c.user_id).where(*self._claim_filter(claim)).with_for_update()
            ).scalar():
                return False
            where = (insight_courses.c.user_id == uid, insight_courses.c.semester == semester,
                     insight_courses.c.course_id == course_id)
            conn.execute(delete(insight_courses).where(*where))
            context = f'course-ai:{uid}:{semester}:{course_id}'
            data = {field: value for field, value in snapshot.items() if field not in {'sources', 'checked'}}
            payload = self.owner._credential_cipher.encrypt(json.dumps(data, ensure_ascii=False), context)
            sources = self.owner._credential_cipher.encrypt(
                json.dumps(snapshot.get('sources', []), ensure_ascii=False), context + ':sources')
            conn.execute(insert(insight_courses).values(user_id=uid, semester=semester,
                course_id=course_id, checked=time.time(), payload=payload, sources=sources))
            return True
