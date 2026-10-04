"""Separate, encrypted cache for each account's E3 course inbox."""

import json

from sqlalchemy import Column, Float, Integer, String, Table, Text

from e3_tracker.platform.persistence.metadata import metadata
from .course_announcements import CourseAnnouncementStorage


course_mail_cache = Table(
    'course_mail_cache', metadata,
    Column('user_id', Integer, primary_key=True),
    Column('semester_key', String(16), primary_key=True),
    Column('payload', Text, nullable=False),
    Column('fetched_at', Float, nullable=False, default=0),
    Column('attempt', Float, nullable=False, default=0),
    Column('status', String(16), nullable=False, default='idle'),
    Column('error', String(300), nullable=False, default=''),
)


class CourseMailStorage(CourseAnnouncementStorage):
    message_cache_table = course_mail_cache

    def __init__(self, storage):
        self._engine, self._lock = storage._engine, storage._lock
        self._credential_cipher = storage._credential_cipher

    def _encode_message_payload(self, items, username, semester):
        return self._credential_cipher.encrypt(json.dumps(items, ensure_ascii=False), f'course-mail:{username}:{semester}')

    def _decode_message_payload(self, payload, username, semester):
        return json.loads(self._credential_cipher.decrypt(payload, f'course-mail:{username}:{semester}'))
