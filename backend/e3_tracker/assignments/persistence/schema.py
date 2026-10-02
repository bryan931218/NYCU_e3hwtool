"""Assignments-owned database table definitions."""

from sqlalchemy import (
    Column,
    Float,
    Index,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from e3_tracker.platform.persistence.metadata import metadata

user_preferences_table = Table(
    "user_preferences",
    metadata,
    Column("user_id", Integer, primary_key=True),
    Column("view_mode", String(32)),
    Column("status_filter", String(32)),
    Column("semester_filter", Text),
    Column("include_ignored_overdue", Integer, nullable=False, default=0),
    Column("show_overdue", Integer, nullable=False, default=0),
    Column("show_completed", Integer, nullable=False, default=0),
    Column("show_graded", Integer, nullable=False, default=0),
    Column("ignored_overdue_uids", Text),
    Column("updated_at", String(64)),
)

courses_table = Table(
    "courses",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, nullable=False),
    Column("course_code", Integer, nullable=False),
    Column("title", Text, nullable=False),
    Column("url", Text),
    Column("semester_key", String(32)),
    Column("semester_label", String(64)),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
    UniqueConstraint("user_id", "course_code", name="uq_courses_user_course"),
)

Index("ix_courses_user_id", courses_table.c.user_id)

assignments_table = Table(
    "assignments",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("course_id", Integer, nullable=False),
    Column("uid", String(255), nullable=False),
    Column("title", Text, nullable=False),
    Column("url", Text),
    Column("due_at", String(64)),
    Column("due_ts", Integer),
    Column("overdue", Integer, nullable=False, default=0),
    Column("completed", Integer, nullable=False, default=0),
    Column("raw_status_text", Text),
    Column("grade_text", Text),
    Column("submitted_at", String(64)),
    Column("submitted_ts", Integer),
    Column("remaining_text", Text),
    Column("submitted_count", Integer),
    Column("participant_count", Integer),
    Column("updated_at", String(64), nullable=False),
    UniqueConstraint("course_id", "uid", name="uq_assignments_course_uid"),
)

Index("ix_assignments_course_id", assignments_table.c.course_id)

Index("ix_assignments_due_ts", assignments_table.c.due_ts)

custom_todos_table = Table(
    "custom_todos",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, nullable=False),
    Column("uid", String(255), nullable=False),
    Column("course", Text, nullable=False),
    Column("title", Text, nullable=False),
    Column("due_ts", Integer, nullable=False),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
    UniqueConstraint("user_id", "uid", name="uq_custom_todos_user_uid"),
)

Index("ix_custom_todos_user_id", custom_todos_table.c.user_id)
Index("ix_custom_todos_due_ts", custom_todos_table.c.due_ts)

assignment_views_table = Table(
    "assignment_views",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, nullable=False),
    Column("assignment_uid", String(255), nullable=False),
    Column("first_seen_at", String(64), nullable=False),
    Column("first_seen_ts", Integer, nullable=False),
    UniqueConstraint("user_id", "assignment_uid", name="uq_assignment_views_user_uid"),
)

Index("ix_assignment_views_user_id", assignment_views_table.c.user_id)

user_fetch_state_table = Table(
    "user_fetch_state",
    metadata,
    Column("user_id", Integer, primary_key=True),
    Column("fetched_at", String(64)),
    Column("fetched_ts", Integer),
    Column("excel_data", Text),
    Column("error_count", Integer, nullable=False, default=0),
    Column("semester_catalog", Text),
    Column("selected_semesters", Text),
)

fetch_errors_table = Table(
    "fetch_errors",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, nullable=False),
    Column("course_code", Integer),
    Column("course_title", Text),
    Column("assignment_title", Text),
    Column("message", Text, nullable=False),
)

google_tokens_table = Table(
    "google_tokens",
    metadata,
    Column("user_id", Integer, primary_key=True),
    Column("access_token", Text),
    Column("refresh_token", Text),
    Column("scope", Text),
    Column("token_type", String(32)),
    Column("expires_at", Float),
    Column("updated_at", String(64)),
)

Index("ix_fetch_errors_user_id", fetch_errors_table.c.user_id)
