"""Transactional removal of assignment data owned by temporary accounts."""

from sqlalchemy import delete, select
from sqlalchemy import inspect
from .notification_schema import NOTIFICATION_TABLES
from .usage_schema import feature_usage

from .schema import (
    assignments_table,
    assignment_views_table,
    courses_table,
    fetch_errors_table,
    google_tokens_table,
    user_fetch_state_table,
    user_preferences_table,
)


def delete_assignment_account_data(conn, user_ids):
    course_ids = select(courses_table.c.id).where(courses_table.c.user_id.in_(user_ids))
    conn.execute(
        delete(assignments_table).where(assignments_table.c.course_id.in_(course_ids))
    )
    for table in (
        courses_table,
        assignment_views_table,
        fetch_errors_table,
        google_tokens_table,
        user_fetch_state_table,
        user_preferences_table,
    ):
        conn.execute(delete(table).where(table.c.user_id.in_(user_ids)))
    for table in (*NOTIFICATION_TABLES, feature_usage):
        if inspect(conn).has_table(table.name):
            conn.execute(delete(table).where(table.c.user_id.in_(user_ids)))
