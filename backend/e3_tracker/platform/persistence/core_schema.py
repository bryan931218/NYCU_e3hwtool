"""Platform-owned database table definitions."""

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

users_table = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("username", String(191), nullable=False, unique=True),
    Column("profile_surname", String(16)),
    Column("profile_name", String(128)),
    Column("student_number", String(9)),
    Column("student_number_sync_after", Float, nullable=False, default=0),
    Column("is_guest", Integer, nullable=False, default=0),
    Column("is_admin", Integer, nullable=False, default=0),
    Column("created_at", String(64), nullable=False),
    Column("last_seen", String(64)),
)

web_sessions_table = Table(
    "web_sessions",
    metadata,
    Column("session_token", String(191), primary_key=True),
    Column("username", String(191), nullable=False),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
    Column("is_guest", Integer, nullable=False, default=0),
    Column("is_admin", Integer, nullable=False, default=0),
    Column("moodle_credential", Text),
    Column("expires_at", Float, nullable=False, default=0),
)

Index("ix_web_sessions_username", web_sessions_table.c.username)

security_limits_table = Table(
    "security_limits",
    metadata,
    Column("key", String(64), primary_key=True),
    Column("expires_at", Float, nullable=False),
    Column("count", Integer, nullable=False),
)

announcements_table = Table(
    "announcements",
    metadata,
    Column("id", String(191), primary_key=True),
    Column("title", Text, nullable=False),
    Column("content", Text, nullable=False),
    Column("author", String(191)),
    Column("created_at", String(64)),
    Column("created_label", String(64)),
)

announcement_votes_table = Table(
    "announcement_votes",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("announcement_id", String(191), nullable=False),
    Column("user_id", Integer, nullable=False),
    Column("vote_type", String(16), nullable=False),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
    UniqueConstraint(
        "announcement_id", "user_id", name="uq_announcement_votes_announcement_user"
    ),
)

Index("ix_announcement_votes_user_id", announcement_votes_table.c.user_id)

feedback_table = Table(
    "feedback",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer),
    Column("username", String(191)),
    Column("email", String(191)),
    Column("message", Text, nullable=False),
    Column("status", String(32)),
    Column("created_at", String(64)),
)

Index("ix_feedback_user_id", feedback_table.c.user_id)

Index("ix_feedback_status", feedback_table.c.status)

traffic_state_table = Table(
    "traffic_state",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("payload", Text, nullable=False),
    Column("updated_at", String(64)),
)

traffic_events_table = Table(
    "traffic_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ts", Float),
    Column("ip", String(128)),
    Column("action", String(128)),
    Column("status", String(32)),
    Column("username", String(191)),
    Column("is_guest", Integer),
    Column("is_admin", Integer),
    Column("meta", Text),
    Column("retained_activity", Integer, nullable=False, server_default="0"),
)

Index("ix_traffic_events_username", traffic_events_table.c.username)

Index("ix_traffic_events_ts", traffic_events_table.c.ts)

Index("ix_traffic_events_action", traffic_events_table.c.action)
Index("ix_traffic_events_activity_id", traffic_events_table.c.retained_activity, traffic_events_table.c.id)
Index("ix_traffic_events_activity_ts", traffic_events_table.c.retained_activity, traffic_events_table.c.ts)

data_repairs = Table(
    "e3_data_repairs",
    metadata,
    Column("repair_key", String(120), primary_key=True),
    Column("details", Text, nullable=False),
    Column("applied_at", String(64), nullable=False),
)
