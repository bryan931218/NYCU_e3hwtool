"""Assignment notification preferences, subscriptions and durable delivery queue."""

from sqlalchemy import Column, Float, Integer, String, Table, Text, UniqueConstraint
from e3_tracker.platform.persistence.metadata import metadata

notification_settings = Table(
    "assignment_notification_settings",
    metadata,
    Column("user_id", Integer, primary_key=True),
    Column("preferences", Text, nullable=False),
    Column("initialized", Integer, nullable=False, default=0),
    Column("sync_after", Float, nullable=False, default=0),
    Column("sync_error", String(32)),
)
notification_seen = Table(
    "assignment_notification_seen",
    metadata,
    Column("user_id", Integer, primary_key=True),
    Column("uid_hash", String(64), primary_key=True),
    Column("graded_observed", Integer, nullable=False, default=0, server_default="0"),
)
push_subscriptions = Table(
    "assignment_push_subscriptions",
    metadata,
    Column("endpoint_hash", String(64), primary_key=True),
    Column("user_id", Integer, nullable=False, index=True),
    Column("subscription", Text, nullable=False),
)
line_bindings = Table(
    "assignment_line_bindings",
    metadata,
    Column("user_id", Integer, primary_key=True),
    Column("target_hash", String(64), nullable=False, unique=True),
    Column("target", Text, nullable=False),
)
line_link_codes = Table(
    "assignment_line_link_codes",
    metadata,
    Column("code_hash", String(64), primary_key=True),
    Column("user_id", Integer, nullable=False, unique=True),
    Column("expires_at", Float, nullable=False),
)
notification_jobs = Table(
    "assignment_notification_jobs",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("user_id", Integer, nullable=False, index=True),
    Column("event_key", String(255), nullable=False),
    Column("channel", String(16), nullable=False),
    Column("target_hash", String(64), nullable=False),
    Column("payload", Text, nullable=False),
    Column("state", String(16), nullable=False, default="pending", index=True),
    Column("attempts", Integer, nullable=False, default=0),
    Column("retry_at", Float, nullable=False, default=0),
    Column("expires_at", Float, nullable=False),
    Column("created_at", Float, nullable=False),
    Column("lease", String(64)),
    Column("error", String(32)),
    UniqueConstraint(
        "user_id",
        "event_key",
        "channel",
        "target_hash",
        name="uq_assignment_notification_delivery",
    ),
)

NOTIFICATION_TABLES = (
    notification_jobs,
    notification_seen,
    push_subscriptions,
    line_bindings,
    line_link_codes,
    notification_settings,
)
