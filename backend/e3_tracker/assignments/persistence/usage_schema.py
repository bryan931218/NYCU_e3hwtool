"""Daily feature aggregates: no IPs, queries, assignment titles or credentials."""

from sqlalchemy import Column, Float, Index, Integer, String, Table
from e3_tracker.platform.persistence.metadata import metadata

feature_usage = Table(
    "assignment_feature_usage", metadata,
    Column("user_id", Integer, primary_key=True),
    Column("day", String(10), primary_key=True),
    Column("feature", String(32), primary_key=True),
    Column("count", Integer, nullable=False),
)
Index("ix_assignment_feature_usage_day", feature_usage.c.day)

usage_state = Table(
    "assignment_usage_state", metadata,
    Column("id", Integer, primary_key=True),
    Column("started_at", Float, nullable=False),
    Column("legacy_samples", Integer, nullable=False, default=0),
)
