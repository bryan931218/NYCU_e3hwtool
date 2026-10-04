"""Ordered, additive database upgrades for fresh and existing installations."""

from datetime import datetime, timezone
from threading import RLock

from sqlalchemy import Column, MetaData, String, Table, inspect, insert, select, text

from e3_tracker.platform.persistence.schema import metadata
from e3_tracker.study.persistence import extensions_schema
from e3_tracker.assignments.persistence.migrations import (
    upgrade_legacy_columns as upgrade_assignment_columns,
)
from e3_tracker.study.persistence.migrations import (
    upgrade_legacy_columns as upgrade_study_columns,
    upgrade_player_columns,
)

history = Table(
    "e3_schema_migrations",
    MetaData(),
    Column("version", String(64), primary_key=True),
    Column("applied_at", String(64), nullable=False),
)
_migration_lock = RLock()


def _add_columns(conn, table, definitions):
    columns = {column["name"] for column in inspect(conn).get_columns(table)}
    added = set()
    for name, definition in definitions.items():
        if name not in columns:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {name} {definition}"))
            added.add(name)
    return added


def _core_schema(conn):
    metadata.create_all(conn)
    upgrade_assignment_columns(conn, _add_columns)
    upgrade_study_columns(conn, _add_columns)


def _feature_schema(conn):
    upgrade_player_columns(conn, _add_columns)
    for table in metadata.sorted_tables:
        for index in table.indexes:
            index.create(conn, checkfirst=True)


def _user_profile_schema(conn):
    _add_columns(conn, "users", {"profile_surname": "VARCHAR(16)"})


def _security_schema(conn):
    from e3_tracker.platform.persistence.core_schema import security_limits_table

    _add_columns(
        conn,
        "web_sessions",
        {
            "is_guest": "INTEGER NOT NULL DEFAULT 0",
            "is_admin": "INTEGER NOT NULL DEFAULT 0",
            "moodle_credential": "TEXT",
            "expires_at": "DOUBLE PRECISION NOT NULL DEFAULT 0",
        },
    )
    security_limits_table.create(conn, checkfirst=True)


def _user_profile_name_schema(conn):
    _add_columns(conn, "users", {"profile_name": "VARCHAR(128)"})


def _guest_retention_cleanup(conn):
    from .guest_cleanup import purge_inactive_guests, remove_legacy_guest_traffic

    guests = purge_inactive_guests(conn)
    remove_legacy_guest_traffic(conn, guests)


def _assignment_notifications(conn):
    from e3_tracker.assignments.persistence.notification_schema import NOTIFICATION_TABLES
    for table in NOTIFICATION_TABLES:
        table.create(conn, checkfirst=True)


def _session_student_number(conn):
    _add_columns(conn, "users", {
        "student_number": "VARCHAR(9)",
        "student_number_sync_after": "DOUBLE PRECISION NOT NULL DEFAULT 0",
    })


def _custom_todos(conn):
    from e3_tracker.assignments.persistence.schema import custom_todos_table

    custom_todos_table.create(conn, checkfirst=True)
    for index in custom_todos_table.indexes:
        index.create(conn, checkfirst=True)


def _assignment_usage(conn):
    from e3_tracker.assignments.persistence.usage import migrate_usage
    migrate_usage(conn)


def _assignment_memberships(conn):
    from e3_tracker.assignments.persistence.membership import migrate_memberships
    migrate_memberships(conn)


MIGRATIONS = (
    ("0001_core_schema", _core_schema),
    ("0002_feature_schema", _feature_schema),
    ("0003_user_profile", _user_profile_schema),
    ("0004_security", _security_schema),
    ("0005_user_profile_name", _user_profile_name_schema),
    ("0006_guest_retention", _guest_retention_cleanup),
    ("0007_assignment_notifications", _assignment_notifications),
    ("0008_session_student_number", _session_student_number),
    ("0009_custom_todos", _custom_todos),
    ("0010_assignment_usage", _assignment_usage),
    ("0011_assignment_memberships", _assignment_memberships),
)


def migration_status(engine):
    with engine.connect() as conn:
        applied = (
            set(conn.execute(select(history.c.version)).scalars())
            if inspect(conn).has_table(history.name)
            else set()
        )
    known = {version for version, _ in MIGRATIONS}
    rows = [
        {"version": version, "applied": version in applied, "known": True}
        for version, _ in MIGRATIONS
    ]
    rows.extend(
        {"version": version, "applied": True, "known": False}
        for version in sorted(applied - known)
    )
    return rows


def run_migrations(engine):
    completed = []
    with _migration_lock, engine.connect() as conn:
        dialect = conn.dialect.name
        if dialect == "mysql":
            if (
                conn.execute(text("SELECT GET_LOCK('e3_tracker_schema', 15)")).scalar()
                != 1
            ):
                raise RuntimeError("Could not acquire the database migration lock")
            conn.commit()
        try:
            with conn.begin():
                if dialect == "sqlite":
                    conn.exec_driver_sql("BEGIN IMMEDIATE")
                elif dialect == "postgresql":
                    conn.execute(text("SELECT pg_advisory_xact_lock(33470001)"))
                history.create(conn, checkfirst=True)
                applied = set(conn.execute(select(history.c.version)).scalars())
                unknown = applied - {version for version, _ in MIGRATIONS}
                if unknown:
                    raise RuntimeError(
                        f"Database requires a newer application: {sorted(unknown)}"
                    )
                for version, upgrade in MIGRATIONS:
                    if version in applied:
                        continue
                    upgrade(conn)
                    conn.execute(
                        insert(history).values(
                            version=version,
                            applied_at=datetime.now(timezone.utc).isoformat(),
                        )
                    )
                    completed.append(version)
        finally:
            if dialect == "mysql":
                conn.execute(text("SELECT RELEASE_LOCK('e3_tracker_schema')"))
                conn.commit()
    return completed
