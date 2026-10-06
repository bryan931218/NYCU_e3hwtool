"""Ordered, additive database upgrades for fresh and existing installations."""

from datetime import datetime, timezone
from threading import RLock

from sqlalchemy import Column, MetaData, String, Table, inspect, insert, select, text

from e3_tracker.platform.persistence.schema import metadata
from e3_tracker.study.persistence import extensions_schema
from e3_tracker.assignments.persistence.migrations import (
    upgrade_legacy_columns as upgrade_assignment_columns,
    upgrade_grading_notifications,
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
    _add_columns(conn, "traffic_events", {"retained_activity": "INTEGER NOT NULL DEFAULT 0"})
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


def _course_announcements(conn):
    from e3_tracker.assignments.persistence.course_announcements import course_announcement_cache
    course_announcement_cache.create(conn, checkfirst=True)


def _course_mail(conn):
    from e3_tracker.assignments.persistence.course_mail import course_mail_cache
    course_mail_cache.create(conn, checkfirst=True)


def _repair_department_profile_names(conn):
    from e3_tracker.platform.persistence.core_schema import users_table
    from e3_tracker.platform.services.profile_names import is_academic_unit_name

    rows = conn.execute(select(users_table.c.id, users_table.c.profile_name)).all()
    ids = [row.id for row in rows if is_academic_unit_name(row.profile_name)]
    for user_id in ids:
        conn.execute(users_table.update().where(users_table.c.id == user_id).values(
            profile_name=None, profile_surname=None,
        ))


def _assignment_actions(conn):
    from e3_tracker.assignments.persistence.assignment_actions import ACTION_TABLES
    for table in ACTION_TABLES:
        table.create(conn, checkfirst=True)


def _grading_notifications(conn):
    upgrade_grading_notifications(conn, _add_columns)


def _traffic_activity_retention(conn):
    import json
    import time
    from .core_schema import traffic_events_table, users_table
    from e3_tracker.platform.services.traffic import ACTIVITY_RETENTION_DAYS, is_assignment_event, is_recent_activity_event
    from e3_tracker.platform.guest_privacy import sanitize_traffic_event
    from e3_tracker.platform.constants import TAIPEI_TZ
    from e3_tracker.assignments.persistence.usage import increment_usage

    _add_columns(conn, "traffic_events", {"retained_activity": "INTEGER NOT NULL DEFAULT 0"})
    now = time.time()
    cutoff = now - ACTIVITY_RETENTION_DAYS * 86400
    today = datetime.fromtimestamp(now, TAIPEI_TZ).date()
    accounts = {row.username: row.id for row in conn.execute(select(users_table.c.id, users_table.c.username).where(users_table.c.is_guest == 0))}
    rows = conn.execute(select(traffic_events_table).where(traffic_events_table.c.ts >= cutoff)).mappings().all()
    for row in rows:
        try:
            meta = json.loads(row["meta"] or "{}")
            if not isinstance(meta, dict):
                continue
            event = sanitize_traffic_event({**row, "meta": meta})
            day = datetime.fromtimestamp(row["ts"], TAIPEI_TZ).date()
        except (TypeError, ValueError, OverflowError, OSError):
            continue
        if event and is_recent_activity_event(event):
            conn.execute(traffic_events_table.update().where(traffic_events_table.c.id == row["id"]).values(retained_activity=1))
        if (event and day == today and is_assignment_event(event)
                and event.get("status") in {"success", "info"} and not meta.get("activity_only")
                and not meta.get("is_guest") and meta.get("username") in accounts):
            increment_usage(conn, accounts[meta["username"]], "__presence", day.isoformat(), once=True)
    for index in traffic_events_table.indexes:
        index.create(conn, checkfirst=True)


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
    ("0012_course_announcements", _course_announcements),
    ("0013_course_mail", _course_mail),
    ("0014_repair_department_profile_names", _repair_department_profile_names),
    ("0015_assignment_actions", _assignment_actions),
    ("0016_grading_notifications", _grading_notifications),
    ("0017_traffic_activity_retention", _traffic_activity_retention),
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
