"""Ordered, additive database upgrades for fresh and existing installations."""
from datetime import datetime, timezone
from threading import RLock

from sqlalchemy import Column, MetaData, String, Table, inspect, insert, select, text

from .schema import metadata
from . import extensions_schema  # Register feature tables in the shared metadata.


history = Table(
    "e3_schema_migrations", MetaData(),
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
    definitions = {
        "user_preferences": {
            "status_filter": "TEXT", "include_ignored_overdue": "INTEGER",
            "show_graded": "INTEGER", "ignored_overdue_uids": "TEXT", "semester_filter": "TEXT",
        },
        "user_fetch_state": {"semester_catalog": "TEXT", "selected_semesters": "TEXT"},
        "courses": {"semester_key": "VARCHAR(32)", "semester_label": "VARCHAR(64)"},
        "study_plan_video_markers": {
            "summary": "TEXT NOT NULL DEFAULT ''",
            "summary_status": "VARCHAR(16) NOT NULL DEFAULT ''",
            "summary_generated_at": "VARCHAR(64)",
        },
        "study_plan_video_records": {
            "playback_seconds": "FLOAT", "progress_version": "INTEGER NOT NULL DEFAULT 0",
        },
        "study_recall_card_reviews": {"ideal_review_at": "VARCHAR(10)"},
        "study_recall_sessions": {
            "source_transcription": "TEXT", "uncertain_fragments": "TEXT",
            "correction_records": "TEXT", "organization_mode": "VARCHAR(32)",
        },
        "study_plan_videos": {
            "youtube_video_id": "VARCHAR(64)", "youtube_playlist_id": "VARCHAR(128)", "youtube_url": "TEXT",
        },
        "assignments": {
            "submitted_count": "INTEGER", "participant_count": "INTEGER", "grade_text": "TEXT",
            "submitted_at": "TEXT", "submitted_ts": "INTEGER", "remaining_text": "TEXT",
        },
    }
    for table, columns in definitions.items():
        added = _add_columns(conn, table, columns)
        if table == "study_plan_video_records" and "playback_seconds" in added:
            conn.execute(text(
                "UPDATE study_plan_video_records SET playback_seconds = watched_seconds "
                "WHERE playback_seconds IS NULL"
            ))


def _feature_schema(conn):
    _add_columns(conn, "study_player_settings", {
        "default_playback_rate": "FLOAT NOT NULL DEFAULT 1.0",
        "seek_back_seconds": "INTEGER NOT NULL DEFAULT 10",
        "seek_forward_seconds": "INTEGER NOT NULL DEFAULT 10",
        "seek_repeat_ms": "INTEGER NOT NULL DEFAULT 150",
        "playback_rate_step": "FLOAT NOT NULL DEFAULT 0.05",
        "volume_step": "INTEGER NOT NULL DEFAULT 5",
        "controls_hide_ms": "INTEGER NOT NULL DEFAULT 2600",
        "pause_on_marker": "INTEGER NOT NULL DEFAULT 0",
        "show_speed_presets": "INTEGER NOT NULL DEFAULT 1",
    })
    for table in metadata.sorted_tables:
        for index in table.indexes:
            index.create(conn, checkfirst=True)


def _user_profile_schema(conn):
    _add_columns(conn, "users", {"profile_surname": "VARCHAR(16)"})


MIGRATIONS = (
    ("0001_core_schema", _core_schema),
    ("0002_feature_schema", _feature_schema),
    ("0003_user_profile", _user_profile_schema),
)


def migration_status(engine):
    with engine.connect() as conn:
        applied = set(conn.execute(select(history.c.version)).scalars()) if inspect(conn).has_table(history.name) else set()
    known = {version for version, _ in MIGRATIONS}
    rows = [{"version": version, "applied": version in applied, "known": True} for version, _ in MIGRATIONS]
    rows.extend({"version": version, "applied": True, "known": False} for version in sorted(applied - known))
    return rows


def run_migrations(engine):
    completed = []
    with _migration_lock, engine.connect() as conn:
        dialect = conn.dialect.name
        if dialect == "mysql":
            if conn.execute(text("SELECT GET_LOCK('e3_tracker_schema', 15)")).scalar() != 1:
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
                    raise RuntimeError(f"Database requires a newer application: {sorted(unknown)}")
                for version, upgrade in MIGRATIONS:
                    if version in applied:
                        continue
                    upgrade(conn)
                    conn.execute(insert(history).values(
                        version=version, applied_at=datetime.now(timezone.utc).isoformat(),
                    ))
                    completed.append(version)
        finally:
            if dialect == "mysql":
                conn.execute(text("SELECT RELEASE_LOCK('e3_tracker_schema')"))
                conn.commit()
    return completed
