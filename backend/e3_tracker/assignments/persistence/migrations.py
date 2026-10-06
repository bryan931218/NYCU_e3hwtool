"""Assignments-owned additive upgrades for existing installations."""


def upgrade_grading_notifications(conn, add_columns):
    add_columns(conn, "assignment_notification_seen", {
        "graded_observed": "INTEGER NOT NULL DEFAULT 0",
    })
    add_columns(conn, "assignments", {"feedback_text": "TEXT"})


def upgrade_legacy_columns(conn, add_columns):
    definitions = {
        "user_preferences": {
            "status_filter": "TEXT",
            "include_ignored_overdue": "INTEGER",
            "show_graded": "INTEGER",
            "ignored_overdue_uids": "TEXT",
            "semester_filter": "TEXT",
        },
        "user_fetch_state": {"semester_catalog": "TEXT", "selected_semesters": "TEXT"},
        "courses": {"semester_key": "VARCHAR(32)", "semester_label": "VARCHAR(64)"},
        "assignments": {
            "submitted_count": "INTEGER",
            "participant_count": "INTEGER",
            "grade_text": "TEXT",
            "submitted_at": "TEXT",
            "submitted_ts": "INTEGER",
            "remaining_text": "TEXT",
        },
    }
    for table, columns in definitions.items():
        add_columns(conn, table, columns)
