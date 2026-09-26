"""Study-owned additive upgrades for existing installations."""

from sqlalchemy import text


def upgrade_legacy_columns(conn, add_columns):
    definitions = {
        "study_plan_video_markers": {
            "summary": "TEXT NOT NULL DEFAULT ''",
            "summary_status": "VARCHAR(16) NOT NULL DEFAULT ''",
            "summary_generated_at": "VARCHAR(64)",
        },
        "study_plan_video_records": {
            "playback_seconds": "FLOAT",
            "progress_version": "INTEGER NOT NULL DEFAULT 0",
        },
        "study_recall_card_reviews": {"ideal_review_at": "VARCHAR(10)"},
        "study_recall_sessions": {
            "source_transcription": "TEXT",
            "uncertain_fragments": "TEXT",
            "correction_records": "TEXT",
            "organization_mode": "VARCHAR(32)",
        },
        "study_plan_videos": {
            "youtube_video_id": "VARCHAR(64)",
            "youtube_playlist_id": "VARCHAR(128)",
            "youtube_url": "TEXT",
        },
    }
    for table, columns in definitions.items():
        added = add_columns(conn, table, columns)
        if table == "study_plan_video_records" and "playback_seconds" in added:
            conn.execute(
                text(
                    "UPDATE study_plan_video_records SET playback_seconds = watched_seconds WHERE playback_seconds IS NULL"
                )
            )


def upgrade_player_columns(conn, add_columns):
    add_columns(
        conn,
        "study_player_settings",
        {
            "default_playback_rate": "FLOAT NOT NULL DEFAULT 1.0",
            "seek_back_seconds": "INTEGER NOT NULL DEFAULT 10",
            "seek_forward_seconds": "INTEGER NOT NULL DEFAULT 10",
            "seek_repeat_ms": "INTEGER NOT NULL DEFAULT 150",
            "playback_rate_step": "FLOAT NOT NULL DEFAULT 0.05",
            "volume_step": "INTEGER NOT NULL DEFAULT 5",
            "controls_hide_ms": "INTEGER NOT NULL DEFAULT 2600",
            "pause_on_marker": "INTEGER NOT NULL DEFAULT 0",
            "show_speed_presets": "INTEGER NOT NULL DEFAULT 1",
        },
    )
