"""Study-owned database table definitions."""
from sqlalchemy import Column, Float, Index, Integer, String, Table, Text, UniqueConstraint
from e3_tracker.platform.persistence.metadata import metadata

study_plan_videos_table = Table(
    "study_plan_videos",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("subject", String(64), nullable=False),
    Column("sequence", Integer, nullable=False),
    Column("title", Text, nullable=False),
    Column("duration_seconds", Float, nullable=False),
    Column("youtube_video_id", String(64)),
    Column("youtube_playlist_id", String(128)),
    Column("youtube_url", Text),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
    UniqueConstraint("subject", "sequence", name="uq_study_plan_video_subject_sequence"),
)

Index("ix_study_plan_videos_subject_sequence", study_plan_videos_table.c.subject, study_plan_videos_table.c.sequence)

study_plan_video_overrides_table = Table(
    "study_plan_video_overrides",
    metadata,
    Column("video_id", Integer, primary_key=True),
    Column("youtube_video_id", String(64)),
    Column("youtube_playlist_id", String(128)),
    Column("youtube_url", Text),
    Column("updated_at", String(64), nullable=False),
)

youtube_storyboard_metadata_table = Table(
    "youtube_storyboard_metadata",
    metadata,
    Column("youtube_video_id", String(64), primary_key=True),
    Column("duration_seconds", Float, nullable=False, default=0),
    Column("storyboard_spec", Text, nullable=False),
    Column("updated_at", String(64), nullable=False),
)

study_plan_video_records_table = Table(
    "study_plan_video_records",
    metadata,
    Column("video_id", Integer, primary_key=True),
    Column("watched_seconds", Float, nullable=False, default=0),
    Column("playback_seconds", Float, nullable=False, default=0),
    Column("progress_version", Integer, nullable=False, default=0),
    Column("notes", Text),
    Column("updated_at", String(64), nullable=False),
)

study_plan_daily_snapshots_table = Table(
    "study_plan_daily_snapshots",
    metadata,
    Column("day", String(10), primary_key=True),
    Column("total_watched_seconds", Float, nullable=False, default=0),
    Column("updated_at", String(64), nullable=False),
)

Index("ix_study_plan_daily_snapshots_day", study_plan_daily_snapshots_table.c.day)

study_plan_activity_events_table = Table(
    "study_plan_activity_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("day", String(10), nullable=False),
    Column("video_id", Integer, nullable=False),
    Column("previous_watched_seconds", Float, nullable=False, default=0),
    Column("watched_seconds", Float, nullable=False, default=0),
    Column("delta_seconds", Float, nullable=False, default=0),
    Column("updated_at", String(64), nullable=False),
)

Index("ix_study_plan_activity_events_day", study_plan_activity_events_table.c.day)

Index("ix_study_plan_activity_events_video_id", study_plan_activity_events_table.c.video_id)

Index("ix_study_plan_activity_events_updated_at", study_plan_activity_events_table.c.updated_at)

study_time_sessions_table = Table(
    "study_time_sessions",
    metadata,
    Column("session_key", String(80), primary_key=True),
    Column("day", String(10), nullable=False),
    Column("kind", String(16), nullable=False),
    Column("video_id", Integer),
    Column("label", String(191), nullable=False),
    Column("elapsed_seconds", Float, nullable=False, default=0),
    Column("completed", Integer, nullable=False, default=0),
    Column("started_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
)

Index("ix_study_time_sessions_day", study_time_sessions_table.c.day)

Index("ix_study_time_sessions_kind", study_time_sessions_table.c.kind)

study_plan_video_markers_table = Table(
    "study_plan_video_markers",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("video_id", Integer, nullable=False),
    Column("playback_seconds", Float, nullable=False),
    Column("note", String(280), nullable=False),
    Column("summary", Text, nullable=False, default=""),
    Column("summary_status", String(16), nullable=False, default=""),
    Column("summary_generated_at", String(64)),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
)

Index("ix_study_plan_video_markers_video", study_plan_video_markers_table.c.video_id)

Index(
    "ix_study_plan_video_markers_timeline",
    study_plan_video_markers_table.c.video_id,
    study_plan_video_markers_table.c.playback_seconds,
)

study_plan_replan_settings_table = Table(
    "study_plan_replan_settings",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("start_date", String(10), nullable=False),
    Column("end_date", String(10), nullable=False),
    Column("weekday_minutes", Float, nullable=False),
    Column("weekend_minutes", Float, nullable=False),
    Column("baseline_by_subject", Text, nullable=False),
    Column("subject_targets", Text, nullable=False),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
)

study_plan_rest_days_table = Table(
    "study_plan_rest_days",
    metadata,
    Column("study_date", String(10), primary_key=True),
    Column("created_at", String(64), nullable=False),
)

study_assistant_actions_table = Table(
    "study_assistant_actions",
    metadata,
    Column("action_id", String(48), primary_key=True),
    Column("username", String(191), nullable=False),
    Column("status", String(16), nullable=False),
    Column("action_type", String(48), nullable=False),
    Column("action_payload", Text, nullable=False),
    Column("before_state", Text, nullable=False),
    Column("after_state", Text),
    Column("summary", String(280), nullable=False),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
)

Index("ix_study_assistant_actions_user", study_assistant_actions_table.c.username)

Index("ix_study_assistant_actions_status", study_assistant_actions_table.c.status)

study_recall_sessions_table = Table(
    "study_recall_sessions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("study_date", String(10), nullable=False),
    Column("subject", String(64), nullable=False),
    Column("title", String(191), nullable=False),
    Column("image_filenames", Text, nullable=False),
    Column("summary", Text, nullable=False),
    Column("key_concepts", Text, nullable=False),
    Column("source_transcription", Text),
    Column("uncertain_fragments", Text),
    Column("correction_records", Text),
    Column("organization_mode", String(32)),
    Column("quiz_data", Text, nullable=False),
    Column("last_score_percent", Float),
    Column("last_self_rating", Integer),
    Column("next_review_at", String(10)),
    Column("review_count", Integer, nullable=False, default=0),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
)

Index("ix_study_recall_sessions_next_review", study_recall_sessions_table.c.next_review_at)

study_recall_glossaries_table = Table(
    "study_recall_glossaries",
    metadata,
    Column("subject", String(64), primary_key=True),
    Column("source_signature", String(64), nullable=False),
    Column("status", String(16), nullable=False),
    Column("terms", Text, nullable=False),
    Column("error", Text),
    Column("updated_at", String(64), nullable=False),
)

Index("ix_study_recall_glossaries_status", study_recall_glossaries_table.c.status)

study_note_upload_jobs_table = Table(
    "study_note_upload_jobs",
    metadata,
    Column("job_id", String(96), primary_key=True),
    Column("username", String(191), nullable=False),
    Column("status", String(24), nullable=False),
    Column("progress", Integer, nullable=False, default=0),
    Column("message", Text, nullable=False),
    Column("session_id", Integer),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
)

Index(
    "ix_study_note_upload_jobs_user_updated",
    study_note_upload_jobs_table.c.username,
    study_note_upload_jobs_table.c.updated_at,
)

study_recall_attempts_table = Table(
    "study_recall_attempts",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("session_id", Integer, nullable=False),
    Column("score_percent", Float, nullable=False),
    Column("self_rating", Integer, nullable=False),
    Column("answers", Text, nullable=False),
    Column("next_review_at", String(10), nullable=False),
    Column("created_at", String(64), nullable=False),
)

Index("ix_study_recall_attempts_session", study_recall_attempts_table.c.session_id)

study_recall_card_reviews_table = Table(
    "study_recall_card_reviews",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("session_id", Integer, nullable=False),
    Column("concept_index", Integer, nullable=False),
    Column("rating", Integer, nullable=False),
    Column("interval_days", Integer, nullable=False),
    Column("ideal_review_at", String(10), nullable=False),
    Column("next_review_at", String(10), nullable=False),
    Column("created_at", String(64), nullable=False),
)

Index("ix_study_recall_card_reviews_session", study_recall_card_reviews_table.c.session_id)

Index("ix_study_recall_card_reviews_next_review", study_recall_card_reviews_table.c.next_review_at)
