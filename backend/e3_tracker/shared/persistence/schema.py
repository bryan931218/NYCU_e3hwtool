"""SQLAlchemy table definitions; one shared metadata instance."""
from sqlalchemy import Column, Float, Index, Integer, MetaData, String, Table, Text, UniqueConstraint

metadata = MetaData()

users_table = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("username", String(191), nullable=False, unique=True),
    Column("is_guest", Integer, nullable=False, default=0),
    Column("is_admin", Integer, nullable=False, default=0),
    Column("created_at", String(64), nullable=False),
    Column("last_seen", String(64)),
)

user_preferences_table = Table(
    "user_preferences",
    metadata,
    Column("user_id", Integer, primary_key=True),
    Column("view_mode", String(32)),
    Column("status_filter", String(32)),
    Column("semester_filter", Text),
    Column("include_ignored_overdue", Integer, nullable=False, default=0),
    Column("show_overdue", Integer, nullable=False, default=0),
    Column("show_completed", Integer, nullable=False, default=0),
    Column("show_graded", Integer, nullable=False, default=0),
    Column("ignored_overdue_uids", Text),
    Column("updated_at", String(64)),
)

courses_table = Table(
    "courses",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, nullable=False),
    Column("course_code", Integer, nullable=False),
    Column("title", Text, nullable=False),
    Column("url", Text),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
    UniqueConstraint("user_id", "course_code", name="uq_courses_user_course"),
)

Index("ix_courses_user_id", courses_table.c.user_id)

assignments_table = Table(
    "assignments",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("course_id", Integer, nullable=False),
    Column("uid", String(255), nullable=False),
    Column("title", Text, nullable=False),
    Column("url", Text),
    Column("due_at", String(64)),
    Column("due_ts", Integer),
    Column("overdue", Integer, nullable=False, default=0),
    Column("completed", Integer, nullable=False, default=0),
    Column("raw_status_text", Text),
    Column("grade_text", Text),
    Column("submitted_at", String(64)),
    Column("submitted_ts", Integer),
    Column("remaining_text", Text),
    Column("submitted_count", Integer),
    Column("participant_count", Integer),
    Column("updated_at", String(64), nullable=False),
    UniqueConstraint("course_id", "uid", name="uq_assignments_course_uid"),
)

Index("ix_assignments_course_id", assignments_table.c.course_id)

Index("ix_assignments_due_ts", assignments_table.c.due_ts)

assignment_views_table = Table(
    "assignment_views",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, nullable=False),
    Column("assignment_uid", String(255), nullable=False),
    Column("first_seen_at", String(64), nullable=False),
    Column("first_seen_ts", Integer, nullable=False),
    UniqueConstraint("user_id", "assignment_uid", name="uq_assignment_views_user_uid"),
)

Index("ix_assignment_views_user_id", assignment_views_table.c.user_id)

user_fetch_state_table = Table(
    "user_fetch_state",
    metadata,
    Column("user_id", Integer, primary_key=True),
    Column("fetched_at", String(64)),
    Column("fetched_ts", Integer),
    Column("excel_data", Text),
    Column("error_count", Integer, nullable=False, default=0),
    Column("semester_catalog", Text),
    Column("selected_semesters", Text),
)

fetch_errors_table = Table(
    "fetch_errors",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, nullable=False),
    Column("course_code", Integer),
    Column("course_title", Text),
    Column("assignment_title", Text),
    Column("message", Text, nullable=False),
)

google_tokens_table = Table(
    "google_tokens",
    metadata,
    Column("user_id", Integer, primary_key=True),
    Column("access_token", Text),
    Column("refresh_token", Text),
    Column("scope", Text),
    Column("token_type", String(32)),
    Column("expires_at", Float),
    Column("updated_at", String(64)),
)

Index("ix_fetch_errors_user_id", fetch_errors_table.c.user_id)

web_sessions_table = Table(
    "web_sessions",
    metadata,
    Column("session_token", String(191), primary_key=True),
    Column("username", String(191), nullable=False),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
)

Index("ix_web_sessions_username", web_sessions_table.c.username)

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
    UniqueConstraint("announcement_id", "user_id", name="uq_announcement_votes_announcement_user"),
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
)

Index("ix_traffic_events_username", traffic_events_table.c.username)

Index("ix_traffic_events_ts", traffic_events_table.c.ts)

Index("ix_traffic_events_action", traffic_events_table.c.action)

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
