"""Deployment schema registry, aggregating independently owned table definitions."""

from .metadata import metadata
from e3_tracker.assignments.persistence.membership import assignment_memberships
from e3_tracker.assignments.persistence.course_announcements import course_announcement_cache
from e3_tracker.assignments.persistence.schema import (
    user_preferences_table,
    courses_table,
    assignments_table,
    assignment_views_table,
    user_fetch_state_table,
    fetch_errors_table,
    google_tokens_table,
)
from e3_tracker.study.persistence.schema import (
    study_plan_videos_table,
    study_plan_video_overrides_table,
    youtube_storyboard_metadata_table,
    study_plan_video_records_table,
    study_plan_daily_snapshots_table,
    study_plan_activity_events_table,
    study_time_sessions_table,
    study_plan_video_markers_table,
    study_plan_replan_settings_table,
    study_plan_rest_days_table,
    study_assistant_actions_table,
    study_recall_sessions_table,
    study_recall_glossaries_table,
    study_note_upload_jobs_table,
    study_recall_attempts_table,
    study_recall_card_reviews_table,
)
from e3_tracker.platform.persistence.core_schema import (
    users_table,
    web_sessions_table,
    announcements_table,
    announcement_votes_table,
    feedback_table,
    traffic_state_table,
    traffic_events_table,
)
