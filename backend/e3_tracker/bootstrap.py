"""Shared application assembly for local development and WSGI deployment."""

from .api import web
from .shared.deployment_runtime import install_deployment_runtime
from .shared.player_control_runtime import install_player_control_dock
from .shared.study_activity_progress_runtime import install_unique_study_activity_progress
from .shared.study_calendar_runtime import install_study_calendar_runtime
from .shared.study_note_upload_runtime import install_study_note_upload_runtime
from .shared.study_recall_favorites_runtime import install_study_recall_favorites_runtime
from .shared.study_recall_library_runtime import install_study_recall_library_runtime
from .shared.study_rest_day_toggle_runtime import install_rest_day_toggle
from .shared.study_rest_day_runtime import install_study_rest_day_runtime
from .shared.study_timeline_rest_cancel_runtime import install_timeline_rest_cancel
from .shared.storage import PersistentStorage


# These installers wrap the app factory and templates; preserve their order.
install_unique_study_activity_progress(PersistentStorage)
install_deployment_runtime(web)
install_player_control_dock(web)
install_study_calendar_runtime(web)
install_study_note_upload_runtime(web)
install_study_recall_library_runtime(web)
install_study_recall_favorites_runtime(web)
install_rest_day_toggle(web)
install_study_rest_day_runtime(web)
install_timeline_rest_cancel(web)


def create_app(**kwargs):
    """Create the same configured app for every server entry point."""
    return web.create_app(**kwargs)
