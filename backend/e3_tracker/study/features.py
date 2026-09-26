"""Application-local feature registration; no template or factory mutation."""
from e3_tracker.platform.paths import ROOT_DIR
from e3_tracker.study.routes.player_settings import register_player_settings_routes
from e3_tracker.study.routes.study_calendar import register_study_calendar_routes
from e3_tracker.study.routes.recall_favorites import register_favorite_routes


def register_application_features(app):
    if "e3_features" in app.extensions:
        return
    storage = app.extensions["e3_storage"]
    root = ROOT_DIR
    settings_template = (root / "frontend/study/templates/admin_study_player_settings.html").read_text(encoding="utf-8")
    register_player_settings_routes(
        app, storage, settings_template, root / "backend/tools/e3_discord_presence.py",
    )
    register_study_calendar_routes(app, storage)
    register_favorite_routes(app, storage)
    app.extensions["e3_features"] = ("player_settings", "study_calendar", "recall_favorites")
