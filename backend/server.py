import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env", override=False)

from e3_tracker.api import web
from e3_tracker.shared.deployment_runtime import install_deployment_runtime
from e3_tracker.shared.player_control_runtime import install_player_control_dock
from e3_tracker.shared.study_activity_progress_runtime import install_unique_study_activity_progress
from e3_tracker.shared.study_calendar_runtime import install_study_calendar_runtime
from e3_tracker.shared.study_note_upload_runtime import install_study_note_upload_runtime
from e3_tracker.shared.study_recall_favorites_runtime import install_study_recall_favorites_runtime
from e3_tracker.shared.study_recall_library_runtime import install_study_recall_library_runtime
from e3_tracker.shared.study_rest_day_toggle_runtime import install_rest_day_toggle
from e3_tracker.shared.storage import PersistentStorage


install_unique_study_activity_progress(PersistentStorage)
install_deployment_runtime(web)
install_player_control_dock(web)
install_study_calendar_runtime(web)
install_study_note_upload_runtime(web)
install_study_recall_library_runtime(web)
install_study_recall_favorites_runtime(web)
install_rest_day_toggle(web)


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def main():
    app = web.create_app()
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    reload_enabled = _env_flag("E3_DEV_RELOAD", default=False)
    template_dir = BASE_DIR / "frontend" / "templates"
    extra_files = [str(path) for path in template_dir.rglob("*.html")]
    app.run(
        host=host,
        port=port,
        debug=reload_enabled,
        use_reloader=reload_enabled,
        extra_files=extra_files if reload_enabled else None,
    )


if __name__ == "__main__":
    main()
