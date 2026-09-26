from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from flask import session
from e3_tracker.study.persistence.favorites import _favorite_key, _concept_for_session, _favorite_rows, _favorite_count, _set_favorite
def _ensure_favorite_table(storage: Any) -> None:
    from e3_tracker.platform.persistence.migrations import run_migrations
    run_migrations(storage._engine)


def _admin_username(storage: Any) -> Optional[str]:
    from e3_tracker.platform.auth import admin_username
    return admin_username(storage)
def _register_favorite_routes(app: Any, storage: Any) -> None:
    from e3_tracker.study.routes.recall_favorites import register_favorite_routes
    return register_favorite_routes(app, storage)


def install_study_recall_favorites_runtime(web_module: Any) -> None:
    if getattr(web_module, "__e3_study_recall_favorites_runtime_installed", False):
        return
    web_module.__e3_study_recall_favorites_runtime_installed = True

    root_dir = Path(__file__).resolve().parents[4]
    partial_path = root_dir / "frontend" / "study" / "templates" / "_study_recall_favorites.html"
    if partial_path.exists():
        partial = partial_path.read_text(encoding="utf-8")
        marker = "__e3StudyRecallFavoritesInstalled"
        template = str(web_module.STUDY_RECALL_TEMPLATE)
        if marker not in template:
            if "</body>" in template:
                web_module.STUDY_RECALL_TEMPLATE = template.replace(
                    "</body>", partial + "\n</body>", 1
                )
            else:
                web_module.STUDY_RECALL_TEMPLATE = template + "\n" + partial

    original_create_app = web_module.create_app

    def create_app_with_recall_favorites(*args: Any, **kwargs: Any):
        app = original_create_app(*args, **kwargs)
        storage = app.extensions.get("e3_storage")
        if storage is not None:
            _ensure_favorite_table(storage)
            _register_favorite_routes(app, storage)
        return app

    web_module.create_app = create_app_with_recall_favorites
