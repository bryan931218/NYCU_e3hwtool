"""Recall-card favorite HTTP endpoints."""
from typing import Any
from flask import request
from e3_tracker.study.persistence.favorites import _favorite_key, _favorite_rows, _favorite_count, _set_favorite
from e3_tracker.platform.auth import admin_username as _admin_username


def register_favorite_routes(app: Any, storage: Any) -> None:
    if "admin_study_recall_favorites" in app.view_functions:
        return

    def admin_study_recall_favorites():
        username = _admin_username(storage)
        if not username:
            return {"ok": False, "error": "unauthorized"}, 401
        cards = _favorite_rows(storage, username)
        return {"ok": True, "count": len(cards), "cards": cards}

    def admin_study_recall_favorite_toggle(session_id: int, concept_index: int):
        username = _admin_username(storage)
        if not username:
            return {"ok": False, "error": "unauthorized"}, 401
        payload = request.get_json(silent=True) or {}
        desired = payload.get("favorite")
        if desired is not None and not isinstance(desired, bool):
            return {"ok": False, "error": "invalid_favorite"}, 400
        try:
            favorite = _set_favorite(
                storage,
                username,
                int(session_id),
                int(concept_index),
                desired,
            )
        except LookupError:
            return {"ok": False, "error": "card_not_found"}, 404
        return {
            "ok": True,
            "favorite": favorite,
            "key": _favorite_key(session_id, concept_index),
            "count": _favorite_count(storage, username),
        }

    app.add_url_rule(
        "/admin/study-recall/favorites.json",
        endpoint="admin_study_recall_favorites",
        view_func=admin_study_recall_favorites,
        methods=["GET"],
    )
    app.add_url_rule(
        "/admin/study-recall/favorites/<int:session_id>/<int:concept_index>",
        endpoint="admin_study_recall_favorite_toggle",
        view_func=admin_study_recall_favorite_toggle,
        methods=["POST"],
    )
