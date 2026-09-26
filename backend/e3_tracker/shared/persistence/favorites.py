"""Recall-card favorites: database operations without Flask dependencies."""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import text

def _favorite_key(session_id: int, concept_index: int) -> str:
    return f"{int(session_id)}:{int(concept_index)}"


def _concept_for_session(storage: Any, session_id: int, concept_index: int) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    recall_session = storage.get_study_recall_session(int(session_id))
    if not recall_session:
        return None, None
    concepts = recall_session.get("key_concepts") or []
    if not isinstance(concepts, list) or concept_index < 0 or concept_index >= len(concepts):
        return recall_session, None
    concept = concepts[concept_index]
    if not isinstance(concept, dict):
        return recall_session, None
    return recall_session, concept


def _favorite_rows(storage: Any, username: str) -> List[Dict[str, Any]]:
    with storage._lock, storage._engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT session_id, concept_index, created_at "
                "FROM study_recall_favorites "
                "WHERE username = :username "
                "ORDER BY created_at DESC, session_id DESC, concept_index ASC"
            ),
            {"username": username},
        ).mappings().all()

    session_cache: Dict[int, Optional[Dict[str, Any]]] = {}
    favorites: List[Dict[str, Any]] = []
    stale: List[Tuple[int, int]] = []
    for row in rows:
        session_id = int(row.get("session_id") or 0)
        concept_index = int(row.get("concept_index") or 0)
        if session_id not in session_cache:
            session_cache[session_id] = storage.get_study_recall_session(session_id)
        recall_session = session_cache.get(session_id)
        concepts = recall_session.get("key_concepts") if recall_session else None
        if not isinstance(concepts, list) or concept_index < 0 or concept_index >= len(concepts):
            stale.append((session_id, concept_index))
            continue
        concept = concepts[concept_index]
        if not isinstance(concept, dict):
            stale.append((session_id, concept_index))
            continue

        card_type = str(concept.get("card_type") or "concept").strip() or "concept"
        favorites.append(
            {
                "key": _favorite_key(session_id, concept_index),
                "session_id": session_id,
                "concept_index": concept_index,
                "favorite_at": str(row.get("created_at") or ""),
                "subject": str(recall_session.get("subject") or "未分類"),
                "session_title": str(recall_session.get("title") or "未命名筆記"),
                "study_date": str(recall_session.get("study_date") or ""),
                "title": str(concept.get("concept") or "未命名重點"),
                "topic": str(concept.get("topic") or ""),
                "card_type": card_type,
                "card_type_label": "例題" if card_type == "example" else "觀念",
                "recall_cue": str(concept.get("recall_cue") or ""),
                "core_summary": str(concept.get("core_summary") or ""),
                "explanation": str(concept.get("explanation") or ""),
                "memory_hint": str(concept.get("memory_hint") or ""),
                "common_confusion": str(concept.get("common_confusion") or ""),
                "simple_example": str(concept.get("simple_example") or ""),
                "url": f"/admin/study-recall?session_id={session_id}#concept-{concept_index}",
            }
        )

    if stale:
        with storage._lock, storage._engine.begin() as conn:
            for session_id, concept_index in stale:
                conn.execute(
                    text(
                        "DELETE FROM study_recall_favorites "
                        "WHERE username = :username AND session_id = :session_id "
                        "AND concept_index = :concept_index"
                    ),
                    {
                        "username": username,
                        "session_id": session_id,
                        "concept_index": concept_index,
                    },
                )
    return favorites


def _favorite_count(storage: Any, username: str) -> int:
    with storage._lock, storage._engine.connect() as conn:
        value = conn.execute(
            text("SELECT COUNT(*) FROM study_recall_favorites WHERE username = :username"),
            {"username": username},
        ).scalar()
    return max(0, int(value or 0))


def _set_favorite(storage: Any, username: str, session_id: int, concept_index: int, desired: Optional[bool]) -> bool:
    recall_session, concept = _concept_for_session(storage, session_id, concept_index)
    if not recall_session or not concept:
        raise LookupError("card_not_found")

    with storage._lock, storage._engine.begin() as conn:
        exists = bool(
            conn.execute(
                text(
                    "SELECT 1 FROM study_recall_favorites "
                    "WHERE username = :username AND session_id = :session_id "
                    "AND concept_index = :concept_index"
                ),
                {
                    "username": username,
                    "session_id": session_id,
                    "concept_index": concept_index,
                },
            ).first()
        )
        favorite = (not exists) if desired is None else bool(desired)
        if favorite and not exists:
            conn.execute(
                text(
                    "INSERT INTO study_recall_favorites "
                    "(username, session_id, concept_index, created_at) "
                    "VALUES (:username, :session_id, :concept_index, :created_at)"
                ),
                {
                    "username": username,
                    "session_id": session_id,
                    "concept_index": concept_index,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        elif not favorite and exists:
            conn.execute(
                text(
                    "DELETE FROM study_recall_favorites "
                    "WHERE username = :username AND session_id = :session_id "
                    "AND concept_index = :concept_index"
                ),
                {
                    "username": username,
                    "session_id": session_id,
                    "concept_index": concept_index,
                },
            )
    return favorite
