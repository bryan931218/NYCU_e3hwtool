"""Persistence operations for recall."""
import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional
from fsrs import Card as FSRSCard
from fsrs import Rating as FSRSRating
from sqlalchemy import delete, insert, select, update
from ..source_localization import canonicalize_source_text

from .schema import study_recall_sessions_table, study_recall_glossaries_table, study_recall_attempts_table, study_recall_card_reviews_table
from .recall_support import _recall_search_similarity, _recall_search_formula_similarity, _recall_search_contains_formula, _recall_search_excerpt, _recall_search_resolved_page, RECALL_DAILY_CAPACITY, RECALL_FSRS_SCHEDULER


class RecallStorage:
    @staticmethod
    def _decode_json_list(value: Any) -> List[Any]:
        try:
            parsed = json.loads(value) if value else []
        except (TypeError, ValueError):
            return []
        return parsed if isinstance(parsed, list) else []

    @staticmethod
    def _decode_json_dict(value: Any) -> Dict[str, Any]:
        try:
            parsed = json.loads(value) if value else {}
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @staticmethod
    def _recall_interval_days(rating: int, previous_rating: Optional[int], previous_interval: int) -> int:
        """Legacy fallback used only when an older FSRS state cannot be reconstructed."""
        rating = max(1, min(int(rating), 5))
        if rating == 1:
            return 1
        if rating == 2:
            return 2
        if previous_rating is None or previous_rating <= 2:
            return {3: 3, 4: 5, 5: 7}[rating]
        multiplier = {3: 1.5, 4: 2.2, 5: 2.8}[rating]
        minimum = {3: 3, 4: 5, 5: 7}[rating]
        return min(120, max(minimum, int(round(max(1, previous_interval) * multiplier))))

    @staticmethod
    def _recall_fsrs_rating(rating: int) -> FSRSRating:
        normalized = max(1, min(int(rating), 5))
        return {
            1: FSRSRating.Again,
            2: FSRSRating.Again,
            3: FSRSRating.Hard,
            4: FSRSRating.Good,
            5: FSRSRating.Easy,
        }[normalized]

    @classmethod
    def _recall_fsrs_card(
        cls,
        *,
        session_id: int,
        concept_index: int,
        concept: Dict[str, Any],
        history: List[Any],
    ) -> FSRSCard:
        stored_state = concept.get("fsrs_card") if isinstance(concept, dict) else None
        if stored_state:
            try:
                serialized = stored_state if isinstance(stored_state, str) else json.dumps(stored_state)
                return FSRSCard.from_json(serialized)
            except (TypeError, ValueError, KeyError):
                pass
        card = FSRSCard(card_id=max(1, session_id * 1000 + concept_index + 1))
        for row in sorted(history, key=lambda item: (str(item.created_at), int(item.id))):
            try:
                reviewed_at = datetime.fromisoformat(str(row.created_at).replace("Z", "+00:00"))
                if reviewed_at.tzinfo is None:
                    reviewed_at = reviewed_at.replace(tzinfo=timezone.utc)
                else:
                    reviewed_at = reviewed_at.astimezone(timezone.utc)
                card, _review_log = RECALL_FSRS_SCHEDULER.review_card(
                    card,
                    cls._recall_fsrs_rating(int(row.rating)),
                    reviewed_at,
                )
            except (TypeError, ValueError):
                continue
        return card

    @staticmethod
    def _balanced_recall_date(
        *,
        review_day,
        ideal_day,
        interval_days: int,
        rating: int,
        scheduled_loads: Dict[str, int],
        daily_capacity: int = RECALL_DAILY_CAPACITY,
    ):
        if rating <= 1 or interval_days <= 1:
            candidates = [ideal_day]
        else:
            flexibility = min(12, max(1, int(round(interval_days * (0.08 if rating <= 3 else 0.15)))))
            offsets = [0]
            for distance in range(1, flexibility + 1):
                offsets.extend((distance, -distance))
            candidates = [
                ideal_day + timedelta(days=offset)
                for offset in offsets
                if ideal_day + timedelta(days=offset) > review_day
            ]
        available = [
            candidate
            for candidate in candidates
            if scheduled_loads.get(candidate.isoformat(), 0) < daily_capacity
        ]
        pool = available or candidates
        return min(
            pool,
            key=lambda candidate: (
                scheduled_loads.get(candidate.isoformat(), 0),
                abs((candidate - ideal_day).days),
                1 if candidate < ideal_day else 0,
                candidate,
            ),
        )

    def create_study_recall_session(
        self,
        *,
        study_date: str,
        subject: str,
        title: str,
        image_filenames: List[str],
        summary: str,
        key_concepts: List[Dict[str, Any]],
        source_transcription: Optional[List[Dict[str, Any]]] = None,
        uncertain_fragments: Optional[List[Dict[str, Any]]] = None,
        correction_records: Optional[List[Dict[str, Any]]] = None,
        organization_mode: str = "faithful",
    ) -> int:
        now = self._now_iso()
        values = {
            "study_date": study_date,
            "subject": subject,
            "title": title,
            "image_filenames": json.dumps(image_filenames, ensure_ascii=False),
            "summary": summary,
            "key_concepts": json.dumps(key_concepts, ensure_ascii=False),
            "source_transcription": json.dumps(source_transcription or [], ensure_ascii=False),
            "uncertain_fragments": json.dumps(uncertain_fragments or [], ensure_ascii=False),
            "correction_records": json.dumps(correction_records or [], ensure_ascii=False),
            "organization_mode": str(organization_mode or "faithful")[:32],
            "quiz_data": "[]",
            "review_count": 0,
            "created_at": now,
            "updated_at": now,
        }
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(insert(study_recall_sessions_table).values(**values))
            return int(result.inserted_primary_key[0])

    def delete_study_recall_session(self, session_id: int) -> bool:
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            exists = conn.execute(
                select(study_recall_sessions_table.c.id).where(study_recall_sessions_table.c.id == session_id)
            ).fetchone()
            if not exists:
                return False
            remaining_sessions = conn.execute(
                select(
                    study_recall_sessions_table.c.id,
                    study_recall_sessions_table.c.key_concepts,
                ).where(study_recall_sessions_table.c.id != session_id)
            ).fetchall()
            for remaining in remaining_sessions:
                concepts = self._decode_json_list(remaining.key_concepts)
                changed = False
                for concept in concepts:
                    if not isinstance(concept, dict) or not isinstance(concept.get("relations"), list):
                        continue
                    filtered = []
                    for relation in concept["relations"]:
                        try:
                            related_session_id = int(relation.get("session_id") or 0) if isinstance(relation, dict) else 0
                        except (TypeError, ValueError):
                            related_session_id = 0
                        if related_session_id != session_id:
                            filtered.append(relation)
                    if len(filtered) != len(concept["relations"]):
                        concept["relations"] = filtered
                        changed = True
                if changed:
                    conn.execute(
                        update(study_recall_sessions_table)
                        .where(study_recall_sessions_table.c.id == int(remaining.id))
                        .values(key_concepts=json.dumps(concepts, ensure_ascii=False), updated_at=now)
                    )
            conn.execute(delete(study_recall_card_reviews_table).where(study_recall_card_reviews_table.c.session_id == session_id))
            conn.execute(delete(study_recall_attempts_table).where(study_recall_attempts_table.c.session_id == session_id))
            conn.execute(delete(study_recall_sessions_table).where(study_recall_sessions_table.c.id == session_id))
        return True

    def rename_study_recall_session(self, session_id: int, title: str) -> bool:
        prepared_title = " ".join(str(title or "").split()).strip()[:120]
        if not prepared_title:
            return False
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(
                update(study_recall_sessions_table)
                .where(study_recall_sessions_table.c.id == int(session_id))
                .values(title=prepared_title, updated_at=self._now_iso())
            )
            return int(result.rowcount or 0) > 0

    def replace_study_recall_concepts_bulk(self, concepts_by_session: Dict[int, List[Dict[str, Any]]]) -> int:
        if not concepts_by_session:
            return 0
        now = self._now_iso()
        updated = 0
        with self._lock, self._engine.begin() as conn:
            for session_id, key_concepts in concepts_by_session.items():
                result = conn.execute(
                    update(study_recall_sessions_table)
                    .where(study_recall_sessions_table.c.id == int(session_id))
                    .values(
                        key_concepts=json.dumps(key_concepts, ensure_ascii=False),
                        updated_at=now,
                    )
                )
                updated += int(result.rowcount or 0)
        return updated

    def replace_study_recall_localization(
        self,
        session_id: int,
        *,
        key_concepts: List[Dict[str, Any]],
        source_transcription: List[Dict[str, Any]],
    ) -> bool:
        """Atomically persist bbox results and their reusable page OCR index."""
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(
                update(study_recall_sessions_table)
                .where(study_recall_sessions_table.c.id == int(session_id))
                .values(
                    key_concepts=json.dumps(key_concepts, ensure_ascii=False),
                    source_transcription=json.dumps(source_transcription, ensure_ascii=False),
                    updated_at=now,
                )
            )
        return bool(result.rowcount)

    def get_study_recall_session(self, session_id: int) -> Optional[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                select(study_recall_sessions_table).where(study_recall_sessions_table.c.id == session_id)
            ).fetchone()
            if not row:
                return None
            attempts = conn.execute(
                select(study_recall_attempts_table)
                .where(study_recall_attempts_table.c.session_id == session_id)
                .order_by(study_recall_attempts_table.c.id.desc())
            ).fetchall()
            card_review_rows = conn.execute(
                select(study_recall_card_reviews_table)
                .where(study_recall_card_reviews_table.c.session_id == session_id)
                .order_by(study_recall_card_reviews_table.c.id.asc())
            ).fetchall()
        item = dict(row._mapping)
        item["image_filenames"] = self._decode_json_list(item.get("image_filenames"))
        item["key_concepts"] = self._decode_json_list(item.get("key_concepts"))
        item["source_transcription"] = self._decode_json_list(item.get("source_transcription"))
        item["uncertain_fragments"] = self._decode_json_list(item.get("uncertain_fragments"))
        item["correction_records"] = self._decode_json_list(item.get("correction_records"))
        item["organization_mode"] = str(item.get("organization_mode") or "legacy")
        item["questions"] = self._decode_json_list(item.get("quiz_data"))
        card_history: Dict[int, List[Dict[str, Any]]] = {}
        for review in card_review_rows:
            card_history.setdefault(int(review.concept_index), []).append(
                {
                    "rating": int(review.rating),
                    "interval_days": int(review.interval_days),
                    "ideal_review_at": review.ideal_review_at or review.next_review_at,
                    "next_review_at": review.next_review_at,
                    "created_at": review.created_at,
                }
            )
        for index, concept in enumerate(item["key_concepts"]):
            if not isinstance(concept, dict):
                continue
            history = card_history.get(index, [])
            latest = history[-1] if history else None
            concept["review"] = {
                "history": history[-12:],
                "last_rating": latest["rating"] if latest else None,
                "interval_days": latest["interval_days"] if latest else None,
                "ideal_review_at": latest["ideal_review_at"] if latest else None,
                "next_review_at": latest["next_review_at"] if latest else None,
            }
        item["attempts"] = [
            {
                "score_percent": round(float(attempt.score_percent or 0), 1),
                "self_rating": int(attempt.self_rating or 0),
                "next_review_at": attempt.next_review_at,
                "created_at": attempt.created_at,
                "answers": self._decode_json_dict(attempt.answers),
            }
            for attempt in attempts
        ]
        return item

    def _study_recall_search_documents(self) -> List[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            signature_rows = conn.execute(
                select(
                    study_recall_sessions_table.c.id,
                    study_recall_sessions_table.c.updated_at,
                ).order_by(study_recall_sessions_table.c.id)
            ).fetchall()
        signature = tuple(
            (int(row.id), str(row.updated_at or ""))
            for row in signature_rows
        )
        with self._recall_search_cache_lock:
            if signature == self._recall_search_cache_signature:
                return self._recall_search_cache_documents

        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                select(
                    study_recall_sessions_table.c.id,
                    study_recall_sessions_table.c.study_date,
                    study_recall_sessions_table.c.subject,
                    study_recall_sessions_table.c.title,
                    study_recall_sessions_table.c.summary,
                    study_recall_sessions_table.c.image_filenames,
                    study_recall_sessions_table.c.source_transcription,
                    study_recall_sessions_table.c.key_concepts,
                    study_recall_sessions_table.c.created_at,
                    study_recall_sessions_table.c.updated_at,
                ).order_by(study_recall_sessions_table.c.created_at.desc())
            ).fetchall()

        documents: List[Dict[str, Any]] = []
        concept_fields = (
            "concept",
            "topic",
            "note_topic",
            "recall_cue",
            "core_summary",
            "explanation",
            "simple_example",
            "example_problem",
            "example_method",
            "common_confusion",
            "memory_hint",
        )
        for row in rows:
            image_filenames = [
                str(filename)
                for filename in self._decode_json_list(row.image_filenames)
                if str(filename or "").strip()
            ]
            if not image_filenames:
                continue
            source_pages = self._decode_json_list(row.source_transcription)
            transcriptions: Dict[int, str] = {}
            for page in source_pages:
                if not isinstance(page, dict):
                    continue
                try:
                    image_index = int(page.get("image_index") or 0)
                except (TypeError, ValueError):
                    continue
                if 1 <= image_index <= len(image_filenames):
                    indexed_texts = [
                        str(line.get("text") or "").strip()
                        for line in (
                            (page.get("localization_index") or {}).get("lines") or []
                        )
                        if isinstance(line, dict)
                        and str(line.get("text") or "").strip()
                    ]
                    transcription = (
                        "\n\n".join(indexed_texts)
                        if indexed_texts
                        else str(page.get("transcription") or "").strip()
                    )
                    visual_text = "\n\n".join(
                        " ".join(
                            str(region.get(field) or "").strip()
                            for field in ("title", "description", "visible_text")
                            if str(region.get(field) or "").strip()
                        )
                        for region in page.get("visual_regions") or []
                        if isinstance(region, dict)
                    )
                    if visual_text:
                        transcription = "\n\n".join(
                            part for part in (transcription, visual_text) if part
                        )
                    transcriptions[image_index] = transcription
            canonical_pages = {
                image_index: canonicalize_source_text(transcription)
                for image_index, transcription in transcriptions.items()
                if transcription
            }

            cards_by_page: Dict[int, List[Dict[str, Any]]] = {}
            for concept_index, concept in enumerate(self._decode_json_list(row.key_concepts)):
                if not isinstance(concept, dict):
                    continue
                concept_title = str(concept.get("concept") or "").strip()
                topic = str(concept.get("topic") or concept.get("note_topic") or "").strip()
                card_type = "example" if concept.get("card_type") == "example" else "concept"
                detail_parts = [str(concept.get(field) or "") for field in concept_fields]
                search_keywords = concept.get("search_keywords")
                keyword_text = ""
                if isinstance(search_keywords, list):
                    keyword_text = " ".join(str(keyword or "") for keyword in search_keywords)
                    detail_parts.append(keyword_text)
                reasoning_steps = concept.get("reasoning_steps")
                if isinstance(reasoning_steps, list):
                    detail_parts.extend(str(step or "") for step in reasoning_steps)
                card_text = " ".join(detail_parts)
                preview_text = next(
                    (
                        str(concept.get(field) or "").strip()
                        for field in (
                            "core_summary",
                            "example_method",
                            "explanation",
                            "simple_example",
                            "example_problem",
                            "recall_cue",
                        )
                        if str(concept.get(field) or "").strip()
                    ),
                    concept_title,
                )
                linked_to_page = False
                for source_ref in concept.get("source_refs") or []:
                    if not isinstance(source_ref, dict):
                        continue
                    try:
                        image_index = int(source_ref.get("image_index") or 0)
                    except (TypeError, ValueError):
                        continue
                    if not (1 <= image_index <= len(image_filenames)):
                        continue
                    linked_to_page = True
                    evidence = " ".join(str(source_ref.get("evidence") or "").split()).strip()
                    if evidence and canonical_pages:
                        image_index = _recall_search_resolved_page(
                            evidence,
                            canonical_pages,
                            image_index,
                        )
                    bbox = source_ref.get("bbox")
                    if isinstance(bbox, dict):
                        try:
                            bbox_image_index = int(bbox.get("source_image_index") or 0)
                        except (TypeError, ValueError):
                            bbox_image_index = 0
                        if bbox_image_index and bbox_image_index != image_index:
                            bbox = None
                    cards_by_page.setdefault(image_index, []).append(
                        {
                            "concept_index": concept_index,
                            "concept_title": concept_title,
                            "topic": topic,
                            "card_type": card_type,
                            "card_text": card_text,
                            "preview_text": preview_text,
                            "keyword_text": keyword_text,
                            "evidence": evidence,
                            "bbox": bbox,
                            "has_formula": _recall_search_contains_formula(
                                " ".join((card_text, evidence))
                            ),
                        }
                    )
                if not linked_to_page:
                    for visual_ref in concept.get("visual_refs") or []:
                        if not isinstance(visual_ref, dict):
                            continue
                        try:
                            image_index = int(visual_ref.get("image_index") or 0)
                        except (TypeError, ValueError):
                            continue
                        if not (1 <= image_index <= len(image_filenames)):
                            continue
                        linked_to_page = True
                        visual_evidence = " ".join(
                            str(visual_ref.get(field) or "").strip()
                            for field in ("title", "description", "visible_text")
                            if str(visual_ref.get(field) or "").strip()
                        )
                        cards_by_page.setdefault(image_index, []).append(
                            {
                                "concept_index": concept_index,
                                "concept_title": concept_title,
                                "topic": topic,
                                "card_type": card_type,
                                "card_text": " ".join((card_text, visual_evidence)),
                                "preview_text": preview_text,
                                "keyword_text": keyword_text,
                                "evidence": visual_evidence,
                                "bbox": visual_ref.get("bbox"),
                                "has_formula": _recall_search_contains_formula(
                                    " ".join((card_text, visual_evidence))
                                ),
                            }
                        )
                if not linked_to_page:
                    if transcriptions:
                        page_index = max(
                            transcriptions,
                            key=lambda candidate_index: _recall_search_similarity(
                                concept_title or card_text,
                                transcriptions[candidate_index],
                            ),
                        )
                        fallback_evidence = transcriptions.get(page_index, "")
                    else:
                        page_index = 1
                        fallback_evidence = ""
                    cards_by_page.setdefault(page_index, []).append(
                        {
                            "concept_index": concept_index,
                            "concept_title": concept_title,
                            "topic": topic,
                            "card_type": card_type,
                            "card_text": card_text,
                            "preview_text": preview_text,
                            "keyword_text": keyword_text,
                            "evidence": fallback_evidence,
                            "bbox": None,
                            "has_formula": _recall_search_contains_formula(card_text),
                        }
                    )

            metadata_text = " ".join(
                str(value or "")
                for value in (row.title, row.subject, row.summary)
            )
            page_indexes = sorted(set(transcriptions) | set(cards_by_page))
            if not page_indexes:
                page_indexes = [1]
            for image_index in page_indexes:
                if not (1 <= image_index <= len(image_filenames)):
                    continue
                transcription = str(transcriptions.get(image_index, ""))
                linked_cards = list(cards_by_page.get(image_index, []))
                documents.append(
                    {
                        "session_id": int(row.id),
                        "study_date": str(row.study_date or ""),
                        "subject": str(row.subject or ""),
                        "title": str(row.title or ""),
                        "summary": str(row.summary or ""),
                        "metadata_text": metadata_text,
                        "image_index": image_index,
                        "image_filename": image_filenames[image_index - 1],
                        "transcription": transcription,
                        "cards": linked_cards,
                        "has_formula": _recall_search_contains_formula(transcription)
                        or any(bool(card.get("has_formula")) for card in linked_cards),
                        "created_at": str(row.created_at or ""),
                        "updated_at": str(row.updated_at or ""),
                    }
                )

        with self._recall_search_cache_lock:
            self._recall_search_cache_signature = signature
            self._recall_search_cache_documents = documents
        return documents

    def search_study_recall_pages(
        self,
        *,
        query: str,
        subject: Optional[str] = None,
        session_id: Optional[int] = None,
        content_type: str = "all",
        sort: str = "relevance",
        limit: int = 16,
    ) -> List[Dict[str, Any]]:
        search_query = " ".join(str(query or "").split()).strip()[:160]
        subject_filter = str(subject or "").strip()
        try:
            session_filter = max(0, int(session_id or 0))
        except (TypeError, ValueError):
            session_filter = 0
        type_filter = str(content_type or "all").strip().lower()
        if type_filter not in {"all", "source", "formula", "example", "concept"}:
            type_filter = "all"
        sort_mode = "recent" if str(sort or "").strip().lower() == "recent" else "relevance"
        if not search_query:
            return []

        results: List[Dict[str, Any]] = []
        for document in self._study_recall_search_documents():
            if subject_filter and document["subject"] != subject_filter:
                continue
            if session_filter and int(document["session_id"]) != session_filter:
                continue
            if type_filter == "formula" and not document["has_formula"]:
                continue

            transcription = str(document["transcription"] or "")
            transcription_score = _recall_search_similarity(search_query, transcription)
            transcription_formula_score = (
                _recall_search_formula_similarity(search_query, transcription)
                if document["has_formula"]
                else 0.0
            )
            card_candidates: List[Dict[str, Any]] = []
            for card in document["cards"]:
                if type_filter in {"example", "concept"} and card.get("card_type") != type_filter:
                    continue
                if type_filter == "formula" and not card.get("has_formula"):
                    continue
                title_score = _recall_search_similarity(search_query, card.get("concept_title"))
                topic_score = _recall_search_similarity(search_query, card.get("topic"))
                body_score = _recall_search_similarity(search_query, card.get("card_text"))
                keyword_score = _recall_search_similarity(search_query, card.get("keyword_text"))
                evidence_score = _recall_search_similarity(search_query, card.get("evidence"))
                formula_score = (
                    _recall_search_formula_similarity(
                        search_query,
                        " ".join((str(card.get("card_text") or ""), str(card.get("evidence") or ""))),
                    )
                    if card.get("has_formula")
                    else 0.0
                )
                card_score = max(
                    title_score * 1.35,
                    topic_score * 1.12,
                    body_score,
                    keyword_score * 1.18,
                    evidence_score * 1.08,
                    formula_score * 1.12,
                )
                card_candidates.append(
                    {
                        **card,
                        "card_score": card_score,
                        "evidence_score": evidence_score,
                        "formula_score": formula_score,
                    }
                )

            best_card = max(
                card_candidates,
                key=lambda item: (
                    float(item["card_score"]),
                    float(item["evidence_score"]),
                ),
                default=None,
            )
            card_score = float(best_card["card_score"]) if best_card else 0.0
            metadata_score = _recall_search_similarity(search_query, document["metadata_text"])
            if type_filter == "source":
                direct_score = max(transcription_score, transcription_formula_score)
            elif type_filter in {"example", "concept"}:
                direct_score = card_score
            else:
                direct_score = max(
                    transcription_score,
                    transcription_formula_score,
                    card_score,
                )
            if direct_score < 24.0:
                if (
                    type_filter != "all"
                    or metadata_score < 24.0
                    or int(document["image_index"]) != 1
                ):
                    continue
                direct_score = metadata_score * 0.72
            rank_score = direct_score + min(metadata_score * 0.18, 34.0)

            best_bbox = best_card.get("bbox") if best_card else None
            if best_card and not best_bbox:
                best_bbox = next(
                    (
                        candidate.get("bbox")
                        for candidate in card_candidates
                        if candidate.get("bbox")
                        and candidate.get("concept_index") == best_card.get("concept_index")
                    ),
                    None,
                )
            if (
                transcription_formula_score >= max(70.0, card_score * 1.05)
                or (best_card and float(best_card.get("formula_score") or 0) >= 75.0)
            ):
                match_reason = "公式與符號相符"
            elif transcription_score >= max(70.0, card_score * 1.08):
                match_reason = "原始筆記文字相符"
            elif best_card and float(best_card["evidence_score"]) >= 55.0:
                match_reason = "重點卡與原文相符"
            elif best_card and best_card.get("card_type") == "example":
                match_reason = "相關例題與解法"
            elif best_card:
                match_reason = "相關重點卡"
            else:
                match_reason = "筆記主題相符"
            excerpt_source = transcription
            if best_card and (
                not excerpt_source
                or float(best_card["card_score"]) >= transcription_score * 0.82
            ):
                excerpt_source = (
                    best_card.get("preview_text")
                    or best_card.get("evidence")
                    or best_card.get("card_text")
                    or best_card.get("concept_title")
                    or excerpt_source
                )
            results.append(
                {
                    "session_id": int(document["session_id"]),
                    "study_date": document["study_date"],
                    "subject": document["subject"],
                    "title": document["title"],
                    "image_index": int(document["image_index"]),
                    "image_filename": document["image_filename"],
                    "concept_index": int(best_card["concept_index"]) if best_card else None,
                    "concept_title": str(best_card.get("concept_title") or "") if best_card else "",
                    "topic": str(best_card.get("topic") or "") if best_card else "",
                    "card_type": str(best_card.get("card_type") or "") if best_card else "",
                    "evidence": str(best_card.get("evidence") or "") if best_card else "",
                    "bbox": best_bbox,
                    "excerpt": _recall_search_excerpt(excerpt_source, search_query),
                    "match_reason": match_reason,
                    "has_formula": bool(document["has_formula"]),
                    "rank_score": round(rank_score, 4),
                    "created_at": document["created_at"],
                }
            )
        if sort_mode == "recent":
            results.sort(
                key=lambda item: (
                    str(item["created_at"]),
                    float(item["rank_score"]),
                ),
                reverse=True,
            )
        else:
            results.sort(
                key=lambda item: (
                    float(item["rank_score"]),
                    str(item["created_at"]),
                ),
                reverse=True,
            )
        return results[:max(1, min(int(limit), 24))]

    def list_due_study_recall_cards(
        self,
        *,
        today: str,
        limit: int = 18,
        concept_filter: Optional[Callable[[Any], bool]] = None,
    ) -> List[Dict[str, Any]]:
        due_cards: List[Dict[str, Any]] = []
        reviewed_today: set[tuple[int, int]] = set()
        full_sessions: List[Dict[str, Any]] = []
        for session in self.list_study_recall_sessions(limit=None):
            full_session = self.get_study_recall_session(int(session["id"]))
            if not full_session or str(full_session.get("study_date") or "") > today:
                continue
            full_sessions.append(full_session)
            for index, concept in enumerate(full_session.get("key_concepts") or []):
                if not isinstance(concept, dict):
                    continue
                if any(
                    self._study_plan_business_day_from_timestamp(str(entry.get("created_at") or "")) == today
                    for entry in (concept.get("review") or {}).get("history") or []
                ):
                    reviewed_today.add((int(full_session["id"]), index))
        daily_limit = max(0, min(max(1, int(limit)), RECALL_DAILY_CAPACITY) - len(reviewed_today))
        if daily_limit <= 0:
            return []
        for full_session in full_sessions:
            for index, concept in enumerate(full_session.get("key_concepts") or []):
                if not isinstance(concept, dict):
                    continue
                if concept_filter is not None and not concept_filter(concept):
                    continue
                review = concept.get("review") or {}
                next_review_at = review.get("next_review_at")
                if next_review_at and next_review_at > today:
                    continue
                due_cards.append(
                    {
                        "session_id": int(full_session["id"]),
                        "concept_index": index,
                        "session_title": str(full_session.get("title") or ""),
                        "subject": str(full_session.get("subject") or ""),
                        "concept": str(concept.get("concept") or ""),
                        "last_rating": review.get("last_rating"),
                        "next_review_at": next_review_at,
                    }
                )
        due_cards.sort(key=lambda item: (item["next_review_at"] or "0000-00-00", item["session_title"], item["concept_index"]))
        return due_cards[:daily_limit]

    def list_study_recall_schedule(
        self,
        *,
        start_date: str,
        days: int = 7,
        daily_capacity: int = 18,
        concept_filter: Optional[Callable[[Any], bool]] = None,
    ) -> List[Dict[str, Any]]:
        start = datetime.fromisoformat(start_date).date()
        schedule_days = [start + timedelta(days=offset) for offset in range(max(1, min(int(days), 31)))]
        loads = {day.isoformat(): 0 for day in schedule_days}
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                select(study_recall_card_reviews_table).order_by(study_recall_card_reviews_table.c.id.desc())
            ).fetchall()
            session_rows = conn.execute(
                select(
                    study_recall_sessions_table.c.id,
                    study_recall_sessions_table.c.study_date,
                    study_recall_sessions_table.c.key_concepts,
                )
            ).fetchall()
        latest_by_card: Dict[tuple[int, int], Any] = {}
        for row in rows:
            latest_by_card.setdefault((int(row.session_id), int(row.concept_index)), row)
        first_schedule_day = schedule_days[0].isoformat()
        last_schedule_day = schedule_days[-1].isoformat()
        completed_today = len(
            {
                (int(row.session_id), int(row.concept_index))
                for row in rows
                if self._study_plan_business_day_from_timestamp(str(row.created_at or "")) == first_schedule_day
            }
        )
        due_items: List[str] = []
        for session_row in session_rows:
            study_date = str(session_row.study_date or first_schedule_day)
            for concept_index, concept in enumerate(self._decode_json_list(session_row.key_concepts)):
                if not isinstance(concept, dict):
                    continue
                if concept_filter is not None and not concept_filter(concept):
                    continue
                latest = latest_by_card.get((int(session_row.id), concept_index))
                due_date = str(latest.next_review_at) if latest and latest.next_review_at else study_date
                if due_date < first_schedule_day:
                    due_date = first_schedule_day
                if due_date <= last_schedule_day:
                    due_items.append(due_date)
        capacities = {
            day.isoformat(): max(
                0,
                int(daily_capacity) - (completed_today if day == schedule_days[0] else 0),
            )
            for day in schedule_days
        }
        for due_date in sorted(due_items):
            candidate = datetime.fromisoformat(due_date).date()
            while candidate.isoformat() <= last_schedule_day:
                candidate_key = candidate.isoformat()
                if loads[candidate_key] < capacities[candidate_key]:
                    loads[candidate_key] += 1
                    break
                candidate += timedelta(days=1)
        return [
            {
                "date": day.isoformat(),
                "count": loads[day.isoformat()],
                "capacity": daily_capacity,
                "is_today": day == start,
            }
            for day in schedule_days
        ]

    def record_study_recall_card_ratings(self, *, session_id: int, ratings: Dict[int, int], review_date: str) -> bool:
        now = self._now_iso()
        review_day = datetime.fromisoformat(review_date).date()
        review_datetime = datetime(
            review_day.year,
            review_day.month,
            review_day.day,
            12,
            tzinfo=timezone.utc,
        )
        review_recorded_at = review_datetime.isoformat()
        with self._lock, self._engine.begin() as conn:
            session_row = conn.execute(
                select(
                    study_recall_sessions_table.c.id,
                    study_recall_sessions_table.c.key_concepts,
                    study_recall_sessions_table.c.review_count,
                ).where(study_recall_sessions_table.c.id == session_id)
            ).fetchone()
            if not session_row:
                return False
            concepts = self._decode_json_list(session_row.key_concepts)
            expected_indexes = set(range(len(concepts)))
            if not ratings or not set(ratings).issubset(expected_indexes):
                return False
            previous_rows = conn.execute(
                select(study_recall_card_reviews_table).order_by(study_recall_card_reviews_table.c.id.desc())
            ).fetchall()
            latest_by_index: Dict[int, Any] = {}
            history_by_card: Dict[tuple[int, int], List[Any]] = {}
            scheduled_loads: Dict[str, int] = {}
            for row in previous_rows:
                card_key = (int(row.session_id), int(row.concept_index))
                history_by_card.setdefault(card_key, []).append(row)
                if card_key in latest_by_index:
                    continue
                latest_by_index[card_key] = row
                if int(row.session_id) != session_id or int(row.concept_index) not in ratings:
                    scheduled_loads[row.next_review_at] = scheduled_loads.get(row.next_review_at, 0) + 1
            next_dates: List[str] = []
            normalized_ratings: List[int] = []
            ordered_indexes = sorted(ratings, key=lambda index: (int(ratings[index]), index))
            assignments: Dict[int, tuple[int, str, str, Dict[str, Any]]] = {}
            for index in ordered_indexes:
                rating = max(1, min(int(ratings[index]), 5))
                previous = latest_by_index.get((session_id, index))
                try:
                    fsrs_card = self._recall_fsrs_card(
                        session_id=session_id,
                        concept_index=index,
                        concept=concepts[index],
                        history=history_by_card.get((session_id, index), []),
                    )
                    fsrs_card, _review_log = RECALL_FSRS_SCHEDULER.review_card(
                        fsrs_card,
                        self._recall_fsrs_rating(rating),
                        review_datetime,
                    )
                    calculated_interval = max(
                        1,
                        int(math.ceil((fsrs_card.due - review_datetime).total_seconds() / 86400)),
                    )
                    interval_days = max({1: 1, 2: 2, 3: 3, 4: 4, 5: 7}[rating], calculated_interval)
                    fsrs_state = json.loads(fsrs_card.to_json())
                except (TypeError, ValueError, KeyError, OverflowError):
                    interval_days = self._recall_interval_days(
                        rating,
                        int(previous.rating) if previous else None,
                        int(previous.interval_days) if previous else 0,
                    )
                    fsrs_state = {}
                ideal_review_at = (review_day + timedelta(days=interval_days)).isoformat()
                ideal_day = datetime.fromisoformat(ideal_review_at).date()
                candidate_day = self._balanced_recall_date(
                    review_day=review_day,
                    ideal_day=ideal_day,
                    interval_days=interval_days,
                    rating=rating,
                    scheduled_loads=scheduled_loads,
                )
                next_review_at = candidate_day.isoformat()
                scheduled_loads[next_review_at] = scheduled_loads.get(next_review_at, 0) + 1
                assignments[index] = (interval_days, ideal_review_at, next_review_at, fsrs_state)
            for index in ordered_indexes:
                rating = max(1, min(int(ratings[index]), 5))
                interval_days, ideal_review_at, next_review_at, fsrs_state = assignments[index]
                if fsrs_state:
                    concepts[index]["fsrs_card"] = fsrs_state
                conn.execute(
                    insert(study_recall_card_reviews_table).values(
                        session_id=session_id,
                        concept_index=index,
                        rating=rating,
                        interval_days=interval_days,
                        ideal_review_at=ideal_review_at,
                        next_review_at=next_review_at,
                        created_at=review_recorded_at,
                    )
                )
                next_dates.append(next_review_at)
                normalized_ratings.append(rating)
            for (row_session_id, _concept_index), row in latest_by_index.items():
                if row_session_id == session_id and int(row.concept_index) not in ratings:
                    next_dates.append(row.next_review_at)
            for index in expected_indexes - set(ratings):
                if (session_id, index) not in latest_by_index:
                    next_dates.append(review_date)
            conn.execute(
                update(study_recall_sessions_table)
                .where(study_recall_sessions_table.c.id == session_id)
                .values(
                    last_score_percent=None,
                    last_self_rating=int(round(sum(normalized_ratings) / len(normalized_ratings))),
                    next_review_at=min(next_dates),
                    review_count=int(session_row.review_count or 0) + 1,
                    key_concepts=json.dumps(concepts, ensure_ascii=False),
                    updated_at=now,
                )
            )
        return True

    def list_study_recall_sessions(self, *, limit: Optional[int] = 24) -> List[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            query = select(study_recall_sessions_table).order_by(study_recall_sessions_table.c.created_at.desc())
            if limit is not None:
                query = query.limit(max(1, min(int(limit), 100)))
            rows = conn.execute(query).fetchall()
        return [
            {
                "id": int(row.id),
                "study_date": row.study_date,
                "subject": row.subject,
                "title": row.title,
                "summary": row.summary,
                "key_concepts": self._decode_json_list(row.key_concepts),
                "organization_mode": str(row.organization_mode or "legacy"),
                "last_score_percent": round(float(row.last_score_percent or 0), 1) if row.last_score_percent is not None else None,
                "last_self_rating": int(row.last_self_rating or 0) if row.last_self_rating is not None else None,
                "next_review_at": row.next_review_at,
                "review_count": int(row.review_count or 0),
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            }
            for row in rows
        ]

    def get_study_recall_glossary(self, subject: str) -> Optional[Dict[str, Any]]:
        normalized_subject = str(subject or "").strip()[:64]
        if not normalized_subject:
            return None
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                select(study_recall_glossaries_table).where(
                    study_recall_glossaries_table.c.subject == normalized_subject
                )
            ).fetchone()
        if not row:
            return None
        return {
            "subject": row.subject,
            "source_signature": row.source_signature,
            "status": row.status,
            "terms": self._decode_json_list(row.terms),
            "error": row.error,
            "updated_at": row.updated_at,
        }

    def save_study_recall_glossary(
        self,
        *,
        subject: str,
        source_signature: str,
        status: str,
        terms: List[Dict[str, Any]],
        error: Optional[str] = None,
    ) -> None:
        normalized_subject = str(subject or "").strip()[:64]
        if not normalized_subject:
            raise ValueError("Glossary subject is required")
        values = {
            "subject": normalized_subject,
            "source_signature": str(source_signature or "")[:64],
            "status": str(status or "ready")[:16],
            "terms": json.dumps(terms or [], ensure_ascii=False),
            "error": str(error or "")[:1000] or None,
            "updated_at": self._now_iso(),
        }
        with self._lock, self._engine.begin() as conn:
            existing = conn.execute(
                select(study_recall_glossaries_table.c.subject).where(
                    study_recall_glossaries_table.c.subject == normalized_subject
                )
            ).fetchone()
            if existing:
                conn.execute(
                    update(study_recall_glossaries_table)
                    .where(study_recall_glossaries_table.c.subject == normalized_subject)
                    .values(**{key: value for key, value in values.items() if key != "subject"})
                )
            else:
                conn.execute(insert(study_recall_glossaries_table).values(**values))

    def record_study_recall_attempt(
        self,
        *,
        session_id: int,
        score_percent: float,
        self_rating: int,
        answers: Dict[str, Any],
        next_review_at: str,
    ) -> bool:
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            session_row = conn.execute(
                select(study_recall_sessions_table.c.id, study_recall_sessions_table.c.review_count)
                .where(study_recall_sessions_table.c.id == session_id)
            ).fetchone()
            if not session_row:
                return False
            conn.execute(
                insert(study_recall_attempts_table).values(
                    session_id=session_id,
                    score_percent=max(0.0, min(float(score_percent), 100.0)),
                    self_rating=max(1, min(int(self_rating), 5)),
                    answers=json.dumps(answers, ensure_ascii=False),
                    next_review_at=next_review_at,
                    created_at=now,
                )
            )
            conn.execute(
                update(study_recall_sessions_table)
                .where(study_recall_sessions_table.c.id == session_id)
                .values(
                    last_score_percent=max(0.0, min(float(score_percent), 100.0)),
                    last_self_rating=max(1, min(int(self_rating), 5)),
                    next_review_at=next_review_at,
                    review_count=int(session_row.review_count or 0) + 1,
                    updated_at=now,
                )
            )
        return True
