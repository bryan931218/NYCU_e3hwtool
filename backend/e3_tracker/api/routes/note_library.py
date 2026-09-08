"""Note library routes and their feature helpers."""

import json
import re
import threading
import hashlib
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from flask import Response, render_template_string, request, session, url_for
from ...shared.source_localization import SOURCE_BBOX_VERSION, collapse_source_refs_by_image
from ...services.note_topics import coarse_study_topic


def register_note_library_routes(*,
    STUDY_PLAN_SUBJECTS,
    STUDY_RECALL_TEMPLATE,
    _call_openai_json,
    _is_recall_concept_eligible,
    _literal_study_source_evidence,
    _normalize_study_concept_title,
    _normalize_study_math_markup,
    _strip_study_process_narration,
    _study_plan_business_date,
    _study_relation_association_issue,
    _validated_study_source_bbox,
    admin_required,
    app,
    current_user,
    openai_api_key,
    storage,
):
    @app.get("/admin/study-recall")
    @admin_required
    def admin_study_recall():
        def decorate_review_curve(concept: Dict[str, Any]) -> None:
            review = concept.get("review") or {}
            history = review.get("history") or []
            total_points = max(1, len(history))
            curve_points = []
            for index, entry in enumerate(history):
                x = 50 if total_points == 1 else 6 + index * 88 / (total_points - 1)
                y = 36 - max(0, min(int(entry.get("rating") or 1) - 1, 4)) * 7
                curve_points.append(f"{x:.1f},{y:.1f}")
            review["curve_points"] = " ".join(curve_points)
            review["history_label"] = " → ".join(str(entry.get("rating")) for entry in history) or "尚未自評"
            review["latest_curve_y"] = 36 - max(0, min(int(history[-1].get("rating") or 1) - 1, 4)) * 7 if history else 36
            fsrs_state = concept.get("fsrs_card") if isinstance(concept.get("fsrs_card"), dict) else {}
            try:
                stability_days = max(0.0, float(fsrs_state.get("stability")))
            except (TypeError, ValueError):
                stability_days = 0.0
            review["stability_label"] = (
                f"記憶穩定約 {stability_days:.1f} 天" if stability_days > 0 else ""
            )
            if len(history) >= 2:
                change = int(history[-1].get("rating") or 0) - int(history[-2].get("rating") or 0)
                review["trend_label"] = (
                    f"比上次 +{change}" if change > 0 else (f"比上次 {change}" if change < 0 else "與上次相同")
                )
            else:
                review["trend_label"] = ""
            concept["review"] = review

        def organize_session_concepts(session: Dict[str, Any], *, replace_concepts: bool) -> Dict[int, Dict[str, Any]]:
            raw_concepts = session.get("key_concepts") or []
            session["summary"] = _strip_study_process_narration(session.get("summary"))
            for page in session.get("source_transcription") or []:
                if isinstance(page, dict):
                    page["transcription"] = _normalize_study_math_markup(page.get("transcription"))
            image_urls = session.get("image_urls") or [
                url_for("admin_study_recall_image", session_id=session["id"], filename=filename)
                for filename in session.get("image_filenames") or []
            ]
            visual_regions_by_id = {
                str(region.get("region_id") or ""): region
                for page in session.get("source_transcription") or []
                if isinstance(page, dict)
                for region in page.get("visual_regions") or []
                if isinstance(region, dict) and str(region.get("region_id") or "")
            }
            indexed_concepts = {
                index: concept
                for index, concept in enumerate(raw_concepts)
                if _is_recall_concept_eligible(concept)
            }
            for concept in indexed_concepts.values():
                concept["topic"] = _normalize_study_concept_title(
                    concept.get("topic"), session.get("title") or "細分觀念"
                )
                concept["concept"] = _normalize_study_concept_title(
                    concept.get("concept"), concept.get("topic") or session.get("title")
                )
            title_indexes = {
                str(concept.get("concept") or "").strip().casefold(): index
                for index, concept in indexed_concepts.items()
            }
            note_topic = next(
                (
                    str(concept.get("note_topic") or "").strip()
                    for concept in indexed_concepts.values()
                    if str(concept.get("note_topic") or "").strip()
                ),
                next(
                    (
                        str(concept.get("topic") or "").strip()
                        for concept in indexed_concepts.values()
                        if str(concept.get("topic") or "").strip()
                    ),
                    str(session.get("title") or "未分類筆記"),
                ),
            )
            topic_groups: Dict[str, List[Dict[str, Any]]] = {}
            for index, concept in indexed_concepts.items():
                concept["display_index"] = index
                concept["recall_cue"] = _normalize_study_math_markup(
                    concept.get("recall_cue")
                    or f"先回想「{concept.get('concept') or '這個觀念'}」的條件、核心關係與結論。"
                )
                concept["core_summary"] = _normalize_study_math_markup(concept.get("core_summary"))
                concept["explanation"] = _normalize_study_math_markup(concept.get("explanation"))
                concept["card_type"] = "example" if concept.get("card_type") == "example" else "concept"
                concept["example_problem"] = _normalize_study_math_markup(concept.get("example_problem"))
                concept["example_method"] = _normalize_study_math_markup(concept.get("example_method"))
                concept["simple_example"] = _normalize_study_math_markup(concept.get("simple_example"))
                concept["reasoning_steps"] = [
                    _normalize_study_math_markup(step)
                    for step in (concept.get("reasoning_steps") or [])[:4]
                    if str(step or "").strip()
                ]
                concept["common_confusion"] = _normalize_study_math_markup(concept.get("common_confusion"))
                concept["memory_hint"] = _normalize_study_math_markup(concept.get("memory_hint"))
                concept["topic"] = str(concept.get("topic") or note_topic).strip() or note_topic
                visible_source_refs = []
                for source_ref in concept.get("source_refs") or []:
                    if not isinstance(source_ref, dict):
                        continue
                    try:
                        image_index = int(source_ref.get("image_index") or 0)
                    except (TypeError, ValueError):
                        continue
                    evidence = " ".join(str(source_ref.get("evidence") or "").split()).strip()
                    if not (1 <= image_index <= len(image_urls)) or not evidence:
                        continue
                    locatable_evidence = _literal_study_source_evidence(evidence)
                    visible_source_refs.append(
                        {
                            "image_index": image_index,
                            "evidence": _normalize_study_math_markup(evidence),
                            "image_url": image_urls[image_index - 1],
                            "locatable": bool(locatable_evidence),
                            "bbox": _validated_study_source_bbox(
                                source_ref.get("bbox"),
                                require_text_verified=True,
                                expected_image_index=image_index,
                            ),
                        }
                    )
                concept["source_refs"] = collapse_source_refs_by_image(visible_source_refs)
                visible_visual_refs: List[Dict[str, Any]] = []
                for visual_ref in concept.get("visual_refs") or []:
                    if not isinstance(visual_ref, dict):
                        continue
                    region_id = str(visual_ref.get("region_id") or "").strip()
                    region = visual_regions_by_id.get(region_id)
                    if region is None or any(
                        item["region_id"] == region_id for item in visible_visual_refs
                    ):
                        continue
                    try:
                        visual_image_index = int(region.get("image_index") or 0)
                    except (TypeError, ValueError):
                        continue
                    if not (1 <= visual_image_index <= len(image_urls)):
                        continue
                    visible_visual_refs.append(
                        {
                            **region,
                            "region_id": region_id,
                            "image_index": visual_image_index,
                            "crop_url": url_for(
                                "admin_study_recall_visual_crop",
                                session_id=session["id"],
                                region_id=region_id,
                                layout="full-width-v1",
                            ),
                            "enhanced_url": url_for(
                                "admin_study_recall_visual_crop",
                                session_id=session["id"],
                                region_id=region_id,
                                variant="enhanced",
                                layout="full-width-v1",
                            ),
                            "redraw_url": (
                                url_for(
                                    "admin_study_recall_visual_redraw",
                                    session_id=session["id"],
                                    region_id=region_id,
                                )
                                if region.get("render_mode") == "svg"
                                else ""
                            ),
                            "image_url": image_urls[visual_image_index - 1],
                        }
                    )
                concept["visual_refs"] = visible_visual_refs
                related_cards = []
                stored_relations = concept.get("relations")
                if isinstance(stored_relations, list):
                    for relation in stored_relations:
                        if not isinstance(relation, dict):
                            continue
                        try:
                            related_session_id = int(relation.get("session_id") or 0)
                            related_index = int(relation.get("concept_index"))
                        except (TypeError, ValueError):
                            continue
                        related_title = _normalize_study_math_markup(relation.get("title"))
                        association = _normalize_study_math_markup(
                            " ".join(str(relation.get("association") or "").split())
                        )
                        visible_card_titles = {
                            f"s{int(session['id'])}:c{index}".casefold(): str(concept.get("concept") or "").strip(),
                            f"s{related_session_id}:c{related_index}".casefold(): related_title,
                        }
                        association = re.sub(
                            r"\bs\d+\s*:\s*c\d+\b",
                            lambda match: visible_card_titles.get(
                                re.sub(r"\s+", "", match.group(0)).casefold(),
                                "",
                            ),
                            association,
                            flags=re.IGNORECASE,
                        )
                        association = re.sub(r"\s+([，。；：、！？])", r"\1", association).strip(" \t:：,，;；-")
                        if (
                            related_session_id <= 0
                            or related_index < 0
                            or not related_title
                            or not association
                            or _study_relation_association_issue(
                                association,
                                source_title=concept.get("concept"),
                                target_title=related_title,
                            )
                        ):
                            continue
                        related_cards.append(
                            {
                                "session_id": related_session_id,
                                "title": related_title,
                                "index": related_index,
                                "association": association,
                            }
                        )
                concept["related_cards"] = related_cards[:2]
                display_topic = coarse_study_topic(concept["topic"], concept["concept"])
                topic_groups.setdefault(display_topic, []).append(concept)
            session["note_topic"] = note_topic
            session["concept_groups"] = [
                {"topic": topic, "concepts": concepts}
                for topic, concepts in topic_groups.items()
            ]
            if replace_concepts:
                session["key_concepts"] = list(indexed_concepts.values())
            return indexed_concepts

        user = current_user()
        try:
            selected_id = int(request.args.get("session_id") or 0)
        except (TypeError, ValueError):
            selected_id = 0
        sessions = sorted(
            storage.list_study_recall_sessions(limit=36),
            key=lambda item: (str(item.get("created_at") or ""), int(item.get("id") or 0)),
        )
        assistant_sessions = sorted(
            storage.list_study_recall_sessions(limit=None),
            key=lambda item: (str(item.get("subject") or ""), str(item.get("created_at") or "")),
        )
        session_groups_by_topic: Dict[str, List[Dict[str, Any]]] = {}
        for recall_session in sessions:
            concepts = recall_session.get("key_concepts") or []
            topic_label = next(
                (
                    str(concept.get("note_topic") or "").strip()
                    for concept in concepts
                    if isinstance(concept, dict) and str(concept.get("note_topic") or "").strip()
                ),
                next(
                    (
                        str(concept.get("topic") or "").strip()
                        for concept in concepts
                        if isinstance(concept, dict) and str(concept.get("topic") or "").strip()
                    ),
                    str(recall_session.get("subject") or recall_session.get("title") or "未分類筆記"),
                ),
            )
            recall_session["topic_label"] = topic_label
            session_groups_by_topic.setdefault(topic_label, []).append(recall_session)
        session_groups = [
            {"topic": topic, "sessions": grouped_sessions}
            for topic, grouped_sessions in session_groups_by_topic.items()
        ]
        selected_session = storage.get_study_recall_session(selected_id) if selected_id else None
        if selected_session is None and sessions:
            selected_session = storage.get_study_recall_session(int(sessions[-1]["id"]))
        if selected_session:
            selected_session["image_urls"] = [
                url_for("admin_study_recall_image", session_id=selected_session["id"], filename=filename)
                for filename in selected_session.get("image_filenames") or []
            ]
            for page in selected_session.get("source_transcription") or []:
                if not isinstance(page, dict):
                    continue
                try:
                    image_index = int(page.get("image_index") or 0)
                except (TypeError, ValueError):
                    continue
                page["image_url"] = (
                    selected_session["image_urls"][image_index - 1]
                    if 1 <= image_index <= len(selected_session["image_urls"])
                    else ""
                )
                for region in page.get("visual_regions") or []:
                    if not isinstance(region, dict):
                        continue
                    region_id = str(region.get("region_id") or "").strip()
                    if not region_id:
                        continue
                    region["crop_url"] = url_for(
                        "admin_study_recall_visual_crop",
                        session_id=selected_session["id"],
                        region_id=region_id,
                    )
                    region["redraw_url"] = (
                        url_for(
                            "admin_study_recall_visual_redraw",
                            session_id=selected_session["id"],
                            region_id=region_id,
                        )
                        if region.get("render_mode") == "svg"
                        else ""
                    )
            for collection_name in ("uncertain_fragments", "correction_records"):
                for record in selected_session.get(collection_name) or []:
                    if not isinstance(record, dict):
                        continue
                    try:
                        image_index = int(record.get("image_index") or 0)
                    except (TypeError, ValueError):
                        continue
                    record["image_url"] = (
                        selected_session["image_urls"][image_index - 1]
                        if 1 <= image_index <= len(selected_session["image_urls"])
                        else ""
                    )
            organize_session_concepts(selected_session, replace_concepts=True)
            selected_session["source_location_total"] = sum(
                sum(
                    1
                    for source_ref in concept.get("source_refs") or []
                    if source_ref.get("locatable")
                )
                for concept in selected_session["key_concepts"]
            )
            selected_session["source_location_count"] = sum(
                1
                for concept in selected_session["key_concepts"]
                for source_ref in concept.get("source_refs") or []
                if source_ref.get("locatable") and source_ref.get("bbox")
            )
            selected_session["source_location_refined_count"] = sum(
                1
                for concept in selected_session["key_concepts"]
                for source_ref in concept.get("source_refs") or []
                if source_ref.get("locatable")
                and source_ref.get("bbox")
                and int(source_ref["bbox"].get("version") or 1) >= SOURCE_BBOX_VERSION
            )
            for concept in selected_session["key_concepts"]:
                decorate_review_curve(concept)
        today = _study_plan_business_date().isoformat()
        due_cards = storage.list_due_study_recall_cards(
            today=today,
            concept_filter=_is_recall_concept_eligible,
        )
        review_cards: List[Dict[str, Any]] = []
        review_sessions: Dict[int, Dict[str, Any]] = {}
        review_concept_indexes: Dict[int, Dict[int, Dict[str, Any]]] = {}
        for due_card in due_cards:
            session_id = int(due_card["session_id"])
            review_session = review_sessions.get(session_id)
            if review_session is None:
                review_session = storage.get_study_recall_session(session_id) or {}
                review_sessions[session_id] = review_session
            concept_index = int(due_card["concept_index"])
            concepts_by_index = review_concept_indexes.get(session_id)
            if concepts_by_index is None:
                concepts_by_index = organize_session_concepts(review_session, replace_concepts=False)
                review_concept_indexes[session_id] = concepts_by_index
            concept = concepts_by_index.get(concept_index)
            if concept is None:
                continue
            decorate_review_curve(concept)
            review_cards.append({**due_card, "concept_data": concept})
        review_schedule = storage.list_study_recall_schedule(
            start_date=today,
            concept_filter=_is_recall_concept_eligible,
        )
        return render_template_string(
            STUDY_RECALL_TEMPLATE,
            admin_user=user,
            subjects=STUDY_PLAN_SUBJECTS,
            today=today,
            sessions=sessions,
            assistant_sessions=assistant_sessions,
            session_groups=session_groups,
            due_cards=due_cards,
            review_cards=review_cards,
            review_schedule=review_schedule,
            selected_session=selected_session,
            openai_ready=bool(openai_api_key),
            nav_active="recall",
        )

    glossary_jobs_lock = threading.Lock()
    glossary_jobs: Set[str] = set()

    def _study_glossary_clean_term(value: Any) -> str:
        term = " ".join(_normalize_study_concept_title(value, "").split()).strip()
        term = term.strip(" \t\r\n，。；：、:;|/·・-–—")
        for suffix in ("定義", "基本定義", "基礎", "公式", "性質", "總覽", "例題", "範例"):
            if term.endswith(suffix) and len(term) > len(suffix) + 1:
                term = term[: -len(suffix)].rstrip(" 的之：:、，-/")
                break
        compact = re.sub(r"\s+", "", term)
        generic = {
            "定義", "例題", "範例", "性質", "公式", "結論", "重點", "補充",
            "注意", "觀念", "方法", "步驟", "證明", "應用", "基礎", "函數",
        }
        if (
            len(compact) < 2
            or len(compact) > 48
            or term.casefold() in generic
            or re.search(r"[\\${}^_=]", term)
            or not re.search(r"[A-Za-z0-9\u3400-\u9fff]", term)
        ):
            return ""
        return term

    def _study_glossary_catalog() -> Dict[str, Dict[str, Any]]:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for recall_session in storage.list_study_recall_sessions(limit=None):
            subject = str(recall_session.get("subject") or "未分類").strip()
            session_id = int(recall_session.get("id") or 0)
            if not subject or session_id <= 0:
                continue
            for concept_index, concept in enumerate(recall_session.get("key_concepts") or []):
                if not isinstance(concept, dict) or not _is_recall_concept_eligible(concept):
                    continue
                raw_title = " ".join(
                    _normalize_study_concept_title(
                        concept.get("concept"), concept.get("topic")
                    ).split()
                ).strip()
                if not raw_title:
                    continue
                proposed_terms: List[str] = []
                for candidate in (
                    raw_title,
                    re.sub(r"[（(][^（）()]{1,80}[）)]", " ", raw_title),
                    *re.findall(r"[（(]([^（）()]{2,60})[）)]", raw_title),
                    *(concept.get("search_keywords") if isinstance(concept.get("search_keywords"), list) else []),
                ):
                    cleaned = _study_glossary_clean_term(candidate)
                    if cleaned and cleaned.casefold() not in {item.casefold() for item in proposed_terms}:
                        proposed_terms.append(cleaned)
                if not proposed_terms:
                    continue
                context_parts = []
                for field in (
                    "core_summary", "explanation", "common_confusion", "simple_example",
                    "example_problem", "example_method", "recall_cue",
                ):
                    value = re.sub(
                        r"\s+", " ",
                        _normalize_study_math_markup(
                            _strip_study_process_narration(concept.get(field))
                        ),
                    ).strip()
                    if value:
                        context_parts.append(f"{field}: {value[:700]}")
                grouped.setdefault(subject, []).append({
                    "candidate_id": f"s{session_id}c{concept_index}",
                    "session_id": session_id,
                    "concept_index": concept_index,
                    "session_title": str(recall_session.get("title") or "未命名筆記"),
                    "card_title": raw_title,
                    "proposed_terms": proposed_terms[:12],
                    "context": "\n".join(context_parts)[:2800],
                })
        result: Dict[str, Dict[str, Any]] = {}
        for subject, candidates in grouped.items():
            signature_source = {
                "definition_policy": "verified-general-v4",
                "cards": [
                    {
                        "id": item["candidate_id"],
                        "title": item["card_title"],
                        "terms": item["proposed_terms"],
                        "context": item["context"],
                    }
                    for item in candidates
                ],
            }
            result[subject] = {
                "candidates": candidates,
                "signature": hashlib.sha256(
                    json.dumps(signature_source, ensure_ascii=False, sort_keys=True).encode("utf-8")
                ).hexdigest(),
            }
        return result

    _glossary_item_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "candidate_id", "canonical_term", "aliases", "definition",
            "definition_scope", "accepted", "confidence", "rejection_reason",
        ],
        "properties": {
            "candidate_id": {"type": "string"},
            "canonical_term": {"type": "string"},
            "aliases": {"type": "array", "items": {"type": "string"}},
            "definition": {"type": "string"},
            "definition_scope": {
                "type": "string",
                "enum": ["general", "special_case", "example", "description"],
            },
            "accepted": {"type": "boolean"},
            "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
            "rejection_reason": {"type": "string"},
        },
    }
    _glossary_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["items"],
        "properties": {
            "items": {"type": "array", "items": _glossary_item_schema},
        },
    }

    def _finalize_verified_glossary_entry(entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        finalized = dict(entry)
        canonical = re.sub(r"\s+", " ", str(finalized.get("canonical_term") or "")).strip()
        folded = canonical.casefold()
        descriptive_or_special = bool(re.search(
            r"(?:二維|三維|2\s*[×x]\s*2).*展開|矩陣跡與特徵值的關係|正定自伴矩陣的特徵值|"
            r"矩陣平方根與\s*gram\s*因子化|常用.*恆等式|加權和的.*生成函數表示|"
            r"奇數\s*/\s*偶數.*生成函數表示|包含-排除記號與性質集合表示法|"
            r"一對一函數.*與排列|亂序的漸近行為|^從\s*(?:集合\s*)?a\s*到\s*(?:集合\s*)?b",
            folded,
            flags=re.IGNORECASE,
        ))
        if not canonical or descriptive_or_special or any(
            marker in folded
            for marker in (
                "對數與指數的等價恆等式",
                "函數大小排序",
                "函數增長排序",
                "對數級函數",
                "時間需求",
            )
        ) or canonical.startswith("從大小為"):
            return None

        replacement: Optional[Tuple[str, str]] = None
        if "little-omega" in folded or re.search(r"小\s*ω", canonical, re.IGNORECASE):
            replacement = (
                "little-ω（嚴格漸進下界）",
                "若 g(n) 最終不為 0，f(n)=ω(g(n)) 表示對每個常數 c>0，皆存在 n₀，使所有 n≥n₀ 都有 |f(n)|>c|g(n)|；等價地，|f(n)/g(n)|→∞。",
            )
        elif "little-o" in folded or re.search(r"小\s*o", canonical, re.IGNORECASE):
            replacement = (
                "little-o（嚴格漸進上界）",
                "若 g(n) 最終不為 0，f(n)=o(g(n)) 表示對每個常數 c>0，皆存在 n₀，使所有 n≥n₀ 都有 |f(n)|<c|g(n)|；等價地，f(n)/g(n)→0。",
            )
        elif "big-omega" in folded or re.search(r"大\s*omega|大\s*Ω", canonical, re.IGNORECASE):
            replacement = (
                "Big-Ω（漸進下界）",
                "對最終非負的函數 f、g，f(n)=Ω(g(n)) 表示存在常數 c>0 與 n₀，使所有 n≥n₀ 都有 f(n)≥c·g(n)。",
            )
        elif "big-theta" in folded or re.search(r"大\s*theta|大\s*Θ", canonical, re.IGNORECASE):
            replacement = (
                "Big-Θ（漸進緊界）",
                "對最終非負的函數 f、g，f(n)=Θ(g(n)) 表示存在常數 c₁,c₂>0 與 n₀，使所有 n≥n₀ 都有 c₁g(n)≤f(n)≤c₂g(n)。",
            )
        elif "big-oh" in folded or "o-notation" in folded or "big-o" in folded:
            replacement = (
                "Big-O（漸進上界）",
                "對最終非負的函數 f、g，f(n)=O(g(n)) 表示存在常數 c>0 與 n₀，使所有 n≥n₀ 都有 f(n)≤c·g(n)。",
            )
        elif "對數級迴圈次數" in canonical or "除以二的迴圈次數" in canonical:
            replacement = (
                "反覆除半的迴圈次數",
                "若正整數 n 每輪以整數除法除以 2，直到降至 1，迴圈次數為 ⌊log₂ n⌋；若停止條件要求累積倍增至至少 n，則為 ⌈log₂ n⌉。兩者的漸進時間皆為 Θ(log n)。",
            )
        elif "log(n!) 的漸進等價" in canonical:
            replacement = (
                "log(n!) 的漸進階",
                "由 Stirling 公式可得 ln(n!)=n ln n−n+O(ln n)，因此 ln(n!)=Θ(n ln n)，且 ln(n!)/(n ln n)→1。對其他固定底數的對數只相差常數因子。",
            )
        elif "stirling" in folded:
            replacement = (
                "Stirling 公式",
                "當 n→∞ 時，n!∼√(2πn)(n/e)^n；符號 ∼ 表示兩側比值趨近 1。等價地，ln(n!)=n ln n−n+(1/2)ln(2πn)+o(1)。",
            )
        elif "調和級數" in canonical or "harmonic series" in folded:
            replacement = (
                "調和數 Hₙ",
                "第 n 個調和數定義為 Hₙ=∑_{k=1}^n 1/k，且 Hₙ=ln n+γ+o(1)，所以 Hₙ=Θ(log n)。無窮調和級數 ∑_{k=1}^∞1/k 則發散。",
            )
        elif "球與盒子 / stars and bars" in folded:
            replacement = (
                "隔板法（Stars and Bars）",
                "對 n≥0、k≥1，方程 x₁+⋯+xₖ=n 的非負整數解共有 C(n+k−1,k−1) 個；等價於把 n 個相同物件分入 k 個可區分且允許空箱的箱子。若每箱至少一個，解數為 C(n−1,k−1)，前提是 n≥k。",
            )
        if replacement:
            finalized["canonical_term"], finalized["explanation"] = replacement
            finalized["explanation_kind"] = "標準定義"
            finalized["confidence"] = 100
        return finalized

    def _glossary_target_score(entry: Dict[str, Any]) -> int:
        score = int(entry.get("confidence") or 0)
        card_title = str(entry.get("card_title") or "")
        canonical = str(entry.get("canonical_term") or "")
        title = str(entry.get("title") or "")
        if "定義" in card_title:
            score += 30
        compact_card = re.sub(r"\s+", "", card_title).casefold()
        for candidate in (canonical, title):
            compact_candidate = re.sub(r"[\s（）()/_-]+", "", candidate).casefold()
            if len(compact_candidate) >= 3 and compact_candidate in compact_card:
                score += 10
                break
        return score

    def _generate_verified_glossary(
        subject: str,
        catalog: Dict[str, Any],
        progress_callback: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
    ) -> List[Dict[str, Any]]:
        candidates = list(catalog.get("candidates") or [])
        by_id = {item["candidate_id"]: item for item in candidates}
        accepted_terms: List[Dict[str, Any]] = []
        for start in range(0, len(candidates), 10):
            batch = candidates[start : start + 10]
            compact_batch = [
                {
                    "candidate_id": item["candidate_id"],
                    "card_title": item["card_title"],
                    "possible_names": item["proposed_terms"],
                    "note_context": item["context"],
                }
                for item in batch
            ]
            draft = _call_openai_json(
                name="study_glossary_definition_draft",
                schema=_glossary_schema,
                content=[{
                    "type": "input_text",
                    "text": (
                        f"你正在建立「{subject}」研究所考試筆記的專有名詞辭典。\n"
                        "每個輸入項目是一張重點卡。請辨認它真正教授的核心專有名詞，給出可獨立理解、"
                        "教科書式且精確的定義。定義必須回答『這個名詞是什麼』，不可只是說本卡介紹、"
                        "用來計算、包含哪些內容或重述章節標題。需要條件時直接寫入定義。不要引用來源編號。\n"
                        "aliases 只放確定同義的中文、英文全名或通用縮寫；不要放上位詞、相關詞、公式或例題名稱。"
                        "條件片段（如『不允許空像』）、描述片段（如『含重複組合數』）絕不是 aliases。"
                        "若卡片同時明確定義多個概念，請為每個概念輸出獨立項目（candidate_id 可重複，最多 3 項），"
                        "不可用『A 與 B』拼成一個不存在的總稱。若筆記只呈現某個標準概念的特例，canonical_term "
                        "應改用一般標準概念，definition 先給一般定義，不可把特例冒充完整定義。"
                        "definition_scope 只有在定義適用該名詞的一般情況時才填 general；只適用特定集合數量、"
                        "特定數值或單一題目時分別填 special_case 或 example。若卡片只是例題、步驟、公式彙整，"
                        "或脈絡不足以可靠辨認名詞，accepted=false。"
                        "所有公式的圓括號、方括號與大括號必須成對，中文敘述必須通順。"
                        "candidate_id 必須原樣保留。confidence 表示定義與卡片對應的信心。\n\n"
                        + json.dumps(compact_batch, ensure_ascii=False)
                    ),
                }],
                timeout=180,
                reasoning_effort="low",
                max_output_tokens=7000,
            )
            audit_input = {
                "subject": subject,
                "source_cards": compact_batch,
                "drafts": draft.get("items") or [],
            }
            audited = _call_openai_json(
                name="study_glossary_definition_audit",
                schema=_glossary_schema,
                content=[{
                    "type": "input_text",
                    "text": (
                        "你是嚴格的研究所教材編審。逐項審核下列詞條，必要時直接修正 canonical_term、"
                        "aliases、definition 與 definition_scope。只接受標準、無歧義、能獨立回答『是什麼』的定義，"
                        "並確認它真的對應指定 candidate_id 的卡片。摘要、用途描述、學習提示、例題敘述、"
                        "循環定義、把相關詞誤當同義詞，一律 accepted=false。數學或演算法名詞要保留成立條件。"
                        "定義若只涵蓋特定數值、三集合等有限特例，而詞名其實指一般概念，必須修成一般定義並將 "
                        "definition_scope 設為 general，否則拒絕。複合卡片中的不同概念要拆成多個項目，"
                        "不可創造『A 與 B』式總稱。aliases 中的條件、用途、章節片段必須移除。"
                        "僅在 confidence>=80 且無實質疑問時接受。不要因原草稿標示 accepted 就放寬。\n\n"
                        + json.dumps(audit_input, ensure_ascii=False)
                    ),
                }],
                timeout=180,
                reasoning_effort="medium",
                max_output_tokens=10000,
            )
            for item in audited.get("items") or []:
                candidate_id = str(item.get("candidate_id") or "")
                target = by_id.get(candidate_id)
                definition = re.sub(r"\s+", " ", str(item.get("definition") or "")).strip()
                canonical = _study_glossary_clean_term(item.get("canonical_term"))
                try:
                    confidence = int(item.get("confidence") or 0)
                except (TypeError, ValueError):
                    confidence = 0
                narration_like = bool(re.search(
                    r"(?:本卡|此卡|這張卡|本節|本章|本文|這份筆記|筆記中|主要介紹|主要說明|內容包含)",
                    definition,
                ))
                unbalanced_symbols = any(
                    definition.count(left) != definition.count(right)
                    for left, right in (("(", ")"), ("[", "]"), ("{", "}"), ("（", "）"))
                )
                special_case_name = bool(re.search(
                    r"^(?:二|三|四|五|六|七|八|九|十)集合|全集與性質(?:集合)?(?:記號|符號)",
                    canonical,
                ))
                if (
                    not target
                    or not item.get("accepted")
                    or item.get("definition_scope") != "general"
                    or confidence < 80
                    or not canonical
                    or len(definition) < 12
                    or len(definition) > 320
                    or narration_like
                    or unbalanced_symbols
                    or special_case_name
                ):
                    continue
                aliases: List[str] = []
                for alias in [canonical, *(item.get("aliases") or [])]:
                    cleaned = _study_glossary_clean_term(alias)
                    if cleaned.casefold() != canonical.casefold() and re.search(
                        r"^(?:含|不含|允許|不允許|使用|利用|求|計算|所有|具有|沒有)|(?:例題|範例|推導|做法|基礎|條件|情況)$|(?:全集與性質符號)$",
                        cleaned,
                    ):
                        continue
                    if cleaned and cleaned.casefold() not in {value.casefold() for value in aliases}:
                        aliases.append(cleaned)
                for term in aliases[:10]:
                    accepted_terms.append({
                        "title": term,
                        "canonical_term": canonical,
                        "aliases": [value for value in aliases if value.casefold() != canonical.casefold()],
                        "card_title": target["card_title"],
                        "explanation": definition,
                        "explanation_kind": "已審核定義",
                        "confidence": confidence,
                        "subject": subject,
                        "session_title": target["session_title"],
                        "session_id": target["session_id"],
                        "concept_index": target["concept_index"],
                    })
            if progress_callback:
                partial_unique: Dict[str, Dict[str, Any]] = {}
                for entry in accepted_terms:
                    key = entry["title"].casefold()
                    existing = partial_unique.get(key)
                    if not existing or _glossary_target_score(entry) > _glossary_target_score(existing):
                        partial_unique[key] = entry
                progress_callback(sorted(
                    partial_unique.values(),
                    key=lambda value: (-len(value["title"]), value["title"].casefold()),
                ))
        unique: Dict[str, Dict[str, Any]] = {}
        for entry in accepted_terms:
            key = entry["title"].casefold()
            existing = unique.get(key)
            if not existing or _glossary_target_score(entry) > _glossary_target_score(existing):
                unique[key] = entry
        return sorted(unique.values(), key=lambda item: (-len(item["title"]), item["title"].casefold()))

    def _start_glossary_refresh(subjects: List[str], catalog_by_subject: Dict[str, Dict[str, Any]]) -> None:
        pending: List[str] = []
        preserved_by_subject: Dict[str, List[Dict[str, Any]]] = {}
        with glossary_jobs_lock:
            for subject in subjects:
                if subject in glossary_jobs or subject not in catalog_by_subject:
                    continue
                glossary_jobs.add(subject)
                pending.append(subject)
        if not pending:
            return
        for subject in pending:
            existing = storage.get_study_recall_glossary(subject)
            preserved_terms = (
                list(existing.get("terms") or [])
                if existing and existing.get("source_signature") == catalog_by_subject[subject]["signature"]
                else []
            )
            preserved_by_subject[subject] = preserved_terms
            storage.save_study_recall_glossary(
                subject=subject,
                source_signature=catalog_by_subject[subject]["signature"],
                status="building",
                terms=preserved_terms,
            )

        def _run() -> None:
            def _merge_progress(
                subject: str,
                catalog: Dict[str, Any],
                partial_terms: List[Dict[str, Any]],
            ) -> None:
                merged: Dict[str, Dict[str, Any]] = {}
                for item in [*(preserved_by_subject.get(subject) or []), *partial_terms]:
                    if not isinstance(item, dict):
                        continue
                    key = str(item.get("title") or "").casefold()
                    existing_item = merged.get(key)
                    if not existing_item or _glossary_target_score(item) >= _glossary_target_score(existing_item):
                        merged[key] = item
                storage.save_study_recall_glossary(
                    subject=subject,
                    source_signature=catalog["signature"],
                    status="building",
                    terms=list(merged.values()),
                )

            for subject in pending:
                catalog = catalog_by_subject[subject]
                try:
                    terms = _generate_verified_glossary(
                        subject,
                        catalog,
                        progress_callback=lambda partial_terms, current_subject=subject, current_catalog=catalog: _merge_progress(
                            current_subject, current_catalog, partial_terms
                        ),
                    )
                    storage.save_study_recall_glossary(
                        subject=subject,
                        source_signature=catalog["signature"],
                        status="ready",
                        terms=terms,
                    )
                except Exception as exc:
                    app.logger.exception("Failed to build glossary for %s", subject)
                    storage.save_study_recall_glossary(
                        subject=subject,
                        source_signature=catalog["signature"],
                        status="failed",
                        terms=[],
                        error=str(exc),
                    )
                finally:
                    with glossary_jobs_lock:
                        glossary_jobs.discard(subject)
        threading.Thread(target=_run, daemon=True).start()

    @app.get("/admin/study-recall/glossary")
    @admin_required
    def admin_study_recall_glossary_verified():
        catalog_by_subject = _study_glossary_catalog()
        requested_subject = str(request.args.get("subject") or "").strip()
        subjects = [requested_subject] if requested_subject in catalog_by_subject else list(catalog_by_subject)
        ready_terms: List[Dict[str, Any]] = []
        stale_subjects: List[str] = []
        statuses: Dict[str, str] = {}
        diagnostics: Dict[str, str] = {}
        for subject in subjects:
            catalog = catalog_by_subject[subject]
            cached = storage.get_study_recall_glossary(subject)
            cache_matches = bool(
                cached and cached.get("source_signature") == catalog["signature"]
            )
            if cache_matches:
                cached_status = str(cached.get("status") or "missing")
                statuses[subject] = cached_status
                if cached.get("error"):
                    diagnostics[subject] = str(cached.get("error"))[:500]
                for entry in cached.get("terms") or []:
                    if not isinstance(entry, dict):
                        continue
                    entry = _finalize_verified_glossary_entry(entry)
                    if entry is None:
                        continue
                    session_id = int(entry.get("session_id") or 0)
                    concept_index = int(entry.get("concept_index") or 0)
                    ready_terms.append({
                        **entry,
                        "url": url_for("admin_study_recall", session_id=session_id)
                        + f"#concept-{concept_index}",
                    })
                if cached_status == "building":
                    with glossary_jobs_lock:
                        locally_running = subject in glossary_jobs
                    if not locally_running:
                        stale_subjects.append(subject)
            else:
                statuses[subject] = str(cached.get("status") if cached else "missing")
                stale_subjects.append(subject)
        if request.args.get("retry") == "1":
            for subject in subjects:
                if statuses.get(subject) == "failed" and subject not in stale_subjects:
                    stale_subjects.append(subject)
        if stale_subjects and openai_api_key:
            _start_glossary_refresh(stale_subjects, catalog_by_subject)
            for subject in stale_subjects:
                statuses[subject] = "building"
        best_target_by_concept: Dict[str, Dict[str, Any]] = {}
        for entry in ready_terms:
            concept_key = f"{str(entry.get('subject') or '').casefold()}\0{str(entry.get('canonical_term') or '').casefold()}"
            existing = best_target_by_concept.get(concept_key)
            if not existing or _glossary_target_score(entry) > _glossary_target_score(existing):
                best_target_by_concept[concept_key] = entry
        for entry in ready_terms:
            concept_key = f"{str(entry.get('subject') or '').casefold()}\0{str(entry.get('canonical_term') or '').casefold()}"
            preferred = best_target_by_concept.get(concept_key)
            if not preferred:
                continue
            for field in ("card_title", "session_title", "session_id", "concept_index", "url"):
                entry[field] = preferred.get(field)
        overall_status = (
            "building"
            if any(value == "building" for value in statuses.values())
            else "unavailable"
            if any(value in {"missing", "failed"} for value in statuses.values())
            else "ready"
        )
        if request.args.get("audit") == "1":
            audit_html = render_template_string(
                """
                <!doctype html><html lang="zh-Hant"><head><meta charset="utf-8">
                <meta name="viewport" content="width=device-width,initial-scale=1">
                {% if overall_status == 'building' %}<meta http-equiv="refresh" content="6">{% endif %}
                <title>詞典稽核</title>
                <style>
                body{font-family:system-ui,sans-serif;margin:24px;color:#17324d;background:#f5f9fc}
                h1{font-size:24px}.status{padding:10px 14px;border-radius:10px;background:#fff;border:1px solid #d7e3ec}
                table{width:100%;margin-top:16px;border-collapse:collapse;background:#fff;font-size:13px}
                th,td{padding:10px;border:1px solid #d7e3ec;text-align:left;vertical-align:top;line-height:1.55}
                th{position:sticky;top:0;background:#eaf4f6}.definition{min-width:360px}.bad{color:#a23b32}
                </style></head><body>
                <h1>詞典稽核</h1>
                <p class="status">狀態：{{ overall_status }}｜科目：{{ statuses }}｜共 {{ terms|length }} 個顯示詞</p>
                {% if diagnostics %}<p class="status bad">錯誤：{{ diagnostics }}</p>{% endif %}
                <table><thead><tr><th>#</th><th>科目</th><th>顯示詞</th><th>標準詞名</th><th class="definition">已審核定義</th><th>信心</th><th>目標卡</th></tr></thead>
                <tbody>{% for term in terms %}<tr><td>{{ loop.index }}</td><td>{{ term.subject }}</td><td>{{ term.title }}</td>
                <td>{{ term.canonical_term }}</td><td class="definition">{{ term.explanation }}</td><td>{{ term.confidence }}</td>
                <td><a href="{{ term.url }}">{{ term.session_title }}／{{ term.card_title }}</a></td></tr>{% endfor %}</tbody></table>
                </body></html>
                """,
                overall_status=overall_status,
                statuses=statuses,
                diagnostics=diagnostics,
                terms=ready_terms,
            )
            response = Response(audit_html, mimetype="text/html")
            response.headers["Cache-Control"] = "private, no-store"
            return response
        response = Response(
            json.dumps(
                {
                    "terms": ready_terms,
                    "status": overall_status,
                    "subjects": statuses,
                    "diagnostics": diagnostics,
                    "source_signatures": {
                        subject: catalog_by_subject[subject]["signature"]
                        for subject in subjects
                    },
                    "definition_policy": "verified-only",
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            mimetype="application/json",
        )
        response.headers["Cache-Control"] = "private, no-store"
        return response

    @app.post("/admin/study-recall/glossary/refresh")
    @admin_required
    def admin_study_recall_glossary_refresh():
        catalog_by_subject = _study_glossary_catalog()
        payload = request.get_json(silent=True) or {}
        requested_subject = str(payload.get("subject") or "").strip()
        subjects = [requested_subject] if requested_subject in catalog_by_subject else list(catalog_by_subject)
        if not openai_api_key:
            return {"ok": False, "error": "AI 定義服務尚未設定。"}, 503
        _start_glossary_refresh(subjects, catalog_by_subject)
        return {"ok": True, "status": "building", "subjects": subjects}

    @app.get("/admin/study-recall/glossary-legacy")
    @admin_required
    def admin_study_recall_glossary():
        """Build a live glossary from every existing recall card.

        The response is generated from current storage instead of being written
        back into individual notes, so old and newly uploaded notes share the
        same cross-note vocabulary immediately.
        """
        generic_titles = {
            "定義",
            "例題",
            "例",
            "性質",
            "公式",
            "結論",
            "重點",
            "補充",
            "注意",
            "觀念",
            "方法",
            "步驟",
            "證明",
            "函數",
            "應用",
            "基礎",
            "查表",
            "第一類",
            "第二類",
            "齊次",
            "非齊次",
            "展開",
            "係數",
            "變數",
            "重複",
            "組合",
            "計數",
            "推導",
            "條件",
            "固定",
            "等價",
            "關係",
            "大小",
            "形式",
            "有限",
            "生成",
            "分解",
            "恆等",
            "判定",
            "判準",
            "線性",
            "運算",
            "基本",
            "表述",
            "展開代入",
            "例子",
            "乘法",
            "反例",
            "非負",
            "ex",
            "example",
        }
        glossary_suffixes = (
            "係數解法例題",
            "計數公式",
            "解法例題",
            "一般形式",
            "通解形式",
            "生成函數例題",
            "數值表",
            "值表",
            "小值查表",
            "未完成展開",
            "定義",
            "公式",
            "性質",
            "表示",
            "推導",
            "做法",
            "例題",
            "範例",
            "示例",
            "例示",
            "應用",
            "技巧",
            "口訣",
            "查表",
            "集合",
            "計算",
            "基礎",
        )

        def preview_text(value: Any) -> str:
            text_value = _normalize_study_math_markup(
                _strip_study_process_narration(value)
            )
            text_value = re.sub(r"\s+", " ", text_value).strip()
            punctuation_positions = [
                text_value.find(mark, 24, 241)
                for mark in "。！？；"
            ]
            punctuation_positions = [
                position for position in punctuation_positions if position >= 24
            ]
            if punctuation_positions:
                punctuation_end = min(punctuation_positions)
                return text_value[: punctuation_end + 1]
            if len(text_value) <= 240:
                return text_value
            return text_value[:237].rstrip() + "…"

        def clean_term(value: Any) -> str:
            term = " ".join(
                _normalize_study_concept_title(value, "").split()
            ).strip(" \t\r\n，。；：、:;|/·・-–—")
            if not term:
                return ""
            changed = True
            while changed:
                changed = False
                for suffix in glossary_suffixes:
                    if term.endswith(suffix) and len(term) > len(suffix) + 1:
                        term = term[: -len(suffix)].rstrip(" 的之：:、，-/")
                        changed = True
                        break
            folded = term.casefold()
            compact_length = len(re.sub(r"\s+", "", term))
            if (
                compact_length < 2
                or compact_length > 42
                or folded in generic_titles
                or re.fullmatch(r"第?[一二三四五六七八九十\d]+(?:類|項|頁|章)?", term)
                or re.fullmatch(r"\d+\s*[×xX]\s*\d+", term)
                or (
                    bool(re.fullmatch(r"[A-Za-z]+\([^)]*\)(?:[+\-*/].*)?", term))
                    and bool(re.search(r"[, +\-*/]", term))
                )
                or re.search(r"[\\${}^_=]", term)
                or not re.search(r"[A-Za-z0-9\u3400-\u9fff]", term)
            ):
                return ""
            return term

        def title_terms(title: str, concept: Dict[str, Any]) -> List[Tuple[str, int]]:
            candidates: List[Tuple[str, int]] = [(title, 120)]
            parenthetical_parts = re.findall(r"[（(]([^（）()]{2,60})[）)]", title)
            candidates.extend((part, 88) for part in parenthetical_parts)
            title_without_parentheses = re.sub(
                r"[（(][^（）()]{1,80}[）)]", " ", title
            )
            candidates.append((title_without_parentheses, 76))
            candidates.extend(
                (part, 82)
                for part in re.split(
                    r"(?:的|之|與|以及|及|：|:|、|，|；|/|\|)",
                    title_without_parentheses,
                )
            )
            candidates.extend(
                (acronym, 96)
                for acronym in re.findall(
                    r"(?<![A-Za-z0-9])[A-Z][A-Z0-9-]{1,9}(?![A-Za-z0-9])",
                    title,
                )
            )
            search_keywords = concept.get("search_keywords")
            if isinstance(search_keywords, list):
                candidates.extend((keyword, 102) for keyword in search_keywords[:20])
            return candidates

        def term_occurs(term: str, value: Any) -> bool:
            text_value = str(value or "")
            if not text_value:
                return False
            escaped = re.escape(term)
            if re.match(r"[A-Za-z0-9_]", term) or re.search(
                r"[A-Za-z0-9_]$", term
            ):
                return bool(
                    re.search(
                        rf"(?<![A-Za-z0-9_]){escaped}(?![A-Za-z0-9_])",
                        text_value,
                        flags=re.IGNORECASE,
                    )
                )
            return term.casefold() in text_value.casefold()

        def grounded_explanation(
            term: str, record: Dict[str, Any]
        ) -> Tuple[str, str]:
            concept = record["concept"]
            sentence_candidates: List[Tuple[int, str, bool]] = []
            for field, field_score in (
                ("explanation", 36),
                ("core_summary", 32),
                ("common_confusion", 18),
                ("simple_example", 12),
                ("example_method", 10),
            ):
                field_text = _normalize_study_math_markup(
                    _strip_study_process_narration(concept.get(field))
                )
                for sentence in re.split(r"(?<=[。！？；])|[\r\n]+", field_text):
                    sentence = re.sub(r"\s+", " ", sentence).strip()
                    if not sentence or not term_occurs(term, sentence):
                        continue
                    term_pattern = re.escape(term)
                    definition_like = bool(
                        re.search(
                            rf"{term_pattern}.{{0,12}}(?:是指|指的是|定義為|定義成|稱為|叫做|表示|代表|用來|意指|是|為)",
                            sentence,
                            flags=re.IGNORECASE,
                        )
                    ) or bool(
                        re.search(
                            rf"(?:稱為|叫做|定義為|定義成|意指).{{0,16}}{term_pattern}",
                            sentence,
                            flags=re.IGNORECASE,
                        )
                    )
                    score = field_score + (55 if definition_like else 0)
                    score += max(0, 18 - abs(len(sentence) - 72) // 8)
                    sentence_candidates.append((score, sentence, definition_like))
            if sentence_candidates:
                _, sentence, definition_like = max(
                    sentence_candidates, key=lambda candidate: candidate[0]
                )
                return preview_text(sentence), (
                    "筆記直接說明" if definition_like else "筆記原句"
                )
            if term_occurs(term, record["card_title"]):
                return record["explanation"], "重點卡摘要"
            return (
                f"「{term}」收錄於重點卡「{record['card_title']}」。點擊可查看筆記中的完整脈絡。",
                "相關重點卡",
            )

        card_records: List[Dict[str, Any]] = []
        for recall_session in storage.list_study_recall_sessions(limit=None):
            session_id = int(recall_session.get("id") or 0)
            if session_id <= 0:
                continue
            for concept_index, concept in enumerate(
                recall_session.get("key_concepts") or []
            ):
                if not _is_recall_concept_eligible(concept):
                    continue
                title = " ".join(
                    _normalize_study_concept_title(
                        concept.get("concept"), concept.get("topic")
                    ).split()
                ).strip()
                if not title:
                    continue
                explanation = preview_text(
                    concept.get("explanation")
                    or concept.get("core_summary")
                    or concept.get("simple_example")
                    or concept.get("recall_cue")
                )
                if not explanation:
                    explanation = f"「{title}」的完整觀念與筆記內容。"
                card_records.append({
                    "card_title": title,
                    "explanation": explanation,
                    "subject": str(recall_session.get("subject") or "未分類"),
                    "session_title": str(
                        recall_session.get("title") or "未命名筆記"
                    ),
                    "session_id": session_id,
                    "concept_index": concept_index,
                    "url": url_for(
                        "admin_study_recall",
                        session_id=session_id,
                    )
                    + f"#concept-{concept_index}",
                    "concept": concept,
                })

        entries_by_title: Dict[str, Dict[str, Any]] = {}
        for record in card_records:
            canonical_title = str(record["card_title"])
            canonical_folded = canonical_title.casefold()
            is_definition_card = bool(
                re.search(r"(?:定義|基本|基礎|總覽|一般形式)", canonical_title)
            )
            for raw_term, base_score in title_terms(
                canonical_title, record["concept"]
            ):
                term = clean_term(raw_term)
                if not term:
                    continue
                folded_term = term.casefold()
                subject_key = str(record["subject"]).casefold()
                scoped_term_key = f"{subject_key}\u0000{folded_term}"
                score = base_score
                if folded_term == canonical_folded:
                    score += 40
                if is_definition_card and folded_term in canonical_folded:
                    score += 24
                existing = entries_by_title.get(scoped_term_key)
                if existing is not None and int(existing["_score"]) >= score:
                    continue
                explanation, explanation_kind = grounded_explanation(term, record)
                entries_by_title[scoped_term_key] = {
                    "title": term,
                    "card_title": canonical_title,
                    "explanation": explanation,
                    "explanation_kind": explanation_kind,
                    "subject": record["subject"],
                    "session_title": record["session_title"],
                    "session_id": record["session_id"],
                    "concept_index": record["concept_index"],
                    "url": record["url"],
                    "_score": score,
                }

        terms = sorted(
            (
                {key: value for key, value in entry.items() if key != "_score"}
                for entry in entries_by_title.values()
            ),
            key=lambda entry: (-len(entry["title"]), entry["title"].casefold()),
        )
        response = Response(
            json.dumps({"terms": terms}, ensure_ascii=False, separators=(",", ":")),
            mimetype="application/json",
        )
        response.headers["Cache-Control"] = "private, no-store"
        return response

    return (
        admin_study_recall,
        _study_glossary_clean_term,
        _study_glossary_catalog,
        _finalize_verified_glossary_entry,
        _glossary_target_score,
        _generate_verified_glossary,
        _start_glossary_refresh,
        admin_study_recall_glossary_verified,
        admin_study_recall_glossary_refresh,
        admin_study_recall_glossary,
    )
