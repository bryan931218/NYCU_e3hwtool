"""Note sources routes and their feature helpers."""

import io
import json
import re
import secrets
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import requests
from PIL import Image, ImageFilter, ImageOps
from flask import Response, request, send_file, url_for
from ...shared.config import normalize_openai_reasoning_effort
from ...shared.visual_notes import render_visual_region_svg, visual_region_crop_box


def register_note_sources_routes(*,
    STUDY_NOTE_STAGING_TTL_SECONDS,
    _NOTE_IMAGE_MIME_TYPES,
    _canonical_study_source_match_text,
    _extract_openai_text,
    _is_openai_quota_error,
    _is_recall_concept_eligible,
    _literal_study_source_evidence,
    _localize_study_card_sources,
    _normalize_study_math_markup,
    _openai_error_details,
    _set_study_source_job,
    _validated_study_source_bbox,
    admin_required,
    app,
    current_user,
    openai_api_key,
    openai_model,
    record_ui_event,
    storage,
    study_source_jobs,
    study_source_jobs_lock,
    study_upload_root,
):
    @app.get("/admin/study-recall/<int:session_id>/image/<filename>")
    @admin_required
    def admin_study_recall_image(session_id: int, filename: str):
        recall_session = storage.get_study_recall_session(session_id)
        allowed_names = set((recall_session or {}).get("image_filenames") or [])
        if filename not in allowed_names or Path(filename).name != filename:
            return Response("Not Found", status=404, mimetype="text/plain")
        image_path = study_upload_root / str(session_id) / filename
        if not image_path.is_file():
            return Response("Not Found", status=404, mimetype="text/plain")
        return send_file(image_path, conditional=True, max_age=0)

    @app.get("/study-progress/notes/<int:session_id>/image/<filename>")
    def public_study_recall_image(session_id: int, filename: str):
        recall_session = storage.get_study_recall_session(session_id)
        allowed_names = set((recall_session or {}).get("image_filenames") or [])
        if filename not in allowed_names or Path(filename).name != filename:
            return Response("Not Found", status=404, mimetype="text/plain")
        image_path = study_upload_root / str(session_id) / filename
        if not image_path.is_file():
            return Response("Not Found", status=404, mimetype="text/plain")
        return send_file(image_path, conditional=True, max_age=3600)

    def _study_visual_region(
        recall_session: Dict[str, Any],
        region_id: str,
    ) -> Optional[Dict[str, Any]]:
        if not re.fullmatch(r"p[1-9][0-9]*v[1-9][0-9]*", str(region_id or "")):
            return None
        for page in recall_session.get("source_transcription") or []:
            if not isinstance(page, dict):
                continue
            for region in page.get("visual_regions") or []:
                if (
                    isinstance(region, dict)
                    and str(region.get("region_id") or "") == region_id
                ):
                    return region
        return None

    @app.get("/admin/study-recall/<int:session_id>/visual/<region_id>/crop")
    @admin_required
    def admin_study_recall_visual_crop(session_id: int, region_id: str):
        recall_session = storage.get_study_recall_session(session_id)
        region = _study_visual_region(recall_session or {}, region_id)
        if recall_session is None or region is None:
            return Response("Not Found", status=404, mimetype="text/plain")
        try:
            image_index = int(region.get("image_index") or 0)
        except (TypeError, ValueError):
            image_index = 0
        filenames = recall_session.get("image_filenames") or []
        if not (1 <= image_index <= len(filenames)):
            return Response("Not Found", status=404, mimetype="text/plain")
        image_path = study_upload_root / str(session_id) / str(filenames[image_index - 1])
        try:
            with Image.open(image_path) as opened:
                source = ImageOps.exif_transpose(opened).convert("RGB")
                crop_box = visual_region_crop_box(
                    region,
                    image_width=source.width,
                    image_height=source.height,
                )
                if crop_box is None:
                    raise ValueError("invalid visual crop")
                crop = source.crop(crop_box)
                if request.args.get("variant") == "enhanced":
                    max_side = max(crop.size)
                    scale = min(2.5, max(1.0, 1800 / max(1, max_side)))
                    if scale > 1.05:
                        crop = crop.resize(
                            (
                                max(1, round(crop.width * scale)),
                                max(1, round(crop.height * scale)),
                            ),
                            Image.Resampling.LANCZOS,
                        )
                    crop = crop.filter(
                        ImageFilter.UnsharpMask(radius=1.0, percent=155, threshold=2)
                    )
                output = io.BytesIO()
                crop.save(output, format="JPEG", quality=92, optimize=True)
                output.seek(0)
        except (OSError, ValueError):
            return Response("Not Found", status=404, mimetype="text/plain")
        return send_file(
            output,
            mimetype="image/jpeg",
            conditional=True,
            max_age=3600,
            download_name=f"{region_id}.jpg",
        )

    @app.get("/admin/study-recall/<int:session_id>/visual/<region_id>/redraw")
    @admin_required
    def admin_study_recall_visual_redraw(session_id: int, region_id: str):
        recall_session = storage.get_study_recall_session(session_id)
        region = _study_visual_region(recall_session or {}, region_id)
        svg = render_visual_region_svg(region or {})
        if recall_session is None or region is None or svg is None:
            return Response("Not Found", status=404, mimetype="text/plain")
        response = Response(svg, status=200, mimetype="image/svg+xml")
        response.headers["Cache-Control"] = "private, max-age=3600"
        response.headers["Content-Security-Policy"] = "default-src 'none'; style-src 'unsafe-inline'"
        return response

    @app.post("/admin/study-recall/<int:session_id>/localize-sources")
    @admin_required
    def admin_study_recall_localize_sources(session_id: int):
        if not openai_api_key:
            return {"ok": False, "error": "來源定位尚未啟用，請先設定 OPENAI_API_KEY。"}, 503
        recall_session = storage.get_study_recall_session(session_id)
        if recall_session is None:
            return {"ok": False, "error": "找不到這份筆記。"}, 404
        concepts = recall_session.get("key_concepts") or []
        source_pages = recall_session.get("source_transcription") or []
        source_refs = [
            source_ref
            for concept in concepts
            if isinstance(concept, dict)
            for source_ref in concept.get("source_refs") or []
            if isinstance(source_ref, dict)
            and _literal_study_source_evidence(source_ref.get("evidence"))
        ]
        total_count = len(source_refs)
        if total_count == 0:
            return {"ok": False, "error": "這份筆記沒有可定位的來源片段。"}, 400

        images: List[Tuple[str, Any, str]] = []
        for filename in recall_session.get("image_filenames") or []:
            image_path = study_upload_root / str(session_id) / filename
            mime_type = _NOTE_IMAGE_MIME_TYPES.get(image_path.suffix.lower())
            if not mime_type or not image_path.is_file():
                return {"ok": False, "error": "找不到完整的原始筆記圖片，無法建立來源定位。"}, 404
            images.append((filename, image_path.read_bytes(), mime_type))
        if not images:
            return {"ok": False, "error": "找不到原始筆記圖片，無法建立來源定位。"}, 404

        user = current_user() or {}
        username = str(user.get("username") or "")
        now = time.time()
        with study_source_jobs_lock:
            expired_job_ids = [
                job_id
                for job_id, job in study_source_jobs.items()
                if float(job.get("updated_at") or 0) < now - STUDY_NOTE_STAGING_TTL_SECONDS
            ]
            for expired_job_id in expired_job_ids:
                study_source_jobs.pop(expired_job_id, None)
            active_job = next(
                (
                    (job_id, job)
                    for job_id, job in study_source_jobs.items()
                    if job.get("username") == username
                    and int(job.get("session_id") or 0) == session_id
                    and job.get("status") == "running"
                ),
                None,
            )
            if active_job is not None:
                active_job_id, active_job_data = active_job
                return {
                    "ok": True,
                    "background": True,
                    "job_id": active_job_id,
                    "status_url": url_for(
                        "admin_study_recall_localization_job",
                        job_id=active_job_id,
                    ),
                    "message": str(active_job_data.get("message") or "來源重新定位仍在進行。"),
                }, 202

            job_id = secrets.token_urlsafe(18)
            study_source_jobs[job_id] = {
                "username": username,
                "session_id": session_id,
                "status": "running",
                "progress": 12,
                "message": "已開始背景重新定位，可繼續使用其他頁面。",
                "created_at": now,
                "updated_at": now,
            }

        def _run_source_localization() -> None:
            try:
                _set_study_source_job(
                    job_id,
                    progress=24,
                    message="正在逐張比對重點卡與原始筆記。",
                )
                located_count, localized_total = _localize_study_card_sources(
                    images,
                    concepts,
                    source_pages,
                )
                _set_study_source_job(
                    job_id,
                    progress=92,
                    message="定位完成，正在安全寫回筆記。",
                )
                latest_session = storage.get_study_recall_session(session_id)
                if latest_session is None:
                    raise ValueError("這份筆記已不存在。")
                latest_concepts = latest_session.get("key_concepts") or []
                localized_sources: Dict[
                    Tuple[int, str], Tuple[int, Dict[str, Any]]
                ] = {}
                for concept_index, localized_concept in enumerate(concepts):
                    if not isinstance(localized_concept, dict):
                        continue
                    for localized_ref in localized_concept.get("source_refs") or []:
                        if not isinstance(localized_ref, dict):
                            continue
                        try:
                            image_index = int(localized_ref.get("image_index") or 0)
                        except (TypeError, ValueError):
                            continue
                        evidence_key = _canonical_study_source_match_text(
                            localized_ref.get("evidence")
                        )
                        bbox = _validated_study_source_bbox(
                            localized_ref.get("bbox"),
                            require_text_verified=True,
                            expected_image_index=image_index,
                        )
                        if image_index > 0 and evidence_key and bbox:
                            localized_sources[(concept_index, evidence_key)] = (
                                image_index,
                                bbox,
                            )
                for concept_index, latest_concept in enumerate(latest_concepts):
                    if not isinstance(latest_concept, dict):
                        continue
                    for latest_ref in latest_concept.get("source_refs") or []:
                        if not isinstance(latest_ref, dict):
                            continue
                        latest_ref.pop("bbox", None)
                        try:
                            image_index = int(latest_ref.get("image_index") or 0)
                        except (TypeError, ValueError):
                            continue
                        evidence_key = _canonical_study_source_match_text(
                            latest_ref.get("evidence")
                        )
                        localized_source = localized_sources.get(
                            (concept_index, evidence_key)
                        )
                        if localized_source:
                            resolved_image_index, bbox = localized_source
                            latest_ref["image_index"] = resolved_image_index
                            latest_ref["bbox"] = bbox
                localized_indexes = {
                    int(page.get("image_index") or 0): page.get("localization_index")
                    for page in source_pages
                    if isinstance(page, dict) and isinstance(page.get("localization_index"), dict)
                }
                latest_source_pages = latest_session.get("source_transcription") or []
                for latest_page in latest_source_pages:
                    if not isinstance(latest_page, dict):
                        continue
                    try:
                        latest_image_index = int(latest_page.get("image_index") or 0)
                    except (TypeError, ValueError):
                        continue
                    localization_index = localized_indexes.get(latest_image_index)
                    if localization_index:
                        latest_page["localization_index"] = localization_index
                storage.replace_study_recall_localization(
                    session_id,
                    key_concepts=latest_concepts,
                    source_transcription=latest_source_pages,
                )
                _set_study_source_job(
                    job_id,
                    status="success",
                    progress=100,
                    message=f"重新定位完成，已保留 {located_count} 個精確來源。",
                    located_count=located_count,
                    total_count=localized_total,
                )
                record_ui_event(
                    "study_recall_sources_relocalized",
                    meta={
                        "username": username,
                        "session_id": session_id,
                        "located_count": located_count,
                        "total_count": localized_total,
                    },
                )
            except Exception as exc:  # pragma: no cover - background integration path
                app.logger.exception("Study-note source localization backfill failed")
                message = str(exc).strip() or "AI 暫時無法完成來源定位，請稍後再試。"
                _set_study_source_job(
                    job_id,
                    status="error",
                    message=message[:240],
                )

        threading.Thread(target=_run_source_localization, daemon=True).start()
        return {
            "ok": True,
            "background": True,
            "job_id": job_id,
            "status_url": url_for(
                "admin_study_recall_localization_job",
                job_id=job_id,
            ),
            "message": "已開始背景重新定位，可繼續使用其他頁面。",
        }, 202

    @app.get("/admin/study-recall/localization-jobs/<job_id>")
    @admin_required
    def admin_study_recall_localization_job(job_id: str):
        user = current_user() or {}
        with study_source_jobs_lock:
            stored_job = study_source_jobs.get(job_id)
            job = dict(stored_job) if stored_job else None
        if not job or job.get("username") != user.get("username"):
            return {"ok": False, "error": "找不到這次重新定位工作。"}, 404
        payload = {
            "ok": True,
            "status": str(job.get("status") or "running"),
            "progress": int(job.get("progress") or 0),
            "message": str(job.get("message") or "正在重新定位來源。"),
        }
        if job.get("status") == "success":
            payload.update(
                located_count=int(job.get("located_count") or 0),
                total_count=int(job.get("total_count") or 0),
                reload_url=url_for(
                    "admin_study_recall",
                    session_id=int(job.get("session_id") or 0),
                ),
            )
        return payload

    @app.get("/admin/study-recall/<int:session_id>/cards/<int:concept_index>")
    @admin_required
    def admin_study_recall_card_detail(session_id: int, concept_index: int):
        recall_session = storage.get_study_recall_session(session_id)
        concepts = (recall_session or {}).get("key_concepts") or []
        if (
            concept_index < 0
            or concept_index >= len(concepts)
            or not isinstance(concepts[concept_index], dict)
            or not _is_recall_concept_eligible(concepts[concept_index])
        ):
            return {"ok": False, "error": "找不到這張關聯重點卡。"}, 404
        concept = concepts[concept_index]
        return {
            "ok": True,
            "card": {
                "title": _normalize_study_math_markup(concept.get("concept")),
                "topic": _normalize_study_math_markup(concept.get("topic")),
                "card_type": "example" if concept.get("card_type") == "example" else "concept",
                "core_summary": _normalize_study_math_markup(concept.get("core_summary")),
                "explanation": _normalize_study_math_markup(concept.get("explanation")),
                "simple_example": _normalize_study_math_markup(concept.get("simple_example")),
                "example_problem": _normalize_study_math_markup(concept.get("example_problem")),
                "example_method": _normalize_study_math_markup(concept.get("example_method")),
                "reasoning_steps": [
                    _normalize_study_math_markup(step)
                    for step in (concept.get("reasoning_steps") or [])[:6]
                    if str(step or "").strip()
                ],
                "common_confusion": _normalize_study_math_markup(concept.get("common_confusion")),
                "memory_hint": _normalize_study_math_markup(concept.get("memory_hint")),
            },
        }

    @app.post("/admin/study-recall/<int:session_id>/cards/<int:concept_index>/ask")
    @admin_required
    def admin_study_recall_ask_card(session_id: int, concept_index: int):
        if not openai_api_key:
            return {"ok": False, "error": "AI 問答尚未啟用，請先設定 OPENAI_API_KEY。"}, 503
        recall_session = storage.get_study_recall_session(session_id)
        concepts = (recall_session or {}).get("key_concepts") or []
        if concept_index < 0 or concept_index >= len(concepts) or not isinstance(concepts[concept_index], dict):
            return {"ok": False, "error": "找不到這張重點卡。"}, 404
        concept = concepts[concept_index]
        if not _is_recall_concept_eligible(concept):
            return {"ok": False, "error": "這張重點卡目前無法使用 AI 問答。"}, 400
        payload = request.get_json(silent=True) or {}
        question = " ".join(str(payload.get("question") or "").split()).strip()
        if not question:
            return {"ok": False, "error": "請輸入想詢問的內容。"}, 400
        if len(question) > 800:
            return {"ok": False, "error": "問題請控制在 800 字以內。"}, 400
        response_style = str(payload.get("response_style") or "concise").strip().lower()
        if response_style not in {"concise", "detailed"}:
            response_style = "concise"
        style_instruction = (
            "採詳細回答：先給結論，再用必要推導、條件與一個例子說清楚，控制在 5 至 10 個短段落。"
            if response_style == "detailed"
            else "採精簡回答：直接回答核心問題，保留必要條件與公式，控制在 2 至 5 個短段落。"
        )

        history: List[Dict[str, str]] = []
        raw_history = payload.get("history")
        if isinstance(raw_history, list):
            for entry in raw_history[-6:]:
                if not isinstance(entry, dict):
                    continue
                role = str(entry.get("role") or "").strip().lower()
                content = " ".join(str(entry.get("content") or "").split()).strip()[:1600]
                if role in {"user", "assistant"} and content:
                    history.append({"role": role, "content": content})

        relations = []
        for relation in (concept.get("relations") or [])[:2]:
            if not isinstance(relation, dict):
                continue
            title = str(relation.get("title") or "").strip()
            association = str(relation.get("association") or "").strip()
            if title and association:
                relations.append({"title": title[:80], "association": association[:240]})
        concept_topic = str(concept.get("topic") or "").strip()
        supporting_cards = []
        candidates = [
            (index, other)
            for index, other in enumerate(concepts)
            if index != concept_index and isinstance(other, dict) and _is_recall_concept_eligible(other)
        ]
        candidates.sort(
            key=lambda item: 0
            if concept_topic and str(item[1].get("topic") or "").strip() == concept_topic
            else 1
        )
        for _index, other in candidates[:6]:
            supporting_cards.append(
                {
                    "concept": str(other.get("concept") or "")[:80],
                    "core_summary": str(other.get("core_summary") or "")[:320],
                    "explanation": str(other.get("explanation") or "")[:600],
                    "simple_example": str(other.get("simple_example") or "")[:420],
                    "memory_hint": str(other.get("memory_hint") or "")[:120],
                }
            )
        card_context = {
            "note_title": str((recall_session or {}).get("title") or "")[:120],
            "note_summary": str((recall_session or {}).get("summary") or "")[:1200],
            "subject": str((recall_session or {}).get("subject") or "")[:48],
            "topic": concept_topic[:48],
            "concept": str(concept.get("concept") or "")[:80],
            "card_type": str(concept.get("card_type") or "concept")[:16],
            "core_summary": str(concept.get("core_summary") or "")[:320],
            "explanation": str(concept.get("explanation") or "")[:1200],
            "simple_example": str(concept.get("simple_example") or "")[:420],
            "example_problem": str(concept.get("example_problem") or "")[:420],
            "example_method": str(concept.get("example_method") or "")[:340],
            "memory_hint": str(concept.get("memory_hint") or "")[:160],
            "relations": relations,
            "supporting_cards": supporting_cards,
        }
        conversation = "\n".join(
            f"{'學生' if entry['role'] == 'user' else '助教'}：{entry['content']}"
            for entry in history
        )
        prompt = (
            "你是研究所考試筆記的即時 AI 助教。請以提供的重點卡為主要脈絡，回答學生目前不熟悉的觀念。"
            "重點卡、同主題卡與最近對話都可能含有錯誤，只能當作問題背景，不可當成事實依據；必須以可靠學科知識重新驗證。"
            "若卡片有可明確判定的錯誤，必須指出正確內容，不可沿用錯誤"
            f"回答使用好理解的繁體中文並完整收尾。{style_instruction}數學表達式一律使用 LaTeX："
            "行內公式用 \\( ... \\)，獨立公式用 \\[ ... \\]。不要輸出 Markdown 標題、粗體標記或程式碼區塊。"
            "不得提及資料庫欄位、session_id、concept_index 或 s6:c6 這類內部代碼。\n\n"
            f"重點卡資料：\n{json.dumps(card_context, ensure_ascii=False)}\n\n"
            f"最近對話：\n{conversation or '尚無'}\n\n"
            f"學生這次的問題：{question}"
        )
        def request_answer(request_prompt: str) -> Tuple[str, bool]:
            response = requests.post(
                "https://api.openai.com/v1/responses",
                headers={"Authorization": f"Bearer {openai_api_key}", "Content-Type": "application/json"},
                json={
                    "model": openai_model,
                    "store": False,
                    "input": [{"role": "user", "content": [{"type": "input_text", "text": request_prompt}]}],
                    "reasoning": {
                        "effort": normalize_openai_reasoning_effort(openai_model, "low")
                    },
                    "max_output_tokens": 3200,
                },
                timeout=90,
            )
            response.raise_for_status()
            response_payload = response.json()
            return _extract_openai_text(response_payload).strip()[:8000], response_payload.get("status") == "incomplete"

        try:
            answer, incomplete = request_answer(prompt)
            if incomplete:
                answer, incomplete = request_answer(
                    prompt
                    + "\n\n上一次回答因長度限制而不完整。請重新從頭回答，不要接續殘句；保留必要公式與條件，"
                    "將完整答案壓縮在 1000 個繁體中文字內，並以完整句子收尾。"
                )
            answer = _normalize_study_math_markup(
                re.sub(r"\bs\d+\s*:\s*c\d+\b", "", answer, flags=re.IGNORECASE).strip()
            )
        except requests.HTTPError as exc:
            error_code, error_type, error_message = _openai_error_details(exc.response)
            if _is_openai_quota_error(error_code, error_type, error_message):
                return {"ok": False, "error": "OpenAI API 額度不足，請管理員補充額度後再使用 AI 助教。"}, 503
            return {"ok": False, "error": "AI 助教暫時無法回答，請稍後再試。"}, 502
        except (requests.RequestException, ValueError, TypeError):
            return {"ok": False, "error": "AI 助教暫時無法回答，請稍後再試。"}, 502
        if incomplete:
            return {"ok": False, "error": "回答內容仍過長，請把問題縮小到一個觀念後再試。"}, 502
        if not answer:
            return {"ok": False, "error": "AI 助教沒有產生有效回答，請換個方式提問。"}, 502
        record_ui_event(
            "study_recall_card_question",
            meta={
                "session_id": session_id,
                "concept_index": concept_index,
                "history_count": len(history),
                "response_style": response_style,
            },
        )
        return {"ok": True, "answer": answer}

    return (
        admin_study_recall_image,
        public_study_recall_image,
        _study_visual_region,
        admin_study_recall_visual_crop,
        admin_study_recall_visual_redraw,
        admin_study_recall_localize_sources,
        admin_study_recall_localization_job,
        admin_study_recall_card_detail,
        admin_study_recall_ask_card,
    )
