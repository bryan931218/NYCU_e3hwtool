"""Note localization; dependencies are bound per application."""

import io
import json
import math
import os
import threading
import hashlib
from typing import Any, Dict, List, Optional, Set, Tuple
import requests
from PIL import Image, ImageDraw, ImageFont, ImageOps
from ..shared.source_localization import SOURCE_BBOX_VERSION, SOURCE_PAGE_INDEX_VERSION, assign_transcription_to_source_sections, detect_source_horizontal_separator_candidates, estimate_source_page_content_bounds, estimated_source_line_count, match_source_evidence_to_sections, match_source_evidence_via_page_alignment, resolve_source_evidence_page, source_bbox_from_lines, source_page_alignment_match_is_candidate, source_page_alignment_match_is_verified, source_section_match_is_candidate, source_section_match_is_verified
from ..shared.utils import json_safe


def build_note_localization(*,
    _call_openai_json,
    _canonical_study_source_match_text,
    _env_flag_truthy,
    _literal_study_source_evidence,
    _raise_if_study_upload_cancelled,
    _study_image_data_url,
    _validated_study_source_bbox,
    app,
    study_upload_context,
):
    def _study_source_index_pages(source_pages: Any) -> List[Dict[str, Any]]:
        """Build page text from OCR that was independently cropped from each image."""
        indexed_pages: List[Dict[str, Any]] = []
        for page in source_pages or []:
            if not isinstance(page, dict):
                continue
            try:
                image_index = int(page.get("image_index") or 0)
            except (TypeError, ValueError):
                continue
            localization_index = page.get("localization_index")
            if (
                image_index <= 0
                or not isinstance(localization_index, dict)
                or int(localization_index.get("version") or 0)
                != SOURCE_PAGE_INDEX_VERSION
                or str(localization_index.get("kind") or "") != "sections"
                or not isinstance(localization_index.get("lines"), list)
            ):
                continue
            section_texts = [
                str(line.get("text") or "").strip()
                for line in localization_index["lines"]
                if isinstance(line, dict) and str(line.get("text") or "").strip()
            ]
            if section_texts:
                indexed_pages.append(
                    {
                        "image_index": image_index,
                        "transcription": "\n\n".join(section_texts),
                    }
                )
        return indexed_pages

    def _resolve_study_source_page(
        evidence: Any,
        source_pages: Any,
        *,
        preferred_image_index: Any = None,
        context: Any = "",
    ) -> Optional[Dict[str, Any]]:
        indexed_pages = _study_source_index_pages(source_pages)
        if indexed_pages:
            indexed_resolution = resolve_source_evidence_page(
                evidence,
                indexed_pages,
                preferred_image_index=preferred_image_index,
                context=context,
            )
            if indexed_resolution:
                return {**indexed_resolution, "page_match_source": "section_ocr"}
        transcription_resolution = resolve_source_evidence_page(
            evidence,
            source_pages or [],
            preferred_image_index=preferred_image_index,
            context=context,
        )
        if transcription_resolution:
            return {
                **transcription_resolution,
                "page_match_source": "page_transcription",
            }
        return None

    def _localize_study_card_sources(
        images: List[Tuple[str, bytes, str]],
        key_concepts: List[Dict[str, Any]],
        source_pages: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[int, int]:
        """Locate sources from image-derived geometry and target-free OCR text."""
        targets_by_image: Dict[int, List[Dict[str, Any]]] = {}
        for concept_index, concept in enumerate(key_concepts):
            if not isinstance(concept, dict):
                continue
            card_context = " ".join(
                str(concept.get(field) or "").strip()
                for field in (
                    "concept",
                    "topic",
                    "core_summary",
                    "explanation",
                    "example_problem",
                    "example_method",
                    "simple_example",
                )
                if str(concept.get(field) or "").strip()
            )[:1800]
            for source_ref_index, source_ref in enumerate(concept.get("source_refs") or []):
                if not isinstance(source_ref, dict):
                    continue
                source_ref.pop("bbox", None)
                try:
                    image_index = int(source_ref.get("image_index") or 0)
                except (TypeError, ValueError):
                    continue
                evidence = _literal_study_source_evidence(source_ref.get("evidence"))
                if not evidence:
                    continue
                page_resolution = (
                    _resolve_study_source_page(
                        evidence,
                        source_pages or [],
                        preferred_image_index=image_index,
                        context=card_context,
                    )
                    if source_pages
                    else {
                        "image_index": image_index,
                        "page_verified": 1 <= image_index <= len(images),
                        "match_kind": "legacy_assigned",
                        "match_margin": 0.0,
                    }
                )
                if not page_resolution:
                    continue
                image_index = int(page_resolution.get("image_index") or 0)
                if not (1 <= image_index <= len(images)):
                    continue
                source_ref["image_index"] = image_index
                targets_by_image.setdefault(image_index, []).append(
                    {
                        "location_id": f"c{concept_index}r{source_ref_index}",
                        "concept": str(concept.get("concept") or "")[:100],
                        "card_context": card_context,
                        "evidence": evidence[:700],
                        "source_ref": source_ref,
                        "page_resolution": page_resolution,
                    }
                )
        total = sum(len(targets) for targets in targets_by_image.values())
        if not total:
            return 0, 0
        debug_localization = _env_flag_truthy(os.getenv("E3_SOURCE_LOCALIZATION_DEBUG"))

        def record_debug(target: Dict[str, Any], stage: str, **details: Any) -> None:
            if debug_localization:
                target["source_ref"]["_localization_debug"] = {
                    "stage": stage,
                    **json_safe(details),
                }

        pages_by_index: Dict[int, Dict[str, Any]] = {}
        for page in source_pages or []:
            if not isinstance(page, dict):
                continue
            try:
                image_index = int(page.get("image_index") or 0)
            except (TypeError, ValueError):
                continue
            if image_index > 0:
                pages_by_index[image_index] = page

        crop_batch_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["crops"],
            "properties": {
                "crops": {
                    "type": "array",
                    "maxItems": 8,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["crop_id", "visible_text", "confidence"],
                        "properties": {
                            "crop_id": {"type": "string", "maxLength": 40},
                            "visible_text": {"type": "string", "maxLength": 3200},
                            "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
                        },
                    },
                }
            },
        }
        section_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["separator_ids"],
            "properties": {
                "separator_ids": {
                    "type": "array",
                    "maxItems": 30,
                    "items": {"type": "string", "maxLength": 8},
                }
            },
        }
        reanchor_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["matches"],
            "properties": {
                "matches": {
                    "type": "array",
                    "maxItems": 8,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": [
                            "location_id",
                            "section_id",
                            "anchor",
                            "confidence",
                        ],
                        "properties": {
                            "location_id": {"type": "string", "maxLength": 40},
                            "section_id": {"type": "integer", "minimum": 0, "maximum": 80},
                            "anchor": {"type": "string", "maxLength": 420},
                            "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
                        },
                    },
                }
            },
        }

        def fallback_page_sections(page: Image.Image) -> List[Dict[str, Any]]:
            bounds = estimate_source_page_content_bounds(page)
            return [
                {
                    "line_id": 1,
                    **bounds,
                    "geometry_confidence": 0.62,
                    "text": "",
                }
            ]

        def separator_marker_image(
            page: Image.Image,
            candidates: List[Dict[str, Any]],
        ) -> Image.Image:
            page = page.convert("RGB")
            margin_width = max(150, round(page.width * 0.14))
            marked = Image.new("RGB", (page.width + margin_width, page.height), "white")
            marked.paste(page, (0, 0))
            draw = ImageDraw.Draw(marked)
            ruler_left = page.width + max(8, round(margin_width * 0.08))
            draw.line(
                (ruler_left, 0, ruler_left, page.height),
                fill=(125, 125, 125),
                width=max(1, round(page.width * 0.0015)),
            )
            try:
                font = ImageFont.load_default(size=max(12, round(page.width * 0.018)))
            except TypeError:
                font = ImageFont.load_default()
            for candidate in candidates:
                pixel_y = round(int(candidate["y"]) * page.height / 1000)
                draw.line(
                    (ruler_left, pixel_y, ruler_left + round(margin_width * 0.32), pixel_y),
                    fill=(220, 20, 105),
                    width=max(2, round(page.width * 0.0025)),
                )
                draw.text(
                    (ruler_left + round(margin_width * 0.38), max(0, pixel_y - 10)),
                    str(candidate["separator_id"]),
                    fill=(180, 0, 80),
                    font=font,
                )
            return marked

        def segment_page_sections(
            page: Image.Image,
            *,
            image_index: int,
            filename: str,
        ) -> Tuple[List[Dict[str, Any]], bool]:
            content_bounds = estimate_source_page_content_bounds(page)
            separator_candidates = detect_source_horizontal_separator_candidates(page)
            if not separator_candidates:
                return fallback_page_sections(page), True
            marked_page = separator_marker_image(page, separator_candidates)
            prompt = (
                "你是筆記區塊分隔線分類員，完全不知道之後要搜尋的卡片內容。原圖右側新增白色標尺欄，"
                "洋紅色短刻度與 S 編號只用來指出像素演算法找到的候選高度，不是筆記內容。請選出真正把上下兩個"
                "筆記主題、題目或觀念區塊分開的候選線編號。橫跨主要筆記寬度的手繪虛線或實線可選；"
                "大型矩形內容框的下緣若確實把框內內容與下方內容分開，也可選。短底線、公式分數線、矩陣線、"
                "刪除線、照片或工具列邊框、文字筆畫都不可選。沒有真正區塊分隔線時輸出空陣列。"
                "只可輸出圖上存在的 S 編號，不可自行估算座標。"
                f"這是第 {image_index} 張 {filename}。只輸出 schema JSON。"
            )
            try:
                result = _call_openai_json(
                    name="study_source_page_sections_v15",
                    schema=section_schema,
                    content=[
                        {"type": "input_text", "text": prompt},
                        {
                            "type": "input_image",
                            "image_url": _study_image_data_url(marked_page),
                            "detail": "high",
                        },
                    ],
                    timeout=240,
                    reasoning_effort="low",
                    max_output_tokens=3200,
                )
                had_response = True
            except (requests.RequestException, ValueError, TypeError):
                app.logger.exception("Study-note page section segmentation failed for image %s", image_index)
                # Continue with high-confidence image-derived separator rules.
                # This keeps section geometry available during API rate limits.
                result = {"separator_ids": []}
                had_response = False
            content_left = max(20, int(content_bounds["left"]))
            content_top = max(0, int(content_bounds["top"]))
            content_right = min(980, int(content_bounds["right"]))
            content_bottom = min(1000, int(content_bounds["bottom"]))
            if content_right - content_left < 80 or content_bottom - content_top < 25:
                return fallback_page_sections(page), had_response
            candidates_by_id = {
                str(candidate["separator_id"]): candidate
                for candidate in separator_candidates
            }
            selected_ids = {
                str(separator_id).strip().upper()
                for separator_id in result.get("separator_ids") or []
            }
            selected_ids.update(
                str(candidate["separator_id"])
                for candidate in separator_candidates
                if float(candidate.get("full_span") or 0.0) >= 0.45
                and (
                    float(candidate.get("full_coverage") or 0.0)
                    / max(1, int(candidate.get("run_count") or 0))
                ) >= 0.018
                and float(candidate.get("full_longest_run") or 0.0) <= 0.065
                and int(candidate.get("run_count") or 0) >= 8
                and int(candidate.get("thickness") or 999) <= 18
                and float(candidate.get("context_density") or 1.0) <= 0.13
                and float(candidate.get("separator_likelihood") or 0.0) >= 0.50
            )
            selected_ids.update(
                str(candidate["separator_id"])
                for candidate in separator_candidates
                if float(candidate.get("full_span") or 0.0) >= 0.40
                and (
                    float(candidate.get("full_coverage") or 0.0)
                    / max(1, int(candidate.get("run_count") or 0))
                ) >= 0.018
                and float(candidate.get("full_longest_run") or 0.0) <= 0.06
                and int(candidate.get("run_count") or 0) >= 8
                and int(candidate.get("thickness") or 999) <= 12
                and float(candidate.get("context_density") or 1.0) <= 0.05
                and float(candidate.get("separator_likelihood") or 0.0) >= 0.42
            )
            selected_ids.update(
                str(candidate["separator_id"])
                for candidate in separator_candidates
                if float(candidate.get("full_span") or 0.0) >= 0.70
                and float(candidate.get("full_coverage") or 0.0) >= 0.18
                and float(candidate.get("full_longest_run") or 0.0) <= 0.09
                and int(candidate.get("run_count") or 0) >= 8
                and int(candidate.get("thickness") or 999) <= 18
                and float(candidate.get("separator_likelihood") or 0.0) >= 0.42
            )
            selected_ids.update(
                str(candidate["separator_id"])
                for candidate in separator_candidates
                if 0.32 <= float(candidate.get("full_span") or 0.0) < 0.43
                and float(candidate.get("full_coverage") or 0.0) >= 0.15
                and (
                    float(candidate.get("full_coverage") or 0.0)
                    / max(1, int(candidate.get("run_count") or 0))
                ) >= 0.019
                and float(candidate.get("full_longest_run") or 0.0) <= 0.04
                and int(candidate.get("run_count") or 0) >= 8
                and int(candidate.get("thickness") or 999) <= 6
                and float(candidate.get("context_density") or 1.0) <= 0.04
                and float(candidate.get("separator_likelihood") or 0.0) >= 0.33
            )
            selected_ids.update(
                str(candidate["separator_id"])
                for candidate in separator_candidates
                if float(candidate.get("full_span") or 0.0) >= 0.80
                and float(candidate.get("full_coverage") or 0.0) >= 0.70
                and float(candidate.get("full_longest_run") or 0.0) >= 0.55
                and int(candidate.get("run_count") or 99) <= 4
                and float(candidate.get("separator_likelihood") or 0.0) >= 0.55
            )
            selected_candidates = sorted(
                (
                    candidate
                    for separator_id, candidate in candidates_by_id.items()
                    if separator_id in selected_ids
                    and content_top + 20 < int(candidate["y"]) < content_bottom - 20
                ),
                key=lambda candidate: int(candidate["y"]),
            )
            snapped_candidates: List[Dict[str, Any]] = []
            used_separator_ids: Set[str] = set()
            for selected_candidate in selected_candidates:
                selected_y = int(selected_candidate["y"])
                nearby = [
                    candidate
                    for candidate in separator_candidates
                    if str(candidate["separator_id"]) not in used_separator_ids
                    and abs(int(candidate["y"]) - selected_y) <= 110
                    and content_top + 20 < int(candidate["y"]) < content_bottom - 20
                ]
                best_nearby = max(
                    nearby or [selected_candidate],
                    key=lambda candidate: (
                        float(candidate.get("separator_likelihood") or 0.0),
                        -abs(int(candidate["y"]) - selected_y),
                    ),
                )
                selected_likelihood = float(
                    selected_candidate.get("separator_likelihood") or 0.0
                )
                snapped = (
                    best_nearby
                    if float(best_nearby.get("separator_likelihood") or 0.0)
                    >= selected_likelihood + 0.05
                    else selected_candidate
                )
                if (
                    float(snapped.get("context_density") or 0.0) > 0.075
                    and int(snapped.get("thickness") or 0) > 20
                    and float(snapped.get("full_longest_run") or 0.0) > 0.09
                    and not (
                        float(snapped.get("full_span") or 0.0) >= 0.80
                        and float(snapped.get("full_coverage") or 0.0) >= 0.70
                    )
                ):
                    continue
                if float(snapped.get("separator_likelihood") or 0.0) < 0.30:
                    continue
                if snapped_candidates and int(snapped["y"]) - int(snapped_candidates[-1]["y"]) < 24:
                    previous = snapped_candidates[-1]
                    if float(snapped.get("separator_likelihood") or 0.0) > float(
                        previous.get("separator_likelihood") or 0.0
                    ):
                        used_separator_ids.discard(str(previous["separator_id"]))
                        snapped_candidates[-1] = snapped
                        used_separator_ids.add(str(snapped["separator_id"]))
                    continue
                snapped_candidates.append(snapped)
                used_separator_ids.add(str(snapped["separator_id"]))
            boundaries = [
                (
                    int(candidate["y"]),
                    round(74 + min(0.22, float(candidate["score"])) * 100),
                )
                for candidate in snapped_candidates
            ]

            sections: List[Dict[str, Any]] = []
            section_top = content_top
            for y, confidence in [*boundaries, (content_bottom, 85)]:
                section_bottom = content_bottom if y == content_bottom else max(section_top + 25, y - 5)
                if section_bottom - section_top < 25:
                    section_top = min(content_bottom, y + 5)
                    continue
                sections.append(
                    {
                        "line_id": len(sections) + 1,
                        "left": content_left,
                        "top": section_top,
                        "right": content_right,
                        "bottom": section_bottom,
                        "geometry_confidence": round(confidence / 100, 4),
                        "text": "",
                    }
                )
                section_top = min(content_bottom, y + 5)
            return (sections or fallback_page_sections(page)), had_response

        def crop_from_bbox(
            page: Image.Image,
            bbox: Dict[str, Any],
            *,
            context_ratio: float,
        ) -> Optional[Image.Image]:
            try:
                left = int(bbox["left"])
                top = int(bbox["top"])
                right = int(bbox["right"])
                bottom = int(bbox["bottom"])
            except (KeyError, TypeError, ValueError):
                return None
            pixel_left = math.floor(left * page.width / 1000)
            pixel_top = math.floor(top * page.height / 1000)
            pixel_right = math.ceil(right * page.width / 1000)
            pixel_bottom = math.ceil(bottom * page.height / 1000)
            vertical_padding = max(5, round((pixel_bottom - pixel_top) * context_ratio))
            horizontal_padding = max(7, round(page.width * 0.007))
            pixel_left = max(0, pixel_left - horizontal_padding)
            pixel_right = min(page.width, pixel_right + horizontal_padding)
            pixel_top = max(0, pixel_top - vertical_padding)
            pixel_bottom = min(page.height, pixel_bottom + vertical_padding)
            if pixel_right - pixel_left < 12 or pixel_bottom - pixel_top < 8:
                return None
            crop = page.crop((pixel_left, pixel_top, pixel_right, pixel_bottom)).convert("RGB")
            if crop.height < 150:
                scale = min(3.2, 150 / max(1, crop.height))
                crop = crop.resize(
                    (max(1, round(crop.width * scale)), max(1, round(crop.height * scale))),
                    Image.Resampling.LANCZOS,
                )
            return crop

        def transcribe_crops(
            crop_specs: List[Tuple[str, Image.Image]],
            *,
            purpose: str,
        ) -> Tuple[Dict[str, Dict[str, Any]], bool]:
            transcribed: Dict[str, Dict[str, Any]] = {}
            had_response = False
            pending_specs = list(crop_specs)
            for attempt in range(2):
                if not pending_specs:
                    break
                for batch_start in range(0, len(pending_specs), 8):
                    _raise_if_study_upload_cancelled()
                    batch = pending_specs[batch_start : batch_start + 8]
                    content: List[Dict[str, Any]] = [
                        {
                            "type": "input_text",
                            "text": (
                                "你是手寫筆記裁切圖的逐字轉錄員。你不知道系統之後要找哪張卡，也不會看到任何待搜尋文字。"
                                "下方每個 crop_id 後緊接該裁切圖；請逐一按圖中自然閱讀順序轉錄真正可見的文字、數字與公式。"
                                "公式請盡量用 LaTeX，保留 =、不等號、箭頭、上下標、矩陣列與運算次序；不得依學科常識修正、"
                                "摘要或補入裁切外文字。無法辨識才使用〔不清楚〕，空白或純分隔線則 visible_text 留空。"
                                "每個輸入 crop_id 必須恰好輸出一次，且不得交換代碼。只輸出 schema JSON。"
                            ),
                        }
                    ]
                    for crop_id, crop in batch:
                        content.append({"type": "input_text", "text": f"crop_id={crop_id}"})
                        content.append(
                            {
                                "type": "input_image",
                                "image_url": _study_image_data_url(crop, max_side=2200),
                                "detail": "high",
                            }
                        )
                    try:
                        result = _call_openai_json(
                            name=f"study_source_section_ocr_{purpose}_v14_retry_{attempt}",
                            schema=crop_batch_schema,
                            content=content,
                            timeout=240,
                            reasoning_effort="low",
                            max_output_tokens=5200,
                        )
                        had_response = True
                    except (requests.RequestException, ValueError, TypeError):
                        app.logger.exception("Target-free source crop OCR failed for %s", purpose)
                        continue
                    expected_ids = {crop_id for crop_id, _crop in batch}
                    for item in result.get("crops") or []:
                        if not isinstance(item, dict):
                            continue
                        crop_id = str(item.get("crop_id") or "").strip()
                        if crop_id not in expected_ids or crop_id in transcribed:
                            continue
                        try:
                            confidence = max(0, min(100, int(item.get("confidence") or 0)))
                        except (TypeError, ValueError):
                            confidence = 0
                        candidate_transcription = {
                            "text": str(item.get("visible_text") or "").strip()[:3200],
                            "confidence": confidence,
                        }
                        if confidence >= 40 or attempt == 1:
                            transcribed[crop_id] = candidate_transcription
                pending_specs = [
                    spec for spec in pending_specs if spec[0] not in transcribed
                ]
            return transcribed, had_response

        def reanchor_targets_to_sections(
            targets: List[Dict[str, Any]],
            lines: List[Dict[str, Any]],
            *,
            image_index: int,
            _retry: bool = False,
        ) -> Dict[str, Dict[str, Any]]:
            """Repair legacy descriptive evidence using literal section OCR text."""
            if not targets or not lines:
                return {}
            lines_by_id = {
                int(line.get("line_id") or 0): line
                for line in lines
                if isinstance(line, dict)
                and int(line.get("line_id") or 0) > 0
                and str(line.get("text") or "").strip()
            }
            if not lines_by_id:
                return {}
            section_catalog = [
                {
                    "section_id": line_id,
                    "visible_text": str(line.get("text") or "")[:3200],
                }
                for line_id, line in sorted(lines_by_id.items())
            ]
            resolved: Dict[str, Dict[str, Any]] = {}
            for batch_start in range(0, len(targets), 8):
                _raise_if_study_upload_cancelled()
                batch = targets[batch_start : batch_start + 8]
                target_catalog = [
                    {
                        "location_id": str(target["location_id"]),
                        "concept": str(target.get("concept") or "")[:100],
                        "card_content": str(target.get("card_context") or "")[:900],
                        "legacy_evidence": str(target.get("evidence") or "")[:700],
                    }
                    for target in batch
                ]
                prompt = (
                    "你是舊筆記來源錨點修復員。section_catalog 是已按原圖方格逐字 OCR 的文字，"
                    "target_catalog 是卡片與舊來源描述。對每個 target，只能在某一個 section 的 visible_text "
                    "確實直接支持該卡片時配對；不確定就不要輸出該 target。section_id=0 表示不配對，但不要為它"
                    "編造 anchor。anchor 必須從所選 visible_text 逐字連續複製 12 至 220 個字元，保留公式、數字與"
                    "運算符，不可摘要、改寫、修正或拼接兩段。優先複製能唯一識別觀念或公式的最短完整片段。"
                    "同一 target 最多一筆；只輸出 schema JSON。\n"
                    + (
                        "這是第二次核對。上一輪未找到可靠錨點，請逐一重新檢查所有 section；仍不確定就省略。\n"
                        if _retry
                        else ""
                    )
                    + f"image_index={image_index}\n"
                    + "section_catalog="
                    + json.dumps(section_catalog, ensure_ascii=False, separators=(",", ":"))
                    + "\ntarget_catalog="
                    + json.dumps(target_catalog, ensure_ascii=False, separators=(",", ":"))
                )
                try:
                    result = _call_openai_json(
                        name=(
                            "study_source_legacy_reanchor_v1_retry"
                            if _retry
                            else "study_source_legacy_reanchor_v1"
                        ),
                        schema=reanchor_schema,
                        content=[{"type": "input_text", "text": prompt}],
                        timeout=180,
                        reasoning_effort="low",
                        max_output_tokens=3600,
                    )
                except (requests.RequestException, ValueError, TypeError):
                    app.logger.exception(
                        "Legacy source re-anchoring failed for image %s",
                        image_index,
                    )
                    continue
                targets_by_id = {
                    str(target["location_id"]): target for target in batch
                }
                for item in result.get("matches") or []:
                    if not isinstance(item, dict):
                        continue
                    location_id = str(item.get("location_id") or "").strip()
                    target = targets_by_id.get(location_id)
                    if target is None or location_id in resolved:
                        continue
                    try:
                        section_id = int(item.get("section_id") or 0)
                        model_confidence = int(item.get("confidence") or 0)
                    except (TypeError, ValueError):
                        continue
                    line = lines_by_id.get(section_id)
                    anchor = _literal_study_source_evidence(item.get("anchor"))
                    canonical_anchor = _canonical_study_source_match_text(anchor)
                    canonical_line = _canonical_study_source_match_text(
                        (line or {}).get("text")
                    )
                    if (
                        line is None
                        or model_confidence < 70
                        or len(canonical_anchor) < 8
                        or canonical_anchor not in canonical_line
                    ):
                        continue
                    anchor_lines, anchor_metrics = match_source_evidence_to_sections(
                        anchor,
                        lines,
                    )
                    if (
                        not anchor_lines
                        or int(anchor_lines[0].get("line_id") or 0) != section_id
                        or not source_section_match_is_verified(anchor, anchor_metrics)
                        or float(anchor_metrics.get("uniqueness") or 0.0) < 0.24
                    ):
                        continue
                    _original_lines, original_metrics = match_source_evidence_to_sections(
                        target["evidence"],
                        [line],
                    )
                    _concept_lines, concept_metrics = match_source_evidence_to_sections(
                        target.get("concept") or "",
                        [line],
                    )
                    _context_lines, context_metrics = match_source_evidence_to_sections(
                        target.get("card_context") or "",
                        [line],
                    )
                    semantic_score = max(
                        float(original_metrics.get("score") or 0.0),
                        float(concept_metrics.get("score") or 0.0),
                        float(context_metrics.get("score") or 0.0),
                    )
                    semantic_coverage = max(
                        float(original_metrics.get("coverage") or 0.0),
                        float(concept_metrics.get("coverage") or 0.0),
                        float(context_metrics.get("coverage") or 0.0),
                    )
                    semantic_formula = float(
                        original_metrics.get("formula_coverage") or 0.0
                    )
                    if (
                        semantic_score < 0.24
                        and semantic_coverage < 0.18
                        and semantic_formula < 0.40
                    ):
                        continue
                    resolved[location_id] = {
                        "line": line,
                        "anchor": anchor,
                        "metrics": anchor_metrics,
                        "model_confidence": model_confidence,
                        "semantic_score": semantic_score,
                    }
            if not _retry:
                still_unresolved = [
                    target
                    for target in targets
                    if str(target["location_id"]) not in resolved
                ]
                if still_unresolved:
                    resolved.update(
                        reanchor_targets_to_sections(
                            still_unresolved,
                            lines,
                            image_index=image_index,
                            _retry=True,
                        )
                    )
            return resolved

        located = 0
        successful_page_ocr = False
        used_cached_index = False
        used_transcription_fallback = False
        parent_cancel_event = getattr(study_upload_context, "cancel_event", None)
        for image_index, page_targets in sorted(targets_by_image.items()):
            _raise_if_study_upload_cancelled()
            filename, image_bytes, _mime_type = images[image_index - 1]
            try:
                with Image.open(io.BytesIO(image_bytes)) as opened:
                    clean_page = ImageOps.exif_transpose(opened).convert("RGB")
            except (OSError, ValueError, TypeError):
                app.logger.exception("Unable to prepare source image %s", image_index)
                continue
            page_digest = hashlib.sha256(image_bytes).hexdigest()
            source_page = pages_by_index.get(image_index)
            page_transcription = str((source_page or {}).get("transcription") or "")
            transcription_isolated = (
                str((source_page or {}).get("transcription_mode") or "")
                == "isolated_v1"
            )
            cached_index = source_page.get("localization_index") if source_page else None
            lines: List[Dict[str, Any]] = []
            if (
                isinstance(cached_index, dict)
                and int(cached_index.get("version") or 0) == SOURCE_PAGE_INDEX_VERSION
                and str(cached_index.get("kind") or "") == "sections"
                and str(cached_index.get("image_sha256") or "") == page_digest
                and isinstance(cached_index.get("lines"), list)
            ):
                for cached_line in cached_index["lines"]:
                    if not isinstance(cached_line, dict):
                        continue
                    try:
                        line = {
                            "line_id": int(cached_line.get("line_id") or len(lines) + 1),
                            "left": int(cached_line["left"]),
                            "top": int(cached_line["top"]),
                            "right": int(cached_line["right"]),
                            "bottom": int(cached_line["bottom"]),
                            "geometry_confidence": float(cached_line.get("geometry_confidence") or 0.0),
                            "ocr_confidence": int(cached_line.get("ocr_confidence") or 0),
                            "text": str(cached_line.get("text") or "")[:3200],
                            "transcription_fallback": bool(
                                cached_line.get("transcription_fallback")
                            ),
                            "transcription_isolated": bool(
                                cached_line.get("transcription_isolated")
                            ),
                        }
                    except (KeyError, TypeError, ValueError):
                        continue
                    if line["text"] and 0 <= line["left"] < line["right"] <= 1000 and 0 <= line["top"] < line["bottom"] <= 1000:
                        lines.append(line)
                used_cached_index = used_cached_index or bool(lines)

            if not lines:
                geometry, segmentation_had_response = segment_page_sections(
                    clean_page,
                    image_index=image_index,
                    filename=filename,
                )
                successful_page_ocr = successful_page_ocr or segmentation_had_response
                line_crops: List[Tuple[str, Image.Image]] = []
                for line in geometry:
                    crop = crop_from_bbox(clean_page, line, context_ratio=0.01)
                    if crop is not None:
                        line_crops.append((f"L{int(line['line_id']):03d}", crop))
                line_texts, had_response = transcribe_crops(
                    line_crops,
                    purpose=f"page_{image_index}_sections",
                )
                successful_page_ocr = successful_page_ocr or had_response
                for line in geometry:
                    crop_id = f"L{int(line['line_id']):03d}"
                    result = line_texts.get(crop_id) or {}
                    text = str(result.get("text") or "").strip()
                    if not text:
                        continue
                    lines.append(
                        {
                            **line,
                            "text": text[:3200],
                            "ocr_confidence": int(result.get("confidence") or 0),
                        }
                    )
                if (
                    transcription_isolated
                    and page_transcription
                    and len(lines) < len(geometry)
                ):
                    fallback_lines = assign_transcription_to_source_sections(
                        page_transcription,
                        geometry,
                    )
                    if fallback_lines:
                        lines = fallback_lines
                        used_transcription_fallback = True
                if source_page is not None:
                    source_page["localization_index"] = {
                        "version": SOURCE_PAGE_INDEX_VERSION,
                        "kind": "sections",
                        "bbox_version": SOURCE_BBOX_VERSION,
                        "image_sha256": page_digest,
                        "image_width": clean_page.width,
                        "image_height": clean_page.height,
                        "lines": lines,
                    }
            if not lines:
                continue

            candidates: List[Dict[str, Any]] = []
            unresolved_targets: List[Dict[str, Any]] = []

            def append_candidate(
                target: Dict[str, Any],
                selected_lines: List[Dict[str, Any]],
                metrics: Dict[str, Any],
                *,
                alignment_fallback: bool = False,
                verification_evidence: Optional[str] = None,
                reanchored: bool = False,
                anchor_model_confidence: int = 0,
            ) -> bool:
                candidate_bbox = source_bbox_from_lines(selected_lines)
                if candidate_bbox is None:
                    return False
                candidate_bbox = {
                    "left": max(20, int(candidate_bbox["left"])),
                    "top": max(0, int(candidate_bbox["top"])),
                    "right": min(980, int(candidate_bbox["right"])),
                    "bottom": min(1000, int(candidate_bbox["bottom"])),
                }
                if (
                    candidate_bbox["right"] - candidate_bbox["left"] < 80
                    or candidate_bbox["bottom"] - candidate_bbox["top"] < 25
                ):
                    return False
                crop = crop_from_bbox(clean_page, candidate_bbox, context_ratio=0.0)
                if crop is None:
                    return False
                candidates.append(
                    {
                        "target": target,
                        "selected_lines": selected_lines,
                        "metrics": metrics,
                        "bbox": candidate_bbox,
                        "crop": crop,
                        "alignment_fallback": alignment_fallback,
                        "verification_evidence": (
                            verification_evidence or target["evidence"]
                        ),
                        "reanchored": reanchored,
                        "anchor_model_confidence": anchor_model_confidence,
                        "transcription_fallback": bool(selected_lines)
                        and all(
                            bool(line.get("transcription_fallback"))
                            and bool(line.get("transcription_isolated"))
                            for line in selected_lines
                        ),
                    }
                )
                return True

            for target in page_targets:
                direct_lines, direct_metrics = match_source_evidence_to_sections(
                    target["evidence"],
                    lines,
                )
                direct_candidate = bool(direct_lines) and source_section_match_is_candidate(
                    target["evidence"], direct_metrics
                )
                alignment_lines, alignment_metrics = (
                    match_source_evidence_via_page_alignment(
                        target["evidence"],
                        page_transcription,
                        lines,
                    )
                    if page_transcription
                    else ([], {})
                )
                alignment_candidate = bool(
                    alignment_lines
                ) and source_page_alignment_match_is_candidate(
                    target["evidence"], alignment_metrics
                )
                selected_lines: List[Dict[str, Any]] = []
                metrics: Dict[str, Any] = {}
                alignment_fallback = False
                if direct_candidate:
                    selected_lines = direct_lines
                    metrics = direct_metrics
                    if (
                        alignment_candidate
                        and int(alignment_lines[0].get("line_id") or 0)
                        == int(direct_lines[0].get("line_id") or 0)
                    ):
                        metrics = {**direct_metrics, **alignment_metrics}
                elif alignment_candidate:
                    selected_lines = alignment_lines
                    metrics = alignment_metrics
                    alignment_fallback = True
                else:
                    unresolved_targets.append(
                        {
                            "target": target,
                            "direct_metrics": direct_metrics,
                            "alignment_metrics": alignment_metrics,
                        }
                    )
                    continue
                if not append_candidate(
                    target,
                    selected_lines,
                    metrics,
                    alignment_fallback=alignment_fallback,
                ):
                    record_debug(target, "candidate_geometry_rejected")

            if unresolved_targets:
                repairs = reanchor_targets_to_sections(
                    [item["target"] for item in unresolved_targets],
                    lines,
                    image_index=image_index,
                )
                for unresolved in unresolved_targets:
                    target = unresolved["target"]
                    repair = repairs.get(str(target["location_id"]))
                    if repair is None or not append_candidate(
                        target,
                        [repair["line"]],
                        repair["metrics"],
                        verification_evidence=str(repair["anchor"]),
                        reanchored=True,
                        anchor_model_confidence=int(
                            repair.get("model_confidence") or 0
                        ),
                    ):
                        record_debug(
                            target,
                            "candidate_match_rejected",
                            direct_metrics=unresolved["direct_metrics"],
                            alignment_metrics=unresolved["alignment_metrics"],
                            reanchor_attempted=True,
                        )

            verification_crops: Dict[Tuple[int, int, int, int], Tuple[str, Image.Image]] = {}
            for candidate in candidates:
                bbox = candidate["bbox"]
                bbox_key = (
                    int(bbox["left"]),
                    int(bbox["top"]),
                    int(bbox["right"]),
                    int(bbox["bottom"]),
                )
                verification_id = f"S{len(verification_crops) + 1:03d}"
                if bbox_key not in verification_crops:
                    verification_crops[bbox_key] = (verification_id, candidate["crop"])
                candidate["verification_id"] = verification_crops[bbox_key][0]
            verification_specs = list(verification_crops.values())
            crop_texts, crop_had_response = transcribe_crops(
                verification_specs,
                purpose=f"page_{image_index}_verify",
            )
            successful_page_ocr = successful_page_ocr or crop_had_response
            for candidate in candidates:
                target = candidate["target"]
                verification_evidence = str(
                    candidate.get("verification_evidence") or target["evidence"]
                )
                crop_result = crop_texts.get(str(candidate["verification_id"])) or {}
                crop_visible_text = str(crop_result.get("text") or "").strip()
                transcription_fallback_verified = False
                if not crop_visible_text:
                    fallback_metrics = candidate["metrics"]
                    transcription_fallback_verified = bool(
                        candidate.get("transcription_fallback")
                        and not candidate.get("alignment_fallback")
                        and target.get("page_resolution", {}).get("page_verified")
                        and source_section_match_is_verified(
                            verification_evidence,
                            fallback_metrics,
                        )
                        and float(fallback_metrics.get("uniqueness") or 0.0)
                        >= 0.20
                    )
                    if not transcription_fallback_verified:
                        record_debug(target, "verification_ocr_missing")
                        continue
                    crop_visible_text = "\n".join(
                        str(line.get("text") or "")
                        for line in candidate["selected_lines"]
                    )
                    crop_result = {"confidence": 72}
                crop_selected, crop_metrics = match_source_evidence_to_sections(
                    verification_evidence,
                    [{"text": crop_visible_text}],
                )
                crop_direct_verified = bool(
                    crop_selected
                ) and source_section_match_is_verified(
                    verification_evidence, crop_metrics
                )
                index_alignment_metrics = candidate["metrics"]
                has_index_alignment = (
                    float(index_alignment_metrics.get("alignment_score") or 0.0) > 0.0
                    and source_page_alignment_match_is_candidate(
                        verification_evidence, index_alignment_metrics
                    )
                )
                crop_alignment_metrics: Dict[str, Any] = {}
                crop_alignment_verified = False
                if has_index_alignment and page_transcription:
                    source_start = int(
                        index_alignment_metrics.get("alignment_source_start") or 0
                    )
                    source_end = int(
                        index_alignment_metrics.get("alignment_source_end") or 0
                    )
                    _crop_alignment_lines, crop_alignment_metrics = (
                        match_source_evidence_via_page_alignment(
                            verification_evidence,
                            page_transcription,
                            [{"line_id": 1, "text": crop_visible_text}],
                            expected_source_span=(source_start, source_end),
                        )
                    )
                    crop_alignment_verified = source_page_alignment_match_is_verified(
                        verification_evidence, crop_alignment_metrics
                    )
                verification_passed = (
                    crop_alignment_verified
                    if candidate.get("alignment_fallback")
                    else crop_direct_verified or crop_alignment_verified
                )
                if not verification_passed:
                    record_debug(
                        target,
                        "verification_match_rejected",
                        crop_text=crop_visible_text,
                        direct_metrics=crop_metrics,
                        alignment_metrics=crop_alignment_metrics,
                    )
                    continue
                index_metrics = candidate["metrics"]
                alignment_verified = bool(
                    crop_alignment_verified and has_index_alignment
                )
                formula_token_count = int(index_metrics.get("formula_token_count") or 0)
                formula_coverage = min(
                    float(index_metrics.get("formula_coverage") or 0.0),
                    float(crop_metrics.get("formula_coverage") or 0.0),
                )
                match_score = min(
                    float(index_metrics.get("score") or 0.0),
                    float(crop_metrics.get("score") or 0.0),
                )
                match_coverage = min(
                    float(index_metrics.get("coverage") or 0.0),
                    float(crop_metrics.get("coverage") or 0.0),
                )
                boundary_coverage = min(
                    float(index_metrics.get("boundary_coverage") or 0.0),
                    float(crop_metrics.get("boundary_coverage") or 0.0),
                )
                uniqueness = float(index_metrics.get("uniqueness") or 0.0)
                segmentation_stability = min(
                    float(line.get("geometry_confidence") or 0.0)
                    for line in candidate["selected_lines"]
                )
                if alignment_verified:
                    alignment_support = min(
                        max(
                            float(index_metrics.get("alignment_evidence_coverage") or 0.0),
                            float(index_metrics.get("alignment_context_coverage") or 0.0),
                        ),
                        max(
                            float(crop_alignment_metrics.get("alignment_evidence_coverage") or 0.0),
                            float(crop_alignment_metrics.get("alignment_context_coverage") or 0.0),
                        ),
                    )
                    confidence_score = (
                        min(
                            float(index_metrics.get("alignment_score") or 0.0),
                            float(crop_alignment_metrics.get("alignment_score") or 0.0),
                        )
                        * 0.34
                        + min(
                            float(index_metrics.get("alignment_interval_coverage") or 0.0),
                            float(crop_alignment_metrics.get("alignment_interval_coverage") or 0.0),
                        )
                        * 0.20
                        + min(
                            float(index_metrics.get("alignment_section_coverage") or 0.0),
                            float(crop_alignment_metrics.get("alignment_section_coverage") or 0.0),
                        )
                        * 0.12
                        + min(
                            float(index_metrics.get("alignment_page_coverage") or 0.0),
                            float(crop_alignment_metrics.get("alignment_page_coverage") or 0.0),
                        )
                        * 0.10
                        + alignment_support * 0.08
                        + uniqueness * 0.08
                        + segmentation_stability * 0.08
                    )
                else:
                    formula_component = formula_coverage if formula_token_count else match_coverage
                    confidence_score = (
                        match_coverage * 0.39
                        + formula_component * 0.20
                        + boundary_coverage * 0.17
                        + float(crop_metrics.get("precision") or 0.0) * 0.02
                        + uniqueness * 0.12
                        + segmentation_stability * 0.10
                    )
                crop_ocr_confidence = int(crop_result.get("confidence") or 0)
                if crop_ocr_confidence < 40:
                    record_debug(
                        target,
                        "verification_ocr_low_confidence",
                        confidence=crop_ocr_confidence,
                        crop_text=crop_visible_text,
                    )
                    continue
                confidence = round(
                    confidence_score * 90 + crop_ocr_confidence * 0.10
                )
                if alignment_verified:
                    # The two transcript alignments and same-span crop check are
                    # the hard acceptance gates. Keep the aggregate score as a
                    # display confidence without rejecting that verified result.
                    confidence = max(72, confidence)
                if transcription_fallback_verified:
                    confidence = max(72, confidence)
                if candidate.get("reanchored"):
                    confidence = max(78, confidence)
                minimum_confidence = (
                    72
                    if alignment_verified or transcription_fallback_verified
                    else 78
                    if candidate.get("reanchored")
                    else 82
                )
                if confidence < minimum_confidence:
                    record_debug(
                        target,
                        "confidence_rejected",
                        confidence=confidence,
                        crop_text=crop_visible_text,
                        index_metrics=index_metrics,
                        crop_metrics=crop_metrics,
                        crop_alignment_metrics=crop_alignment_metrics,
                    )
                    continue
                expected_lines = max(
                    1,
                    estimated_source_line_count(verification_evidence),
                    len(candidate["selected_lines"]),
                )
                bbox = {
                    **candidate["bbox"],
                    "confidence": confidence,
                    "version": SOURCE_BBOX_VERSION,
                    "text_verified": True,
                    "match_score": round(match_score, 4),
                    "match_coverage": round(match_coverage, 4),
                    "boundary_coverage": round(boundary_coverage, 4),
                    "evidence_length": len(
                        _canonical_study_source_match_text(verification_evidence)
                    ),
                    "expected_lines": expected_lines,
                    "span_verified": True,
                    "crop_verified": not transcription_fallback_verified,
                    "transcription_fallback_verified": transcription_fallback_verified,
                    "transcription_isolated": bool(
                        transcription_fallback_verified
                    ),
                    "crop_match_score": round(float(crop_metrics.get("score") or 0.0), 4),
                    "crop_match_coverage": round(float(crop_metrics.get("coverage") or 0.0), 4),
                    "crop_boundary_coverage": round(
                        float(crop_metrics.get("boundary_coverage") or 0.0), 4
                    ),
                    "crop_match_precision": round(float(crop_metrics.get("precision") or 0.0), 4),
                    "geometry_verified": True,
                    "page_verified": bool(
                        target.get("page_resolution", {}).get("page_verified")
                    ),
                    "source_image_index": image_index,
                    "page_match_kind": str(
                        target.get("page_resolution", {}).get("match_kind") or ""
                    ),
                    "page_match_margin": round(
                        float(
                            target.get("page_resolution", {}).get("match_margin")
                            or 0.0
                        ),
                        4,
                    ),
                    "formula_coverage": round(formula_coverage, 4),
                    "formula_token_count": formula_token_count,
                    "uniqueness": round(uniqueness, 4),
                    "segmentation_stability": round(segmentation_stability, 4),
                    "localization_method": (
                        "section_transcription_fallback"
                        if transcription_fallback_verified
                        else "section_ocr_reanchored"
                        if candidate.get("reanchored")
                        else "section_ocr_alignment"
                        if alignment_verified
                        else "section_ocr_rag"
                    ),
                    "anchor_verified": bool(candidate.get("reanchored")),
                    "localization_anchor": (
                        verification_evidence[:420]
                        if candidate.get("reanchored")
                        else ""
                    ),
                    "alignment_verified": alignment_verified,
                    "alignment_score": round(
                        float(index_metrics.get("alignment_score") or 0.0), 4
                    ),
                    "alignment_evidence_coverage": round(
                        float(index_metrics.get("alignment_evidence_coverage") or 0.0), 4
                    ),
                    "alignment_interval_coverage": round(
                        float(index_metrics.get("alignment_interval_coverage") or 0.0), 4
                    ),
                    "alignment_context_coverage": round(
                        float(index_metrics.get("alignment_context_coverage") or 0.0), 4
                    ),
                    "alignment_section_coverage": round(
                        float(index_metrics.get("alignment_section_coverage") or 0.0), 4
                    ),
                    "alignment_page_coverage": round(
                        float(index_metrics.get("alignment_page_coverage") or 0.0), 4
                    ),
                    "alignment_expected_span_agreement": round(
                        float(index_metrics.get("alignment_expected_span_agreement") or 0.0), 4
                    ),
                    "crop_alignment_score": round(
                        float(crop_alignment_metrics.get("alignment_score") or 0.0), 4
                    ),
                    "crop_alignment_evidence_coverage": round(
                        float(crop_alignment_metrics.get("alignment_evidence_coverage") or 0.0), 4
                    ),
                    "crop_alignment_interval_coverage": round(
                        float(crop_alignment_metrics.get("alignment_interval_coverage") or 0.0), 4
                    ),
                    "crop_alignment_context_coverage": round(
                        float(crop_alignment_metrics.get("alignment_context_coverage") or 0.0), 4
                    ),
                    "crop_alignment_section_coverage": round(
                        float(crop_alignment_metrics.get("alignment_section_coverage") or 0.0), 4
                    ),
                    "crop_alignment_page_coverage": round(
                        float(crop_alignment_metrics.get("alignment_page_coverage") or 0.0), 4
                    ),
                    "crop_alignment_expected_span_agreement": round(
                        float(crop_alignment_metrics.get("alignment_expected_span_agreement") or 0.0), 4
                    ),
                }
                validated = _validated_study_source_bbox(
                    bbox,
                    require_text_verified=True,
                    expected_image_index=image_index,
                )
                if validated is None:
                    record_debug(target, "bbox_validation_rejected", bbox=bbox)
                    continue
                target["source_ref"]["bbox"] = validated
                target["source_ref"].pop("_localization_debug", None)
                located += 1

        if (
            not successful_page_ocr
            and not used_cached_index
            and not used_transcription_fallback
        ):
            raise ValueError("Source page OCR failed for every image")
        if isinstance(parent_cancel_event, threading.Event):
            study_upload_context.cancel_event = parent_cancel_event
        return located, total

    return (
        _study_source_index_pages,
        _resolve_study_source_page,
        _localize_study_card_sources,
    )
