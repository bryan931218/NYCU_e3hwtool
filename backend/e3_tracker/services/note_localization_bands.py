"""Note localization bands; dependencies are bound per application."""

import io
import json
import math
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from statistics import median
from typing import Any, Dict, List, Optional, Set, Tuple
import requests
from PIL import Image, ImageOps
from ..shared.source_localization import SOURCE_BBOX_VERSION, source_line_match_is_verified


def build_note_localization_bands(*,
    _StudyUploadCancelled,
    _call_openai_json,
    _canonical_study_source_match_text,
    _literal_study_source_evidence,
    _match_study_source_evidence_to_lines,
    _raise_if_study_upload_cancelled,
    _study_coordinate_guide_data_url,
    _study_image_data_url,
    _study_source_band_sheet_data_urls,
    _study_source_visual_line_bands,
    _validated_study_source_bbox,
    app,
    study_upload_context,
):
    def _localize_study_card_sources_band_experiment(
        images: List[Tuple[str, bytes, str]],
        key_concepts: List[Dict[str, Any]],
    ) -> Tuple[int, int]:
        requests_by_id: Dict[str, Dict[str, Any]] = {}
        requests_by_image: Dict[int, List[Dict[str, Any]]] = {}
        for concept_index, concept in enumerate(key_concepts):
            if not isinstance(concept, dict):
                continue
            for source_ref_index, source_ref in enumerate(concept.get("source_refs") or []):
                if not isinstance(source_ref, dict):
                    continue
                source_ref.pop("bbox", None)
                try:
                    image_index = int(source_ref.get("image_index") or 0)
                except (TypeError, ValueError):
                    continue
                evidence = _literal_study_source_evidence(source_ref.get("evidence"))
                if not (1 <= image_index <= len(images)) or not evidence:
                    continue
                location_id = f"c{concept_index}r{source_ref_index}"
                requests_by_id[location_id] = source_ref
                requests_by_image.setdefault(image_index, []).append(
                    {
                        "location_id": location_id,
                        "concept": str(concept.get("concept") or "")[:80],
                        "evidence": evidence[:600],
                        "start_anchor_text": evidence[:36],
                        "end_anchor_text": evidence[-36:],
                    }
                )
        total = len(requests_by_id)
        if not total:
            return 0, 0

        located = 0
        failed_pages = 0
        for image_index, localization_input in sorted(requests_by_image.items()):
            _raise_if_study_upload_cancelled()
            item_count = len(localization_input)
            location_schema = {
                "type": "object",
                "additionalProperties": False,
                "required": ["locations"],
                "properties": {
                    "locations": {
                        "type": "array",
                        "minItems": item_count,
                        "maxItems": item_count,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "location_id",
                                "left",
                                "top",
                                "right",
                                "bottom",
                                "start_x",
                                "start_y",
                                "end_x",
                                "end_y",
                                "confidence",
                            ],
                            "properties": {
                                "location_id": {"type": "string", "maxLength": 12},
                                "left": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "top": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "right": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "bottom": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "start_x": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "start_y": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "end_x": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "end_y": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
                            },
                        },
                    }
                },
            }
            filename, image_bytes, _mime_type = images[image_index - 1]
            try:
                with Image.open(io.BytesIO(image_bytes)) as source_image:
                    clean_page = ImageOps.exif_transpose(source_image).convert("RGB")
                pixel_width, pixel_height = clean_page.size
                clean_data_url = _study_image_data_url(clean_page)
                guide_data_url = _study_coordinate_guide_data_url(clean_page)
                prompt = (
                    f"你是手寫筆記來源的精確視覺定位員。這是第 {image_index} 張、檔名 {filename}、"
                    f"canonical bitmap {pixel_width}×{pixel_height}。待定位項目都只來自這一張圖。"
                    "逐項比對 evidence，先找出 start_anchor_text 對應之第一段可見文字的中心，再找出 "
                    "end_anchor_text 對應之最後一段可見文字的中心；start_x/start_y 與 end_x/end_y 必須是這兩個"
                    "實際文字錨點的中心，不是矩形角落。接著以 left/top/right/bottom 框住從首錨點至尾錨點"
                    "所涵蓋的全部定義、條件、公式及必要推導。不得納入 evidence 結束後的下一個標題、例題或定義，"
                    "也不得因為先找到關鍵詞就漏掉 evidence 後半段。若 evidence 是較短內容，即使附近另有相關筆記也只框"
                    "該 evidence 本身。所有座標都相對於完整 canonical bitmap，左上 (0,0)、右下 (1000,1000)。"
                    "你會看到完全相同方向與長寬比的乾淨圖及紅色座標網格圖；乾淨圖用於逐字辨識，網格圖只用於讀座標。"
                    "矩形四周保留約 6 至 12 個座標單位，必須完整涵蓋上下標、矩陣、分數與公式末端。"
                    "每個 location_id 恰好輸出一次且不得改名；無法唯一辨識時 confidence 低於 60。只輸出 schema JSON。\n\n"
                    + json.dumps(localization_input, ensure_ascii=False, separators=(",", ":"))
                )
                source_content = [
                    {"type": "input_text", "text": prompt},
                    {"type": "input_image", "image_url": clean_data_url, "detail": "high"},
                    {"type": "input_text", "text": "同一張 canonical bitmap 的 0–1000 座標網格："},
                    {"type": "input_image", "image_url": guide_data_url, "detail": "high"},
                ]
                locations = None
                for source_attempt in range(2):
                    try:
                        result = _call_openai_json(
                            name="study_recall_source_locations_v3",
                            schema=location_schema,
                            content=source_content,
                            timeout=240,
                            reasoning_effort="minimal",
                            max_output_tokens=10000,
                            repair_simple_location_json=True,
                        )
                        locations = result.get("locations") if isinstance(result, dict) else None
                        if not isinstance(locations, list):
                            raise ValueError("Missing source locations")
                        break
                    except ValueError:
                        if source_attempt:
                            raise
                        app.logger.warning("Retrying incomplete source localization for image %s", image_index)
            except (OSError, requests.RequestException, ValueError, TypeError):
                failed_pages += 1
                app.logger.exception("Study-note source localization v3 failed for image %s", image_index)
                continue

            page_targets = {item["location_id"]: item for item in localization_input}
            coarse_by_id: Dict[str, Dict[str, int]] = {}
            for item in locations:
                if not isinstance(item, dict):
                    continue
                location_id = str(item.get("location_id") or "")
                if location_id not in page_targets or location_id in coarse_by_id:
                    continue
                try:
                    left = int(item.get("left"))
                    top = int(item.get("top"))
                    right = int(item.get("right"))
                    bottom = int(item.get("bottom"))
                    start_x = int(item.get("start_x"))
                    start_y = int(item.get("start_y"))
                    end_x = int(item.get("end_x"))
                    end_y = int(item.get("end_y"))
                    confidence = int(item.get("confidence"))
                except (TypeError, ValueError):
                    continue
                if confidence < 60 or not all(
                    0 <= value <= 1000
                    for value in (left, top, right, bottom, start_x, start_y, end_x, end_y)
                ):
                    continue
                coarse = {
                    "left": max(0, min(left, start_x - 12, end_x - 12)),
                    "top": max(0, min(top, start_y - 12, end_y - 12)),
                    "right": min(1000, max(right, start_x + 12, end_x + 12)),
                    "bottom": min(1000, max(bottom, start_y + 12, end_y + 12)),
                    "confidence": confidence,
                    "version": 3,
                }
                if _validated_study_source_bbox(coarse) is None:
                    continue
                coarse_by_id[location_id] = {
                    **coarse,
                    "start_x": start_x,
                    "start_y": start_y,
                    "end_x": end_x,
                    "end_y": end_y,
                }

            crop_specs: Dict[str, Dict[str, Any]] = {}
            refinement_prompt = (
                "你是手寫筆記的逐行轉錄員。第一張圖是完整局部裁切，只用來理解前後文；後續 BAND 圖由"
                "影像演算法將同一裁切中的實體書寫列切開並固定編號。你必須為每個 BAND 恰好輸出一筆，"
                "line_id 就是藍色 B 編號的數字，順序不可交換、遺漏或重複。text 只能轉錄該 BAND 圖內"
                "實際看得到的文字或公式，不得把相鄰 BAND 的內容移入、摘要、修正或依上下文補寫；若只有"
                "分隔線、零碎筆畫或沒有可辨識文字，text 輸出空字串且 confidence 為 0。left/right 相對於"
                "原始裁切的筆記寬度，不包含 BAND 圖左側藍色標籤欄，最左為 0、最右為 1000。只輸出 schema JSON。"
            )
            for location_id, coarse in coarse_by_id.items():
                coarse_width = coarse["right"] - coarse["left"]
                coarse_height = coarse["bottom"] - coarse["top"]
                padding_x = max(90, min(180, round(coarse_width * 0.28)))
                padding_y = max(90, min(190, round(coarse_height * 0.65)))
                target = page_targets[location_id]
                if len(str(target.get("evidence") or "")) > 160:
                    padding_x = max(padding_x, 150)
                    padding_y = max(padding_y, 150)
                crop_left = max(0, math.floor(pixel_width * max(0, coarse["left"] - padding_x) / 1000))
                crop_top = max(0, math.floor(pixel_height * max(0, coarse["top"] - padding_y) / 1000))
                crop_right = min(
                    pixel_width,
                    math.ceil(pixel_width * min(1000, coarse["right"] + padding_x) / 1000),
                )
                crop_bottom = min(
                    pixel_height,
                    math.ceil(pixel_height * min(1000, coarse["bottom"] + padding_y) / 1000),
                )
                if crop_right - crop_left < 20 or crop_bottom - crop_top < 20:
                    continue
                crop = clean_page.crop((crop_left, crop_top, crop_right, crop_bottom))
                visual_bands = _study_source_visual_line_bands(crop)
                if not visual_bands or len(visual_bands) > 60:
                    continue
                band_sheet_urls = _study_source_band_sheet_data_urls(crop, visual_bands)
                if not band_sheet_urls:
                    continue
                crop_specs[location_id] = {
                    "left": round(crop_left * 1000 / pixel_width),
                    "top": round(crop_top * 1000 / pixel_height),
                    "right": round(crop_right * 1000 / pixel_width),
                    "bottom": round(crop_bottom * 1000 / pixel_height),
                    "image": crop,
                    "clean_url": _study_image_data_url(crop),
                    "visual_bands": visual_bands,
                    "band_sheet_urls": band_sheet_urls,
                }

            refined_by_id: Dict[str, Dict[str, int]] = {}
            if crop_specs:
                refinement_schema = {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["lines"],
                    "properties": {
                        "lines": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 60,
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["line_id", "text", "left", "right", "confidence"],
                                "properties": {
                                    "line_id": {"type": "integer", "minimum": 1},
                                    "text": {"type": "string", "maxLength": 260},
                                    "left": {"type": "integer", "minimum": 0, "maximum": 1000},
                                    "right": {"type": "integer", "minimum": 0, "maximum": 1000},
                                    "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
                                },
                            },
                        }
                    },
                }
                parent_cancel_event = getattr(study_upload_context, "cancel_event", None)

                def refine_one_source(location_id: str) -> Tuple[str, Optional[Dict[str, int]]]:
                    if isinstance(parent_cancel_event, threading.Event):
                        study_upload_context.cancel_event = parent_cancel_event
                    crop_spec = crop_specs[location_id]
                    try:
                        visual_bands = crop_spec["visual_bands"]
                        refinement_content: List[Dict[str, Any]] = [
                            {"type": "input_text", "text": refinement_prompt},
                            {
                                "type": "input_text",
                                "text": (
                                    f"TARGET {location_id} 共有 {len(visual_bands)} 個 BAND。"
                                    "先看完整裁切理解內容，再逐張轉錄 BAND 圖。"
                                ),
                            },
                            {"type": "input_image", "image_url": crop_spec["clean_url"], "detail": "high"},
                        ]
                        for sheet_index, sheet_url in enumerate(crop_spec["band_sheet_urls"], start=1):
                            first_band = (sheet_index - 1) * 8 + 1
                            last_band = min(len(visual_bands), first_band + 7)
                            refinement_content.extend(
                                [
                                    {
                                        "type": "input_text",
                                        "text": f"BAND B{first_band:02d} 至 B{last_band:02d}：",
                                    },
                                    {"type": "input_image", "image_url": sheet_url, "detail": "high"},
                                ]
                            )
                        refinement_result = _call_openai_json(
                            name="study_recall_source_bands_v7",
                            schema=refinement_schema,
                            content=refinement_content,
                            timeout=180,
                            reasoning_effort="minimal",
                            max_output_tokens=12000,
                        )
                        raw_lines = refinement_result.get("lines") if isinstance(refinement_result, dict) else None
                        if not isinstance(raw_lines, list):
                            raise ValueError("Missing source text lines")
                        valid_lines: List[Dict[str, Any]] = []
                        seen_band_ids: Set[int] = set()
                        for line in raw_lines:
                            if not isinstance(line, dict):
                                continue
                            try:
                                line_id = int(line.get("line_id"))
                                if not (1 <= line_id <= len(visual_bands)) or line_id in seen_band_ids:
                                    continue
                                band_top, band_bottom = visual_bands[line_id - 1]
                                normalized_line = {
                                    "line_id": line_id,
                                    "text": str(line.get("text") or "").strip(),
                                    "left": int(line.get("left")),
                                    "top": band_top,
                                    "right": int(line.get("right")),
                                    "bottom": band_bottom,
                                    "confidence": int(line.get("confidence")),
                                }
                            except (TypeError, ValueError):
                                continue
                            seen_band_ids.add(line_id)
                            if (
                                normalized_line["text"]
                                and normalized_line["confidence"] >= 45
                                and 0 <= normalized_line["left"] < normalized_line["right"] <= 1000
                                and 0 <= normalized_line["top"] < normalized_line["bottom"] <= 1000
                            ):
                                valid_lines.append(normalized_line)
                        if seen_band_ids != set(range(1, len(visual_bands) + 1)):
                            raise ValueError("Incomplete or duplicate source bands")
                        valid_lines.sort(key=lambda line: line["line_id"])
                        selected_lines, match_metrics = _match_study_source_evidence_to_lines(
                            str(page_targets[location_id].get("evidence") or ""),
                            valid_lines,
                        )
                        if not selected_lines or not source_line_match_is_verified(
                            str(page_targets[location_id].get("evidence") or ""),
                            match_metrics,
                        ):
                            return location_id, None
                        selected_ids = {id(line) for line in selected_lines}
                        selected_indices = [
                            index for index, line in enumerate(valid_lines) if id(line) in selected_ids
                        ]
                        first_index = min(selected_indices)
                        last_index = max(selected_indices)
                        selected_left = min(line["left"] for line in selected_lines)
                        selected_top = min(line["top"] for line in selected_lines)
                        selected_right = max(line["right"] for line in selected_lines)
                        selected_bottom = max(line["bottom"] for line in selected_lines)
                        visual_alignment = {
                            id(line): (line["top"], line["bottom"])
                            for line in valid_lines
                        }
                        using_visual_alignment = True
                        typical_line_height = median(
                            line["bottom"] - line["top"] for line in selected_lines
                        )
                        vertical_padding = max(18, min(52, round(typical_line_height * 0.58)))
                        horizontal_padding = max(18, min(38, round(typical_line_height * 0.34)))

                        def overlaps_selected_width(line: Dict[str, Any]) -> bool:
                            overlap = max(
                                0,
                                min(selected_right, line["right"]) - max(selected_left, line["left"]),
                            )
                            narrower_width = max(
                                1,
                                min(selected_right - selected_left, line["right"] - line["left"]),
                            )
                            return overlap / narrower_width >= 0.18

                        local_top = max(0, selected_top - vertical_padding)
                        local_bottom = min(1000, selected_bottom + vertical_padding)
                        if first_index > 0:
                            previous_line = valid_lines[first_index - 1]
                            previous_bottom = (
                                visual_alignment[id(previous_line)][1]
                                if using_visual_alignment and id(previous_line) in visual_alignment
                                else previous_line["bottom"]
                            )
                            if (
                                overlaps_selected_width(previous_line)
                                and previous_bottom <= selected_top
                            ):
                                local_top = max(
                                    local_top,
                                    round((previous_bottom + selected_top) / 2),
                                )
                        if last_index + 1 < len(valid_lines):
                            next_line = valid_lines[last_index + 1]
                            next_top = (
                                visual_alignment[id(next_line)][0]
                                if using_visual_alignment and id(next_line) in visual_alignment
                                else next_line["top"]
                            )
                            if (
                                overlaps_selected_width(next_line)
                                and next_top >= selected_bottom
                            ):
                                local_bottom = min(
                                    local_bottom,
                                    round((selected_bottom + next_top) / 2),
                                )
                        local_left = max(0, selected_left - horizontal_padding)
                        local_right = min(1000, selected_right + horizontal_padding)
                        first_line = selected_lines[0]
                        last_line = selected_lines[-1]
                        local_start_x = round((first_line["left"] + first_line["right"]) / 2)
                        local_start_y_candidate = (
                            round(sum(visual_alignment[id(first_line)]) / 2)
                            if using_visual_alignment
                            else round((first_line["top"] + first_line["bottom"]) / 2)
                        )
                        local_start_y = max(selected_top, min(selected_bottom, local_start_y_candidate))
                        local_end_x = round((last_line["left"] + last_line["right"]) / 2)
                        local_end_y_candidate = (
                            round(sum(visual_alignment[id(last_line)]) / 2)
                            if using_visual_alignment
                            else round((last_line["top"] + last_line["bottom"]) / 2)
                        )
                        local_end_y = max(selected_top, min(selected_bottom, local_end_y_candidate))
                        verification_confidence = round(
                            (
                                float(match_metrics.get("score") or 0.0) * 0.30
                                + float(match_metrics.get("coverage") or 0.0) * 0.42
                                + float(match_metrics.get("boundary_coverage") or 0.0) * 0.28
                            )
                            * 100
                        )
                        refined_confidence = min(
                            coarse_by_id[location_id]["confidence"],
                            round(sum(line["confidence"] for line in selected_lines) / len(selected_lines)),
                            verification_confidence,
                        )
                        if refined_confidence < 60:
                            return location_id, None
                        crop_width = crop_spec["right"] - crop_spec["left"]
                        crop_height = crop_spec["bottom"] - crop_spec["top"]

                        def full_x(value: int) -> int:
                            return crop_spec["left"] + round(value * crop_width / 1000)

                        def full_y(value: int) -> int:
                            return crop_spec["top"] + round(value * crop_height / 1000)

                        refined_left = full_x(local_left)
                        refined_top = full_y(local_top)
                        refined_right = full_x(local_right)
                        refined_bottom = full_y(local_bottom)
                        return location_id, {
                            "left": max(0, refined_left),
                            "top": max(0, refined_top),
                            "right": min(1000, refined_right),
                            "bottom": min(1000, refined_bottom),
                            "start_x": full_x(local_start_x),
                            "start_y": full_y(local_start_y),
                            "end_x": full_x(local_end_x),
                            "end_y": full_y(local_end_y),
                            "confidence": refined_confidence,
                            "version": SOURCE_BBOX_VERSION,
                            "text_verified": True,
                            "match_score": round(float(match_metrics.get("score") or 0.0), 4),
                            "match_coverage": round(float(match_metrics.get("coverage") or 0.0), 4),
                            "boundary_coverage": round(
                                float(match_metrics.get("boundary_coverage") or 0.0),
                                4,
                            ),
                            "evidence_length": len(
                                _canonical_study_source_match_text(
                                    page_targets[location_id].get("evidence")
                                )
                            ),
                        }
                    except _StudyUploadCancelled:
                        raise
                    except (requests.RequestException, ValueError, TypeError, IndexError):
                        app.logger.exception(
                            "Study-note source line matching v5 failed for image %s target %s",
                            image_index,
                            location_id,
                        )
                        return location_id, None
                    finally:
                        if hasattr(study_upload_context, "cancel_event"):
                            del study_upload_context.cancel_event

                executor = ThreadPoolExecutor(
                    max_workers=min(4, len(crop_specs)),
                    thread_name_prefix="study-source-locator",
                )
                try:
                    futures = [executor.submit(refine_one_source, location_id) for location_id in crop_specs]
                    for future in as_completed(futures):
                        _raise_if_study_upload_cancelled()
                        location_id, refined = future.result()
                        if refined is not None:
                            refined_by_id[location_id] = refined
                except _StudyUploadCancelled:
                    for future in futures:
                        future.cancel()
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise
                else:
                    executor.shutdown(wait=True)

            seen_page: Set[str] = set()
            for location_id in coarse_by_id:
                model_location = refined_by_id.get(location_id)
                if model_location is None:
                    continue
                start_x = int(model_location["start_x"])
                start_y = int(model_location["start_y"])
                end_x = int(model_location["end_x"])
                end_y = int(model_location["end_y"])
                confidence = int(model_location["confidence"])
                location_version = int(model_location.get("version") or 3)
                candidate_seed = {
                    "left": max(0, min(int(model_location["left"]), start_x - 12, end_x - 12)),
                    "top": max(0, min(int(model_location["top"]), start_y - 12, end_y - 12)),
                    "right": min(1000, max(int(model_location["right"]), start_x + 12, end_x + 12)),
                    "bottom": min(1000, max(int(model_location["bottom"]), start_y + 12, end_y + 12)),
                    "confidence": confidence,
                    "version": location_version,
                    "text_verified": bool(model_location.get("text_verified")),
                    "match_score": model_location.get("match_score"),
                    "match_coverage": model_location.get("match_coverage"),
                    "boundary_coverage": model_location.get("boundary_coverage"),
                    "evidence_length": model_location.get("evidence_length"),
                }
                if _validated_study_source_bbox(candidate_seed) is None:
                    continue
                candidate = candidate_seed
                if _validated_study_source_bbox(candidate) is None:
                    continue
                requests_by_id[location_id]["bbox"] = candidate
                seen_page.add(location_id)
                located += 1

        if failed_pages == len(requests_by_image):
            raise ValueError("Source localization failed for every image")
        return located, total

    return (
        _localize_study_card_sources_band_experiment,
    )
