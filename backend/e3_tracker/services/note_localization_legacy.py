"""Note localization legacy; dependencies are bound per application."""

import base64
import io
import json
import math
import re
from typing import Any, Dict, List, Optional, Set, Tuple
import requests
from PIL import Image, ImageOps


def build_note_localization_legacy(*,
    _call_openai_json,
    _expand_study_source_bbox_through_edge_ink,
    _literal_study_source_evidence,
    _raise_if_study_upload_cancelled,
    _snap_study_source_bbox_to_ink,
    _study_coordinate_guide_data_url,
    _study_image_data_url,
    _validated_study_source_bbox,
    app,
):
    def _localize_study_card_sources_legacy(
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
                        "image_index": image_index,
                        "concept": str(concept.get("concept") or "")[:80],
                        "evidence": evidence[:240],
                    }
                )
        total = len(requests_by_id)
        if not total:
            return 0, 0
        located = 0
        seen: Set[str] = set()
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
                            "required": ["location_id", "left", "top", "right", "bottom", "confidence"],
                            "properties": {
                                "location_id": {"type": "string", "maxLength": 12},
                                "left": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "top": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "right": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "bottom": {"type": "integer", "minimum": 0, "maximum": 1000},
                                "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
                            },
                        },
                    },
                },
            }
            filename, image_bytes, mime_type = images[image_index - 1]
            with Image.open(io.BytesIO(image_bytes)) as source_image:
                clean_page = ImageOps.exif_transpose(source_image).convert("RGB")
            pixel_width, pixel_height = clean_page.size
            guide_data_url = _study_coordinate_guide_data_url(clean_page)
            prompt = (
                f"你是手寫筆記的精確視覺定位員。現在只提供 image_index={image_index} 這一張完整原圖，"
                f"原始 bitmap 尺寸為 {pixel_width}×{pixel_height} 像素，檔名為 {filename}。"
                "每個 location_id 都有卡片名稱與來源 evidence；請直接在眼前這張圖逐字比對，不得依段落順序或內容類型猜位置。"
                "先辨認支撐 evidence 的第一個可見字與最後一個可見字，再框出它們實際占用的最小連續區域；"
                "evidence 可能同時包含兩個定義、數條公式或題幹加推導；必須逐項確認 evidence 明確提到的每一部分都落在框內，"
                "不能找到第一個關鍵詞後就停止。決定 bottom 前，務必確認 evidence 最後一個定義或公式完整位於 bottom 上方。"
                "若是例題或推導，框必須涵蓋該卡使用的題幹與必要計算行，但排除相鄰且無關的題目或章節。"
                "座標必須相對於完整 bitmap（包含工具列、黑邊與頁面空白），左上為 (0,0)、右下為 (1000,1000)。"
                "left/top/right/bottom 使用整數，四周只保留約 8 至 15 個座標單位，不能用粗略的半頁或整段區帶。"
                "完整涵蓋來源的優先順序高於框得極小；需要涵蓋多個相鄰項目時可以適度擴張，但仍排除下一個無關段落。"
                "你會依序看到乾淨原圖與完全相同尺寸的紅色座標網格圖；用乾淨圖辨字，用網格上的 X0..X900、Y0..Y900 讀取位置。"
                "禁止使用模型內部縮圖的像素座標，輸出的數字必須直接對齊第二張圖的紅色網格標籤。"
                "若同一 evidence 跨相鄰數行，以一個矩形完整包住；若圖片無法唯一確認位置，confidence 必須低於 60。"
                "每個 location_id 恰好輸出一次且不得改名，只輸出 schema 指定 JSON。\n\n待定位來源：\n"
                + json.dumps(localization_input, ensure_ascii=False, separators=(",", ":"))
            )
            content: List[Dict[str, Any]] = [
                {"type": "input_text", "text": prompt},
                {
                    "type": "input_image",
                    "image_url": f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}",
                    "detail": "high",
                },
                {"type": "input_text", "text": "以下是同一張原圖的紅色 0–1000 座標網格輔助圖。"},
                {"type": "input_image", "image_url": guide_data_url, "detail": "high"},
            ]
            try:
                result = _call_openai_json(
                    name="study_recall_source_locations",
                    schema=location_schema,
                    content=content,
                    timeout=240,
                    reasoning_effort="medium",
                    max_output_tokens=8000,
                )
                locations = result.get("locations") if isinstance(result, dict) else None
                if not isinstance(locations, list):
                    raise ValueError("Missing source locations")
            except (requests.RequestException, ValueError, TypeError):
                failed_pages += 1
                app.logger.exception("Study-note source localization failed for image %s", image_index)
                continue
            coarse_by_id: Dict[str, Dict[str, int]] = {}
            for item in locations:
                if not isinstance(item, dict):
                    continue
                location_id = str(item.get("location_id") or "")
                source_ref = requests_by_id.get(location_id)
                if source_ref is None or location_id in coarse_by_id or location_id in seen:
                    continue
                try:
                    left = int(item.get("left"))
                    top = int(item.get("top"))
                    right = int(item.get("right"))
                    bottom = int(item.get("bottom"))
                    confidence = int(item.get("confidence"))
                except (TypeError, ValueError):
                    continue
                width = right - left
                height = bottom - top
                if confidence < 60 or not (0 <= left < right <= 1000 and 0 <= top < bottom <= 1000):
                    continue
                if width < 12 or height < 8 or width > 960 or height > 850:
                    continue
                coarse_by_id[location_id] = {
                    "left": left,
                    "top": top,
                    "right": right,
                    "bottom": bottom,
                    "confidence": confidence,
                }
            if not coarse_by_id:
                continue

            crop_specs: Dict[str, Dict[str, Any]] = {}
            refine_content: List[Dict[str, Any]] = []
            refinement_targets = {
                item["location_id"]: item
                for item in localization_input
                if item.get("location_id") in coarse_by_id
            }
            refine_prompt = (
                "你是手寫筆記來源框的第二階段精校員。每個 target 會依序提供同一個局部裁切的乾淨圖與紅色 "
                "0–1000 網格圖；座標只相對於該 target 的局部裁切，不是整張原圖。"
                "請逐字比對 concept 與 evidence，框住真正支撐卡片內容的第一個可見字到最後一個可見字。"
                "evidence 若明確包含定義、公式、條件或推導中的多個部分，矩形必須完整包含它們，但排除相鄰無關段落。"
                "先在乾淨圖確認文字，再用網格讀取 left/top/right/bottom；不得沿用或猜測第一階段座標。"
                "框的四周只留約 5 至 12 個局部座標單位，不能裁掉上下標、分數、矩陣、根號或公式末端。"
                "只有在裁切內可唯一辨識完整來源時 found=true；找不到、只有部分內容或有多個無法區分的位置時，"
                "found=false 且 confidence 低於 60。每個 location_id 必須恰好輸出一次且不得改名，只輸出 schema 指定 JSON。"
                "start_x/start_y 是起點錨點可見文字的中心，end_x/end_y 是終點錨點可見文字的中心；"
                "四者也使用局部 0–1000 座標，且 found=true 時兩個錨點都必須落在輸出矩形內。"
            )
            refine_content.append({"type": "input_text", "text": refine_prompt})
            for location_id, coarse in coarse_by_id.items():
                target = refinement_targets[location_id]
                target_evidence_length = len(str(target.get("evidence") or ""))
                coarse_width = coarse["right"] - coarse["left"]
                coarse_height = coarse["bottom"] - coarse["top"]
                padding_x = max(120, min(220, round(coarse_width / 3)))
                padding_y = max(140, min(240, coarse_height * 2))
                if target_evidence_length > 160:
                    padding_x = max(padding_x, 320)
                    padding_y = max(padding_y, 220)
                normalized_left = max(0, coarse["left"] - padding_x)
                normalized_top = max(0, coarse["top"] - padding_y)
                normalized_right = min(1000, coarse["right"] + padding_x)
                normalized_bottom = min(1000, coarse["bottom"] + padding_y)
                pixel_left = max(0, min(pixel_width - 1, math.floor(pixel_width * normalized_left / 1000)))
                pixel_top = max(0, min(pixel_height - 1, math.floor(pixel_height * normalized_top / 1000)))
                pixel_right = max(pixel_left + 1, min(pixel_width, math.ceil(pixel_width * normalized_right / 1000)))
                pixel_bottom = max(pixel_top + 1, min(pixel_height, math.ceil(pixel_height * normalized_bottom / 1000)))
                crop = clean_page.crop((pixel_left, pixel_top, pixel_right, pixel_bottom))
                crop_bounds = {
                    "left": round(pixel_left * 1000 / pixel_width),
                    "top": round(pixel_top * 1000 / pixel_height),
                    "right": round(pixel_right * 1000 / pixel_width),
                    "bottom": round(pixel_bottom * 1000 / pixel_height),
                }
                target_evidence = " ".join(str(target.get("evidence") or "").split())
                start_anchor = target_evidence[:28]
                end_anchor = target_evidence[-28:]
                target_label = (
                    f"TARGET {location_id}｜卡片：{target.get('concept') or ''}｜"
                    f"來源：{target_evidence}｜必須框入的起點錨點：{start_anchor}｜"
                    f"必須框入的終點錨點：{end_anchor}。輸出前逐字確認兩個錨點都在矩形內。"
                )
                clean_crop_url = _study_image_data_url(crop)
                guide_crop_url = _study_coordinate_guide_data_url(crop)
                crop_specs[location_id] = {
                    "bounds": crop_bounds,
                    "target_label": target_label,
                    "clean_url": clean_crop_url,
                    "guide_url": guide_crop_url,
                }
                refine_content.extend(
                    [
                        {"type": "input_text", "text": target_label},
                        {"type": "input_image", "image_url": clean_crop_url, "detail": "high"},
                        {
                            "type": "input_text",
                            "text": f"TARGET {location_id} 的同一裁切，以下為局部 0–1000 座標網格。",
                        },
                        {"type": "input_image", "image_url": guide_crop_url, "detail": "high"},
                    ]
                )

            refine_item_count = len(coarse_by_id)
            refinement_schema = {
                "type": "object",
                "additionalProperties": False,
                "required": ["locations"],
                "properties": {
                    "locations": {
                        "type": "array",
                        "minItems": refine_item_count,
                        "maxItems": refine_item_count,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "location_id",
                                "found",
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
                                "found": {"type": "boolean"},
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
            group_refinement_failed = False
            try:
                if refine_item_count > 1:
                    group_refinement_failed = True
                    refined_locations = []
                else:
                    refinement_result = _call_openai_json(
                        name="study_recall_source_locations_refined",
                        schema=refinement_schema,
                        content=refine_content,
                        timeout=240,
                        reasoning_effort="minimal",
                        max_output_tokens=4000,
                        repair_simple_location_json=True,
                    )
                    refined_locations = (
                        refinement_result.get("locations") if isinstance(refinement_result, dict) else None
                    )
                if not isinstance(refined_locations, list):
                    raise ValueError("Missing refined source locations")
            except ValueError as exc:
                app.logger.warning("Study-note grouped source refinement was incomplete for image %s: %s", image_index, exc)
                group_refinement_failed = True
                refined_locations = []
            except (requests.RequestException, TypeError):
                app.logger.exception("Study-note source refinement failed for image %s", image_index)
                group_refinement_failed = True
                refined_locations = []

            refined_by_id: Dict[str, Dict[str, Any]] = {}
            for item in refined_locations:
                if not isinstance(item, dict):
                    continue
                location_id = str(item.get("location_id") or "")
                if location_id not in coarse_by_id or location_id in refined_by_id:
                    continue
                refined_by_id[location_id] = item

            def refinement_is_usable(location_id: str, item: Any) -> bool:
                if not isinstance(item, dict) or item.get("found") is not True:
                    return False
                try:
                    local_left = int(item.get("left"))
                    local_top = int(item.get("top"))
                    local_right = int(item.get("right"))
                    local_bottom = int(item.get("bottom"))
                    start_x = int(item.get("start_x"))
                    start_y = int(item.get("start_y"))
                    end_x = int(item.get("end_x"))
                    end_y = int(item.get("end_y"))
                    confidence = int(item.get("confidence"))
                except (TypeError, ValueError):
                    return False
                if not all(0 <= value <= 1000 for value in (start_x, start_y, end_x, end_y)):
                    return False
                local_left = max(0, min(local_left, start_x - 12, end_x - 12))
                local_top = max(0, min(local_top, start_y - 12, end_y - 12))
                local_right = min(1000, max(local_right, start_x + 12, end_x + 12))
                local_bottom = min(1000, max(local_bottom, start_y + 12, end_y + 12))
                width = local_right - local_left
                height = local_bottom - local_top
                if not (
                    confidence >= 70
                    and 0 <= local_left < local_right <= 1000
                    and 0 <= local_top < local_bottom <= 1000
                    and 12 <= width <= 980
                    and 8 <= height <= 920
                ):
                    return False
                crop_bounds = crop_specs[location_id]["bounds"]
                crop_width = crop_bounds["right"] - crop_bounds["left"]
                crop_height = crop_bounds["bottom"] - crop_bounds["top"]
                refined_center_x = crop_bounds["left"] + (local_left + local_right) * crop_width / 2000
                refined_center_y = crop_bounds["top"] + (local_top + local_bottom) * crop_height / 2000
                coarse = coarse_by_id[location_id]
                coarse_center_x = (coarse["left"] + coarse["right"]) / 2
                coarse_center_y = (coarse["top"] + coarse["bottom"]) / 2
                max_center_shift_x = max(140, (coarse["right"] - coarse["left"]) * 0.45)
                max_center_shift_y = max(140, (coarse["bottom"] - coarse["top"]) * 0.45)
                refined_width = width * crop_width / 1000
                refined_height = height * crop_height / 1000
                coarse_width = coarse["right"] - coarse["left"]
                coarse_height = coarse["bottom"] - coarse["top"]
                evidence_length = len(str(refinement_targets[location_id].get("evidence") or ""))
                refined_area = refined_width * refined_height
                refined_aspect_ratio = refined_width / max(1, refined_height)
                if (
                    evidence_length > 160
                    and refined_width < coarse_width * 0.75
                    and refined_height < coarse_height * 0.55
                ):
                    return False
                if evidence_length > 180 and refined_area < 60_000 and refined_aspect_ratio < 8:
                    return False
                return (
                    abs(refined_center_x - coarse_center_x) <= max_center_shift_x
                    and abs(refined_center_y - coarse_center_y) <= max_center_shift_y
                )

            retry_ids = [
                location_id
                for location_id in coarse_by_id
                if not refinement_is_usable(location_id, refined_by_id.get(location_id))
            ]
            if not group_refinement_failed:
                retry_ids = retry_ids[:3]
            if retry_ids:
                single_refinement_schema = json.loads(json.dumps(refinement_schema))
                single_locations_schema = single_refinement_schema["properties"]["locations"]
                single_locations_schema["minItems"] = 1
                single_locations_schema["maxItems"] = 1
                for location_id in retry_ids:
                    _raise_if_study_upload_cancelled()
                    crop_spec = crop_specs[location_id]
                    prior_item: Optional[Dict[str, Any]] = None
                    for retry_attempt in range(2):
                        _raise_if_study_upload_cancelled()
                        second_attempt_note = ""
                        if retry_attempt and prior_item:
                            second_attempt_note = (
                                "上一個候選框未通過完整性或位置一致性檢查。請重新檢查 evidence 的最後一行，"
                                "必要時擴大框；不要重複上一組座標："
                                + json.dumps(prior_item, ensure_ascii=False, separators=(",", ":"))
                            )
                        retry_content = [
                            {
                                "type": "input_text",
                                "text": (
                                    refine_prompt
                                    + "現在只處理下列唯一 target。請先逐項核對 evidence 的起點、每條公式與終點；"
                                    "框內必須完整包含 evidence 的所有可見內容，但不能納入 evidence 結束後的下一個定義、例題或段落。"
                                    "請從乾淨裁切逐字找到來源，再用網格獨立讀取局部座標。"
                                    + second_attempt_note
                                ),
                            },
                            {"type": "input_text", "text": crop_spec["target_label"]},
                            {"type": "input_image", "image_url": crop_spec["clean_url"], "detail": "high"},
                            {
                                "type": "input_text",
                                "text": f"TARGET {location_id} 的同一裁切，以下為局部 0–1000 座標網格。",
                            },
                            {"type": "input_image", "image_url": crop_spec["guide_url"], "detail": "high"},
                        ]
                        try:
                            retry_result = _call_openai_json(
                                name="study_recall_source_location_retry",
                                schema=single_refinement_schema,
                                content=retry_content,
                                timeout=180,
                                reasoning_effort="low",
                                max_output_tokens=8000,
                                repair_simple_location_json=True,
                            )
                            retry_locations = retry_result.get("locations") if isinstance(retry_result, dict) else None
                            retry_item = retry_locations[0] if isinstance(retry_locations, list) and retry_locations else None
                            prior_item = retry_item if isinstance(retry_item, dict) else None
                            if (
                                isinstance(retry_item, dict)
                                and str(retry_item.get("location_id") or "") == location_id
                                and refinement_is_usable(location_id, retry_item)
                            ):
                                refined_by_id[location_id] = retry_item
                                break
                        except (requests.RequestException, ValueError, TypeError, IndexError):
                            app.logger.exception(
                                "Study-note individual source refinement failed for image %s target %s",
                                image_index,
                                location_id,
                            )
            for location_id, coarse in coarse_by_id.items():
                item = refined_by_id.get(location_id)
                if not refinement_is_usable(location_id, item):
                    continue
                try:
                    local_left = int(item.get("left"))
                    local_top = int(item.get("top"))
                    local_right = int(item.get("right"))
                    local_bottom = int(item.get("bottom"))
                    start_x = int(item.get("start_x"))
                    start_y = int(item.get("start_y"))
                    end_x = int(item.get("end_x"))
                    end_y = int(item.get("end_y"))
                    refined_confidence = int(item.get("confidence"))
                except (TypeError, ValueError):
                    continue
                local_left = max(0, min(local_left, start_x - 12, end_x - 12))
                local_top = max(0, min(local_top, start_y - 12, end_y - 12))
                local_right = min(1000, max(local_right, start_x + 12, end_x + 12))
                local_bottom = min(1000, max(local_bottom, start_y + 12, end_y + 12))
                local_width = local_right - local_left
                local_height = local_bottom - local_top
                if refined_confidence < 70:
                    continue
                if not (0 <= local_left < local_right <= 1000 and 0 <= local_top < local_bottom <= 1000):
                    continue
                if local_width < 12 or local_height < 8 or local_width > 980 or local_height > 920:
                    continue
                crop_bounds = crop_specs[location_id]["bounds"]
                crop_width = crop_bounds["right"] - crop_bounds["left"]
                crop_height = crop_bounds["bottom"] - crop_bounds["top"]
                candidate = {
                    "left": crop_bounds["left"] + round(local_left * crop_width / 1000),
                    "top": crop_bounds["top"] + round(local_top * crop_height / 1000),
                    "right": crop_bounds["left"] + round(local_right * crop_width / 1000),
                    "bottom": crop_bounds["top"] + round(local_bottom * crop_height / 1000),
                    "confidence": min(coarse["confidence"], refined_confidence),
                    "version": 2,
                }
                candidate = _snap_study_source_bbox_to_ink(image_bytes, candidate)
                end_x_full = crop_bounds["left"] + round(end_x * crop_width / 1000)
                end_y_full = crop_bounds["top"] + round(end_y * crop_height / 1000)
                candidate = _expand_study_source_bbox_through_edge_ink(
                    image_bytes,
                    candidate,
                    end_x=end_x_full,
                    end_y=end_y_full,
                    evidence_length=len(str(refinement_targets[location_id].get("evidence") or "")),
                )
                if _validated_study_source_bbox(candidate) is None:
                    continue
                requests_by_id[location_id]["bbox"] = candidate
                seen.add(location_id)
                located += 1

            def canonical_evidence(value: Any) -> str:
                return re.sub(r"\s+", "", str(value or "")).casefold()

            for location_id in coarse_by_id:
                if location_id not in seen:
                    continue
                target_evidence = canonical_evidence(refinement_targets[location_id].get("evidence"))
                target_bbox = _validated_study_source_bbox(requests_by_id[location_id].get("bbox"))
                if not target_evidence or not target_bbox:
                    continue
                containing_boxes = []
                for candidate_id in coarse_by_id:
                    if candidate_id == location_id or candidate_id not in seen:
                        continue
                    candidate_evidence = canonical_evidence(refinement_targets[candidate_id].get("evidence"))
                    candidate_bbox = _validated_study_source_bbox(requests_by_id[candidate_id].get("bbox"))
                    if (
                        not candidate_bbox
                        or len(candidate_evidence) <= len(target_evidence) + 5
                        or target_evidence not in candidate_evidence
                    ):
                        continue
                    overlap_width = max(
                        0,
                        min(target_bbox["right"], candidate_bbox["right"])
                        - max(target_bbox["left"], candidate_bbox["left"]),
                    )
                    overlap_height = max(
                        0,
                        min(target_bbox["bottom"], candidate_bbox["bottom"])
                        - max(target_bbox["top"], candidate_bbox["top"]),
                    )
                    target_area = (
                        (target_bbox["right"] - target_bbox["left"])
                        * (target_bbox["bottom"] - target_bbox["top"])
                    )
                    overlap_ratio = overlap_width * overlap_height / max(1, target_area)
                    if overlap_ratio >= 0.2:
                        continue
                    candidate_area = (
                        (candidate_bbox["right"] - candidate_bbox["left"])
                        * (candidate_bbox["bottom"] - candidate_bbox["top"])
                    )
                    containing_boxes.append((candidate_area, candidate_bbox))
                if containing_boxes:
                    _, containing_bbox = min(containing_boxes, key=lambda value: value[0])
                    requests_by_id[location_id]["bbox"] = {
                        **containing_bbox,
                        "confidence": max(60, containing_bbox["confidence"] - 5),
                        "version": 2,
                    }

            for location_id in coarse_by_id:
                if location_id in seen:
                    continue
                target_evidence = canonical_evidence(refinement_targets[location_id].get("evidence"))
                if not target_evidence:
                    continue
                containing_boxes = []
                for candidate_id in coarse_by_id:
                    if candidate_id == location_id or candidate_id not in seen:
                        continue
                    candidate_evidence = canonical_evidence(refinement_targets[candidate_id].get("evidence"))
                    candidate_bbox = _validated_study_source_bbox(requests_by_id[candidate_id].get("bbox"))
                    if (
                        not candidate_bbox
                        or len(candidate_evidence) <= len(target_evidence) + 5
                        or target_evidence not in candidate_evidence
                    ):
                        continue
                    area = (
                        (candidate_bbox["right"] - candidate_bbox["left"])
                        * (candidate_bbox["bottom"] - candidate_bbox["top"])
                    )
                    containing_boxes.append((area, candidate_bbox))
                if not containing_boxes:
                    continue
                _, containing_bbox = min(containing_boxes, key=lambda value: value[0])
                requests_by_id[location_id]["bbox"] = {
                    **containing_bbox,
                    "confidence": max(60, containing_bbox["confidence"] - 5),
                    "version": 2,
                }
                seen.add(location_id)
                located += 1
        if failed_pages == len(requests_by_image):
            raise ValueError("Source localization failed for every image")
        return located, total

    return (
        _localize_study_card_sources_legacy,
    )
