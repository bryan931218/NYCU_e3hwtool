"""Note localization consensus; dependencies are bound per application."""

import io
import math
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from statistics import median
from typing import Any, Dict, List, Optional, Tuple
import requests
from PIL import Image, ImageOps
from ..shared.source_localization import SOURCE_BBOX_VERSION, estimated_source_line_count, source_bbox_span_is_plausible, source_line_match_is_verified
from ..shared.utils import json_safe


def build_note_localization_consensus(*,
    _StudyUploadCancelled,
    _call_openai_json,
    _canonical_study_source_match_text,
    _env_flag_truthy,
    _literal_study_source_evidence,
    _match_study_source_evidence_to_lines,
    _raise_if_study_upload_cancelled,
    _study_coordinate_guide_data_url,
    _study_image_data_url,
    _validated_study_source_bbox,
    app,
    study_upload_context,
):
    def _localize_study_card_sources_model_consensus_legacy(
        images: List[Tuple[str, bytes, str]],
        key_concepts: List[Dict[str, Any]],
    ) -> Tuple[int, int]:
        targets_by_image: Dict[int, List[Dict[str, Any]]] = {}
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
                targets_by_image.setdefault(image_index, []).append(
                    {
                        "location_id": f"c{concept_index}r{source_ref_index}",
                        "concept": str(concept.get("concept") or "")[:100],
                        "card_context": " ".join(
                            str(concept.get(field) or "").strip()
                            for field in (
                                "core_summary",
                                "explanation",
                                "simple_example",
                                "example_problem",
                                "example_method",
                                "memory_hint",
                            )
                            if str(concept.get(field) or "").strip()
                        )[:900],
                        "evidence": evidence[:700],
                        "source_ref": source_ref,
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

        location_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "found",
                "visible_excerpt",
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
                "found": {"type": "boolean"},
                "visible_excerpt": {"type": "string", "maxLength": 900},
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
        }
        crop_transcription_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["visible_text"],
            "properties": {
                "visible_text": {"type": "string", "maxLength": 2500},
            },
        }

        def agreement_between(
            first: Dict[str, Any],
            second: Dict[str, Any],
        ) -> Optional[Tuple[float, Dict[str, Any]]]:
            intersection_width = max(
                0,
                min(first["right"], second["right"]) - max(first["left"], second["left"]),
            )
            intersection_height = max(
                0,
                min(first["bottom"], second["bottom"]) - max(first["top"], second["top"]),
            )
            first_width = first["right"] - first["left"]
            first_height = first["bottom"] - first["top"]
            second_width = second["right"] - second["left"]
            second_height = second["bottom"] - second["top"]
            intersection_area = intersection_width * intersection_height
            smaller_area = min(first_width * first_height, second_width * second_height)
            smaller_overlap = intersection_area / max(1, smaller_area)
            horizontal_overlap = intersection_width / max(1, min(first_width, second_width))
            vertical_overlap = intersection_height / max(1, min(first_height, second_height))
            first_center = (
                (first["left"] + first["right"]) / 2,
                (first["top"] + first["bottom"]) / 2,
            )
            second_center = (
                (second["left"] + second["right"]) / 2,
                (second["top"] + second["bottom"]) / 2,
            )
            center_distance = math.hypot(
                (first_center[0] - second_center[0]) / max(80, first_width, second_width),
                (first_center[1] - second_center[1]) / max(55, first_height, second_height),
            )
            center_score = max(0.0, 1.0 - center_distance)
            anchor_y_tolerance = max(38, round(max(first_height, second_height) * 0.28))
            anchor_x_tolerance = max(70, round(max(first_width, second_width) * 0.38))
            anchor_differences = (
                abs(first["start_x"] - second["start_x"]),
                abs(first["start_y"] - second["start_y"]),
                abs(first["end_x"] - second["end_x"]),
                abs(first["end_y"] - second["end_y"]),
            )
            if (
                anchor_differences[0] > anchor_x_tolerance
                or anchor_differences[2] > anchor_x_tolerance
                or anchor_differences[1] > anchor_y_tolerance
                or anchor_differences[3] > anchor_y_tolerance
            ):
                return None
            anchor_score = max(
                0.0,
                1.0
                - (
                    anchor_differences[1] + anchor_differences[3]
                )
                / max(1, anchor_y_tolerance * 2),
            )
            agreement = (
                smaller_overlap * 0.38
                + vertical_overlap * 0.20
                + horizontal_overlap * 0.13
                + center_score * 0.11
                + anchor_score * 0.18
            )
            if (
                smaller_overlap < 0.52
                or vertical_overlap < 0.62
                or horizontal_overlap < 0.42
                or agreement < 0.64
            ):
                return None
            first_area = first_width * first_height
            second_area = second_width * second_height
            tighter = first if first_area <= second_area else second
            return agreement, tighter

        located = 0
        failed_pages = 0
        parent_cancel_event = getattr(study_upload_context, "cancel_event", None)
        for image_index, page_targets in sorted(targets_by_image.items()):
            _raise_if_study_upload_cancelled()
            filename, image_bytes, _mime_type = images[image_index - 1]
            try:
                with Image.open(io.BytesIO(image_bytes)) as opened:
                    clean_page = ImageOps.exif_transpose(opened).convert("RGB")
                clean_data_url = _study_image_data_url(clean_page)
                guide_data_url = _study_coordinate_guide_data_url(clean_page)
                page_width, page_height = clean_page.size
            except (OSError, ValueError, TypeError):
                failed_pages += 1
                app.logger.exception("Unable to prepare source image %s", image_index)
                continue

            def locate_one_target(
                target: Dict[str, Any],
            ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], bool]:
                if isinstance(parent_cancel_event, threading.Event):
                    study_upload_context.cancel_event = parent_cancel_event
                successful_response = False
                candidates: List[Dict[str, Any]] = []
                try:
                    for pass_index in range(2):
                        _raise_if_study_upload_cancelled()
                        prompt = (
                            f"你是手寫筆記的單一來源定位員。這次只定位一個項目，不得搜尋或輸出其他重點。"
                            f"圖片是第 {image_index} 張 {filename}，canonical bitmap 為 {page_width}×{page_height}。"
                            f"卡片標題：{target['concept']}。待找原文 evidence：{target['evidence']}。"
                            "先逐字確認 evidence 的開頭、公式關係與結尾都真的出現在同一個連續區塊；只看到相同關鍵詞、"
                            "相鄰例題或語意相關內容都不算。start_x/start_y 是 evidence 第一個可見字元或公式的中心，"
                            "end_x/end_y 是最後一個可見字元或公式的中心，兩者都不是矩形角落。visible_excerpt 必須逐字轉錄矩形內實際看見、且與 evidence"
                            "對應的完整文字，不可直接複製提示中的 evidence，不可摘要或補字。矩形只框該連續原文及其"
                            "必要公式，不含上一個標題、下一題、相鄰定義或大片空白。所有座標相對完整 canonical bitmap，"
                            "左上 (0,0)、右下 (1000,1000)，四周只留 5 至 10 單位。若無法同時確認首尾文字或位置不唯一，"
                            "found=false，所有矩形與首尾錨點座標填 0、visible_excerpt 留空、confidence 低於 60。"
                            f"這是第 {pass_index + 1} 次獨立定位，不得假設另一輪的答案。只輸出 schema JSON。"
                        )
                        if pass_index == 0:
                            content = [
                                {"type": "input_text", "text": prompt},
                                {"type": "input_image", "image_url": clean_data_url, "detail": "high"},
                                {"type": "input_text", "text": "同一張圖片的座標網格，只用來讀座標："},
                                {"type": "input_image", "image_url": guide_data_url, "detail": "high"},
                            ]
                        else:
                            content = [
                                {"type": "input_text", "text": prompt},
                                {"type": "input_text", "text": "先用網格確認區域，再回乾淨圖逐字核對："},
                                {"type": "input_image", "image_url": guide_data_url, "detail": "high"},
                                {"type": "input_image", "image_url": clean_data_url, "detail": "high"},
                            ]
                        try:
                            result = _call_openai_json(
                                name=f"study_recall_source_consensus_v12_{pass_index + 1}",
                                schema=location_schema,
                                content=content,
                                timeout=240,
                                reasoning_effort="low",
                                max_output_tokens=1800,
                            )
                            successful_response = True
                        except (requests.RequestException, ValueError, TypeError):
                            app.logger.exception(
                                "Independent source locator failed for image %s target %s pass %s",
                                image_index,
                                target["location_id"],
                                pass_index + 1,
                            )
                            continue
                        if not bool(result.get("found")):
                            continue
                        try:
                            candidate = {
                                "left": int(result.get("left")),
                                "top": int(result.get("top")),
                                "right": int(result.get("right")),
                                "bottom": int(result.get("bottom")),
                                "start_x": int(result.get("start_x")),
                                "start_y": int(result.get("start_y")),
                                "end_x": int(result.get("end_x")),
                                "end_y": int(result.get("end_y")),
                                "confidence": int(result.get("confidence")),
                            }
                        except (TypeError, ValueError):
                            continue
                        if candidate["confidence"] < 68 or _validated_study_source_bbox(
                            {**candidate, "version": 1}
                        ) is None:
                            continue
                        if not all(
                            0 <= candidate[key] <= 1000
                            for key in ("start_x", "start_y", "end_x", "end_y")
                        ):
                            continue
                        visible_excerpt = str(result.get("visible_excerpt") or "").strip()
                        selected, metrics = _match_study_source_evidence_to_lines(
                            target["evidence"],
                            [{"text": visible_excerpt}],
                        )
                        if not selected or not source_line_match_is_verified(target["evidence"], metrics):
                            continue
                        candidate["match_metrics"] = metrics
                        candidates.append(candidate)
                    if len(candidates) != 2:
                        return target, None, successful_response
                    consensus = agreement_between(candidates[0], candidates[1])
                    if consensus is None:
                        return target, None, successful_response
                    agreement, tighter = consensus
                    metrics = {
                        key: min(
                            float(candidates[0]["match_metrics"].get(key) or 0.0),
                            float(candidates[1]["match_metrics"].get(key) or 0.0),
                        )
                        for key in ("score", "coverage", "boundary_coverage")
                    }
                    confidence = min(
                        int(candidates[0]["confidence"]),
                        int(candidates[1]["confidence"]),
                        round(agreement * 100),
                        round(
                            (
                                metrics["score"] * 0.32
                                + metrics["coverage"] * 0.42
                                + metrics["boundary_coverage"] * 0.26
                            )
                            * 100
                        ),
                    )
                    if confidence < 64:
                        return target, None, successful_response
                    evidence_length = len(_canonical_study_source_match_text(target["evidence"]))
                    expected_lines = estimated_source_line_count(target["evidence"])
                    start_anchor_y = round(median(candidate["start_y"] for candidate in candidates))
                    end_anchor_y = round(median(candidate["end_y"] for candidate in candidates))
                    anchor_margin = min(46, 30 + max(0, expected_lines - 1) * 2)
                    candidate_seed = {
                        "left": tighter["left"],
                        "top": max(
                            tighter["top"],
                            min(start_anchor_y, end_anchor_y) - anchor_margin,
                        ),
                        "right": tighter["right"],
                        "bottom": min(
                            tighter["bottom"],
                            max(start_anchor_y, end_anchor_y) + anchor_margin,
                        ),
                        "confidence": confidence,
                        "version": SOURCE_BBOX_VERSION,
                    }
                    if (
                        _validated_study_source_bbox(
                            {**candidate_seed, "version": 1}
                        )
                        is None
                        or not source_bbox_span_is_plausible(
                            target["evidence"], candidate_seed
                        )
                    ):
                        return target, None, successful_response
                    crop_left = max(
                        0, math.floor(candidate_seed["left"] * page_width / 1000)
                    )
                    crop_top = max(
                        0, math.floor(candidate_seed["top"] * page_height / 1000)
                    )
                    crop_right = min(
                        page_width,
                        math.ceil(candidate_seed["right"] * page_width / 1000),
                    )
                    crop_bottom = min(
                        page_height,
                        math.ceil(candidate_seed["bottom"] * page_height / 1000),
                    )
                    if crop_right <= crop_left or crop_bottom <= crop_top:
                        return target, None, successful_response
                    crop_image = clean_page.crop(
                        (crop_left, crop_top, crop_right, crop_bottom)
                    )
                    try:
                        crop_result = _call_openai_json(
                            name="study_recall_source_crop_transcription_v12",
                            schema=crop_transcription_schema,
                            content=[
                                {
                                    "type": "input_text",
                                    "text": (
                                        "你只會看到一張從手寫筆記裁下的小圖，且不知道系統正在尋找什麼。"
                                        "請按由上到下、由左到右的順序，逐字轉錄裁切範圍內真正可見的所有文字、"
                                        "數字與公式。不得猜測裁切外內容，不得依學科常識補句，不得摘要、改寫或"
                                        "修正；被邊界切斷而無法辨識的字元以〔截斷〕表示。只輸出 schema JSON。"
                                    ),
                                },
                                {
                                    "type": "input_image",
                                    "image_url": _study_image_data_url(crop_image),
                                    "detail": "high",
                                },
                            ],
                            timeout=180,
                            reasoning_effort="low",
                            max_output_tokens=2200,
                        )
                    except (requests.RequestException, ValueError, TypeError):
                        app.logger.exception(
                            "Blind crop verification failed for image %s target %s",
                            image_index,
                            target["location_id"],
                        )
                        return target, None, successful_response
                    crop_visible_text = str(crop_result.get("visible_text") or "").strip()
                    crop_selected, crop_metrics = _match_study_source_evidence_to_lines(
                        target["evidence"],
                        [{"text": crop_visible_text}],
                    )
                    if not crop_selected or not source_line_match_is_verified(
                        target["evidence"], crop_metrics
                    ):
                        return target, None, successful_response
                    metrics = {
                        key: min(float(metrics.get(key) or 0.0), float(crop_metrics.get(key) or 0.0))
                        for key in ("score", "coverage", "boundary_coverage")
                    }
                    candidate = {
                        **candidate_seed,
                        "confidence": confidence,
                        "version": SOURCE_BBOX_VERSION,
                        "text_verified": True,
                        "match_score": round(metrics["score"], 4),
                        "match_coverage": round(metrics["coverage"], 4),
                        "boundary_coverage": round(metrics["boundary_coverage"], 4),
                        "evidence_length": evidence_length,
                        "coordinate_agreement": round(agreement, 4),
                        "expected_lines": expected_lines,
                        "span_verified": True,
                        "crop_verified": True,
                        "crop_match_score": round(float(crop_metrics.get("score") or 0.0), 4),
                        "crop_match_coverage": round(float(crop_metrics.get("coverage") or 0.0), 4),
                        "crop_boundary_coverage": round(
                            float(crop_metrics.get("boundary_coverage") or 0.0), 4
                        ),
                        "crop_match_precision": round(
                            float(crop_metrics.get("precision") or 0.0), 4
                        ),
                    }
                    if _validated_study_source_bbox(candidate) is None:
                        return target, None, successful_response
                    return target, candidate, successful_response
                finally:
                    if hasattr(study_upload_context, "cancel_event"):
                        del study_upload_context.cancel_event

            executor = ThreadPoolExecutor(
                max_workers=min(4, len(page_targets)),
                thread_name_prefix="study-source-consensus",
            )
            page_had_response = False
            try:
                futures = [executor.submit(locate_one_target, target) for target in page_targets]
                for future in as_completed(futures):
                    _raise_if_study_upload_cancelled()
                    target, candidate, successful_response = future.result()
                    page_had_response = page_had_response or successful_response
                    if candidate is None:
                        continue
                    target["source_ref"]["bbox"] = candidate
                    located += 1
            except _StudyUploadCancelled:
                for future in futures:
                    future.cancel()
                executor.shutdown(wait=False, cancel_futures=True)
                raise
            else:
                executor.shutdown(wait=True)
            if not page_had_response:
                failed_pages += 1

        if failed_pages == len(targets_by_image):
            raise ValueError("Source localization failed for every image")
        return located, total

    return (
        _localize_study_card_sources_model_consensus_legacy,
    )
