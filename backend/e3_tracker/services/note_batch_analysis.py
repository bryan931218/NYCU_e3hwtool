"""Note batch analysis; dependencies are bound per application."""

import base64
import copy
import io
import json
import math
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests
from PIL import Image, ImageFilter, ImageOps
from ..shared.visual_notes import merge_visual_regions, normalize_visual_regions
from ..shared.study_note_composer import StudyNoteToolError


def build_note_batch_analysis(*,
    STUDY_PLAN_SUBJECTS,
    _call_openai_json,
    _call_openai_study_note_tools,
    _canonical_study_source_match_text,
    _enrich_study_card_coverage_ids,
    _is_openai_quota_error,
    _literal_study_source_evidence,
    _localize_study_card_sources,
    _normalize_study_concept_title,
    _normalize_study_math_markup,
    _openai_error_details,
    _raise_if_study_upload_cancelled,
    _study_has_invalid_negation_counterexample,
    _study_recall_coverage_gaps,
    _study_recall_coverage_metrics,
    _study_recall_coverage_needs_repair,
    _study_recall_page_coverage_met,
    _study_source_coverage_items,
    _study_source_page_coverage_plan,
    _study_text_quality_issue,
    _validate_recall_output,
    _validated_study_source_bbox,
    app,
    openai_api_key,
    study_upload_context,
    visual_note_pipeline_enabled,
):
    def _analyze_study_note_image_batch(
        images: List[Tuple[str, bytes, str]],
        *,
        subject: str,
        allow_corrections: bool,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        _raise_if_study_upload_cancelled()
        if not openai_api_key:
            return None, "尚未設定 OPENAI_API_KEY，無法分析筆記。"

        def review_zoom_crops(image_bytes: bytes) -> List[Dict[str, Any]]:
            try:
                with Image.open(io.BytesIO(image_bytes)) as opened:
                    source = ImageOps.exif_transpose(opened).convert("RGB")
                    width, height = source.size
                    if width < 320 or height < 320:
                        return []
                    if height >= width:
                        boxes = [
                            (0, 0, width, max(1, round(height * 0.5))),
                            (0, min(height - 1, round(height * 0.25)), width, max(1, round(height * 0.75))),
                            (0, min(height - 1, round(height * 0.5)), width, height),
                        ]
                    else:
                        boxes = [
                            (0, 0, max(1, round(width * 0.5)), height),
                            (min(width - 1, round(width * 0.25)), 0, max(1, round(width * 0.75)), height),
                            (min(width - 1, round(width * 0.5)), 0, width, height),
                        ]
                    crops: List[Dict[str, Any]] = []
                    for crop_index, box in enumerate(boxes, start=1):
                        original_crop = source.crop(box)
                        max_dimension = max(original_crop.size)
                        scale = min(2.4, max(1.6, 2800 / max(1, max_dimension)))
                        target_size = (
                            max(1, round(original_crop.width * scale)),
                            max(1, round(original_crop.height * scale)),
                        )

                        color_crop = original_crop.resize(target_size, Image.Resampling.LANCZOS)
                        color_crop = color_crop.filter(
                            ImageFilter.UnsharpMask(radius=1.1, percent=170, threshold=2)
                        )
                        color_output = io.BytesIO()
                        color_crop.save(color_output, format="JPEG", quality=94, optimize=True)
                        crops.append(
                            {
                                "label": f"重疊區塊 {crop_index} 的彩色銳化放大版",
                                "bytes": color_output.getvalue(),
                            }
                        )

                        contrast_crop = ImageOps.grayscale(original_crop)
                        contrast_crop = ImageOps.autocontrast(contrast_crop, cutoff=0.5)
                        contrast_crop = contrast_crop.resize(target_size, Image.Resampling.LANCZOS)
                        contrast_crop = contrast_crop.filter(
                            ImageFilter.UnsharpMask(radius=1.0, percent=190, threshold=1)
                        ).convert("RGB")
                        contrast_output = io.BytesIO()
                        contrast_crop.save(contrast_output, format="JPEG", quality=94, optimize=True)
                        crops.append(
                            {
                                "label": f"重疊區塊 {crop_index} 的灰階高對比放大版",
                                "bytes": contrast_output.getvalue(),
                            }
                        )
                    return crops
            except (OSError, ValueError):
                return []

        def transcription_has_example_signals(value: Any) -> bool:
            text = " ".join(str(value or "").split())
            return bool(
                re.search(
                    r"(?:例題|範例|算例|反例|練習題|題目|問題|解答|"
                    r"\b(?:worked\s+example|example|ex\.|exercise|problem|question|solution)\b|"
                    r"(?:求出|求解|計算|判斷|證明|找出|解出|求其|是否|試證).{0,80}(?:[?？=→≤≥]|答案|解：|解:))",
                    text,
                    flags=re.IGNORECASE,
                )
            )
        visual_region_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "region_type",
                "title",
                "description",
                "visible_text",
                "bbox",
                "nodes",
                "edges",
                "confidence",
            ],
            "properties": {
                "region_type": {
                    "type": "string",
                    "enum": [
                        "tree",
                        "flowchart",
                        "graph",
                        "chart",
                        "table",
                        "diagram",
                        "architecture",
                        "circuit",
                        "geometry",
                        "image",
                        "other",
                    ],
                },
                "title": {"type": "string", "maxLength": 100},
                "description": {"type": "string", "maxLength": 700},
                "visible_text": {"type": "string", "maxLength": 800},
                "bbox": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["left", "top", "right", "bottom"],
                    "properties": {
                        "left": {"type": "integer", "minimum": 0, "maximum": 1000},
                        "top": {"type": "integer", "minimum": 0, "maximum": 1000},
                        "right": {"type": "integer", "minimum": 0, "maximum": 1000},
                        "bottom": {"type": "integer", "minimum": 0, "maximum": 1000},
                    },
                },
                "nodes": {
                    "type": "array",
                    "maxItems": 36,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["id", "label", "x", "y"],
                        "properties": {
                            "id": {"type": "string", "maxLength": 32},
                            "label": {"type": "string", "maxLength": 100},
                            "x": {"type": "integer", "minimum": 0, "maximum": 1000},
                            "y": {"type": "integer", "minimum": 0, "maximum": 1000},
                        },
                    },
                },
                "edges": {
                    "type": "array",
                    "maxItems": 60,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["from", "to", "label"],
                        "properties": {
                            "from": {"type": "string", "maxLength": 32},
                            "to": {"type": "string", "maxLength": 32},
                            "label": {"type": "string", "maxLength": 80},
                        },
                    },
                },
                "confidence": {
                    "type": "string",
                    "enum": ["high", "medium", "low"],
                },
            },
        }
        page_required = ["image_index", "transcription", "uncertain_fragments"]
        page_properties: Dict[str, Any] = {
            "image_index": {"type": "integer", "minimum": 1, "maximum": len(images)},
            "transcription": {"type": "string", "maxLength": 8000},
            "uncertain_fragments": {
                "type": "array",
                "maxItems": 20,
                "items": {"type": "string", "maxLength": 240},
            },
        }
        if visual_note_pipeline_enabled:
            page_required.append("visual_regions")
            page_properties["visual_regions"] = {
                "type": "array",
                "maxItems": 20,
                "items": visual_region_schema,
            }
        transcription_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["detected_topic", "pages"],
            "properties": {
                "detected_topic": {"type": "string", "maxLength": 80},
                "pages": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": len(images),
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": page_required,
                        "properties": page_properties,
                    },
                },
            },
        }
        def single_page_transcription_schema(image_index: int) -> Dict[str, Any]:
            schema = json.loads(json.dumps(transcription_schema))
            schema["properties"]["pages"]["minItems"] = 1
            schema["properties"]["pages"]["maxItems"] = 1
            schema["properties"]["pages"]["items"]["properties"]["image_index"] = {
                "type": "integer",
                "enum": [image_index],
            }
            return schema

        def parsed_single_page(result: Any, image_index: int) -> Dict[str, Any]:
            pages = result.get("pages") if isinstance(result, dict) else None
            if not isinstance(pages, list) or len(pages) != 1 or not isinstance(pages[0], dict):
                raise ValueError(f"Incomplete transcription for image {image_index}")
            page = pages[0]
            returned_index = int(page.get("image_index") or 0)
            page_text = str(page.get("transcription") or "").strip()
            visual_regions = (
                normalize_visual_regions(
                    page.get("visual_regions"),
                    image_index=image_index,
                )
                if visual_note_pipeline_enabled
                else []
            )
            if returned_index != image_index or (not page_text and not visual_regions):
                raise ValueError(f"Invalid transcription for image {image_index}")
            parsed_page = {
                "image_index": image_index,
                "transcription": page_text[:8000],
                "transcription_mode": "isolated_v1",
                "uncertain_fragments": [
                    " ".join(str(value).split())[:240]
                    for value in (page.get("uncertain_fragments") or [])[:20]
                    if str(value).strip()
                ],
            }
            if visual_note_pipeline_enabled:
                parsed_page["visual_regions"] = visual_regions
            return parsed_page

        def run_isolated_page_jobs(
            page_indices: List[int],
            worker: Callable[[int, Tuple[str, bytes, str]], Dict[str, Any]],
            on_completed: Optional[Callable[[int, int, int], None]] = None,
        ) -> Dict[int, Dict[str, Any]]:
            """Run page-isolated model calls concurrently without sharing images."""
            if not page_indices:
                return {}
            parent_cancel_event = getattr(study_upload_context, "cancel_event", None)

            def wrapped(image_index: int) -> Tuple[int, Dict[str, Any]]:
                if isinstance(parent_cancel_event, threading.Event):
                    study_upload_context.cancel_event = parent_cancel_event
                try:
                    _raise_if_study_upload_cancelled()
                    return image_index, worker(image_index, images[image_index - 1])
                finally:
                    if hasattr(study_upload_context, "cancel_event"):
                        del study_upload_context.cancel_event

            results: Dict[int, Dict[str, Any]] = {}
            executor = ThreadPoolExecutor(
                max_workers=min(3, len(page_indices)),
                thread_name_prefix="study-page-ocr",
            )
            futures = {
                executor.submit(wrapped, image_index): image_index
                for image_index in page_indices
            }
            try:
                completed = 0
                for future in as_completed(futures):
                    _raise_if_study_upload_cancelled()
                    image_index, result = future.result()
                    results[image_index] = result
                    completed += 1
                    if on_completed:
                        on_completed(image_index, completed, len(page_indices))
            except BaseException:
                for future in futures:
                    future.cancel()
                executor.shutdown(wait=False, cancel_futures=True)
                raise
            else:
                executor.shutdown(wait=True)
            return results

        transcription_instruction = (
                    f"你是跨學科筆記的忠實轉錄員。這次網站選定科目是「{subject}」，網站允許的科目只有：{ '、'.join(STUDY_PLAN_SUBJECTS) }。"
                    "逐張轉錄看得清楚的標題、敘述、定義、步驟、例子、表格文字、專有名詞與符號，不要整理、解釋、修正知識內容或補充原圖沒有的觀念；只允許依下述規則做最小的字元級上下文補全。例題辨識要逐區塊執行：檢查『例題、範例、算例、反例、練習題、題目、問題、案例、解答』等中文標籤，以及 Example、Ex.、Exercise、Problem、Question、Solution 等英文標籤；也檢查有編號的小題、問號或明確的求解／計算／證明／判斷要求，和題目後接解答或計算過程的版面。每一個獨立題設都要原樣轉錄，不能只記錄最後答案，也不能把相鄰不同題目的數值或解法合併。"
                    "transcription 只能依自然閱讀順序寫入圖片上實際存在的字元；不得用括號或句子描述頁面位置、顏色、圖示、照片、版面或內容大意，例如『頁中列出』『右側原稿示意』『下方有例子』都不屬於轉錄。也禁止自行加入『已保留所有可見文字／數字／括號／箭頭』『來源保留說明』『轉錄完整性』等完成聲明或工作紀錄。圖形若沒有可辨識文字就不要替它寫說明。"
                    "凡是原圖中的數學式、物理量關係、化學方程式、統計式或其他數學符號表達，請使用可渲染的 LaTeX：行內使用 \\( ... \\)，獨立式使用 \\[ ... \\]；保留等號、條件、上下標、矩陣、反應箭頭與原本順序。普通文字、專有名詞與非公式內容不要硬改成 LaTeX。"
                    "程式碼不是數學公式，禁止轉成 LaTeX。完整程式、函式、類別、虛擬碼或連續兩行以上的操作必須原樣放入 Markdown fenced code block：第一行使用 ```cpp、```c、```python、```java 或 ```pseudocode 等最符合原圖的語言，最後以 ``` 結束；未知語言使用 ```text。保留縮排、大小寫、括號、分號、運算子與註解。句中的變數名、函式名或單行短指令使用單反引號，例如 `push()`，不要使用數學定界符。"
                    "若局部字元、數字或符號無法直接辨認，先利用同頁前後文、同份筆記重複出現的記號、公式成對結構、表格欄列與相鄰推導判斷。只有候選內容可被這些局部證據唯一決定時才補入 transcription，不可用課本常識延伸整句或補入新觀念。"
                    "每個補全都要在 uncertain_fragments 記錄『已補全｜推定：...｜依據：...｜信心：高／中』；若仍有兩種以上合理結果，才在原位置寫〔無法推定〕，並記錄『未補全｜上下文：...』。補全後的 transcription 直接放可讀文字，不要插入待確認說明。"
                    "所有原文中的具體名稱、數值、單位、變數、符號、版本、日期、條件與例外都必須保留；不得擅自泛化、特例化、翻譯成不同概念或套用其他科目的知識。detected_topic 只能描述這份「{subject}」筆記實際出現的主題，不得建立第七個科目。"
                    "你這次只會看到一張原圖；不得想像、延續或抄入其他頁的內容。detected_topic 只依目前這張圖實際內容命名。"
        )
        if visual_note_pipeline_enabled:
            transcription_instruction += (
                " transcription 仍只放實際字元；另外必須用 visual_regions 盤點所有具有學習意義的非純文字區塊，"
                "包括表格、樹狀圖、流程圖、節點關係圖、統計圖、座標圖、架構圖、電路圖、幾何圖與其他示意圖。"
                "不要把裝飾線、單純框線或一般段落誤判為圖。bbox 使用整張原圖 0 到 1000 的座標，只框完整視覺"
                "本體與圖內標籤；區塊外的章節標題、相鄰題目或下一張圖不得納入，也不要只框其中一個字。"
                "title 是圖的短名稱；description 忠實說明圖上直接可見的關係，不得"
                "補充圖片外知識；visible_text 收錄圖內標籤、軸名、圖例與數值。樹、流程、graph 或架構圖需盡量"
                "列出 nodes 與 edges，節點 x/y 使用該視覺區塊內 0 到 1000 的相對座標；無法可靠結構化時"
                " nodes、edges 留空，仍必須保留 visual region，不能因為無文字就漏掉。"
            )
        card_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["detected_topic", "summary", "key_concepts"],
            "properties": {
                "detected_topic": {"type": "string", "maxLength": 80},
                "summary": {"type": "string", "maxLength": 900},
                "key_concepts": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["concept", "card_type", "recall_cue", "core_summary", "explanation", "simple_example", "example_problem", "example_method", "reasoning_steps", "common_confusion", "memory_hint", "topic", "related_concepts", "search_keywords", "source_refs", "visual_refs", "coverage_ids", "correction"],
                        "properties": {
                            "concept": {"type": "string", "maxLength": 80},
                            "card_type": {"type": "string", "enum": ["concept", "example"]},
                            "recall_cue": {"type": "string", "maxLength": 160},
                            "core_summary": {"type": "string", "maxLength": 280},
                            "explanation": {"type": "string", "maxLength": 620},
                            "simple_example": {"type": "string", "maxLength": 360},
                            "example_problem": {"type": "string", "maxLength": 360},
                            "example_method": {"type": "string", "maxLength": 280},
                            "reasoning_steps": {
                                "type": "array",
                                "maxItems": 4,
                                "items": {"type": "string", "maxLength": 180},
                            },
                            "common_confusion": {"type": "string", "maxLength": 180},
                            "memory_hint": {"type": "string", "maxLength": 120},
                            "topic": {"type": "string", "maxLength": 48},
                            "related_concepts": {
                                "type": "array",
                                "maxItems": 2,
                                "items": {"type": "string", "maxLength": 80},
                            },
                            "search_keywords": {
                                "type": "array",
                                "maxItems": 8,
                                "items": {"type": "string", "maxLength": 40},
                            },
                            "source_refs": {
                                "type": "array",
                                "minItems": 0,
                                "maxItems": 4,
                                "items": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "required": ["image_index", "evidence"],
                                    "properties": {
                                        "image_index": {"type": "integer", "minimum": 1, "maximum": len(images)},
                                        "evidence": {"type": "string", "maxLength": 240},
                                    },
                                },
                            },
                            "visual_refs": {
                                "type": "array",
                                "maxItems": 6,
                                "items": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "required": ["region_id"],
                                    "properties": {
                                        "region_id": {
                                            "type": "string",
                                            "pattern": "^p[1-9][0-9]*v[1-9][0-9]*$",
                                        },
                                    },
                                },
                            },
                            "coverage_ids": {
                                "type": "array",
                                "minItems": 1,
                                "maxItems": 8,
                                "items": {"type": "string", "maxLength": 16},
                            },
                            "correction": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["applied", "original", "corrected", "reason"],
                                "properties": {
                                    "applied": {"type": "boolean"},
                                    "original": {"type": "string", "maxLength": 240},
                                    "corrected": {"type": "string", "maxLength": 240},
                                    "reason": {"type": "string", "maxLength": 300},
                                },
                            },
                        },
                    },
                },
            },
        }

        if not visual_note_pipeline_enabled:
            card_item_schema = card_schema["properties"]["key_concepts"]["items"]
            card_item_schema["required"].remove("visual_refs")
            card_item_schema["properties"].pop("visual_refs", None)
            card_item_schema["properties"]["source_refs"]["minItems"] = 1

        def finalize_tool_composed_note(
            payload: Dict[str, Any],
            source_pages: List[Dict[str, Any]],
        ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
            """Apply deterministic validation and source localization to tool output."""

            validated = _validate_recall_output(payload, source_pages)
            if not validated:
                return None, (
                    "筆記內容已完成轉錄，但沒有可通過來源驗證的內容區塊；"
                    "請稍後重試，不需要把其他科目的內容改成數學例題。"
                )
            _enrich_study_card_coverage_ids(validated, source_pages)
            remaining_example_items = (
                _study_recall_coverage_gaps(validated, source_pages).get("example_items") or []
            )
            if remaining_example_items:
                app.logger.warning(
                    "Tool-composed note has partially uncovered examples; preserving validated cards: %s",
                    [item.get("id") for item in remaining_example_items],
                )
                validated["processing_warnings"] = [
                    (
                        f"有 {len(remaining_example_items)} 個疑似例題區塊未能可靠轉成卡片；"
                        "其餘已驗證內容已完整保留。"
                    )
                ]
            validated["source_transcription"] = source_pages
            validated["uncertain_fragments"] = [
                {"image_index": page["image_index"], "text": fragment}
                for page in source_pages
                for fragment in page.get("uncertain_fragments") or []
            ]
            if progress_callback:
                progress_callback(82, "內容工具已完成六科通用驗證，正在定位原文區塊。")
            _raise_if_study_upload_cancelled()
            try:
                _localize_study_card_sources(
                    images,
                    validated["key_concepts"],
                    validated["source_transcription"],
                )
                retry_concepts: List[Dict[str, Any]] = []
                source_groups = 0
                located_groups = 0
                for concept in validated["key_concepts"]:
                    if not isinstance(concept, dict):
                        continue
                    refs_by_page: Dict[int, List[Dict[str, Any]]] = {}
                    for source_ref in concept.get("source_refs") or []:
                        if not isinstance(source_ref, dict):
                            continue
                        try:
                            image_index = int(source_ref.get("image_index") or 0)
                        except (TypeError, ValueError):
                            continue
                        if (
                            1 <= image_index <= len(images)
                            and _literal_study_source_evidence(source_ref.get("evidence"))
                        ):
                            refs_by_page.setdefault(image_index, []).append(source_ref)
                    missing_refs: List[Dict[str, Any]] = []
                    for image_index, page_refs in refs_by_page.items():
                        source_groups += 1
                        if any(
                            _validated_study_source_bbox(
                                source_ref.get("bbox"),
                                require_text_verified=True,
                                expected_image_index=image_index,
                            )
                            is not None
                            for source_ref in page_refs
                        ):
                            located_groups += 1
                        else:
                            missing_refs.append(
                                max(
                                    page_refs,
                                    key=lambda source_ref: len(
                                        _literal_study_source_evidence(
                                            source_ref.get("evidence")
                                        )
                                    ),
                                )
                            )
                    if missing_refs:
                        retry_concept = {
                            field: copy.deepcopy(concept.get(field))
                            for field in (
                                "concept",
                                "topic",
                                "content_kind",
                                "core_summary",
                                "explanation",
                                "example_problem",
                                "example_method",
                                "simple_example",
                            )
                        }
                        retry_concept["source_refs"] = missing_refs
                        retry_concepts.append(retry_concept)
                if (
                    retry_concepts
                    and source_groups
                    and located_groups / source_groups < 0.90
                ):
                    if progress_callback:
                        progress_callback(
                            88,
                            f"首次定位完成 {located_groups}/{source_groups} 個來源區塊，"
                            "正在重試未定位區塊。",
                        )
                    _raise_if_study_upload_cancelled()
                    _localize_study_card_sources(
                        images,
                        retry_concepts,
                        validated["source_transcription"],
                    )
            except (requests.RequestException, ValueError, TypeError):
                app.logger.exception("Tool-composed study-note source localization failed")
            if progress_callback:
                progress_callback(100, "來源區塊定位完成，本批筆記已完成整理。")
            validated["organization_mode"] = "tool_composer_v1"
            return validated, None

        try:
            if progress_callback:
                progress_callback(20, "正在逐頁忠實轉錄文字、符號與公式，不做延伸解釋。")
            def transcribe_initial_page(
                image_index: int,
                image: Tuple[str, bytes, str],
            ) -> Dict[str, Any]:
                _filename, image_bytes, mime_type = image
                transcription = _call_openai_json(
                    name=f"study_note_transcription_page_{image_index}",
                    schema=single_page_transcription_schema(image_index),
                    content=[
                        {
                            "type": "input_text",
                            "text": (
                                transcription_instruction
                                + f" 這是整批中的第 {image_index} 張；pages 必須只輸出 image_index={image_index}。"
                            ),
                        },
                        {
                            "type": "input_image",
                            "image_url": f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}",
                            "detail": "high",
                        },
                    ],
                    timeout=180,
                )
                return parsed_single_page(
                    transcription,
                    image_index,
                )

            def report_initial_page(
                _image_index: int,
                completed: int,
                total_pages: int,
            ) -> None:
                if progress_callback:
                    progress_callback(
                        20 + round(completed / max(1, total_pages) * 11),
                        f"已完成 {completed}/{total_pages} 張逐頁轉錄，頁面來源彼此隔離。",
                    )
            pages_by_index = run_isolated_page_jobs(
                list(range(1, len(images) + 1)),
                transcribe_initial_page,
                report_initial_page,
            )
            source_pages = [pages_by_index[index] for index in range(1, len(images) + 1) if index in pages_by_index]
            if len(source_pages) != len(images):
                raise ValueError("Incomplete transcription")
            initial_source_pages = json.loads(json.dumps(source_pages, ensure_ascii=False))

            symbol_audit_instruction = (
                "你是獨立的跨學科逐字元核對與上下文補全員。第一輪 transcription 只是待核草稿；請重新查看目前這一張完整原圖與其放大裁切，輸出修正過的完整單頁轉錄。"
                "逐一核對所有文字、專有名詞、數字、單位、符號、標點、大小寫、上下標、指數、分數、表格欄位、公式、方程式、反應式、程式碼與條件。字跡不清時，必須主動比較同頁前後句、同頁重複記號、公式左右結構、表格欄列和相鄰推導。"
                "例題與題組要執行額外數字符號稽核：先逐項列出題號、所有常數、係數、座標、矩陣元素、範圍端點、單位及答案數值，再逐一對照完整原圖、彩色銳化版與灰階高對比版。特別區分 0／6／8／9、1／7、3／5、正負號、小數點、逗號、分數線、括號、次方與上下標；兩個版本衝突時回到完整原圖與公式內部一致性判斷，不能只採信其中一張增強圖。"
                "輸出只能包含原圖實際書寫的字元，不得以『頁中／圖中／上方／下方／右側／原稿／黑板』等敘事描述圖片、位置或圖形；第一輪若有這類描述必須刪除，不能當作原文保留。"
                "只有上下文使缺字或符號只剩一個合理結果時才補上；可做最小字元級推理，但不得用外部課本知識補成更完整的定義、定理、結論或解法。不要因單一筆畫模糊就刪掉整段。"
                "原圖的具體值不得改成變數，變數不得改成具體值，專有名詞不得換成相近名詞，原圖未寫出的定義域、範圍、因果或結論不得補入。"
                "若草稿與原圖不一致，以原圖為準。每個採用的上下文補全都列入 uncertain_fragments，格式為『已補全｜推定：...｜依據：...｜信心：高／中』；若仍有兩種以上合理結果，才在 transcription 對應位置寫〔無法推定〕並記錄『未補全｜上下文：...』。"
                "不得寫入目前原圖以外的其他頁內容，即使草稿看起來像有接續內容也必須以目前原圖為準。"
            )
            if visual_note_pipeline_enabled:
                symbol_audit_instruction += (
                    " visual_regions 與 transcription 分開處理：重新盤點原圖中的表格、樹、流程、圖表與示意圖，"
                    "核對其完整 bbox、圖內文字、節點與箭頭。不得因 transcription 禁止視覺敘事而把"
                    " visual_regions 清空；無法可靠讀出節點關係時保留原圖區塊並降低 confidence。"
                )
            def audit_symbol_page(
                image_index: int,
                image: Tuple[str, bytes, str],
            ) -> Dict[str, Any]:
                _filename, image_bytes, mime_type = image
                symbol_audit_content: List[Dict[str, Any]] = [
                    {
                        "type": "input_text",
                        "text": (
                            symbol_audit_instruction
                            + f" 目前是第 {image_index} 張，pages 只能輸出 image_index={image_index}。"
                            + "\n待核草稿="
                            + json.dumps(
                                pages_by_index[image_index],
                                ensure_ascii=False,
                                separators=(",", ":"),
                            )
                        ),
                    },
                    {
                        "type": "input_text",
                        "text": "以下是目前唯一允許轉錄的完整原圖；後面的圖都是同一頁重疊裁切，不是新頁面。",
                    },
                    {
                        "type": "input_image",
                        "image_url": f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}",
                        "detail": "high",
                    },
                ]
                for crop in review_zoom_crops(image_bytes):
                    symbol_audit_content.extend(
                        [
                            {
                                "type": "input_text",
                                "text": (
                                    f"目前原圖的{crop['label']}，只用來核對同一頁的小字、數字與符號。"
                                    "請逐字比較原圖與不同增強版本；增強造成的邊緣或雜點不得當成小數點、負號或筆畫。"
                                ),
                            },
                            {
                                "type": "input_image",
                                "image_url": f"data:image/jpeg;base64,{base64.b64encode(crop['bytes']).decode('ascii')}",
                                "detail": "high",
                            },
                        ]
                    )
                symbol_audit = _call_openai_json(
                    name=f"study_note_symbol_audit_page_{image_index}",
                    schema=single_page_transcription_schema(image_index),
                    content=symbol_audit_content,
                    timeout=210,
                )
                parsed = parsed_single_page(
                    symbol_audit,
                    image_index,
                )
                if visual_note_pipeline_enabled:
                    parsed["visual_regions"] = merge_visual_regions(
                        [
                            pages_by_index[image_index].get("visual_regions"),
                            parsed.get("visual_regions"),
                        ],
                        image_index=image_index,
                    )
                return parsed

            def report_audited_page(
                _image_index: int,
                completed: int,
                total_pages: int,
            ) -> None:
                if progress_callback:
                    progress_callback(
                        32 + round(completed / max(1, total_pages) * 10),
                        f"已完成 {completed}/{total_pages} 張獨立符號核對。",
                    )
            audited_pages_by_index = run_isolated_page_jobs(
                list(range(1, len(images) + 1)),
                audit_symbol_page,
                report_audited_page,
            )
            source_pages = [
                audited_pages_by_index[index]
                for index in range(1, len(images) + 1)
                if index in audited_pages_by_index
            ]
            if len(source_pages) != len(images):
                raise ValueError("Incomplete symbol-audited transcription")

            reconciliation_instruction = (
                "你是單頁轉錄完整性仲裁員。initial_transcription 與 symbol_audit 是兩次獨立查看目前同一張圖片的結果，兩者都可能漏段或誤讀。"
                "請重新查看目前原圖，逐段比較兩稿的每一個標題、定義、公式、例題、證明、表格、程式碼與結論，輸出目前這一頁的完整最終轉錄。不得在轉錄中加入已完成核對、保留全部文字或符號等流程聲明。"
                "任何只出現在其中一稿的段落都必須回到圖片確認：圖片可見就完整保留，不可因另一稿漏掉而刪除；圖片不支持就不要保留，也不可把兩稿內容直接盲目拼接。"
                "最終 transcription 仍只能是圖片上實際可見字元；不得新增頁面位置、顏色、圖示、照片、版面或內容摘要等視覺敘事。若任一稿含『頁中列出』『右側原稿』『下方示意』等描述，除非這些字真的寫在圖上，否則一律移除。"
                "逐頁由上到下輸出，不可省略側欄、右半部、頁尾、小字例題或接續公式。例題的題設、操作與結論均須轉錄；請逐一盤點中文例題／範例／算例／反例／練習題／題目／問題／解答標籤、英文 Example／Ex.／Exercise／Problem／Question／Solution 標籤、編號小題，以及含明確求解要求的題組；不同題設必須分開保留，不能只留下答案或把多題合併。原圖本身的錯誤也照原樣轉錄，留給後續內容校正。"
                "仍無法唯一辨識的字元依前後文做最小補全並記錄信心；無法唯一推定才標〔無法推定〕。不得用課本知識補入圖片沒有的定義或解法。"
                "數學與結構化符號使用可渲染的 LaTeX。你看不到也不得輸出其他頁內容；pages 只能有目前這一頁。"
            )
            if visual_note_pipeline_enabled:
                reconciliation_instruction += (
                    " 同時仲裁 visual_regions：任何一稿辨識出的圖形都必須回到原圖確認；原圖存在就保留，"
                    "並輸出完整 bbox、可見標籤、節點與邊。不要因另一稿漏圖而刪除，也不要把同一張圖重複輸出。"
                )
            initial_pages_by_index = {
                int(page.get("image_index") or 0): page
                for page in initial_source_pages
                if isinstance(page, dict)
            }
            reconciliation_required_indices: Set[int] = set()
            for audited_page in source_pages:
                image_index = int(audited_page.get("image_index") or 0)
                initial_page = initial_pages_by_index.get(image_index) or {}
                initial_text = _canonical_study_source_match_text(
                    initial_page.get("transcription") or ""
                )
                audited_text = _canonical_study_source_match_text(
                    audited_page.get("transcription") or ""
                )
                shorter_length = min(len(initial_text), len(audited_text))
                longer_length = max(len(initial_text), len(audited_text), 1)
                similarity = (
                    SequenceMatcher(None, initial_text, audited_text, autojunk=False).ratio()
                    if initial_text and audited_text
                    else 0.0
                )
                uncertainty_text = " ".join(
                    str(value or "")
                    for value in (
                        *(initial_page.get("uncertain_fragments") or []),
                        *(audited_page.get("uncertain_fragments") or []),
                    )
                )
                has_example_signals = transcription_has_example_signals(
                    " ".join(
                        (
                            str(initial_page.get("transcription") or ""),
                            str(audited_page.get("transcription") or ""),
                        )
                    )
                )
                if (
                    has_example_signals
                    or similarity < 0.985
                    or shorter_length / longer_length < 0.97
                    or "未補全" in uncertainty_text
                    or "〔無法推定〕" in str(initial_page.get("transcription") or "")
                    or "〔無法推定〕" in str(audited_page.get("transcription") or "")
                ):
                    reconciliation_required_indices.add(image_index)
            if progress_callback:
                progress_callback(
                    43,
                    "正在比對兩輪轉錄差異，補回任何被單次辨識遺漏的段落。"
                    if reconciliation_required_indices
                    else "兩輪轉錄高度一致，已略過不必要的第三次辨識。",
                )
            reconciled_pages_by_index: Dict[int, Dict[str, Any]] = {
                image_index: audited_pages_by_index[image_index]
                for image_index in range(1, len(images) + 1)
                if image_index not in reconciliation_required_indices
            }

            def reconcile_page(
                image_index: int,
                image: Tuple[str, bytes, str],
            ) -> Dict[str, Any]:
                _filename, image_bytes, mime_type = image
                reconciliation_content: List[Dict[str, Any]] = [
                    {
                        "type": "input_text",
                        "text": (
                            reconciliation_instruction
                            + f" 目前是第 {image_index} 張，pages 只能輸出 image_index={image_index}。"
                            + "\ninitial_transcription="
                            + json.dumps(
                                initial_pages_by_index[image_index],
                                ensure_ascii=False,
                                separators=(",", ":"),
                            )
                            + "\nsymbol_audit="
                            + json.dumps(
                                audited_pages_by_index[image_index],
                                ensure_ascii=False,
                                separators=(",", ":"),
                            )
                        ),
                    },
                    {
                        "type": "input_image",
                        "image_url": f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}",
                        "detail": "high",
                    },
                ]
                if transcription_has_example_signals(
                    " ".join(
                        (
                            str(initial_pages_by_index[image_index].get("transcription") or ""),
                            str(audited_pages_by_index[image_index].get("transcription") or ""),
                        )
                    )
                ):
                    reconciliation_content[0]["text"] += (
                        "\n這一頁含有疑似例題。請建立數字符號核對清單，逐一比對題號、所有數值、"
                        "小數點、正負號、分子分母、括號、上下標、矩陣元素與運算符；兩稿即使一致，"
                        "仍必須以原圖和放大版重新確認，不能沿用共同誤讀。只把核對後結果寫入 transcription。"
                    )
                    for crop in review_zoom_crops(image_bytes):
                        reconciliation_content.extend(
                            [
                                {
                                    "type": "input_text",
                                    "text": (
                                        f"例題數字符號專用：{crop['label']}。"
                                        "同時參照完整原圖判斷，不可把影像增強雜點當成符號。"
                                    ),
                                },
                                {
                                    "type": "input_image",
                                    "image_url": f"data:image/jpeg;base64,{base64.b64encode(crop['bytes']).decode('ascii')}",
                                    "detail": "high",
                                },
                            ]
                        )
                reconciled = _call_openai_json(
                    name=f"study_note_transcription_reconciled_page_{image_index}",
                    schema=single_page_transcription_schema(image_index),
                    content=reconciliation_content,
                    timeout=240,
                    max_output_tokens=9000,
                )
                parsed = parsed_single_page(
                    reconciled,
                    image_index,
                )
                if visual_note_pipeline_enabled:
                    parsed["visual_regions"] = merge_visual_regions(
                        [
                            initial_pages_by_index[image_index].get("visual_regions"),
                            audited_pages_by_index[image_index].get("visual_regions"),
                            parsed.get("visual_regions"),
                        ],
                        image_index=image_index,
                    )
                return parsed
            reconciled_pages_by_index.update(
                run_isolated_page_jobs(
                    sorted(reconciliation_required_indices),
                    reconcile_page,
                )
            )
            source_pages = [
                reconciled_pages_by_index[index]
                for index in range(1, len(images) + 1)
                if index in reconciled_pages_by_index
            ]
            if len(source_pages) != len(images):
                raise ValueError("Incomplete reconciled transcription")
            for page in source_pages:
                transcription_text = str(page.get("transcription") or "")
                transcription_text = re.sub(
                    r"\$\$(.+?)\$\$",
                    lambda match: "\\[" + match.group(1).strip() + "\\]",
                    transcription_text,
                    flags=re.DOTALL,
                )
                transcription_text = re.sub(
                    r"(?<!\$)\$(?!\$)([^$\n]+?)(?<!\$)\$(?!\$)",
                    lambda match: "\\(" + match.group(1).strip() + "\\)",
                    transcription_text,
                )
                page["transcription"] = transcription_text

            coverage_plan = _study_source_page_coverage_plan(source_pages)
            audit_card_floor = int(coverage_plan["target_cards"])
            page_card_quotas = {
                str(image_index): quota
                for image_index, quota in coverage_plan["page_quotas"].items()
            }
            coverage_checklist = [
                {
                    "id": item["id"],
                    "image_index": item["image_index"],
                    "text": str(item["text"])[:500],
                    "content_type": item.get("content_type") or "concept",
                    "priority": item.get("priority") or "supporting",
                    "is_example": bool(item.get("is_example")),
                    "visual_region_id": str(item.get("visual_region_id") or ""),
                }
                for item in _study_source_coverage_items(source_pages)
            ]
            correction_rule = (
                "只可修正可由來源已有的標準定義、公式或直接計算毫無歧義地確認的錯誤；遇到這類錯誤時必須保留該觀念、輸出最小必要修正後的正確卡片並填寫 correction，不得以刪卡取代校正。不得為了修正補入來源未使用的新觀念或解法。"
                if allow_corrections
                else "不得修正任何內容；所有 correction.applied 必須是 false，其餘欄位為空字串。"
            )
            if progress_callback:
                progress_callback(52, "符號核對完成，正在用六科通用內容工具建立筆記。")
            try:
                tool_composed = _call_openai_study_note_tools(
                    subject=subject,
                    source_pages=source_pages,
                    coverage_checklist=coverage_checklist,
                    allow_corrections=allow_corrections,
                )
                finalized_tool_note, tool_note_error = finalize_tool_composed_note(
                    tool_composed,
                    source_pages,
                )
                if finalized_tool_note:
                    return finalized_tool_note, None
                raise StudyNoteToolError(
                    tool_note_error or "tool-composed note failed deterministic validation"
                )
            except (json.JSONDecodeError, StudyNoteToolError, ValueError, TypeError) as exc:
                # Keep the established JSON pipeline as a compatibility fallback
                # for models or gateways that do not yet return valid function calls.
                app.logger.warning(
                    "Study-note tool composer failed; using legacy compatibility path: %s",
                    exc,
                )
            organizer_prompt = (
                "你是跨學科的忠實筆記編輯，只能根據下方逐頁轉錄稿整理重點卡。不得加入轉錄稿沒有的定義、背景知識、例子、用途、推導、因果關係或專有名詞解釋；"
                "不得把僅被提到的術語另做定義卡。可以改寫語序與合併同一觀念的重複句，但每一句資訊都必須能在轉錄稿找到。"
                "每張卡只處理一個原筆記正在記錄的觀念，concept 使用不超過 18 個中文字的短標題。recall_cue 是揭示內容前的回想線索，以 2 至 4 個來源已有的關鍵詞呈現，可使用『條件 → 關係 → 結論』等短結構，但不能直接洩露完整答案、不能使用問號，也不能寫成考題。core_summary 用 1 至 2 個短句或一個完整公式直接寫出這張卡最需要記住的結論。explanation 再補足成立條件、觀念脈絡與來源確實寫出的最短推導；通常 2 至 4 個短句即可。"
                "reasoning_steps 只在來源確實包含推導、程序或解題步驟時填入 2 至 4 個可重用短步驟，純定義卡輸出空陣列。common_confusion 只有來源明確比較兩個觀念、指出易錯處，或允許校正且有直接可驗證錯誤時才填寫，否則輸出空字串；不可自行猜測學生會錯在哪裡。"
                "若來源是例題、範例或算例，card_type 必須為 example；一般觀念卡為 concept。example 卡的 example_problem 必須寫成一個可直接作答的具體題目示例，完整保留必要的數值、向量、矩陣、函數、條件與明確要求，不可只寫『判斷某性質』卻省略實際題設；example_method 只用 1 至 2 個短句寫來源實際採用、可重複使用的核心判斷或策略，不得編號或逐步列舉；reasoning_steps 再列必要操作。題目、方法與步驟不可混寫或重複。example 卡的 simple_example 必須是空字串。"
                "concept 卡的 simple_example 只整理來源中原有的最短例子；來源沒有例子時必須留空，不得創造數值、公式代入、情境或程式。concept 卡的 example_problem 與 example_method 必須是空字串。"
                "例題卡可刪除不影響作答的冗長背景，但不得刪掉具體題設、必要數值、給定式或要求，也不可只給最終答案。不得把偶然數值寫進 core_summary。只有用來否定性質的最小反例，才可保留證明失敗所必需的數值。"
                "例題方法可以把來源中反覆使用或清楚展示的操作抽象成一般步驟，但每個步驟都必須能由該例題的 source_refs 支持；不可補入來源沒用到的定理、捷徑或新解法。若來源只有題目和答案、沒有可辨識過程，就不要臆造解法。"
                "刪除教學口吻、重要性說明、驗證過程、重複結論、公式的文字重述、同義改寫、延伸提醒與『換句話說』『這表示』『可以看出』『值得注意』等填充語。"
                "所有文字欄位與 summary 都必須直接陳述知識，禁止使用『筆記給出』『筆記註明』『筆記記載／紀載』『筆記指出』『筆記中提到』『根據筆記可知』『筆記的重點是』或任何同義前綴。不要考題，也不要提到原文、來源、OCR、核對或修正過程。輸出前逐欄自檢；只要出現這類來源敘事，就重新改寫成直接知識陳述後才輸出。"
                "『來源保留說明』『來源明文聲明』『轉錄完整性』『已保留所有可見文字、數字、括號、方框、圓圈、箭頭或結尾符號』及任何相近的處理紀錄沒有複習價值，必須完全忽略，不可建立卡片，也不可放進 summary。"
                "影像中清楚可辨的公式、方程式、反應式、統計式、程式碼或其他關鍵結構都要保留；卡片數量不設上限。由你依複習目標判斷拆分或合併，但不可漏掉來源中的核心內容。coverage_checklist 中每一個 is_example=true 的獨立例題、範例、反例、練習題、題目或英文 Example/Exercise/Problem 都必須各自出現在一張 card_type=example 卡中；不可只把題目塞進 concept 卡的 explanation 或 simple_example，也不可把兩個有不同題設的例題合成一張。題目若只有題設沒有解法，仍要建立例題卡，example_method 誠實填寫『來源未提供完整解法』，不得自行補解。"
                f"先按頁建立完整內容清單，再依實際資訊量分配卡片。建議總卡數至少約 {audit_card_floor} 張、各頁資訊量參考值為 {json.dumps(page_card_quotas, ensure_ascii=False)}，但不是固定張數。優先保留每個清楚公式、定義、方法、例題策略與結論；同一觀念的成對定義、同方法例題或同一定理下的緊密內容可由你合併，一張卡若包含不同複習目標則應拆開，不得為湊張數重複寫卡。"
                f"coverage_checklist={json.dumps(coverage_checklist, ensure_ascii=False, separators=(',', ':'))}。priority=required 的區塊應優先進入卡片；supporting 區塊可在不破壞單一卡片主題的前提下併入 explanation。is_example=true 的區塊是不可合併遺漏的逐題清單，coverage_ids 與 source_refs 必須真實對應，不可虛報。"
                "所有數學公式與數學結構符號一律使用 \\( ... \\) 或 \\[ ... \\] 包住的 LaTeX；長公式用 aligned 合理換行。矩陣與向量必須使用可渲染的 matrix/bmatrix/pmatrix 環境，欄之間用 &，列之間用 \\\\，不可把多個分量直接黏在同一格。不要使用裸露的 $...$、$$...$$ 或只寫線性純文字公式。"
                "程式碼一律不得轉成 LaTeX。完整程式、函式、類別、虛擬碼或連續兩行以上操作使用 Markdown fenced code block，起始圍欄標明 cpp、c、python、java、javascript、sql 或 pseudocode；不確定時用 text。保留來源縮排、大小寫、括號、分號、陣列索引、指標符號、運算子與註解；不可為了排版改寫程式邏輯。句中的識別字或單行短指令使用單反引號。普通文字保持自然繁體中文。memory_hint 只有原文存在明確記憶線索時才填寫，否則輸出空字串。"
                + (
                    "source_pages.visual_regions 中每個 region_id 都代表原圖上已框出的表格、樹、流程圖、圖表或示意圖。"
                    "卡片只要使用該圖的關係、標籤或趨勢，就必須在 visual_refs 引用 region_id；純視覺卡可讓"
                    " source_refs 為空，但 source_refs 與 visual_refs 不得同時為空。content_type=visual 且"
                    " priority=required 的 coverage item 一定要由引用相同 region_id 的卡片覆蓋。不要把圖形"
                    "自行改寫成來源沒有的知識，也不要因為圖中文字少就略過。"
                    if visual_note_pipeline_enabled
                    else ""
                )
                +
                "非純視覺卡提供 1 至 4 個 source_refs。緊密相關的連續內容可由同一卡引用多段，讓卡片在不混雜不同觀念的前提下保留更多資訊。evidence 必須逐字複製對應頁 transcription 中一段連續、且能直接在原圖看到的文字，連空白與 LaTeX 都不要改，作為可程式比對的來源。evidence 禁止改寫、摘要或描述頁面位置與圖形；不得輸出『頁中列出』『右側原稿示意』『下方有例子』等非原文字句。若卡片跨兩段，分成兩個 source_refs，不可自行寫一段銜接敘述。"
                "source_pages 的 uncertain_fragments 若標為『已補全』且信心為高或中，代表該缺字已由第二輪模型依局部上下文獨立核對，可將 transcription 中補全後的連續文字正常整理成卡片；標為『未補全』或仍含〔無法推定〕的片段不得作為關鍵事實。不要在卡片正文提到補全過程。"
                "整理時盡量沿用轉錄稿原本的名詞、短語、變數、條件排列與公式，不要為了流暢改成課本式同義說法。只有字元辨識不清、前後自相矛盾、公式結構不可能成立或可由來源直接驗算出錯時，才做最小必要補全或校正。每張卡的 search_keywords 保留 3 至 8 個最可能被使用者回想起來搜尋的原文詞、專有名詞、縮寫、變數組合或公式名稱；只能取自該卡內容、source_refs 或已完成的高信心校正，不得加入來源外同義詞。"
                "topic 必須是科目底下精確的細分觀念，例如『線性映射判定』『像與反像』『直和與基底』『矩陣可逆性』；禁止直接使用線性代數、離散數學、資料結構、演算法、作業系統、計算機組織等科目名稱，也不要使用『其他』『綜合重點』『課堂筆記』等空泛名稱。"
                "依內容自然分成 2 至 6 個母主題；同一知識對象的定義、性質、操作、方法與例題應共用 topic，只有不同資料結構、理論或章節才分開。例如 Heap 的定義、建構、插入與刪除都歸入 Heap，Deap 與 SMMH 各自成組。topic 不得為湊數而用斜線、頓號或『與』串接互不從屬的分類。related_concepts 最多 2 個，只連結本批卡片中明確有推導、比較或前置關係者。summary 最多 5 個完整短句，只列最後保留的核心結論，不重複推導與例子，不可新增資訊，也不可在句中截斷。"
                + correction_rule
                + "\n\n逐頁忠實轉錄稿：\n"
                + json.dumps(source_pages, ensure_ascii=False, separators=(",", ":"))
            )
            if progress_callback:
                progress_callback(52, "符號核對完成，正在只依原文整理重點卡。")
            draft = _call_openai_json(
                name="study_recall_grounded_draft",
                schema=card_schema,
                content=[{"type": "input_text", "text": organizer_prompt}],
                timeout=300,
                max_output_tokens=20000,
            )
            verifier_prompt = (
                "你是跨學科筆記忠實度審核員。請重新查看隨附原始圖片，逐句核對 draft、source_pages 與圖片，輸出修訂後的完整 JSON。"
                "圖片是最終依據：先修正 OCR 對係數、正負號、上下標、矩陣分量或 LaTeX 的誤讀，再確認卡片與原圖一致。"
                "逐一核對名稱、數值、單位、符號、範圍、條件、例外、表格欄位與步驟；一般知識卡的具體內容必須原樣保留，不可改成更一般或更特殊的形式。例題方法卡可以省略非必要題目數值並抽出來源已展示的解題流程，但不得改變方法成立的條件或增加來源沒有的步驟。"
                "對來源中明確寫出的公式、方程式、反應式、統計式、程式碼或推導做相應的內部一致性檢查；若結果與來源不相容，依原圖修正，原圖本身確實錯誤且允許校正時才建立 correction。不要把數學驗證規則套用到沒有公式的科目。程式碼必須維持帶語言名稱的 Markdown fenced code block，禁止改成 LaTeX，並逐字核對縮排、大小寫、括號、分號、陣列索引、指標與運算子。"
                "刪除任何無法由來源直接支持的句子、卡片、口訣或關聯；不得自行補上更完整的課本知識。"
                "每個 source_refs.evidence 必須仍是對應 transcription 中逐字連續出現、而且能直接在原圖看到的片段，錯頁或不完全相同就修正引用。evidence 不得以括號描述頁面位置、圖示、照片、顏色、原稿或內容大意；遇到這類非原文字句要改成真正可見的連續文字。已由上下文唯一補全且列有高／中信心紀錄的文字可正常保留；只有仍含〔無法推定〕或沒有可靠來源的卡片才刪除。"
                "正文與 search_keywords 都應優先保留原筆記實際使用的詞彙、記號和條件順序；不要用外部同義詞取代。search_keywords 只保留能在卡片、source_refs 或高信心校正結果中找到的搜尋詞。"
                "保留清楚可辨的原筆記公式。correction 只有在錯誤毫無歧義且允許校正時才能保留，否則恢復原文或刪除該項。"
                "再次排除重複句、公式的文字重述、教學口吻與不影響複習的補充。所有卡片文字欄位與 summary 只要仍含『筆記給出／註明／記載／指出／提到』及其同義寫法，就視為審核不合格並改寫為直接知識陳述。recall_cue 不得洩露 core_summary，也不得寫成問題；reasoning_steps 與 common_confusion 沒有直接來源支持時必須留空。summary 只能用最多 5 個短句直接摘要核心結論。不要輸出審核說明。"
                "另外檢查所有例題卡：card_type 必須為 example，example_problem 必須包含一個具體、可直接作答的完整題目示例與明確要求，example_method 只留來源真正使用的可重用判斷，reasoning_steps 只留操作順序，simple_example 留空。三者不可互相重複，也不能創造新技巧。concept 卡只有在原文確實提供例子時才保留 simple_example，否則留空。"
                "所有聲稱某性質不成立的例題都必須重新驗算前提與運算：測試輸入若不符合該性質的前提、計算錯誤，或實際上反而滿足該性質，就不能當成反例。來源清楚且允許校正時，必須依來源已有的映射、定義與直接計算修正成正確卡片並記入 correction；只有原圖與前後文仍無法唯一推定時才略過。"
                "先逐段清點 source_pages 與圖片中的標題、定義、公式、例題方法與結論；draft 漏掉但圖片或高／中信心的上下文補全仍足以確認核心觀念時，必須補回卡片。局部字跡不清不代表整段都要刪除；先利用重複記號、句法、公式結構與相鄰推導補全，只略過仍有多種合理結果且會影響正確性的字元。來源中清楚可辨但結論錯誤的觀念必須修正後補回，不得視為無來源。"
                f"各頁資訊量參考值為 {json.dumps(page_card_quotas, ensure_ascii=False)}。逐頁檢查 source_refs，優先讓有公式、定義、方法或例題策略的頁面得到代表卡；一般補充內容可併入相關卡片，不得用無關卡片虛報引用。"
                f"逐一核對 coverage_checklist={json.dumps(coverage_checklist, ensure_ascii=False, separators=(',', ':'))}；priority=required 的 id 優先保留，supporting id 可合併；coverage_ids 的來源 evidence 仍必須與該段重疊。"
                "逐張檢查 topic：不得等於任何科目名稱或空泛大分類，必須使用能涵蓋同一知識對象多張卡片的母主題；同一對象的定義、性質、操作與例題共用 topic，明顯不同資料結構、理論或章節才分開。依全部內容整理成 2 至 6 個群組。"
                f"\nallow_corrections={str(allow_corrections).lower()}"
                "\nsource_pages=" + json.dumps(source_pages, ensure_ascii=False, separators=(",", ":"))
                + "\ndraft=" + json.dumps(draft, ensure_ascii=False, separators=(",", ":"))
            )
            if progress_callback:
                progress_callback(70, "正在逐句核對來源，移除無依據的延伸內容。")
            verifier_content: List[Dict[str, Any]] = [{"type": "input_text", "text": verifier_prompt}]
            for _filename, image_bytes, mime_type in images:
                verifier_content.append(
                    {
                        "type": "input_image",
                        "image_url": f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}",
                        "detail": "high",
                    }
                )
            verified = _call_openai_json(
                name="study_recall_grounded_verified",
                schema=card_schema,
                content=verifier_content,
                timeout=300,
                max_output_tokens=20000,
            )
            formula_audit_instruction = (
                "你是最後一道跨學科內容與忠實度稽核。根據原始圖片、source_pages 與 verified，輸出通過稽核的完整 JSON，不要輸出稽核過程。"
                "逐卡檢查內容是否能由原圖直接支持，並檢查名稱、數值、單位、條件、範圍、符號、步驟、表格與結論是否一致。原圖有公式、方程式、反應式、統計式或程式碼時，才對其做相應的結構與內部一致性檢查；沒有這些內容時不要自行補公式。"
                "程式碼與虛擬碼必須保留為帶語言名稱的 Markdown fenced code block，禁止轉成 LaTeX；保留縮排、大小寫、括號、分號、陣列索引、指標、運算子與註解。行內識別字或短指令使用單反引號。"
                "所有推導、計算、分類、因果或比較都必須符合來源中明示的前提。來源本身清楚但內容有誤，且 allow_corrections=true 時，必須做最小必要修正、保留為正確卡片並記入 correction，不得以刪卡代替校正；只有結果完全沒有來源支持時才能刪除。"
                "重新對照圖片，OCR 與圖片不同時以圖片為準；這類 OCR、上下文補字、漏字、標點或 LaTeX 排版修復不算筆記內容校正，不得建立 correction。correction 只記錄原圖筆記本身可直接驗證的知識、公式、計算或結論錯誤。若某個文字、數字或符號無法直接辨認，先以同頁前後文、重複記號、公式結構與相鄰推導做最小補全；已被唯一推定且有高／中信心紀錄時保留。仍有多種合理結果時才刪除受影響的卡片，不可在卡片中寫模糊或待確認說明。"
                "除例題方法卡可從來源已展示的操作整理成可重用步驟外，禁止在具體內容與一般內容之間擅自轉換，禁止加入課本延伸、跨科聯想或來源沒有的教學解釋。方法卡只能描述 source_refs 實際出現的判斷與操作，不得替操作新增來源沒寫的理論名稱、資料結構、演算法、定理、空間分類、證明或另一套解法。"
                "同時刪除原筆記未明說的通則、額外定義、延伸例子與教學詮釋；concept 卡 simple_example 只能保留來源明示的例子，沒有時留空。source_refs.evidence 仍必須逐字存在於對應 transcription，並且是原圖可見字元，不得是頁面位置、圖示或內容大意的描述；"
                "優先保留來源原本的專有名詞、關鍵短語、變數與條件順序，只做最小必要的模糊字補全或可直接驗證校正。search_keywords 必須是可從卡片、source_refs 或高信心校正結果直接找到的原始搜尋詞，不得自行擴充同義詞。"
                "若修正結果是由來源中的明確內容直接得到，可引用包含該內容的原文。卡片正文只寫可複習的來源內容，絕對不可提到筆記、原稿、OCR、核對、稽核或修正過程；"
                "每張卡只輸出一個核心觀念所需的最短完整內容：recall_cue 提供不洩漏答案的關鍵詞，core_summary 放最需要記住的結論，explanation 放條件與脈絡，reasoning_steps 只放來源已有的必要推導或操作。不得輸出重複結論、驗證代回、同義重述、重要性說明與教學填充語。所有文字欄位與 summary 禁止出現『筆記給出』『筆記註明』『筆記記載／紀載』『根據筆記』『原文指出』等來源敘事；發現時必須先改寫，不能原樣輸出。"
                "來源保留範圍、OCR／轉錄／核對流程、可見字元與符號完整性等聲明一律不是知識內容；整張卡若在說明這些事情就直接刪除，不可換句話說後保留。"
                "例題卡的 card_type 必須為 example，並把具體且可直接作答的完整必要題設、可重用解法、操作順序分別寫入 example_problem、example_method、reasoning_steps，simple_example 留空；可刪冗長背景但不可省略數值、給定式、條件或要求。若來源沒有足夠過程可整理方法，仍保留該例題卡，example_method 填『來源未提供完整解法』，不可臆造解法。一般卡為 concept，example_problem 與 example_method 留空，simple_example 只有來源存在例子時才填寫。"
                "對每個反例或性質判定例，逐項驗證輸入是否符合欲檢查性質的前提，並重新計算映射結果；來源清楚且 allow_corrections=true 時，錯誤反例、錯誤等號或錯誤結論必須校正並記錄，不能刪除該觀念。只有影像與前後文都不足以唯一判定正確內容時才略過。"
                f"稽核前先建立 source_pages 的內容清單，逐段比對 verified；依目前資訊量，本批建議至少約 {audit_card_floor} 張互不重複的候選卡片，但卡片數量不設上限，也不可為達成張數拆出空泛卡。圖片或高／中信心補全仍可確認核心定義、公式、方法或結論的段落若遭漏掉，應優先補回；同一張卡若混入可各自複習的獨立定義、方法或章節，才需要拆卡。局部不清先做最小上下文補全，不得刪除其餘可確認觀念；若清楚觀念本身寫錯且允許校正，補回修正後的正確卡片。"
                f"各頁資訊量參考值為 {json.dumps(page_card_quotas, ensure_ascii=False)}。逐頁計數 source_refs，優先補回缺頁的公式、定義、方法、例題策略與結論；一般補充段落可以併入同觀念卡。不可用與該頁無關的卡片虛報引用。"
                f"輸出前再核對 coverage_checklist={json.dumps(coverage_checklist, ensure_ascii=False, separators=(',', ':'))}；priority=required 的區塊優先進入卡片，supporting 區塊可合併；coverage_ids 與 evidence 必須實際對應。"
                "topic 必須是科目內可涵蓋同一知識對象的母主題，不得使用六科科目名稱、整份筆記標題或『綜合重點』等空泛文字，也不得用斜線、頓號或『與』把無直接從屬關係的分類硬併在一起；同一對象的定義、性質、操作與例題必須放在一起，依最後保留卡片整理成 2 至 6 個群組。"
                "修正過程只放在 correction。summary 最多 5 個完整短句，只可摘要最後保留的卡片，禁止句中截斷或用空公式結尾。"
                f"\nallow_corrections={str(allow_corrections).lower()}"
            )
            if progress_callback:
                progress_callback(78, "正在驗證上下文補全與各科內容一致性，排除仍無法判定的片段。")
            verified_cards = [
                card
                for card in (verified.get("key_concepts") or [])
                if isinstance(card, dict)
            ]

            def audit_formula_card_batch(
                batch: List[Dict[str, Any]],
                *,
                batch_label: str,
            ) -> List[Dict[str, Any]]:
                _raise_if_study_upload_cancelled()
                if not batch:
                    return []
                relevant_indices = sorted(
                    {
                        int(source_ref.get("image_index") or 0)
                        for card in batch
                        for source_ref in (card.get("source_refs") or [])
                        if isinstance(source_ref, dict)
                        and 1 <= int(source_ref.get("image_index") or 0) <= len(images)
                    }
                )
                if not relevant_indices:
                    relevant_indices = list(range(1, len(images) + 1))
                relevant_pages = [
                    page
                    for page in source_pages
                    if int(page.get("image_index") or 0) in relevant_indices
                ]
                batch_payload = {
                    "detected_topic": str(verified.get("detected_topic") or "")[:80],
                    "summary": str(verified.get("summary") or "")[:900],
                    "key_concepts": batch,
                }
                batch_schema = json.loads(json.dumps(card_schema))
                batch_schema["properties"]["key_concepts"]["minItems"] = 0
                batch_schema["properties"]["key_concepts"]["maxItems"] = len(batch)
                batch_prompt = (
                    formula_audit_instruction
                    + "\n這是完整卡片集合中的獨立稽核批次。只能核對、最小修正或刪除 batch_cards 已有卡片；"
                    "不得新增其他卡片、不得補做其他頁內容、不得把兩張卡合成一張。輸出順序必須保持與 batch_cards 相同。"
                    f"\nbatch_label={batch_label}"
                    "\nsource_pages="
                    + json.dumps(relevant_pages, ensure_ascii=False, separators=(",", ":"))
                    + "\nbatch_cards="
                    + json.dumps(batch_payload, ensure_ascii=False, separators=(",", ":"))
                )
                batch_content: List[Dict[str, Any]] = [
                    {"type": "input_text", "text": batch_prompt}
                ]
                for image_index in relevant_indices:
                    _filename, image_bytes, mime_type = images[image_index - 1]
                    batch_content.extend(
                        [
                            {
                                "type": "input_text",
                                "text": f"此圖是 source_pages 的 image_index={image_index}。",
                            },
                            {
                                "type": "input_image",
                                "image_url": f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}",
                                "detail": "high",
                            },
                        ]
                    )
                try:
                    result = _call_openai_json(
                        name=f"study_recall_formula_audited_{batch_label}",
                        schema=batch_schema,
                        content=batch_content,
                        timeout=300,
                        reasoning_effort="medium",
                        max_output_tokens=max(4200, min(10000, len(batch) * 1800)),
                    )
                    output_cards = [
                        card
                        for card in (result.get("key_concepts") or [])
                        if isinstance(card, dict)
                    ]
                    if len(output_cards) <= len(batch):
                        return output_cards
                    raise ValueError("Formula audit returned extra cards")
                except (requests.RequestException, ValueError, TypeError) as exc:
                    if len(batch) > 1:
                        midpoint = max(1, len(batch) // 2)
                        app.logger.warning(
                            "Formula audit batch %s failed; splitting %s cards: %s",
                            batch_label,
                            len(batch),
                            exc,
                        )
                        return audit_formula_card_batch(
                            batch[:midpoint],
                            batch_label=f"{batch_label}a",
                        ) + audit_formula_card_batch(
                            batch[midpoint:],
                            batch_label=f"{batch_label}b",
                        )
                    app.logger.warning(
                        "Formula audit for one card failed; preserving verified card %s: %s",
                        str(batch[0].get("concept") or "")[:80],
                        exc,
                    )
                    return list(batch)

            audited_cards: List[Dict[str, Any]] = []
            formula_batches = [
                verified_cards[index : index + 6]
                for index in range(0, len(verified_cards), 6)
            ]
            for batch_index, batch in enumerate(formula_batches, start=1):
                audited_cards.extend(
                    audit_formula_card_batch(
                        batch,
                        batch_label=f"batch_{batch_index}",
                    )
                )
                if progress_callback:
                    progress_callback(
                        78 + round(batch_index / max(1, len(formula_batches)) * 2),
                        f"已完成第 {batch_index}/{len(formula_batches)} 批內容稽核。",
                    )
            audited = {
                "detected_topic": str(verified.get("detected_topic") or "")[:80],
                "summary": str(verified.get("summary") or "")[:900],
                "key_concepts": audited_cards or verified_cards,
            }
            _enrich_study_card_coverage_ids(audited, source_pages)
            initial_coverage_metrics = _study_recall_coverage_metrics(audited, source_pages)
            if _study_recall_coverage_needs_repair(audited, source_pages):
                coverage_gaps = _study_recall_coverage_gaps(audited, source_pages)
                if progress_callback:
                    progress_callback(
                        81,
                        "正在補強缺少的公式、定義、方法與例題策略，已完成內容會完整保留。",
                    )
                _raise_if_study_upload_cancelled()
                coverage_repair_schema = json.loads(json.dumps(card_schema))
                coverage_repair_prompt = (
                    "你是筆記資訊補強編輯。current_cards 已通過原圖內容審核；請完整保留其中正確、互不重複的卡片，只補強 coverage_gaps 中真正重要的遺漏內容並輸出完整 JSON。example_items 是逐題不可遺漏清單；其中每個 id 都必須新增或修正為一張獨立的 card_type=example 卡，不能以一般 concept 卡代替，也不能把不同 id 合併。"
                    "只能使用 source_pages 與原圖已有內容，不得補充課本知識。priority=required 的公式、定義、方法、例題策略與結論優先補回；supporting 一般敘述可併入最相關卡片，不必獨立成卡。"
                    "同一觀念的成對定義、連續推導或同方法例題可以合併成資訊完整的一張卡；不同章節或不同複習目標才拆卡。卡片數量不設上限，由你依來源資訊與複習目標決定。"
                    "source_refs.evidence 必須逐字連續存在於對應頁 transcription，並且能在原圖直接看到，不得用頁面位置、圖示或內容大意取代原文；coverage_ids 只能標記該卡實際整理的區塊，不可用無關卡片虛報。"
                    + (
                        "visual coverage item 必須在 visual_refs 引用相同 region_id；純視覺卡可以沒有 source_refs，"
                        "但一定要有 visual_refs。保留 current_cards 已有的 visual_refs，不得在補強時移除。"
                        if visual_note_pipeline_enabled
                        else ""
                    )
                    +
                    "例題必須使用 card_type=example，example_problem 要保留可直接作答的具體題設、數值／給定式、條件與要求，並和可重用解法 example_method、操作 reasoning_steps 分欄；simple_example 留空。若來源未提供解法，example_method 填『來源未提供完整解法』，不可臆造。一般卡使用 card_type=concept，兩個 example 欄位留空；simple_example 只有來源明示例子時才填寫，否則留空。"
                    "來源含程式碼或虛擬碼時，完整程式、函式、類別或連續兩行以上操作保留為帶語言名稱的 Markdown fenced code block，禁止轉成 LaTeX；行內識別字或短指令使用單反引號。"
                    "卡片正文不得出現來源敘事、稽核說明、外部延伸或來源沒有的術語。錯誤只做可由原圖內容直接驗證的最小修正。不可因卡片數量而刪除重點。"
                    f"\nallow_corrections={str(allow_corrections).lower()}"
                    f"\npage_information_targets={json.dumps(coverage_plan['page_quotas'], ensure_ascii=False)}"
                    f"\ncurrent_coverage_metrics={json.dumps(initial_coverage_metrics, ensure_ascii=False, separators=(',', ':'))}"
                    f"\ncoverage_gaps={json.dumps(coverage_gaps, ensure_ascii=False, separators=(',', ':'))}"
                    f"\ncoverage_checklist={json.dumps(coverage_checklist, ensure_ascii=False, separators=(',', ':'))}"
                    "\nsource_pages=" + json.dumps(source_pages, ensure_ascii=False, separators=(",", ":"))
                    + "\ncurrent_cards=" + json.dumps(audited, ensure_ascii=False, separators=(",", ":"))
                )
                coverage_repair_content: List[Dict[str, Any]] = [
                    {"type": "input_text", "text": coverage_repair_prompt}
                ]
                for _filename, image_bytes, mime_type in images:
                    coverage_repair_content.append(
                        {
                            "type": "input_image",
                            "image_url": f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}",
                            "detail": "high",
                        }
                    )
                try:
                    repaired_audited = _call_openai_json(
                        name="study_recall_coverage_repair",
                        schema=coverage_repair_schema,
                        content=coverage_repair_content,
                        timeout=240,
                        reasoning_effort="medium",
                        max_output_tokens=16000,
                    )
                    _enrich_study_card_coverage_ids(repaired_audited, source_pages)
                    repaired_metrics = _study_recall_coverage_metrics(repaired_audited, source_pages)
                    initial_rank = (
                        initial_coverage_metrics["example_ratio"],
                        initial_coverage_metrics["required_ratio"],
                        initial_coverage_metrics["page_ratio"],
                        initial_coverage_metrics["quality_score"],
                        initial_coverage_metrics["overall_ratio"],
                    )
                    repaired_rank = (
                        repaired_metrics["example_ratio"],
                        repaired_metrics["required_ratio"],
                        repaired_metrics["page_ratio"],
                        repaired_metrics["quality_score"],
                        repaired_metrics["overall_ratio"],
                    )
                    if repaired_rank > initial_rank:
                        audited = repaired_audited
                    else:
                        app.logger.warning(
                            "Study-note coverage repair did not improve reliable coverage; preserving original cards: before=%s after=%s",
                            initial_coverage_metrics,
                            repaired_metrics,
                        )
                except (requests.RequestException, ValueError, TypeError) as exc:
                    app.logger.warning(
                        "Study-note coverage repair failed; preserving the already validated cards: %s",
                        exc,
                    )

            missing_example_items = list(
                _study_recall_coverage_gaps(audited, source_pages).get("example_items") or []
            )
            if missing_example_items:
                if progress_callback:
                    progress_callback(
                        82,
                        f"正在逐題補回 {len(missing_example_items)} 個遺漏例題，並重新核對數字符號。",
                    )
                existing_titles = [
                    str(card.get("concept") or "")[:80]
                    for card in audited.get("key_concepts") or []
                    if isinstance(card, dict) and str(card.get("concept") or "").strip()
                ]
                for missing_index, missing_item in enumerate(missing_example_items, start=1):
                    _raise_if_study_upload_cancelled()
                    target_id = str(missing_item.get("id") or "")
                    try:
                        target_image_index = int(missing_item.get("image_index") or 0)
                    except (TypeError, ValueError):
                        continue
                    if not target_id or not (1 <= target_image_index <= len(images)):
                        continue
                    relevant_page = next(
                        (
                            page
                            for page in source_pages
                            if int(page.get("image_index") or 0) == target_image_index
                        ),
                        None,
                    )
                    if not relevant_page:
                        continue
                    recovery_schema = json.loads(json.dumps(card_schema))
                    recovery_schema["properties"]["key_concepts"]["minItems"] = 1
                    recovery_schema["properties"]["key_concepts"]["maxItems"] = 1
                    recovery_prompt = (
                        "你是遺漏例題的逐題補卡員。只處理 missing_example，不得重寫、刪除或合併既有卡片。"
                        "輸出恰好一張 card_type=example 卡，coverage_ids 必須只包含 missing_example.id。"
                        "example_problem 要完整保留可直接作答所需的題號、所有數字、係數、向量、矩陣、函數、"
                        "條件與要求；example_method 與 reasoning_steps 只整理來源實際出現的解法。"
                        "若來源只有題目沒有解法，example_method 填『來源未提供完整解法』，不可自行補解。"
                        "請把完整原圖、彩色銳化放大版與灰階高對比放大版交叉比較，逐一確認 0/6/8/9、1/7、3/5、"
                        "正負號、小數點、分數線、括號、上下標、矩陣元素與運算符；增強圖有衝突時以完整原圖和公式"
                        "內部一致性為準。source_refs.evidence 必須逐字連續存在於 page.transcription。"
                        "其餘欄位遵守既有卡片 schema；不得增加來源外知識、另一種解法或不在圖片中的數值。"
                        f"\nallow_corrections={str(allow_corrections).lower()}"
                        f"\nmissing_example={json.dumps(missing_item, ensure_ascii=False, separators=(',', ':'))}"
                        f"\npage={json.dumps(relevant_page, ensure_ascii=False, separators=(',', ':'))}"
                        f"\nexisting_titles={json.dumps(existing_titles, ensure_ascii=False, separators=(',', ':'))}"
                    )
                    _filename, image_bytes, mime_type = images[target_image_index - 1]
                    recovery_content: List[Dict[str, Any]] = [
                        {"type": "input_text", "text": recovery_prompt},
                        {
                            "type": "input_image",
                            "image_url": f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}",
                            "detail": "high",
                        },
                    ]
                    for crop in review_zoom_crops(image_bytes):
                        recovery_content.extend(
                            [
                                {
                                    "type": "input_text",
                                    "text": f"例題數字符號核對用：{crop['label']}。",
                                },
                                {
                                    "type": "input_image",
                                    "image_url": f"data:image/jpeg;base64,{base64.b64encode(crop['bytes']).decode('ascii')}",
                                    "detail": "high",
                                },
                            ]
                        )
                    recovered_card: Optional[Dict[str, Any]] = None
                    for recovery_attempt in range(2):
                        try:
                            recovery_result = _call_openai_json(
                                name=f"study_recall_missing_example_{missing_index}_{recovery_attempt + 1}",
                                schema=recovery_schema,
                                content=recovery_content,
                                timeout=240,
                                reasoning_effort="medium",
                                max_output_tokens=5200,
                            )
                            _enrich_study_card_coverage_ids(recovery_result, source_pages)
                            candidate = next(
                                (
                                    card
                                    for card in recovery_result.get("key_concepts") or []
                                    if isinstance(card, dict)
                                    and card.get("card_type") == "example"
                                    and target_id in (card.get("coverage_ids") or [])
                                    and str(card.get("example_problem") or "").strip()
                                ),
                                None,
                            )
                            if candidate is not None:
                                recovered_card = candidate
                                break
                        except (requests.RequestException, ValueError, TypeError) as exc:
                            app.logger.warning(
                                "Missing example recovery %s attempt %s failed: %s",
                                target_id,
                                recovery_attempt + 1,
                                exc,
                            )
                    if recovered_card is not None:
                        audited.setdefault("key_concepts", []).append(recovered_card)
                        existing_titles.append(str(recovered_card.get("concept") or "")[:80])
                    else:
                        app.logger.warning(
                            "Missing example recovery remained incomplete for %s",
                            target_id,
                        )
                    if progress_callback:
                        progress_callback(
                            82 + round(missing_index / max(1, len(missing_example_items)) * 2),
                            f"已完成 {missing_index}/{len(missing_example_items)} 個遺漏例題的逐題核對。",
                        )
                _enrich_study_card_coverage_ids(audited, source_pages)

            if not _study_recall_page_coverage_met(audited, source_pages):
                app.logger.warning(
                    "Study-note coverage has non-blocking gaps; preserving reliable cards: %s",
                    _study_recall_coverage_metrics(audited, source_pages),
                )

            card_indices: List[int] = []
            example_indices: List[int] = []
            for index, card in enumerate(audited.get("key_concepts") or []):
                if not isinstance(card, dict):
                    continue
                card_indices.append(index)
                evidence_text = " ".join(
                    str(source_ref.get("evidence") or "")
                    for source_ref in card.get("source_refs") or []
                    if isinstance(source_ref, dict)
                )
                source_marks_example = bool(
                    re.search(
                        r"(?:\bEx(?:ample)?\s*\.|例題|範例|算例|反例|題目\s*[：:])",
                        evidence_text,
                        flags=re.IGNORECASE,
                    )
                )
                if card.get("card_type") == "example" or source_marks_example:
                    card["card_type"] = "example"
                    example_indices.append(index)

            if card_indices:
                example_schema = {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["cards"],
                    "properties": {
                        "cards": {
                            "type": "array",
                            "minItems": len(card_indices),
                            "maxItems": len(card_indices),
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": [
                                    "concept_index",
                                    "card_type",
                                    "simple_example",
                                    "example_problem",
                                    "example_method",
                                ],
                                "properties": {
                                    "concept_index": {"type": "integer", "minimum": 0, "maximum": max(card_indices)},
                                    "card_type": {"type": "string", "enum": ["concept", "example"]},
                                    "simple_example": {"type": "string", "maxLength": 360},
                                    "example_problem": {"type": "string", "maxLength": 360},
                                    "example_method": {"type": "string", "maxLength": 280},
                                },
                            },
                        }
                    },
                }
                example_catalog = [
                    {
                        "concept_index": index,
                        "concept": audited["key_concepts"][index].get("concept") or "",
                        "card_type": audited["key_concepts"][index].get("card_type") or "concept",
                        "core_summary": audited["key_concepts"][index].get("core_summary") or "",
                        "explanation": audited["key_concepts"][index].get("explanation") or "",
                        "current_simple_example": audited["key_concepts"][index].get("simple_example") or "",
                        "current_problem": audited["key_concepts"][index].get("example_problem") or "",
                        "current_method": audited["key_concepts"][index].get("example_method") or "",
                        "current_steps": audited["key_concepts"][index].get("reasoning_steps") or [],
                        "source_evidence": [
                            source_ref.get("evidence") or ""
                            for source_ref in audited["key_concepts"][index].get("source_refs") or []
                            if isinstance(source_ref, dict)
                        ],
                    }
                    for index in card_indices
                ]
                example_prompt = (
                    "你是重點卡例子格式編輯。只根據 source_evidence 與卡片既有內容，替每張卡整理一個清楚、短而具體的示例，不得加入來源外的知識。"
                    "card_type=example 的卡不可改成 concept。example_problem 必須是一個拿到後可以直接開始作答的具體題目示例：保留所有必要數值、向量、矩陣、函數、給定式、條件及明確要求；可以刪除不影響作答的背景敘述，但不可只留下抽象的『判斷是否成立』而沒有實際題設，也不可包含完整運算或最終答案。simple_example 必須留空。"
                    "example_method 用 1 至 2 個短句說明來源實際採用、可重用的判斷或策略，不得編號、不得寫『步驟一』，也不得直接重複 reasoning_steps；"
                    "既有 reasoning_steps 已在前一階段完成，不要在這次輸出重寫。"
                    "card_type=concept 的卡不可改成 example。simple_example 只可整理 source_evidence 原有的例子；來源沒有例子時留空，不得創造數值、情境、公式代入或程式。example_problem、example_method 與 reasoning_steps 維持原本內容，其中兩個 example 欄位必須留空。"
                    "來源只有最終結果而沒有方法時，不可發明新解法；可把來源明示的直接代入、列式、比較或計算寫成最小方法。"
                    "逐題重新檢查題設前提、函數或映射輸入、維度、正負號、上下標、代入、算術、等號與結論。若用反例否定性質，必須先確認測試值滿足該性質要求的關係；例如檢查齊次性 f(-u)=-f(u) 時，左側輸入必須真的是 -u。"
                    "內容與公式已由前一階段校正；本次只整理 simple_example、example_problem、example_method 三欄，不可重寫其他卡片內容。"
                    "不得加入來源沒有的定理、術語、公式、數值或另一套解法。數學式使用 KaTeX LaTeX，行內用 \\( ... \\)，獨立式用 \\[ ... \\]。"
                    "程式碼與虛擬碼不得轉成 LaTeX；完整程式、函式、類別或連續兩行以上操作使用帶語言名稱的 Markdown fenced code block，並保留縮排與所有程式符號。行內識別字或短指令使用單反引號。"
                    "每個指定 concept_index 必須恰好輸出一次。example 卡的 example_problem 與 example_method 必須非空；concept 卡允許 simple_example 為空字串。只輸出 schema JSON。\n\n"
                    f"allow_corrections={str(allow_corrections).lower()}\n"
                    + json.dumps(example_catalog, ensure_ascii=False, separators=(",", ":"))
                )
                prepared_examples: Dict[int, Dict[str, Any]] = {}
                for example_attempt in range(2):
                    try:
                        example_result = _call_openai_json(
                            name="study_recall_card_examples",
                            schema=example_schema,
                            content=[{"type": "input_text", "text": example_prompt}],
                            timeout=240,
                            max_output_tokens=12000,
                        )
                    except (requests.RequestException, ValueError, TypeError) as exc:
                        app.logger.warning(
                            "Study-note example presentation pass failed; preserving usable card content: %s",
                            exc,
                        )
                        if example_attempt == 0:
                            continue
                        break
                    for item in example_result.get("cards") or []:
                        if not isinstance(item, dict):
                            continue
                        concept_index = int(item.get("concept_index", -1))
                        if concept_index not in card_indices or concept_index in prepared_examples:
                            continue
                        expected_type = "example" if concept_index in example_indices else "concept"
                        if item.get("card_type") != expected_type:
                            continue
                        problem = str(item.get("example_problem") or "").strip()
                        method = str(item.get("example_method") or "").strip()
                        simple_example = str(item.get("simple_example") or "").strip()
                        if (
                            (expected_type == "example" and (not problem or not method or simple_example))
                            or (expected_type == "concept" and (problem or method))
                            or (_study_text_quality_issue(problem, max_length=420) if problem else None)
                            or (_study_text_quality_issue(method, max_length=340) if method else None)
                            or (_study_text_quality_issue(simple_example, max_length=420) if simple_example else None)
                        ):
                            continue
                        prepared_examples[concept_index] = {
                            "simple_example": simple_example if expected_type == "concept" else "",
                            "example_problem": problem,
                            "example_method": method,
                        }
                    if set(prepared_examples) == set(card_indices):
                        break
                    missing_examples = sorted(set(card_indices) - set(prepared_examples))
                    example_prompt += (
                        f"\n\n前一次缺少或格式不合格的 concept_index={missing_examples}。"
                        "下一次仍輸出完整 cards 陣列，並確保 example 卡的具體題目與方法、concept 卡的簡單例子都非空且公式格式完整。"
                    )
                if set(prepared_examples) != set(card_indices):
                    missing_examples = sorted(set(card_indices) - set(prepared_examples))
                    app.logger.warning(
                        "Study-note example presentation remained incomplete; preserving existing valid card sections: %s",
                        missing_examples,
                    )
                    for index in missing_examples:
                        current_card = audited["key_concepts"][index]
                        if index in example_indices and not (
                            str(current_card.get("example_problem") or "").strip()
                            and str(current_card.get("example_method") or "").strip()
                        ):
                            current_card["card_type"] = "concept"
                            current_card["example_problem"] = ""
                            current_card["example_method"] = ""
                for index, prepared_example in prepared_examples.items():
                    audited["key_concepts"][index].update(prepared_example)
            latex_schema = {
                "type": "object",
                "additionalProperties": False,
                "required": ["summary", "cards"],
                "properties": {
                    "summary": {"type": "string", "maxLength": 900},
                    "cards": {
                        "type": "array",
                        "minItems": len(audited.get("key_concepts") or []),
                        "maxItems": len(audited.get("key_concepts") or []),
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["concept_index", "concept", "topic", "card_type", "recall_cue", "core_summary", "explanation", "simple_example", "example_problem", "example_method", "reasoning_steps", "common_confusion", "memory_hint", "correction_applied", "correction_original", "correction_reason"],
                            "properties": {
                                "concept_index": {
                                    "type": "integer",
                                    "minimum": 0,
                                    "maximum": max(0, len(audited.get("key_concepts") or []) - 1),
                                },
                                "concept": {"type": "string", "maxLength": 80},
                                "topic": {"type": "string", "maxLength": 48},
                                "card_type": {"type": "string", "enum": ["concept", "example"]},
                                "recall_cue": {"type": "string", "maxLength": 160},
                                "core_summary": {"type": "string", "maxLength": 280},
                                "explanation": {"type": "string", "maxLength": 620},
                                "simple_example": {"type": "string", "maxLength": 360},
                                "example_problem": {"type": "string", "maxLength": 360},
                                "example_method": {"type": "string", "maxLength": 280},
                                "reasoning_steps": {
                                    "type": "array",
                                    "maxItems": 4,
                                    "items": {"type": "string", "maxLength": 180},
                                },
                                "common_confusion": {"type": "string", "maxLength": 180},
                                "memory_hint": {"type": "string", "maxLength": 120},
                                "correction_applied": {"type": "boolean"},
                                "correction_original": {"type": "string", "maxLength": 240},
                                "correction_reason": {"type": "string", "maxLength": 300},
                            },
                        },
                    },
                },
            }
            audited_cards = audited.get("key_concepts") if isinstance(audited, dict) else None
            if not isinstance(audited_cards, list) or not audited_cards:
                raise ValueError("Missing audited cards")
            latex_input = {
                "summary": audited.get("summary") or "",
                "cards": [
                    {
                        "concept_index": index,
                        "concept": item.get("concept") or "",
                        "topic": item.get("topic") or "",
                        "card_type": item.get("card_type") or "concept",
                        "recall_cue": item.get("recall_cue") or "",
                        "core_summary": item.get("core_summary") or "",
                        "explanation": item.get("explanation") or "",
                        "simple_example": item.get("simple_example") or "",
                        "example_problem": item.get("example_problem") or "",
                        "example_method": item.get("example_method") or "",
                        "reasoning_steps": item.get("reasoning_steps") or [],
                        "common_confusion": item.get("common_confusion") or "",
                        "memory_hint": item.get("memory_hint") or "",
                        "source_evidence": [
                            ref.get("evidence") or ""
                            for ref in item.get("source_refs") or []
                            if isinstance(ref, dict)
                        ],
                    }
                    for index, item in enumerate(audited_cards)
                    if isinstance(item, dict)
                ],
            }
            latex_prompt = (
                "你是最後的筆記呈現校對器。先根據每張卡的 source_evidence 判斷它是一般知識還是例題。一般知識卡必須刪除 source_evidence 不支持的補充、證明與術語，只保留來源內容並修正 LaTeX；除下述高信心錯誤校正外，不可改變知識內容。例題卡必須同時保留可直接作答的具體題目示例與可重用解題方法：example_problem 保留必要數值、給定式、條件和明確要求，只刪不影響作答的背景敘述及最終答案；example_method 與 reasoning_steps 保留來源實際展示的關鍵判斷與必要操作。"
                "例題方法不得加入 source_evidence 沒有使用的定理、術語或步驟，也不得替來源中的操作命名新的理論、演算法、資料結構、空間分類或證明方式。每張卡完成後逐一比對名詞：來源只有公式或操作時就直接保留公式或操作，不得補上課本分類名稱；例如來源只有 rank(A)=n 或 rank(A)=m，就不可額外稱為滿列秩、滿行秩或滿柱秩。其他科目也使用相同規則。只有最小反例卡可保留否定性質必需的數值。若任何卡片的前提、定義、計算、矩陣維度、等號或結論錯誤，但可由 source_evidence 中已有的定義、公式或直接計算明確判定，allow_corrections=true 時必須做最小必要修正、輸出正確卡片，不得刪除。"
                "修正時 correction_applied=true，correction_original 逐字放入來源中最小的錯誤片段，correction_reason 簡述可直接驗證的原因；未修正時 correction_applied=false 且兩個字串留空。OCR 誤讀、上下文補字、漏字、標點、用詞潤飾與 LaTeX 排版修復不算筆記內容錯誤，correction_applied 必須是 false。不得為修正補入來源沒有使用的新觀念、定理或另一套解法。已由前後文唯一補全且有高／中信心紀錄的文字必須正常保留；只有仍含〔無法推定〕的片段才維持略過結果。"
                "summary 可重新整理成最後保留卡片的核心總結，但不得列出例題的具體答案，也不得加入新知識。concept 改成不含公式、變數、題號或題目數值的簡短純中文名稱，忠實描述原卡核心；topic 可重新整理成科目內精確的細分觀念。recall_cue 只保留來源已有的 2 至 4 個提示關鍵詞，不可使用問號或直接揭露 core_summary。core_summary、explanation、example_problem、example_method、reasoning_steps、common_confusion 與 memory_hint 都只能修正排版，不能補入 source_evidence 沒有的知識；沒有直接來源支持的欄位必須留空。例題維持 card_type=example，將具體可作答的必要題設、可重用解法、操作步驟分欄，simple_example 留空；example_method 只能用 1 至 2 句寫策略，不得包含 1)、2)、『步驟』等逐項內容，也不得重複 reasoning_steps。一般卡維持 concept，example_problem 與 example_method 留空；simple_example 只保留前一階段已有且可由 source_evidence 支持的例子，來源沒有例子時留空。"
                "每個數學符號表達都必須使用可由 KaTeX 渲染的 LaTeX：行內一律用 \\( ... \\)，獨立式一律用 \\[ ... \\]。矩陣與向量的每個分量必須分格，欄用 &、列用 \\\\，禁止把兩個分量或兩列直接相連。"
                "包括變數與函數式、集合與邏輯式、上下標、向量、矩陣、映射、等式、不等式、複雜度、機率、求和、遞迴式及所有含運算符的式子。普通中文必須留在 LaTeX 定界符外；禁止輸出 \\(T為線性\\) 這類把中文直接放進數學模式的格式，應寫成 \\(T\\) 為線性。禁止在一組 \\( ... \\) 或 \\[ ... \\] 內再嵌套另一組定界符。"
                "禁止使用 $ 或 $$；禁止留下像 T(a,b)=...、R^2、x_i、rank(A) 這種沒有分隔符的裸露公式。"
                "程式碼與虛擬碼是唯一例外：完整程式、函式、類別或連續兩行以上操作必須使用帶語言名稱的 Markdown fenced code block，禁止放進 LaTeX。保留原有縮排、大小寫、括號、分號、陣列索引、指標符號、運算子與註解；行內識別字或短指令使用單反引號。"
                "topic 必須依全部卡片自然整理成 2 至 6 個母主題群組；不得等於任何六科科目名稱、整份筆記標題、其他、綜合重點或課堂筆記，也不得用斜線、頓號或『與』把無直接從屬關係的分類硬併在一起。同一知識對象的定義、性質、操作、方法與例題共用 topic，明顯不同資料結構、理論或章節才分開。"
                "cards 的數量、順序與 concept_index 必須完全不變。不要在 explanation 或 summary 中提及修正過程，也不要重複 source_evidence。輸出文字禁止出現『筆記給出』『筆記註明』『筆記記載』『根據筆記』『保留來源內容』『如來源所列』或任何描述整理過程與引用來源的套話，直接陳述觀念。只輸出 schema 指定的 JSON。\n\n待整理內容：\n"
                f"allow_corrections={str(allow_corrections).lower()}。allow_corrections=false 時 correction_applied 必須全部為 false。\n"
                + json.dumps(latex_input, ensure_ascii=False, separators=(",", ":"))
            )
            if progress_callback:
                progress_callback(80, "內容審核完成，正在統一所有公式的 LaTeX 格式。")
            try:
                latex_result = _call_openai_json(
                    name="study_recall_latex_formatted",
                    schema=latex_schema,
                    content=[{"type": "input_text", "text": latex_prompt}],
                    timeout=300,
                    max_output_tokens=16000,
                )
            except (requests.RequestException, ValueError, TypeError) as exc:
                app.logger.warning(
                    "Study-note LaTeX presentation pass failed; using validated card content: %s",
                    exc,
                )
                latex_result = {"summary": audited.get("summary") or "", "cards": []}
            formatted_cards = {
                int(item.get("concept_index")): item
                for item in latex_result.get("cards") or []
                if isinstance(item, dict)
            }
            for index, card in enumerate(audited_cards):
                formatted_cards.setdefault(index, {"concept_index": index, **card})

            def safe_formatted_text(
                primary: Any,
                fallback: Any,
                *,
                max_length: int,
                allow_empty: bool = False,
                normalize_math: bool = True,
            ) -> str:
                for candidate in (primary, fallback):
                    raw = str(candidate or "").strip()
                    if not raw:
                        continue
                    raw_issue = _study_text_quality_issue(raw, max_length=max_length)
                    if raw_issue:
                        continue
                    if not normalize_math:
                        return raw
                    prepared = _normalize_study_math_markup(raw)
                    prepared_issue = _study_text_quality_issue(prepared, max_length=max_length)
                    if not prepared_issue:
                        return prepared
                    # The audited fallback has already passed the strict text and
                    # LaTeX validator. Normalization is a presentation enhancement;
                    # never let a non-idempotent edge case invalidate that content.
                    app.logger.warning(
                        "Study-card math normalization was rejected; preserving validated text "
                        "(raw_length=%s, prepared_length=%s, issue=%s)",
                        len(raw),
                        len(prepared),
                        prepared_issue,
                    )
                    return raw
                if allow_empty:
                    return ""
                raise ValueError("Corrupted study-card text")

            for index, card in enumerate(audited_cards):
                formatted = formatted_cards[index]
                card["topic"] = safe_formatted_text(
                    formatted.get("topic"), card.get("topic"), max_length=80, normalize_math=False
                )
                card["topic"] = _normalize_study_concept_title(
                    card["topic"], audited.get("detected_topic") or "細分觀念"
                )
                card["concept"] = _normalize_study_concept_title(
                    safe_formatted_text(
                        formatted.get("concept"), card.get("concept"), max_length=120, normalize_math=False
                    ),
                    card["topic"],
                )
                card["recall_cue"] = safe_formatted_text(
                    formatted.get("recall_cue"),
                    card.get("recall_cue"),
                    max_length=180,
                    allow_empty=True,
                )
                if not card["recall_cue"]:
                    card["recall_cue"] = f"先回想「{card['concept']}」的條件、核心關係與結論。"
                card["core_summary"] = safe_formatted_text(
                    formatted.get("core_summary"),
                    card.get("core_summary"),
                    max_length=320,
                    allow_empty=True,
                )
                if not card["core_summary"]:
                    card["core_summary"] = card["concept"]
                card["explanation"] = safe_formatted_text(
                    formatted.get("explanation"), card.get("explanation"), max_length=900
                )
                card["card_type"] = (
                    "example"
                    if formatted.get("card_type") == "example" or card.get("card_type") == "example"
                    else "concept"
                )
                card["example_problem"] = safe_formatted_text(
                    formatted.get("example_problem"),
                    card.get("example_problem"),
                    max_length=420,
                    allow_empty=True,
                )
                card["example_method"] = safe_formatted_text(
                    formatted.get("example_method"),
                    card.get("example_method"),
                    max_length=340,
                    allow_empty=True,
                )
                card["simple_example"] = safe_formatted_text(
                    formatted.get("simple_example"),
                    card.get("simple_example"),
                    max_length=420,
                    allow_empty=True,
                )
                if card["card_type"] != "example":
                    card["example_problem"] = ""
                    card["example_method"] = ""
                elif not card["example_problem"] or not card["example_method"]:
                    # Keep older/fallback model outputs usable while ensuring
                    # new strict-schema responses always produce both sections.
                    card["card_type"] = "concept"
                    card["example_problem"] = ""
                    card["example_method"] = ""
                else:
                    card["simple_example"] = ""
                formatted_steps = formatted.get("reasoning_steps")
                fallback_steps = card.get("reasoning_steps")
                primary_steps = formatted_steps if isinstance(formatted_steps, list) else []
                original_steps = fallback_steps if isinstance(fallback_steps, list) else []
                card["reasoning_steps"] = []
                for step_index in range(min(4, max(len(primary_steps), len(original_steps)))):
                    primary_step = primary_steps[step_index] if step_index < len(primary_steps) else ""
                    original_step = original_steps[step_index] if step_index < len(original_steps) else ""
                    prepared_step = safe_formatted_text(
                        primary_step,
                        original_step,
                        max_length=220,
                        allow_empty=True,
                    )
                    if prepared_step:
                        card["reasoning_steps"].append(prepared_step)
                card["common_confusion"] = safe_formatted_text(
                    formatted.get("common_confusion"),
                    card.get("common_confusion"),
                    max_length=240,
                    allow_empty=True,
                )
                card["memory_hint"] = safe_formatted_text(
                    formatted.get("memory_hint"),
                    card.get("memory_hint"),
                    max_length=240,
                    allow_empty=True,
                )
                if bool(formatted.get("correction_applied")):
                    card["correction"] = {
                        "applied": True,
                        "original": str(formatted.get("correction_original") or ""),
                        "corrected": card["explanation"],
                        "reason": str(formatted.get("correction_reason") or ""),
                    }
            audited["summary"] = safe_formatted_text(
                latex_result.get("summary"), audited.get("summary"), max_length=1200
            )

            card_count = len(audited_cards)
            desired_topic_count = min(6, max(2, round(math.sqrt(card_count) / 1.4)))
            desired_topic_count = min(card_count, desired_topic_count)
            current_topics = {
                str(card.get("topic") or "").strip()
                for card in audited_cards
                if str(card.get("topic") or "").strip()
            }
            if card_count >= 3 and len(current_topics) != desired_topic_count:
                topic_schema = {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["assignments"],
                    "properties": {
                        "assignments": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["concept_index", "topic"],
                                "properties": {
                                    "concept_index": {
                                        "type": "integer",
                                        "minimum": 0,
                                        "maximum": max(0, card_count - 1),
                                    },
                                    "topic": {"type": "string", "maxLength": 36},
                                },
                            },
                        }
                    },
                }
                topic_catalog = [
                    {
                        "concept_index": index,
                        "concept": card.get("concept") or "",
                        "current_topic": card.get("topic") or "",
                        "core_summary": card.get("core_summary") or "",
                    }
                    for index, card in enumerate(audited_cards)
                ]
                topic_prompt = (
                    "你是筆記章節編輯。請只重新分組下列重點卡，不可改寫卡片內容。"
                    f"必須把 {card_count} 張卡完整分成恰好 {desired_topic_count} 個不重複的母主題；"
                    "同一知識對象的定義、性質、操作、方法與例題應共用主題，不可讓每張卡各自成為一個主題。"
                    "例如 Heap 的定義、建構、插入與刪除都使用 Heap；Deap、SMMH 等不同資料結構則各自成組。"
                    "主題名稱用 4 至 14 個繁體中文字直接描述共同觀念，不可含公式、變數、題號、破折號、未閉合括號，"
                    "不可等於線性代數、離散數學、資料結構、演算法、作業系統、計算機組織等科目名稱，也不可使用其他、綜合重點或課堂筆記。"
                    "每個 concept_index 必須恰好出現一次；相同群組的 topic 字串必須逐字完全相同。只輸出 schema 指定的 JSON。\n\n"
                    + json.dumps(topic_catalog, ensure_ascii=False, separators=(",", ":"))
                )
                grouped_topics: Optional[Dict[int, str]] = None
                for topic_attempt in range(1):
                    try:
                        topic_result = _call_openai_json(
                            name="study_recall_topic_groups",
                            schema=topic_schema,
                            content=[{"type": "input_text", "text": topic_prompt}],
                            timeout=180,
                            max_output_tokens=5000,
                        )
                    except (requests.RequestException, ValueError, TypeError) as exc:
                        app.logger.warning(
                            "Study-note topic regrouping failed; preserving existing detailed topics: %s",
                            exc,
                        )
                        break
                    assignments = topic_result.get("assignments") or []
                    candidate_topics: Dict[int, str] = {}
                    for assignment in assignments:
                        if not isinstance(assignment, dict):
                            continue
                        concept_index = int(assignment.get("concept_index", -1))
                        if concept_index in candidate_topics or not 0 <= concept_index < card_count:
                            candidate_topics = {}
                            break
                        topic = _normalize_study_concept_title(assignment.get("topic"), "")
                        if (
                            not topic
                            or topic in STUDY_PLAN_SUBJECTS
                            or _study_text_quality_issue(topic, max_length=36)
                            or any(character in topic for character in ("—", "–", "/", "／"))
                            or topic.count("（") != topic.count("）")
                            or topic.count("(") != topic.count(")")
                        ):
                            candidate_topics = {}
                            break
                        candidate_topics[concept_index] = topic
                    distinct_topics = set(candidate_topics.values())
                    if (
                        len(candidate_topics) == card_count
                        and len(distinct_topics) == desired_topic_count
                    ):
                        grouped_topics = candidate_topics
                        break
                    topic_prompt += (
                        f"\n\n前一次分組不合格。這次必須輸出 {card_count} 筆唯一 concept_index，"
                        f"且 topic 去重後必須恰好是 {desired_topic_count} 個。"
                    )
                if not grouped_topics:
                    app.logger.warning(
                        "Study-note topic regrouping was incomplete; preserving existing detailed topics"
                    )
                else:
                    for index, card in enumerate(audited_cards):
                        card["topic"] = grouped_topics[index]

            def card_integrity_text(card: Dict[str, Any]) -> str:
                return " ".join(
                    str(value or "")
                    for value in (
                        card.get("core_summary"),
                        card.get("explanation"),
                        card.get("simple_example"),
                        card.get("example_problem"),
                        card.get("example_method"),
                        *(card.get("reasoning_steps") or []),
                    )
                )

            invalid_claim_indices = [
                index
                for index, card in enumerate(audited_cards)
                if isinstance(card, dict)
                and _study_has_invalid_negation_counterexample(card_integrity_text(card))
            ]
            if invalid_claim_indices:
                integrity_schema = {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["cards"],
                    "properties": {
                        "cards": {
                            "type": "array",
                            "minItems": len(invalid_claim_indices),
                            "maxItems": len(invalid_claim_indices),
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": [
                                    "concept_index",
                                    "repairable",
                                    "core_summary",
                                    "explanation",
                                    "simple_example",
                                    "example_problem",
                                    "example_method",
                                    "reasoning_steps",
                                    "correction_original",
                                    "correction_reason",
                                ],
                                "properties": {
                                    "concept_index": {
                                        "type": "integer",
                                        "minimum": 0,
                                        "maximum": max(0, len(audited_cards) - 1),
                                    },
                                    "repairable": {"type": "boolean"},
                                    "core_summary": {"type": "string", "maxLength": 280},
                                    "explanation": {"type": "string", "maxLength": 620},
                                    "simple_example": {"type": "string", "maxLength": 360},
                                    "example_problem": {"type": "string", "maxLength": 360},
                                    "example_method": {"type": "string", "maxLength": 280},
                                    "reasoning_steps": {
                                        "type": "array",
                                        "maxItems": 4,
                                        "items": {"type": "string", "maxLength": 180},
                                    },
                                    "correction_original": {"type": "string", "maxLength": 240},
                                    "correction_reason": {"type": "string", "maxLength": 300},
                                },
                            },
                        }
                    },
                }
                integrity_catalog = [
                    {
                        "concept_index": index,
                        "concept": audited_cards[index].get("concept") or "",
                        "current_content": {
                            key: audited_cards[index].get(key) or ([] if key == "reasoning_steps" else "")
                            for key in (
                                "core_summary",
                                "explanation",
                                "simple_example",
                                "example_problem",
                                "example_method",
                                "reasoning_steps",
                            )
                        },
                        "source_evidence": [
                            source_ref.get("evidence") or ""
                            for source_ref in audited_cards[index].get("source_refs") or []
                            if isinstance(source_ref, dict)
                        ],
                    }
                    for index in invalid_claim_indices
                ]
                integrity_prompt = (
                    "你是錯誤反例修正員。程式已確認下列卡片使用 f(v) != -f(u) 否定齊次性，但 v 並不是 -u，因此該反例前提無效。"
                    "請根據 source_evidence 中的函數或映射定義重新計算，不得沿用原筆記錯誤結論，也不得刪除卡片。"
                    "source_evidence 有足夠的函數或映射定義可直接重算時 repairable=true 並完成修正；若來源完全沒有定義、條件或足以重算的公式，repairable=false 且其餘文字欄位與 correction 字串全部留空，不可猜測。"
                    "先以一般輸入直接驗證加法與齊次性；映射若實際為線性，就明確改成『為線性』並移除所有『不是線性／非線性』結論；若確實非線性，必須換成符合欲檢查性質前提且可由來源定義直接算出的有效反例。"
                    "同步修正 core_summary、explanation、simple_example、題目、方法與步驟，保留 card_type=example 所需的可讀分欄；例題卡的 simple_example 留空。"
                    "correction_original 放原來源中最小錯誤比較，correction_reason 說明輸入不符合測試關係及重新計算後的正確結論。"
                    "不得加入來源沒有的術語或另一套進階解法。公式使用 KaTeX LaTeX。每個 concept_index 恰好輸出一次，只輸出 schema JSON。\n\n"
                    + json.dumps(integrity_catalog, ensure_ascii=False, separators=(",", ":"))
                )
                corrected_claims: Optional[Dict[int, Dict[str, Any]]] = None
                for integrity_attempt in range(1):
                    try:
                        integrity_result = _call_openai_json(
                            name="study_recall_invalid_claim_repair",
                            schema=integrity_schema,
                            content=[{"type": "input_text", "text": integrity_prompt}],
                            timeout=240,
                            reasoning_effort="medium",
                            max_output_tokens=8000,
                        )
                    except (requests.RequestException, ValueError, TypeError) as exc:
                        app.logger.warning(
                            "Study-note invalid-claim repair failed; the final validator will discard only affected cards: %s",
                            exc,
                        )
                        break
                    candidate_claims: Dict[int, Dict[str, Any]] = {}
                    unrepairable_indices: Set[int] = set()
                    for item in integrity_result.get("cards") or []:
                        if not isinstance(item, dict):
                            continue
                        concept_index = int(item.get("concept_index", -1))
                        if concept_index not in invalid_claim_indices or concept_index in candidate_claims:
                            candidate_claims = {}
                            break
                        if not bool(item.get("repairable")):
                            unrepairable_indices.add(concept_index)
                            continue
                        prepared_claim = {
                            "core_summary": _normalize_study_math_markup(item.get("core_summary")),
                            "explanation": _normalize_study_math_markup(item.get("explanation")),
                            "simple_example": _normalize_study_math_markup(item.get("simple_example")),
                            "example_problem": _normalize_study_math_markup(item.get("example_problem")),
                            "example_method": _normalize_study_math_markup(item.get("example_method")),
                            "reasoning_steps": [
                                _normalize_study_math_markup(step)
                                for step in (item.get("reasoning_steps") or [])[:4]
                                if str(step or "").strip()
                            ],
                        }
                        correction_original = str(item.get("correction_original") or "").strip()
                        correction_reason = str(item.get("correction_reason") or "").strip()
                        if (
                            not all(prepared_claim[key] for key in ("core_summary", "explanation", "example_problem", "example_method"))
                            or not correction_original
                            or not correction_reason
                            or any(
                                _study_text_quality_issue(prepared_claim[key], max_length=900)
                                for key in ("core_summary", "explanation", "example_problem", "example_method")
                            )
                            or _study_has_invalid_negation_counterexample(card_integrity_text(prepared_claim))
                        ):
                            candidate_claims = {}
                            break
                        prepared_claim["correction"] = {
                            "applied": True,
                            "original": correction_original,
                            "corrected": prepared_claim["explanation"],
                            "reason": correction_reason,
                        }
                        candidate_claims[concept_index] = prepared_claim
                    if set(candidate_claims) | unrepairable_indices == set(invalid_claim_indices):
                        corrected_claims = candidate_claims
                        break
                    integrity_prompt += "\n\n前一次仍保留無效的負向量比較或欄位不完整。請重新計算後輸出完整正確卡片。"
                if corrected_claims is None:
                    app.logger.warning(
                        "Study-note invalid-claim repair remained incomplete; preserving unaffected cards"
                    )
                else:
                    for index, corrected_claim in corrected_claims.items():
                        audited_cards[index].update(corrected_claim)
        except json.JSONDecodeError:
            app.logger.exception("Faithful study-note JSON output remained incomplete after retry")
            return None, "AI 回傳格式不完整，系統已自動重試仍未成功；請稍後重新上傳，不需要更換圖片。"
        except requests.Timeout:
            app.logger.exception("Faithful study-note model request timed out after retry")
            return None, "AI 服務處理逾時，系統已自動重試仍未完成；請稍後重新上傳，不需要更換圖片。"
        except requests.HTTPError as exc:
            status_code = getattr(exc.response, "status_code", None)
            error_code = str(getattr(exc, "openai_error_code", "") or "")
            error_type = str(getattr(exc, "openai_error_type", "") or "")
            error_message = str(getattr(exc, "openai_error_message", "") or "")
            if not (error_code or error_type or error_message):
                error_code, error_type, error_message = _openai_error_details(exc.response)
            app.logger.exception("Faithful study-note model request failed with HTTP %s", status_code)
            if status_code == 429 and _is_openai_quota_error(error_code, error_type, error_message):
                return None, (
                    "OpenAI API 額度不足或已達每月使用上限。請管理員至 OpenAI 計費設定補充額度"
                    "或提高使用上限後再上傳；系統已停止無效重試。"
                )
            if status_code == 429:
                return None, "AI 服務目前使用量過高，系統已自動退避重試仍受限；請稍後再試，不是圖片內容有問題。"
            return None, f"AI 服務暫時無法處理（HTTP {status_code or '錯誤'}），請稍後再試。"
        except (requests.RequestException, ValueError, TypeError):
            app.logger.exception("Faithful study-note analysis failed")
            return None, "筆記忠實整理暫時失敗，請確認圖片清晰度、API 金鑰、模型設定與網路後重試。"
        validated = _validate_recall_output(audited, source_pages)
        if not validated:
            return None, "筆記內容不足以產生可靠的重點卡，請上傳更清晰或更多頁筆記。"
        _enrich_study_card_coverage_ids(validated, source_pages)
        remaining_example_items = (
            _study_recall_coverage_gaps(validated, source_pages).get("example_items") or []
        )
        if remaining_example_items:
            app.logger.warning(
                "Final example coverage is partial; preserving validated cards: %s",
                [item.get("id") for item in remaining_example_items],
            )
            validated["processing_warnings"] = [
                (
                    f"有 {len(remaining_example_items)} 個疑似例題區塊未能可靠轉成卡片；"
                    "其餘已驗證內容已完整保留。"
                )
            ]
        validated["source_transcription"] = source_pages
        validated["uncertain_fragments"] = [
            {"image_index": page["image_index"], "text": fragment}
            for page in source_pages
            for fragment in page.get("uncertain_fragments") or []
        ]
        if progress_callback:
            progress_callback(82, "重點卡已驗證，正在建立頁面文字索引並複核來源裁切。")
        _raise_if_study_upload_cancelled()
        try:
            _localize_study_card_sources(
                images,
                validated["key_concepts"],
                validated["source_transcription"],
            )
            retry_concepts: List[Dict[str, Any]] = []
            source_groups = 0
            located_groups = 0
            for concept in validated["key_concepts"]:
                if not isinstance(concept, dict):
                    continue
                refs_by_page: Dict[int, List[Dict[str, Any]]] = {}
                for source_ref in concept.get("source_refs") or []:
                    if not isinstance(source_ref, dict):
                        continue
                    try:
                        image_index = int(source_ref.get("image_index") or 0)
                    except (TypeError, ValueError):
                        continue
                    if (
                        1 <= image_index <= len(images)
                        and _literal_study_source_evidence(source_ref.get("evidence"))
                    ):
                        refs_by_page.setdefault(image_index, []).append(source_ref)
                missing_refs: List[Dict[str, Any]] = []
                for image_index, page_refs in refs_by_page.items():
                    source_groups += 1
                    if any(
                        _validated_study_source_bbox(
                            source_ref.get("bbox"),
                            require_text_verified=True,
                            expected_image_index=image_index,
                        )
                        is not None
                        for source_ref in page_refs
                    ):
                        located_groups += 1
                    else:
                        # One reliable source box per card and page is sufficient.
                        missing_refs.append(
                            max(
                                page_refs,
                                key=lambda source_ref: len(
                                    _literal_study_source_evidence(
                                        source_ref.get("evidence")
                                    )
                                ),
                            )
                        )
                if missing_refs:
                    retry_concept = {
                        field: copy.deepcopy(concept.get(field))
                        for field in (
                            "concept",
                            "topic",
                            "core_summary",
                            "explanation",
                            "example_problem",
                            "example_method",
                            "simple_example",
                        )
                    }
                    # Keep references shared so successful retry boxes are written
                    # directly back to the validated card set.
                    retry_concept["source_refs"] = missing_refs
                    retry_concepts.append(retry_concept)
            if (
                retry_concepts
                and source_groups
                and located_groups / source_groups < 0.90
            ):
                if progress_callback:
                    progress_callback(
                        88,
                        f"首次定位完成 {located_groups}/{source_groups} 個來源區塊，"
                        "正在只重試未定位區塊。",
                    )
                _raise_if_study_upload_cancelled()
                _localize_study_card_sources(
                    images,
                    retry_concepts,
                    validated["source_transcription"],
                )
        except (requests.RequestException, ValueError, TypeError):
            app.logger.exception("Study-note source localization failed")
        if progress_callback:
            progress_callback(100, "來源區塊定位完成，本批筆記已完成整理。")
        validated["organization_mode"] = "faithful"
        return validated, None

    return (
        _analyze_study_note_image_batch,
    )
