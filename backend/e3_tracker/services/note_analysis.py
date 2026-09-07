"""Note analysis; dependencies are bound per application."""

from ..services.study_upload_batches import _study_upload_parallel_progress, _study_note_batch_signature, _load_study_note_batch_checkpoint, _save_study_note_batch_checkpoint, _offset_study_note_batch_analysis
import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests


def build_note_analysis(*,
    STUDY_NOTE_AI_BATCH_SIZE,
    STUDY_NOTE_MAX_IMAGE_BYTES,
    _analyze_study_note_image_batch,
    _call_openai_json,
    _raise_if_study_upload_cancelled,
    app,
    study_upload_context,
):
    def _consolidate_study_note_batch_cards(
        cards: List[Dict[str, Any]],
        *,
        subject: str,
    ) -> List[Dict[str, Any]]:
        if len(cards) < 2:
            return cards

        window_size = 100
        window_step = 80
        windows: List[List[int]] = []
        if len(cards) <= window_size:
            windows.append(list(range(len(cards))))
        else:
            for start in range(0, len(cards), window_step):
                window = list(range(start, min(len(cards), start + window_size)))
                if len(window) >= 2:
                    windows.append(window)
                if window and window[-1] == len(cards) - 1:
                    break

            topic_groups: Dict[str, List[int]] = {}
            for index, card in enumerate(cards):
                topic_key = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", str(card.get("topic") or "").lower())
                if topic_key:
                    topic_groups.setdefault(topic_key, []).append(index)
            for group in topic_groups.values():
                if len(group) < 2:
                    continue
                for start in range(0, len(group), window_step):
                    window = group[start : start + window_size]
                    if len(window) >= 2:
                        windows.append(window)

        merge_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["duplicate_groups"],
            "properties": {
                "duplicate_groups": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["concept_indexes", "keep_index"],
                        "properties": {
                            "concept_indexes": {
                                "type": "array",
                                "minItems": 2,
                                "items": {"type": "integer", "minimum": 0, "maximum": len(cards) - 1},
                            },
                            "keep_index": {"type": "integer", "minimum": 0, "maximum": len(cards) - 1},
                        },
                    },
                }
            },
        }
        proposed_groups: List[Tuple[List[int], int]] = []
        seen_windows: Set[Tuple[int, ...]] = set()
        for window in windows:
            window_key = tuple(window)
            if window_key in seen_windows:
                continue
            seen_windows.add(window_key)
            catalog = [
                {
                    "concept_index": index,
                    "topic": cards[index].get("topic") or "",
                    "concept": cards[index].get("concept") or "",
                    "card_type": cards[index].get("card_type") or "concept",
                    "core_summary": cards[index].get("core_summary") or "",
                    "explanation": cards[index].get("explanation") or "",
                    "source_pages": sorted(
                        {
                            int(source_ref.get("image_index") or 0)
                            for source_ref in cards[index].get("source_refs") or []
                            if isinstance(source_ref, dict)
                        }
                    ),
                }
                for index in window
            ]
            prompt = (
                f"你是「{subject}」筆記卡片的跨頁去重編輯。判斷下列卡片中哪些其實是同一個知識點、同一個公式或同一個複習目標，只有真正重複時才能合併。"
                "彼此相關、前後承接、同章節但可分別複習的卡片不是重複，必須保留。例題與一般觀念卡不得互相合併；題目條件或解法不同的例題也不得合併。"
                "每組 concept_indexes 放所有應合併的索引，keep_index 選內容最完整、最清楚且公式無缺漏的一張。沒有重要重複就輸出空陣列。不要因卡片很多而刪除任何不重複重點。只輸出 schema JSON。\n\n"
                + json.dumps(catalog, ensure_ascii=False, separators=(",", ":"))
            )
            try:
                merge_result = _call_openai_json(
                    name="study_note_cross_batch_merge",
                    schema=merge_schema,
                    content=[{"type": "input_text", "text": prompt}],
                    timeout=180,
                    reasoning_effort="medium",
                    max_output_tokens=5000,
                )
            except (requests.RequestException, ValueError, TypeError):
                app.logger.exception("Study-note cross-batch merge planning failed; preserving every card")
                continue
            allowed = set(window)
            for item in merge_result.get("duplicate_groups") or []:
                if not isinstance(item, dict):
                    continue
                indexes = sorted(
                    {
                        int(index)
                        for index in item.get("concept_indexes") or []
                        if isinstance(index, int) and index in allowed
                    }
                )
                keep_index = int(item.get("keep_index", -1))
                if len(indexes) >= 2 and keep_index in indexes:
                    proposed_groups.append((indexes, keep_index))

        removed: Set[int] = set()
        for indexes, requested_keep_index in proposed_groups:
            active_indexes = [index for index in indexes if index not in removed]
            if len(active_indexes) < 2:
                continue
            keep_index = requested_keep_index if requested_keep_index in active_indexes else max(
                active_indexes,
                key=lambda index: len(str(cards[index].get("explanation") or "")),
            )
            keep_card = cards[keep_index]
            for duplicate_index in active_indexes:
                if duplicate_index == keep_index:
                    continue
                duplicate = cards[duplicate_index]
                combined_refs: List[Dict[str, Any]] = []
                seen_refs: Set[Tuple[int, str]] = set()
                for source_ref in (keep_card.get("source_refs") or []) + (duplicate.get("source_refs") or []):
                    if not isinstance(source_ref, dict):
                        continue
                    key = (
                        int(source_ref.get("image_index") or 0),
                        " ".join(str(source_ref.get("evidence") or "").split()),
                    )
                    if key in seen_refs:
                        continue
                    seen_refs.add(key)
                    combined_refs.append(source_ref)
                keep_card["source_refs"] = combined_refs
                keep_card["visual_refs"] = list(
                    {
                        str(visual_ref.get("region_id") or ""): visual_ref
                        for visual_ref in (
                            (keep_card.get("visual_refs") or [])
                            + (duplicate.get("visual_refs") or [])
                        )
                        if isinstance(visual_ref, dict)
                        and str(visual_ref.get("region_id") or "")
                    }.values()
                )
                keep_card["coverage_ids"] = list(
                    dict.fromkeys((keep_card.get("coverage_ids") or []) + (duplicate.get("coverage_ids") or []))
                )
                if not bool((keep_card.get("correction") or {}).get("applied")) and bool(
                    (duplicate.get("correction") or {}).get("applied")
                ):
                    keep_card["correction"] = duplicate["correction"]
                removed.add(duplicate_index)
        return [card for index, card in enumerate(cards) if index not in removed]

    def _analyze_study_note_images(
        images: List[Tuple[str, Any, str]],
        *,
        subject: str,
        allow_corrections: bool,
        progress_callback: Optional[Callable[[int, str], None]] = None,
        checkpoint_directory: Optional[Path] = None,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        def materialize(selected_images: List[Tuple[str, Any, str]]) -> List[Tuple[str, bytes, str]]:
            prepared: List[Tuple[str, bytes, str]] = []
            for filename, source, mime_type in selected_images:
                if isinstance(source, Path):
                    image_bytes = source.read_bytes()
                elif isinstance(source, bytes):
                    image_bytes = source
                else:
                    raise ValueError("Unsupported study-note image source")
                if not image_bytes or len(image_bytes) > STUDY_NOTE_MAX_IMAGE_BYTES:
                    raise ValueError(f"筆記圖片 {filename} 大小不正確。")
                prepared.append((filename, image_bytes, mime_type))
            return prepared

        batch_specs = [
            (batch_index, batch_start, images[batch_start : batch_start + STUDY_NOTE_AI_BATCH_SIZE])
            for batch_index, batch_start in enumerate(
                range(0, len(images), STUDY_NOTE_AI_BATCH_SIZE)
            )
        ]
        batch_count = len(batch_specs)
        analyses_by_index: Dict[int, Dict[str, Any]] = {}
        progress_by_batch = [20] * batch_count
        progress_lock = threading.Lock()
        parent_cancel_event = getattr(study_upload_context, "cancel_event", None)
        parent_job_id = getattr(study_upload_context, "job_id", None)

        def analyze_batch(
            batch_index: int,
            batch_start: int,
            batch_images: List[Tuple[str, Any, str]],
        ) -> Tuple[int, Optional[Dict[str, Any]], Optional[str]]:
            if isinstance(parent_cancel_event, threading.Event):
                study_upload_context.cancel_event = parent_cancel_event
            if parent_job_id:
                study_upload_context.job_id = parent_job_id

            def report_batch_progress(progress: int, message: str) -> None:
                if not progress_callback:
                    return
                with progress_lock:
                    progress_by_batch[batch_index] = max(
                        progress_by_batch[batch_index],
                        int(progress),
                    )
                    combined_progress = _study_upload_parallel_progress(
                        progress_by_batch
                    )
                    completed = sum(value >= 100 for value in progress_by_batch)
                status_prefix = (
                    f"第 {batch_index + 1}/{batch_count} 批"
                    if batch_count == 1
                    else f"AI 同時整理 {batch_count} 批，已完成 {completed} 批；第 {batch_index + 1} 批"
                )
                progress_callback(combined_progress, f"{status_prefix}：{message}")

            try:
                _raise_if_study_upload_cancelled()
                materialized_images = materialize(batch_images)
                checkpoint_signature = _study_note_batch_signature(
                    materialized_images,
                    batch_start=batch_start,
                )
                checkpoint = _load_study_note_batch_checkpoint(
                    checkpoint_directory,
                    batch_index=batch_index,
                    signature=checkpoint_signature,
                )
                if checkpoint is not None:
                    report_batch_progress(100, "已載入先前完成的批次，從中斷處繼續。")
                    return batch_index, checkpoint, None
                analysis, error = _analyze_study_note_image_batch(
                    materialized_images,
                    subject=subject,
                    allow_corrections=allow_corrections,
                    progress_callback=report_batch_progress,
                )
                if analysis:
                    _offset_study_note_batch_analysis(analysis, batch_start)
                    _save_study_note_batch_checkpoint(
                        checkpoint_directory,
                        batch_index=batch_index,
                        signature=checkpoint_signature,
                        analysis=analysis,
                    )
                return batch_index, analysis, error
            finally:
                if hasattr(study_upload_context, "cancel_event"):
                    del study_upload_context.cancel_event
                if hasattr(study_upload_context, "job_id"):
                    del study_upload_context.job_id

        if batch_count == 1:
            results = [analyze_batch(*batch_specs[0])]
        else:
            with ThreadPoolExecutor(max_workers=min(2, batch_count)) as executor:
                futures = [executor.submit(analyze_batch, *spec) for spec in batch_specs]
                results = [future.result() for future in as_completed(futures)]
        for batch_index, analysis, error in results:
            if error or not analysis:
                return None, f"第 {batch_index + 1}/{batch_count} 批處理失敗：{error or '筆記分析失敗。'}"
            analyses_by_index[batch_index] = analysis
        analyses = [analyses_by_index[index] for index in range(batch_count)]

        _raise_if_study_upload_cancelled()
        if batch_count == 1:
            if progress_callback:
                progress_callback(96, "所有頁面與來源定位均已完成，正在準備儲存。")
            return analyses[0], None
        if progress_callback:
            progress_callback(95, "所有批次已完成，正在由 AI 判斷跨批次卡片的合併與去重。")
        combined_cards = [
            card
            for analysis in analyses
            for card in analysis.get("key_concepts") or []
            if isinstance(card, dict)
        ]
        combined_cards = _consolidate_study_note_batch_cards(combined_cards, subject=subject)
        summaries: List[str] = []
        for analysis in analyses:
            summary = str(analysis.get("summary") or "").strip()
            if summary and summary not in summaries:
                summaries.append(summary)
        combined_summary = "\n".join(summaries)
        if len(combined_summary) > 900:
            combined_summary = combined_summary[:897].rstrip() + "..."
        topics = list(
            dict.fromkeys(
                str(analysis.get("detected_topic") or "").strip()
                for analysis in analyses
                if str(analysis.get("detected_topic") or "").strip()
            )
        )
        combined_topic = "、".join(topics)
        if len(combined_topic) > 80:
            combined_topic = topics[0][:80] if topics else subject
        combined_analysis = {
            "detected_topic": combined_topic or subject,
            "summary": combined_summary,
            "key_concepts": combined_cards,
            "source_transcription": [
                page for analysis in analyses for page in analysis.get("source_transcription") or []
            ],
            "uncertain_fragments": [
                fragment for analysis in analyses for fragment in analysis.get("uncertain_fragments") or []
            ],
            "correction_records": [
                record for analysis in analyses for record in analysis.get("correction_records") or []
            ],
            "processing_warnings": list(
                dict.fromkeys(
                    str(warning).strip()
                    for analysis in analyses
                    for warning in analysis.get("processing_warnings") or []
                    if str(warning).strip()
                )
            ),
            "organization_mode": "faithful",
        }
        if progress_callback:
            progress_callback(96, "跨批次卡片合併完成，正在準備儲存。")
        return combined_analysis, None

    return (
        _consolidate_study_note_batch_cards,
        _analyze_study_note_images,
    )
