"""Upload progress and resumable note batch checkpoints."""

import json
import re
import time
import hashlib
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

STUDY_NOTE_BATCH_CHECKPOINT_VERSION = 1

STUDY_UPLOAD_ANALYSIS_START_PROGRESS = 10

STUDY_UPLOAD_BATCHES_END_PROGRESS = 94


def _study_upload_time_weighted_progress(
    local_progress: int,
    *,
    batch_index: int,
    batch_count: int,
) -> int:
    """Map one batch's expensive AI work onto most of the visible progress bar."""
    safe_batch_count = max(1, int(batch_count))
    safe_batch_index = min(max(0, int(batch_index)), safe_batch_count - 1)
    local_ratio = min(1.0, max(0.0, (int(local_progress) - 20) / 80))
    overall_ratio = (safe_batch_index + local_ratio) / safe_batch_count
    progress_span = STUDY_UPLOAD_BATCHES_END_PROGRESS - STUDY_UPLOAD_ANALYSIS_START_PROGRESS
    return STUDY_UPLOAD_ANALYSIS_START_PROGRESS + round(overall_ratio * progress_span)


def _study_upload_parallel_progress(batch_progress: Iterable[int]) -> int:
    """Combine independently running batch progress without overstating completion."""
    values = list(batch_progress)
    if not values:
        return STUDY_UPLOAD_ANALYSIS_START_PROGRESS
    ratios = [min(1.0, max(0.0, (int(value) - 20) / 80)) for value in values]
    progress_span = STUDY_UPLOAD_BATCHES_END_PROGRESS - STUDY_UPLOAD_ANALYSIS_START_PROGRESS
    return STUDY_UPLOAD_ANALYSIS_START_PROGRESS + round(
        (sum(ratios) / len(ratios)) * progress_span
    )


def _study_note_batch_signature(
    images: Iterable[Tuple[str, bytes, str]],
    *,
    batch_start: int,
) -> str:
    digest = hashlib.sha256()
    digest.update(f"v{STUDY_NOTE_BATCH_CHECKPOINT_VERSION}:{int(batch_start)}".encode("ascii"))
    for filename, image_bytes, mime_type in images:
        digest.update(str(filename or "").encode("utf-8", errors="replace"))
        digest.update(b"\0")
        digest.update(str(mime_type or "").encode("ascii", errors="replace"))
        digest.update(b"\0")
        digest.update(image_bytes)
    return digest.hexdigest()


def _load_study_note_batch_checkpoint(
    directory: Optional[Path],
    *,
    batch_index: int,
    signature: str,
) -> Optional[Dict[str, Any]]:
    if directory is None:
        return None
    checkpoint_path = directory / f"analysis-batch-{int(batch_index) + 1:04d}.json"
    try:
        payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    if (
        not isinstance(payload, dict)
        or int(payload.get("version") or 0) != STUDY_NOTE_BATCH_CHECKPOINT_VERSION
        or str(payload.get("signature") or "") != signature
        or not isinstance(payload.get("analysis"), dict)
    ):
        return None
    return payload["analysis"]


def _save_study_note_batch_checkpoint(
    directory: Optional[Path],
    *,
    batch_index: int,
    signature: str,
    analysis: Dict[str, Any],
) -> None:
    if directory is None:
        return
    directory.mkdir(parents=True, exist_ok=True)
    checkpoint_path = directory / f"analysis-batch-{int(batch_index) + 1:04d}.json"
    temporary_path = checkpoint_path.with_suffix(".tmp")
    temporary_path.write_text(
        json.dumps(
            {
                "version": STUDY_NOTE_BATCH_CHECKPOINT_VERSION,
                "signature": signature,
                "analysis": analysis,
                "saved_at": time.time(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    temporary_path.replace(checkpoint_path)


def _offset_study_note_batch_analysis(analysis: Dict[str, Any], image_offset: int) -> None:
    if image_offset <= 0:
        return

    def offset_image_index(item: Any) -> None:
        if not isinstance(item, dict):
            return
        try:
            item["image_index"] = int(item.get("image_index") or 0) + image_offset
        except (TypeError, ValueError):
            pass

    for page in analysis.get("source_transcription") or []:
        offset_image_index(page)
        for region in page.get("visual_regions") or []:
            if not isinstance(region, dict):
                continue
            offset_image_index(region)
            region["region_id"] = re.sub(
                r"^p(\d+)v",
                lambda match: f"p{int(match.group(1)) + image_offset}v",
                str(region.get("region_id") or ""),
            )
    for fragment in analysis.get("uncertain_fragments") or []:
        offset_image_index(fragment)
    for record in analysis.get("correction_records") or []:
        offset_image_index(record)
    for concept in analysis.get("key_concepts") or []:
        if not isinstance(concept, dict):
            continue
        for source_ref in concept.get("source_refs") or []:
            offset_image_index(source_ref)
            bbox = source_ref.get("bbox") if isinstance(source_ref, dict) else None
            if isinstance(bbox, dict):
                try:
                    bbox["source_image_index"] = (
                        int(bbox.get("source_image_index") or 0) + image_offset
                    )
                except (TypeError, ValueError):
                    pass
        for visual_ref in concept.get("visual_refs") or []:
            if not isinstance(visual_ref, dict):
                continue
            offset_image_index(visual_ref)
            visual_ref["region_id"] = re.sub(
                r"^p(\d+)v",
                lambda match: f"p{int(match.group(1)) + image_offset}v",
                str(visual_ref.get("region_id") or ""),
            )
        concept["coverage_ids"] = [
            re.sub(
                r"^p(\d+)([bv])",
                lambda match: (
                    f"p{int(match.group(1)) + image_offset}{match.group(2)}"
                ),
                str(coverage_id),
            )
            for coverage_id in concept.get("coverage_ids") or []
        ]
