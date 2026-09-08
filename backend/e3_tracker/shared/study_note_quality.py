"""Deterministic quality checks shared by note generation and retrieval."""

from __future__ import annotations

import re
from typing import Any, Mapping


_CARD_TEXT_FIELDS = (
    "concept",
    "title",
    "topic",
    "recall_cue",
    "core_summary",
    "key_point",
    "explanation",
    "simple_example",
    "example_problem",
    "example_method",
    "common_confusion",
    "memory_hint",
)

_PROCESS_TITLE_PATTERN = re.compile(
    r"(?:來源|原文|原圖|影像)(?:明文)?(?:保留|核對|轉錄|完整性)"
    r"(?:說明|聲明|紀錄|記錄|清單)?|"
    r"(?:轉錄|辨識|OCR|來源)(?:完整性|保留範圍|核對)(?:說明|聲明|清單)?",
    flags=re.IGNORECASE,
)

_PROCESS_SENTENCE_PATTERNS = (
    re.compile(r"已保留.{0,36}(?:所有|全部).{0,24}可見(?:文字|內容|符號)", re.IGNORECASE),
    re.compile(r"(?:此卡|本卡).{0,20}(?:記錄|紀錄).{0,20}來源.{0,20}保留說明", re.IGNORECASE),
    re.compile(r"(?:轉錄|使用)時.{0,24}(?:遵循|依照).{0,20}(?:原圖|來源).{0,20}(?:保留|完整)", re.IGNORECASE),
    re.compile(r"(?:確認|確保).{0,24}(?:轉錄|辨識).{0,24}(?:符號|內容).{0,20}完整", re.IGNORECASE),
)


def _compact(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or ""))


def is_study_note_process_metadata_text(value: Any, *, title: bool = False) -> bool:
    """Return true for model workflow claims that are not study material."""

    text = _compact(value)
    if not text:
        return False
    if title and _PROCESS_TITLE_PATTERN.search(text):
        return True
    if any(pattern.search(text) for pattern in _PROCESS_SENTENCE_PATTERNS):
        return True

    source_terms = sum(term in text for term in ("來源", "原文", "原圖", "影像"))
    preservation_terms = sum(term in text for term in ("保留", "轉錄", "核對", "辨識完整"))
    inventory_terms = sum(
        term in text
        for term in ("可見文字", "所有文字", "數字", "括號", "方框", "圓圈", "箭頭", "結尾符號")
    )
    return source_terms >= 1 and preservation_terms >= 1 and inventory_terms >= 3


def is_study_note_process_metadata_card(card: Any) -> bool:
    """Detect a whole card created from OCR/audit instructions rather than knowledge."""

    if not isinstance(card, Mapping):
        return False
    title_text = " ".join(
        str(card.get(field) or "") for field in ("concept", "title", "topic")
    )
    if is_study_note_process_metadata_text(title_text, title=True):
        return True
    body = "\n".join(str(card.get(field) or "") for field in _CARD_TEXT_FIELDS)
    details = card.get("details") or card.get("reasoning_steps") or []
    if isinstance(details, list):
        body += "\n" + "\n".join(str(value or "") for value in details)
    return is_study_note_process_metadata_text(body)
