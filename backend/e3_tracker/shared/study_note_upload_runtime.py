from __future__ import annotations

from typing import Any


SAFE_STUDY_NOTE_AI_BATCH_SIZE = 4


def install_study_note_upload_runtime(web_module: Any) -> None:
    """Keep dense study-note AI requests small enough to avoid truncated JSON."""

    current = getattr(web_module, "STUDY_NOTE_AI_BATCH_SIZE", SAFE_STUDY_NOTE_AI_BATCH_SIZE)
    try:
        current_size = max(1, int(current))
    except (TypeError, ValueError):
        current_size = SAFE_STUDY_NOTE_AI_BATCH_SIZE

    web_module.STUDY_NOTE_AI_BATCH_SIZE = min(
        current_size,
        SAFE_STUDY_NOTE_AI_BATCH_SIZE,
    )
