"""Bundled video inventory for the administrator study plan."""

import json
from pathlib import Path
from typing import Any, Dict, List


_VIDEO_DATA_PATH = Path(__file__).with_name("study_plan_videos.json")


def load_study_plan_videos() -> List[Dict[str, Any]]:
    """Load the checked-in MP4 duration inventory without relying on local media paths."""
    payload = json.loads(_VIDEO_DATA_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Study plan video inventory must be a list.")
    return [item for item in payload if isinstance(item, dict)]


STUDY_PLAN_VIDEO_INVENTORY = load_study_plan_videos()
