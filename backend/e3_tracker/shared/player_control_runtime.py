from __future__ import annotations

from pathlib import Path
from typing import Any


def install_player_control_dock(web_module: Any) -> None:
    """Append the polished player controls to the study-plan page once."""

    if getattr(web_module, "__e3_player_control_dock_installed", False):
        return
    web_module.__e3_player_control_dock_installed = True

    root_dir = Path(__file__).resolve().parents[3]
    partial_path = root_dir / "frontend" / "templates" / "_player_control_dock.html"
    if not partial_path.exists():
        return

    partial = partial_path.read_text(encoding="utf-8")
    marker = "__e3PlayerControlDockInstalled"
    if marker not in web_module.STUDY_UPLOAD_TRACKER_TEMPLATE:
        web_module.STUDY_UPLOAD_TRACKER_TEMPLATE += "\n" + partial
