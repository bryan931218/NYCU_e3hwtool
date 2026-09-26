from __future__ import annotations

from pathlib import Path
from typing import Any


_INSTALL_MARKER = "__e3TimelineRestCancelInstalled"
_STYLE_MARKER = "e3-timeline-rest-cancel-style"
_STYLE_PATH = Path(__file__).resolve().parents[3] / "frontend/static/css/study-timeline.css"
_STYLE = f'<style id="{_STYLE_MARKER}">\n{_STYLE_PATH.read_text(encoding="utf-8")}\n</style>'


def decorate_timeline_rest_cancel(template: str) -> str:
    """Keep the existing restore form visible inside narrow timeline day cards."""

    text = str(template or "")
    if _STYLE_MARKER in text:
        return text
    if "</head>" in text:
        return text.replace("</head>", _STYLE + "</head>", 1)
    return _STYLE + text


def install_timeline_rest_cancel(web_module: Any) -> None:
    """Patch STUDY_PLAN_TEMPLATE after the rest-day runtime has built its toggle markup."""

    template = getattr(web_module, "STUDY_PLAN_TEMPLATE", None)
    if not isinstance(template, str) or getattr(web_module, _INSTALL_MARKER, False):
        return
    web_module.STUDY_PLAN_TEMPLATE = decorate_timeline_rest_cancel(template)
    setattr(web_module, _INSTALL_MARKER, True)
