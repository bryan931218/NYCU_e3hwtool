from __future__ import annotations

from typing import Any


_INSTALL_MARKER = "__e3TimelineRestCancelInstalled"
_STYLE_MARKER = "e3-timeline-rest-cancel-style"
_STYLE = f"""
<style id=\"{_STYLE_MARKER}\">
.week-daily .day-chip.rest .day-status {{ display:none; }}
.week-daily .day-chip.rest .day-rest-form {{ display:block; }}
.week-daily .day-chip.rest .day-rest-button.restore {{
    position:absolute;
    top:5px;
    right:5px;
    z-index:3;
    width:auto;
    min-width:30px;
    height:24px;
    padding:0 7px;
    color:#087979;
    border-color:#8fc9c4;
    background:#f1fbfa;
    box-shadow:0 1px 3px rgba(16,42,67,.12);
}}
.week-daily .day-chip.rest .day-rest-button.restore:hover {{
    color:#075f60;
    border-color:#6db8b2;
    background:#e4f7f5;
}}
</style>
"""


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
