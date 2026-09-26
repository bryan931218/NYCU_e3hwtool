from __future__ import annotations

import re
from functools import wraps
from typing import Any


_INSTALL_MARKER = "__e3RestDayToggleInstalled"
_RESTORE_BUTTON = re.compile(
    r'<button class="day-rest-button restore" type="submit" '
    r'aria-label="恢復 \{\{ day\.date \}\} 的原定安排" '
    r'title="恢復原定安排">復</button>'
)


def decorate_rest_day_toggle_markup(template: str) -> str:
    """Render an active rest-day control as the same `休` button used to enable it."""

    return _RESTORE_BUTTON.sub(
        '<button class="day-rest-button restore is-active" type="submit" '
        'aria-label="取消 {{ day.date }} 的休息日" title="取消休息日" '
        'aria-pressed="true">休</button>',
        template,
    )


def install_rest_day_toggle(web_module: Any) -> None:
    """Patch study-plan templates without changing the existing restore endpoint."""

    render = getattr(web_module, "render_template_string", None)
    if render is None or getattr(render, _INSTALL_MARKER, False):
        return

    @wraps(render)
    def wrapped_render(template: str, *args: Any, **kwargs: Any):
        if "day-rest-button restore" in template:
            template = decorate_rest_day_toggle_markup(template)
        return render(template, *args, **kwargs)

    setattr(wrapped_render, _INSTALL_MARKER, True)
    web_module.render_template_string = wrapped_render
