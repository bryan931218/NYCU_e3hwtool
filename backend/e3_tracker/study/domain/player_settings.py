"""Player preference rules and defaults."""

from typing import Any, Dict, Mapping

PLAYER_RATE_MIN = 1.05

PLAYER_RATE_MAX = 2.0

PLAYER_RATE_STEP = 0.05

PLAYER_SETTINGS_DEFAULTS: Dict[str, Any] = {
    "default_playback_rate": 1.0,
    "hold_space_rate": 2.0,
    "hold_delay_ms": 300,
    "seek_back_seconds": 10,
    "seek_forward_seconds": 10,
    "seek_repeat_ms": 150,
    "playback_rate_step": 0.05,
    "volume_step": 5,
    "controls_hide_ms": 2600,
    "center_click_toggle": True,
    "pause_on_marker": False,
    "show_speed_presets": True,
    "show_shortcut_hint": True,
    "hint_duration_ms": 1400,
}


def _number(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed == parsed else default


def _integer(value: Any, default: int) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _boolean(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def normalize_player_settings(values: Mapping[str, Any] | None) -> Dict[str, Any]:
    source = dict(values or {})
    default_playback_rate = min(
        2.0,
        max(0.25, _number(source.get("default_playback_rate"), 1.0)),
    )
    default_playback_rate = round(round(default_playback_rate * 20) / 20, 2)
    requested_rate = _number(
        source.get("hold_space_rate"),
        float(PLAYER_SETTINGS_DEFAULTS["hold_space_rate"]),
    )
    hold_space_rate = min(PLAYER_RATE_MAX, max(PLAYER_RATE_MIN, requested_rate))
    hold_space_rate = round(
        round((hold_space_rate - PLAYER_RATE_MIN) / PLAYER_RATE_STEP) * PLAYER_RATE_STEP
        + PLAYER_RATE_MIN,
        2,
    )
    hold_delay_ms = min(
        1200,
        max(150, _integer(source.get("hold_delay_ms"), 300)),
    )
    legacy_seek_seconds = source.get("seek_seconds", 10)
    seek_back_seconds = min(
        120,
        max(1, _integer(source.get("seek_back_seconds", legacy_seek_seconds), 10)),
    )
    seek_forward_seconds = min(
        120,
        max(1, _integer(source.get("seek_forward_seconds", legacy_seek_seconds), 10)),
    )
    seek_repeat_ms = min(
        500,
        max(75, _integer(source.get("seek_repeat_ms"), 150)),
    )
    requested_rate_step = _number(source.get("playback_rate_step"), 0.05)
    playback_rate_step = min(
        (0.05, 0.1, 0.25),
        key=lambda option: abs(option - requested_rate_step),
    )
    volume_step = min(
        20,
        max(1, _integer(source.get("volume_step"), 5)),
    )
    controls_hide_ms = min(
        8000,
        max(1200, _integer(source.get("controls_hide_ms"), 2600)),
    )
    hint_duration_ms = min(
        5000,
        max(500, _integer(source.get("hint_duration_ms"), 1400)),
    )
    return {
        "default_playback_rate": float(default_playback_rate),
        "hold_space_rate": float(hold_space_rate),
        "hold_delay_ms": hold_delay_ms,
        "seek_back_seconds": seek_back_seconds,
        "seek_forward_seconds": seek_forward_seconds,
        "seek_repeat_ms": seek_repeat_ms,
        "playback_rate_step": float(playback_rate_step),
        "volume_step": volume_step,
        "controls_hide_ms": controls_hide_ms,
        "center_click_toggle": _boolean(
            source.get("center_click_toggle"),
            bool(PLAYER_SETTINGS_DEFAULTS["center_click_toggle"]),
        ),
        "pause_on_marker": _boolean(
            source.get("pause_on_marker"),
            bool(PLAYER_SETTINGS_DEFAULTS["pause_on_marker"]),
        ),
        "show_speed_presets": _boolean(
            source.get("show_speed_presets"),
            bool(PLAYER_SETTINGS_DEFAULTS["show_speed_presets"]),
        ),
        "show_shortcut_hint": _boolean(
            source.get("show_shortcut_hint"),
            bool(PLAYER_SETTINGS_DEFAULTS["show_shortcut_hint"]),
        ),
        "hint_duration_ms": hint_duration_ms,
    }
