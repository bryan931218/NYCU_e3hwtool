"""Tables used by optional application features; no runtime DDL."""
from sqlalchemy import Column, Float, Index, Integer, String, Table, Text

from .schema import metadata


player_settings = Table(
    "study_player_settings", metadata,
    Column("id", Integer, primary_key=True),
    Column("default_playback_rate", Float, nullable=False, server_default="1.0"),
    Column("hold_space_rate", Float, nullable=False),
    Column("hold_delay_ms", Integer, nullable=False),
    Column("seek_back_seconds", Integer, nullable=False, server_default="10"),
    Column("seek_forward_seconds", Integer, nullable=False, server_default="10"),
    Column("seek_repeat_ms", Integer, nullable=False, server_default="150"),
    Column("playback_rate_step", Float, nullable=False, server_default="0.05"),
    Column("volume_step", Integer, nullable=False, server_default="5"),
    Column("controls_hide_ms", Integer, nullable=False, server_default="2600"),
    Column("center_click_toggle", Integer, nullable=False),
    Column("pause_on_marker", Integer, nullable=False, server_default="0"),
    Column("show_speed_presets", Integer, nullable=False, server_default="1"),
    Column("show_shortcut_hint", Integer, nullable=False),
    Column("hint_duration_ms", Integer, nullable=False),
    Column("updated_at", String(64), nullable=False),
)
discord_settings = Table(
    "discord_presence_settings", metadata,
    Column("id", Integer, primary_key=True),
    Column("application_id", String(32), nullable=False),
    Column("token_hash", String(64), nullable=False),
    Column("enabled", Integer, nullable=False, server_default="0"),
    Column("updated_at", String(64), nullable=False),
)
data_repairs = Table(
    "e3_data_repairs", metadata,
    Column("repair_key", String(120), primary_key=True),
    Column("details", Text, nullable=False),
    Column("applied_at", String(64), nullable=False),
)
favorites = Table(
    "study_recall_favorites", metadata,
    Column("username", String(191), primary_key=True),
    Column("session_id", Integer, primary_key=True),
    Column("concept_index", Integer, primary_key=True),
    Column("created_at", String(64), nullable=False),
)
Index("ix_study_recall_favorites_user_created", favorites.c.username, favorites.c.created_at)
