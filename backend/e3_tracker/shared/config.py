import os
from pathlib import Path
from typing import Dict, Optional


DEFAULT_OPENAI_MODEL = "gpt-5-mini"


def normalize_openai_reasoning_effort(
    model: str,
    requested: Optional[str],
    *,
    compatible_default: str = "low",
) -> Optional[str]:
    """Keep current low-cost reasoning models compatible with legacy calls."""
    normalized_model = str(model or "").strip().lower()
    normalized_effort = str(requested or "").strip().lower()
    needs_explicit_effort = (
        normalized_model.startswith("gpt-5.6")
        or normalized_model == "gpt-5.4-mini"
    )
    if not needs_explicit_effort:
        return normalized_effort or None
    if normalized_effort == "minimal":
        return "none"
    return normalized_effort or compatible_default


def _railway_volume_path(*parts: str) -> str:
    mount_path = str(os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or "").strip()
    if not mount_path:
        return ""
    return str(Path(mount_path).expanduser().joinpath(*parts))


def _database_url() -> str:
    explicit = str(
        os.getenv("E3_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or ""
    ).strip()
    if explicit:
        return explicit
    return _railway_volume_path("e3_tracker.sqlite3")


def load_env_defaults() -> Dict[str, str]:
    return {
        "base_url": os.getenv("E3_BASE_URL", "https://e3p.nycu.edu.tw"),
        "username": os.getenv("E3_USERNAME", ""),
        "password": os.getenv("E3_PASSWORD", ""),
        "session": os.getenv("E3_SESSION", ""),
        "scope": os.getenv("E3_SCOPE", "assignment"),
        "web_secret": os.getenv("E3_WEB_SECRET", "e3-web-secret"),
        "google_client_id": os.getenv("E3_GOOGLE_CLIENT_ID", ""),
        "google_client_secret": os.getenv("E3_GOOGLE_CLIENT_SECRET", ""),
        "google_redirect_uri": os.getenv("E3_GOOGLE_REDIRECT_URI", ""),
        "google_calendar_id": os.getenv("E3_GOOGLE_CALENDAR_ID", "primary"),
        "admin_user_id": os.getenv("E3_ADMIN_USER_ID", "112550103"),
        "canonical_host": os.getenv("E3_CANONICAL_HOST", ""),
        "cache_dir": os.getenv("E3_CACHE_DIR", "") or _railway_volume_path(),
        "session_cookie_secure": os.getenv("E3_SESSION_COOKIE_SECURE", "1"),
        "session_cookie_samesite": os.getenv("E3_SESSION_COOKIE_SAMESITE", "Lax"),
        "database_url": _database_url(),
        "support_email": os.getenv("E3_SUPPORT_EMAIL", "bryan931218@gmail.com"),
        "app_home_url": os.getenv("E3_APP_HOME_URL", "https://www.e3hwtool.space/"),
        "legal_entity_name": os.getenv("E3_LEGAL_ENTITY_NAME", "E3 Homework Tracker Project"),
        "legal_effective_date": os.getenv("E3_LEGAL_EFFECTIVE_DATE", "2024-11-19"),
        "openai_api_key": os.getenv("OPENAI_API_KEY", ""),
        "openai_model": os.getenv("E3_OPENAI_MODEL", DEFAULT_OPENAI_MODEL),
        "study_upload_dir": os.getenv("E3_STUDY_UPLOAD_DIR", "")
        or _railway_volume_path("study_note_images"),
    }
