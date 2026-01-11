import os
from typing import Dict


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
        "cache_dir": os.getenv("E3_CACHE_DIR", ""),
        "session_cookie_secure": os.getenv("E3_SESSION_COOKIE_SECURE", "1"),
        "session_cookie_samesite": os.getenv("E3_SESSION_COOKIE_SAMESITE", "Lax"),
        "database_url": os.getenv("E3_DATABASE_URL", ""),
        "support_email": os.getenv("E3_SUPPORT_EMAIL", "bryan931218@gmail.com"),
        "app_home_url": os.getenv("E3_APP_HOME_URL", "https://e3hwtool.space/"),
        "legal_entity_name": os.getenv("E3_LEGAL_ENTITY_NAME", "E3 Homework Tracker Project"),
        "legal_effective_date": os.getenv("E3_LEGAL_EFFECTIVE_DATE", "2024-11-19"),
    }
