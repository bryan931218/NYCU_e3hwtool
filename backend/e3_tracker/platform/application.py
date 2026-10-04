from e3_tracker.platform.assets import configure_frontend, register_frontend_assets
from e3_tracker.platform.routes.administration import register_administration_routes
from e3_tracker.platform.routes.common import register_platform_routes
from e3_tracker.study.health import youtube_inventory_health
from e3_tracker.assignments.services.google_calendar import GOOGLE_CALENDAR_SCOPE
from e3_tracker.platform.services.traffic import TrafficTracker, traffic_event_site, QUIET_ACTIVITY_ACTIONS
import os
import secrets
import threading
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from flask import (
    Flask,
    Response,
    flash,
    redirect,
    request,
    session,
    url_for,
    has_request_context,
)
from e3_tracker.platform.config import load_env_defaults
from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.platform.storage import PersistentStorage
from e3_tracker.platform.security import signing_key
from e3_tracker.platform.http_security import configure_http_security

from e3_tracker.assignments.application import register_assignment_site
from e3_tracker.study.application import (
    register_study_site,
    _study_plan_schedule_definitions,
    STUDY_NOTE_MAX_REQUEST_BYTES,
)

from e3_tracker.platform.paths import ROOT_DIR

TRAFFIC_TEMPLATE_PATH = (
    ROOT_DIR / "frontend" / "shared" / "templates" / "admin_traffic.html"
)

TRAFFIC_TEMPLATE = TRAFFIC_TEMPLATE_PATH.read_text(encoding="utf-8")

ANNOUNCEMENTS_TEMPLATE_PATH = (
    ROOT_DIR / "frontend" / "shared" / "templates" / "admin_announcements.html"
)

ANNOUNCEMENTS_TEMPLATE = ANNOUNCEMENTS_TEMPLATE_PATH.read_text(encoding="utf-8")

PRIVACY_TEMPLATE_PATH = ROOT_DIR / "frontend" / "shared" / "templates" / "privacy.html"

PRIVACY_TEMPLATE = PRIVACY_TEMPLATE_PATH.read_text(encoding="utf-8")

TERMS_TEMPLATE_PATH = ROOT_DIR / "frontend" / "shared" / "templates" / "terms.html"

TERMS_TEMPLATE = TERMS_TEMPLATE_PATH.read_text(encoding="utf-8")

FEEDBACK_TEMPLATE_PATH = (
    ROOT_DIR / "frontend" / "shared" / "templates" / "feedback.html"
)

FEEDBACK_TEMPLATE = FEEDBACK_TEMPLATE_PATH.read_text(encoding="utf-8")

ADMIN_FEEDBACK_TEMPLATE_PATH = (
    ROOT_DIR / "frontend" / "shared" / "templates" / "admin_feedback.html"
)

ADMIN_FEEDBACK_TEMPLATE = ADMIN_FEEDBACK_TEMPLATE_PATH.read_text(encoding="utf-8")


def _env_flag_truthy(value: Optional[str]) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def create_app(
    *,
    default_base_url: Optional[str] = None,
    default_scope: str = "assignment",
    default_timeout: int = 30,
    storage_class=None,
    schedule_builder=None,
) -> Flask:
    storage_class = storage_class or PersistentStorage

    schedule_builder = schedule_builder or _study_plan_schedule_definitions

    env_defaults = load_env_defaults()

    def _ensure_private_dir(path: Path) -> Path:
        path.mkdir(parents=True, exist_ok=True)
        try:
            if os.name != "nt":
                os.chmod(path, 0o700)
        except Exception:
            pass
        return path

    configured_cache_dir = env_defaults.get("cache_dir")

    if configured_cache_dir:
        data_root = Path(configured_cache_dir).expanduser()
    else:
        data_root = ROOT_DIR / ".localdata"

    _ensure_private_dir(data_root)

    database_url = env_defaults.get("database_url") or ""

    if database_url:
        db_location = database_url
    else:
        db_location = str((data_root / "e3_tracker.sqlite3").resolve())

    secret_key = signing_key(data_root)
    storage = storage_class(db_location)

    app = Flask(__name__, static_folder=None)
    configure_frontend(app)

    register_frontend_assets(app)

    app.secret_key = secret_key

    app.extensions["e3_storage"] = storage

    session_cookie_secure = _env_flag_truthy(env_defaults.get("session_cookie_secure"))

    session_cookie_samesite = env_defaults.get("session_cookie_samesite") or "Lax"

    app.config.update(
        PERMANENT_SESSION_LIFETIME=timedelta(days=1),
        SESSION_COOKIE_SECURE=session_cookie_secure,
        SESSION_COOKIE_SAMESITE=session_cookie_samesite,
        SESSION_COOKIE_HTTPONLY=True,
        PREFERRED_URL_SCHEME="https",
        MAX_CONTENT_LENGTH=STUDY_NOTE_MAX_REQUEST_BYTES,
        E3_CHROME_EXTENSION_URL=env_defaults["chrome_extension_url"],
        E3_EDGE_EXTENSION_URL=env_defaults["edge_extension_url"],
    )
    try:
        configure_http_security(app, storage)
    except Exception:
        storage._engine.dispose()
        raise

    @app.get("/favicon.ico")
    def favicon():
        return Response(status=204)

    admin_user_id = (env_defaults.get("admin_user_id") or "112550103").strip()

    canonical_host = (env_defaults.get("canonical_host") or "").strip()

    if canonical_host == "":
        canonical_host = None

    support_email = (
        env_defaults.get("support_email") or "support@e3hwtool.space"
    ).strip()

    if not support_email:
        support_email = "support@e3hwtool.space"

    app_home_url = (
        env_defaults.get("app_home_url") or "https://www.e3hwtool.space/"
    ).strip()

    if app_home_url and not app_home_url.startswith(("http://", "https://")):
        app_home_url = f"https://{app_home_url.lstrip('/')}"

    if not app_home_url:
        app_home_url = "https://www.e3hwtool.space/"

    if not app_home_url.endswith("/"):
        app_home_url = f"{app_home_url}/"

    legal_entity_name = (
        env_defaults.get("legal_entity_name") or "E3 Homework Tracker Project"
    )

    legal_effective_date = env_defaults.get("legal_effective_date") or "2024-11-19"

    traffic_event_limit = 500

    traffic_tracker = TrafficTracker(
        activity_window=300,
        count_interval=3600,
        storage_path=None,
        log_path=None,
        max_events=traffic_event_limit,
        state_loader=storage.load_traffic_state,
        state_saver=storage.save_traffic_state,
        event_loader=lambda limit: storage.recent_traffic_events(limit),
        event_writer=lambda event: storage.append_traffic_event(
            event, traffic_event_limit
        ),
        event_clearer=storage.clear_traffic_events,
    )

    def _start_web_session(
        username: str,
        *,
        moodle_session: Optional[str],
        is_guest: bool,
        is_admin: bool,
        permanent: bool,
    ) -> None:
        if session.get("session_token"):
            storage.clear_web_session(session["session_token"])
        session.clear()
        session_token = secrets.token_urlsafe(24)
        storage.save_web_session(session_token, username, is_guest=is_guest, is_admin=is_admin, moodle_session=moodle_session)
        session["session_token"] = session_token
        session.permanent = permanent

    @app.before_request
    def clean_expired_guest_data():
        storage.purge_expired_guest_data()

    def current_user() -> Optional[Dict[str, Any]]:
        session_token = session.get("session_token")
        user = storage.load_web_session(session_token)
        if user:
            return user
        if session_token or session.get("username"):
            session.clear()
            session.modified = True
        return None

    def login_required(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not current_user():
                return redirect(url_for("login"))
            return fn(*args, **kwargs)

        return wrapper

    def admin_required(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            user = current_user()
            if not user:
                return redirect(url_for("login"))
            if not user.get("is_admin"):
                flash("僅限管理員使用讀書計畫。", "error")
                return redirect(url_for("index"))
            return fn(*args, **kwargs)

        return wrapper

    ANNOUNCEMENT_LIMIT = 50

    def _serialize_announcement(entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(entry, dict):
            return None
        title = str(entry.get("title") or "").strip()
        content = str(entry.get("content") or "").strip()
        if not title or not content:
            return None
        created_at = str(entry.get("created_at") or "").strip()
        created_label = str(entry.get("created_label") or "").strip()
        author = str(entry.get("author") or "").strip()
        ident = str(entry.get("id") or "").strip()
        if not ident:
            ident = secrets.token_hex(6)
        if not created_label and created_at:
            try:
                created_label = (
                    datetime.fromisoformat(created_at)
                    .astimezone(TAIPEI_TZ)
                    .strftime("%Y-%m-%d %H:%M")
                )
            except Exception:
                created_label = created_at
        try:
            like_count = int(entry.get("like_count") or 0)
        except (TypeError, ValueError):
            like_count = 0
        try:
            dislike_count = int(entry.get("dislike_count") or 0)
        except (TypeError, ValueError):
            dislike_count = 0
        user_vote = str(entry.get("user_vote") or "").strip().lower() or None
        return {
            "id": ident,
            "title": title,
            "content": content,
            "created_at": created_at,
            "created_label": created_label,
            "author": author,
            "like_count": like_count,
            "dislike_count": dislike_count,
            "user_vote": user_vote,
        }

    def load_announcements(username: Optional[str] = None) -> List[Dict[str, Any]]:
        if username is None and has_request_context():
            user = current_user()
            if user:
                username = user.get("username")
        items: List[Dict[str, Any]] = []
        for raw in storage.list_announcements_with_votes(
            ANNOUNCEMENT_LIMIT, username=username
        ):
            parsed = _serialize_announcement(raw)
            if parsed:
                items.append(parsed)
        return items

    def add_announcement(title: str, content: str, author: Optional[str]) -> None:
        title = title.strip()
        content = content.strip()
        if not title or not content:
            return
        now = datetime.now(TAIPEI_TZ)
        entry = {
            "id": secrets.token_hex(6),
            "title": title,
            "content": content,
            "author": author or "",
            "created_at": now.isoformat(),
            "created_label": now.strftime("%Y-%m-%d %H:%M"),
        }
        storage.insert_announcement(entry, ANNOUNCEMENT_LIMIT)

    def delete_announcement_entry(announcement_id: str) -> bool:
        announcement_id = (announcement_id or "").strip()
        if not announcement_id:
            return False
        return storage.delete_announcement(announcement_id)

    def set_announcement_vote(
        announcement_id: str, username: str, vote_type: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        updated = storage.set_announcement_vote(announcement_id, username, vote_type)
        if not updated:
            return None
        return _serialize_announcement(updated)

    FEEDBACK_LIMIT = 200

    VALID_FEEDBACK_STATUS = {"open", "resolved"}

    def add_feedback_entry(
        message: str, email: Optional[str], username: Optional[str]
    ) -> int:
        message = (message or "").strip()
        email = (email or "").strip()
        username = (username or "").strip()
        if not message:
            return 0
        now = datetime.now(TAIPEI_TZ)
        entry = {
            "username": username or None,
            "email": email or None,
            "message": message,
            "status": "open",
            "created_at": now.isoformat(),
        }
        return storage.add_feedback(entry)

    def list_feedback_entries() -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for raw in storage.list_feedback(FEEDBACK_LIMIT):
            parsed = {
                "id": raw.get("id"),
                "username": (raw.get("username") or "-"),
                "email": (raw.get("email") or "-"),
                "message": raw.get("message") or "",
                "status": raw.get("status") or "open",
                "created_at": raw.get("created_at") or "",
            }
            ts_raw = parsed["created_at"]
            if ts_raw:
                try:
                    dt = datetime.fromisoformat(ts_raw)
                    if not dt.tzinfo:
                        dt = dt.replace(tzinfo=TAIPEI_TZ)
                    parsed["created_label"] = dt.astimezone(TAIPEI_TZ).strftime(
                        "%Y-%m-%d %H:%M"
                    )
                except Exception:
                    parsed["created_label"] = ts_raw
            else:
                parsed["created_label"] = "-"
            items.append(parsed)
        return items

    def update_feedback_status_entry(feedback_id: int, status: str) -> bool:
        if status not in VALID_FEEDBACK_STATUS:
            return False
        try:
            fid = int(feedback_id)
        except (TypeError, ValueError):
            return False
        return storage.update_feedback_status(fid, status)

    def _client_ip() -> Optional[str]:
        if not has_request_context():
            return None
        return request.remote_addr

    def record_ui_event(
        action: str, status: str = "success", meta: Optional[Dict[str, Any]] = None
    ) -> None:
        if not action:
            return
        details = dict(meta or {})
        handler = app.view_functions.get(request.endpoint) if has_request_context() else None
        details["site"] = traffic_event_site(action, getattr(handler, "__module__", ""))
        user = current_user() if has_request_context() else None
        if user:
            details["username"] = user["username"]
            details["is_guest"] = user.get("is_guest")
            details["is_admin"] = user.get("is_admin")
        if user and not user.get("is_guest") and details["site"] == "assignments":
            try:
                storage.record_assignment_usage(user["username"], action, status, details)
            except Exception:
                app.logger.warning("Assignment usage aggregate unavailable")
        if str(action).strip().lower() not in QUIET_ACTIVITY_ACTIONS:
            traffic_tracker.record_visit(
                _client_ip(), action=action, status=status, metadata=details
            )

    def usage_stats() -> Dict[str, int]:
        return traffic_tracker.snapshot()

    def current_stats_version() -> int:
        return traffic_tracker.version()

    register_platform_routes(
        app=app,
        canonical_host=canonical_host,
        study_health=lambda: youtube_inventory_health(storage),
        usage_stats=usage_stats,
        current_stats_version=current_stats_version,
        PRIVACY_TEMPLATE=PRIVACY_TEMPLATE,
        TERMS_TEMPLATE=TERMS_TEMPLATE,
        app_home_url=app_home_url,
        support_email=support_email,
        google_scope=GOOGLE_CALENDAR_SCOPE,
        legal_entity_name=legal_entity_name,
        legal_effective_date=legal_effective_date,
        login_required=login_required,
        record_ui_event=record_ui_event,
        current_user=current_user,
    )

    list_admin_view_options = register_assignment_site(
        default_scope=default_scope,
        PRIVACY_TEMPLATE=PRIVACY_TEMPLATE,
        ROOT_DIR=ROOT_DIR,
        TERMS_TEMPLATE=TERMS_TEMPLATE,
        _env_flag_truthy=_env_flag_truthy,
        _start_web_session=_start_web_session,
        admin_user_id=admin_user_id,
        app=app,
        app_home_url=app_home_url,
        canonical_host=canonical_host,
        current_stats_version=current_stats_version,
        current_user=current_user,
        default_base_url=default_base_url,
        default_timeout=default_timeout,
        env_defaults=env_defaults,
        legal_effective_date=legal_effective_date,
        legal_entity_name=legal_entity_name,
        load_announcements=load_announcements,
        login_required=login_required,
        record_ui_event=record_ui_event,
        record_activity=traffic_tracker.record_event,
        set_announcement_vote=set_announcement_vote,
        storage=storage,
        support_email=support_email,
        usage_stats=usage_stats,
    )
    (
        admin_traffic,
        admin_traffic_reset,
        admin_traffic_reset_user,
        admin_feedback,
        admin_announcements,
        delete_announcement,
        feedback,
    ) = register_administration_routes(
        ADMIN_FEEDBACK_TEMPLATE=ADMIN_FEEDBACK_TEMPLATE,
        ANNOUNCEMENTS_TEMPLATE=ANNOUNCEMENTS_TEMPLATE,
        FEEDBACK_TEMPLATE=FEEDBACK_TEMPLATE,
        TRAFFIC_TEMPLATE=TRAFFIC_TEMPLATE,
        add_announcement=add_announcement,
        add_feedback_entry=add_feedback_entry,
        app=app,
        app_home_url=app_home_url,
        current_stats_version=current_stats_version,
        current_user=current_user,
        delete_announcement_entry=delete_announcement_entry,
        list_admin_view_options=list_admin_view_options,
        list_feedback_entries=list_feedback_entries,
        load_announcements=load_announcements,
        login_required=login_required,
        record_ui_event=record_ui_event,
        storage=storage,
        support_email=support_email,
        traffic_tracker=traffic_tracker,
        update_feedback_status_entry=update_feedback_status_entry,
        usage_stats=usage_stats,
    )

    register_study_site(
        _ensure_private_dir=_ensure_private_dir,
        _env_flag_truthy=_env_flag_truthy,
        admin_required=admin_required,
        app=app,
        current_user=current_user,
        data_root=data_root,
        env_defaults=env_defaults,
        record_ui_event=record_ui_event,
        schedule_builder=schedule_builder,
        storage=storage,
    )

    return app
