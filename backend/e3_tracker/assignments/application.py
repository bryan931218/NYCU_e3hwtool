from e3_tracker.assignments.routes.assignments import register_assignments_routes
from e3_tracker.assignments.routes.dashboard import register_dashboard_routes
from e3_tracker.assignments.routes.notifications import register_notification_routes
from e3_tracker.assignments.routes.course_announcements import register_course_announcement_routes
from e3_tracker.assignments.services.course_announcements import CourseAnnouncementService
from e3_tracker.assignments.services.notifications import NotificationService
from e3_tracker.assignments.services.session_identity import SessionIdentitySync
import base64
import json
import secrets
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from flask import flash, request, url_for
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from e3_tracker.assignments.services.collector import (
    CollectOptions,
    annotate_result_semesters,
    collect_assignments,
    current_semester_key,
    merge_current_semester_cache,
    normalize_semester_keys,
    normalize_semester_selection,
)
from e3_tracker.assignments.services.google_calendar import (
    compute_expiry,
    refresh_google_token,
)
from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.assignments.domain.excel import build_excel
from e3_tracker.platform.utils import json_safe
from e3_tracker.platform.paths import ROOT_DIR

TEMPLATE_PATH = ROOT_DIR / "frontend" / "assignments" / "templates" / "web.html"

WEB_TEMPLATE = TEMPLATE_PATH.read_text(encoding="utf-8")

LOGIN_TEMPLATE_PATH = ROOT_DIR / "frontend" / "assignments" / "templates" / "login.html"

LOGIN_TEMPLATE = LOGIN_TEMPLATE_PATH.read_text(encoding="utf-8")

HOME_TEMPLATE_PATH = ROOT_DIR / "frontend" / "assignments" / "templates" / "home.html"

HOME_TEMPLATE = HOME_TEMPLATE_PATH.read_text(encoding="utf-8")


def register_assignment_site(
    *,
    default_scope,
    PRIVACY_TEMPLATE,
    ROOT_DIR,
    TERMS_TEMPLATE,
    _env_flag_truthy,
    _start_web_session,
    admin_user_id,
    app,
    app_home_url,
    canonical_host,
    current_stats_version,
    current_user,
    default_base_url,
    default_timeout,
    env_defaults,
    legal_effective_date,
    legal_entity_name,
    load_announcements,
    login_required,
    record_ui_event,
    record_activity,
    set_announcement_vote,
    storage,
    support_email,
    usage_stats,
):
    base_url = default_base_url or env_defaults["base_url"]

    default_scope = default_scope or env_defaults["scope"]

    default_moodle_session = env_defaults["session"]

    cafile = env_defaults.get("cafile") or None

    insecure_tls = _env_flag_truthy(env_defaults.get("insecure_tls"))

    google_client_id = env_defaults.get("google_client_id")

    google_client_secret = env_defaults.get("google_client_secret")

    google_redirect_uri = env_defaults.get("google_redirect_uri")

    google_calendar_id = env_defaults.get("google_calendar_id") or "primary"
    notification_service = NotificationService(storage, app_home_url=app_home_url)
    app.extensions["e3_notifications"] = notification_service
    session_identity_sync = SessionIdentitySync(storage, base_url, default_timeout)
    app.extensions["e3_session_identity"] = session_identity_sync

    DEFAULT_PREFERENCES = {
        "view_mode": "due",
        "status_filter": ["pending"],
        "semester_filter": [],
        "include_ignored_overdue": False,
        "show_overdue": False,
        "show_completed": False,
        "show_graded": False,
        "ignored_assignment_uids": [],
    }

    NEW_ASSIGNMENT_WINDOW_SECONDS = 5 * 60

    refresh_jobs_lock = threading.Lock()

    refresh_jobs: Dict[str, Dict[str, Any]] = {}

    def _refresh_job_state(username: str) -> Optional[Dict[str, Any]]:
        if not username:
            return None
        with refresh_jobs_lock:
            job = refresh_jobs.get(username)
            if not job:
                return None
            started_at = float(job.get("started_at") or 0)
            finished_at = float(job.get("finished_at") or 0)
            if finished_at and time.time() - finished_at > 300:
                refresh_jobs.pop(username, None)
                return None
            if not finished_at and started_at and time.time() - started_at > 600:
                refresh_jobs.pop(username, None)
                return None
            return dict(job)

    def _mark_refresh_job_started(username: str) -> bool:
        if not username:
            return False
        with refresh_jobs_lock:
            job = refresh_jobs.get(username)
            started_at = float(job.get("started_at") or 0) if job else 0
            finished_at = float(job.get("finished_at") or 0) if job else 0
            if started_at and not finished_at and time.time() - started_at <= 600:
                return False
            refresh_jobs[username] = {"started_at": time.time(), "status": "running"}
            return True

    def _mark_refresh_job_done(
        username: str, *, status: str = "success", error: Optional[str] = None
    ) -> None:
        if not username:
            return
        with refresh_jobs_lock:
            job = refresh_jobs.get(username) or {"started_at": time.time()}
            job["status"] = status
            job["finished_at"] = time.time()
            if error:
                job["error"] = str(error)
            else:
                job.pop("error", None)
            refresh_jobs[username] = job

    def load_cache_from_disk(username: str) -> Optional[Dict[str, Any]]:
        return storage.load_user_cache(username)

    def save_cache_to_disk(username: str, payload: Dict[str, Any]) -> None:
        storage.save_user_cache(username, payload)

    def _coerce_bool(value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "yes", "on"}:
                return True
            if lowered in {"0", "false", "no", "off"}:
                return False
            return None
        if isinstance(value, (int, float)):
            return bool(value)
        return None

    def _sanitize_preferences(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        clean: Dict[str, Any] = {}
        if not isinstance(raw, dict):
            return clean
        view_mode = raw.get("view_mode")
        if view_mode is None:
            view_mode = raw.get("viewMode")
        if isinstance(view_mode, str):
            lowered = view_mode.strip().lower()
            if lowered in {"course", "due", "calendar"}:
                clean["view_mode"] = lowered
        valid_status_filters = ("pending", "completed", "graded", "overdue")

        def _normalize_status_filters(value: Any) -> List[str]:
            if isinstance(value, str):
                stripped = value.strip()
                if not stripped:
                    return []
                try:
                    parsed = json.loads(stripped)
                except Exception:
                    parsed = None
                if isinstance(parsed, list):
                    value = parsed
                else:
                    value = [stripped]
            if not isinstance(value, list):
                return []
            normalized: List[str] = []
            seen: Set[str] = set()
            for item in value:
                lowered = str(item or "").strip().lower()
                if lowered == "all":
                    return list(valid_status_filters)
                if lowered in valid_status_filters and lowered not in seen:
                    seen.add(lowered)
                    normalized.append(lowered)
            return normalized

        status_filter_provided = False
        status_filter = raw.get("status_filter")
        if "status_filter" in raw:
            status_filter_provided = True
        if status_filter is None:
            status_filter = raw.get("statusFilter")
            if "statusFilter" in raw:
                status_filter_provided = True
        if status_filter is None:
            status_filter = raw.get("statusFilters")
            if "statusFilters" in raw:
                status_filter_provided = True
        normalized_status_filters = _normalize_status_filters(status_filter)
        if status_filter_provided:
            clean["status_filter"] = normalized_status_filters
        semester_filter_provided = False
        semester_filter = raw.get("semester_filter")
        if "semester_filter" in raw:
            semester_filter_provided = True
        if semester_filter is None:
            semester_filter = raw.get("semesterFilter")
            if "semesterFilter" in raw:
                semester_filter_provided = True
        if semester_filter is None:
            semester_filter = raw.get("semesterFilters")
            if "semesterFilters" in raw:
                semester_filter_provided = True
        if semester_filter_provided:
            clean["semester_filter"] = normalize_semester_selection(semester_filter)
        include_ignored_overdue = raw.get("include_ignored_overdue")
        if include_ignored_overdue is None:
            include_ignored_overdue = raw.get("includeIgnoredOverdue")
        coerced_include = _coerce_bool(include_ignored_overdue)
        if coerced_include is not None:
            clean["include_ignored_overdue"] = coerced_include
        for key, alias in (
            ("show_overdue", "showOverdue"),
            ("show_completed", "showCompleted"),
            ("show_graded", "showGraded"),
        ):
            value = raw.get(key)
            if value is None and alias:
                value = raw.get(alias)
            coerced = _coerce_bool(value)
            if coerced is not None:
                clean[key] = coerced
        ignored_assignment_uids = next(
            (raw[key] for key in (
                "ignored_assignment_uids", "ignoredAssignmentUids",
                "ignored_overdue_uids", "ignoredOverdueUids",
            ) if key in raw), None
        )
        if isinstance(ignored_assignment_uids, list):
            clean["ignored_assignment_uids"] = list(dict.fromkeys(
                str(item).strip() for item in ignored_assignment_uids if str(item).strip()
            ))[:500]
        return clean

    def _selected_view_username(
        raw_username: Optional[str], *, actor: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        user = actor or current_user()
        if not user:
            return None
        candidate = (raw_username or "").strip()
        if user.get("is_admin") and candidate:
            return candidate
        return user["username"]

    def _request_view_username() -> Optional[str]:
        raw = request.args.get("view_user")
        if raw is None and request.method != "GET":
            raw = request.form.get("view_user")
        return raw

    def get_viewed_username(*, actor: Optional[Dict[str, Any]] = None) -> Optional[str]:
        return _selected_view_username(_request_view_username(), actor=actor)

    def is_admin_viewing_other_user(
        *, actor: Optional[Dict[str, Any]] = None, viewed_username: Optional[str] = None
    ) -> bool:
        user = actor or current_user()
        if not user or not user.get("is_admin"):
            return False
        target_username = (
            viewed_username or get_viewed_username(actor=user) or ""
        ).strip()
        return bool(target_username and target_username != user["username"])

    def list_admin_view_options(limit: int = 500) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        guest_prefix = f"{chr(0x8A2A)}{chr(0x5BA2)}_"
        for raw in storage.list_cached_users(limit=limit):
            username = str(raw.get("username") or "").strip()
            if not username:
                continue
            if username.startswith(guest_prefix):
                continue
            fetched_ts = raw.get("fetched_ts")
            fetched_label = "尚未更新"
            if fetched_ts:
                try:
                    fetched_label = datetime.fromtimestamp(
                        int(fetched_ts), TAIPEI_TZ
                    ).strftime("%Y-%m-%d %H:%M")
                except Exception:
                    fetched_label = str(fetched_ts)
            try:
                assignment_count = int(raw.get("assignment_count") or 0)
            except (TypeError, ValueError):
                assignment_count = 0
            try:
                course_count = int(raw.get("course_count") or 0)
            except (TypeError, ValueError):
                course_count = 0
            items.append(
                {
                    "username": username,
                    "is_admin": bool(raw.get("is_admin")),
                    "fetched_ts": fetched_ts,
                    "fetched_label": fetched_label,
                    "assignment_count": assignment_count,
                    "course_count": course_count,
                }
            )
        return items

    def get_user_preferences(username: Optional[str] = None) -> Dict[str, Any]:
        prefs = dict(DEFAULT_PREFERENCES)
        resolved_username = _selected_view_username(username)
        if not resolved_username:
            return prefs
        stored = storage.load_user_preferences(resolved_username)
        prefs.update(_sanitize_preferences(stored))
        prefs["ignored_overdue_uids"] = prefs["ignored_assignment_uids"]
        return prefs

    def update_user_preferences(
        partial: Dict[str, Any], *, username: Optional[str] = None
    ) -> Dict[str, Any]:
        prefs = get_user_preferences(username)
        sanitized = _sanitize_preferences(partial)
        prefs.update(sanitized)
        prefs["ignored_overdue_uids"] = prefs["ignored_assignment_uids"]
        resolved_username = _selected_view_username(username)
        if not resolved_username:
            return prefs
        storage.save_user_preferences(resolved_username, prefs)
        return prefs

    def get_assign_cache(username: Optional[str] = None) -> Optional[Dict[str, Any]]:
        resolved_username = _selected_view_username(username)
        if not resolved_username:
            return None
        return load_cache_from_disk(resolved_username)

    def _annotate_new_assignments(
        result: Optional[Dict[str, Any]],
        *,
        username: Optional[str],
        readonly: bool,
        now_ts: int,
    ) -> None:
        if not username or not isinstance(result, dict):
            return
        assignments = result.get("all_assignments")
        if not isinstance(assignments, list) or not assignments:
            return
        assignment_uids: List[str] = []
        for item in assignments:
            if not isinstance(item, dict):
                continue
            try:
                course_id = int(item.get("course_id"))
            except (TypeError, ValueError):
                continue
            uid = storage.assignment_uid(
                course_id, str(item.get("title") or "").strip(), item.get("url")
            )
            if not uid.strip():
                continue
            item["assignment_uid"] = uid
            assignment_uids.append(uid)
        if not assignment_uids:
            return
        first_seen_map = (
            storage.load_assignment_view_map(username, assignment_uids)
            if readonly
            else storage.mark_assignment_views(
                username, assignment_uids, seen_ts=now_ts
            )
        )
        for item in assignments:
            uid = str(item.get("assignment_uid") or "").strip()
            first_seen_ts = first_seen_map.get(uid)
            is_new = bool(
                first_seen_ts is not None
                and now_ts - int(first_seen_ts) <= NEW_ASSIGNMENT_WINDOW_SECONDS
            )
            item["first_seen_ts"] = first_seen_ts
            item["is_new"] = is_new
            item["new_until_ts"] = (
                (int(first_seen_ts) + NEW_ASSIGNMENT_WINDOW_SECONDS)
                if first_seen_ts is not None
                else None
            )
        for course in result.get("courses") or []:
            if not isinstance(course, dict):
                continue
            for item in course.get("assignments") or []:
                if not isinstance(item, dict):
                    continue
                try:
                    course_id = int(item.get("course_id"))
                except (TypeError, ValueError):
                    course_id = None
                uid = (
                    storage.assignment_uid(
                        course_id,
                        str(item.get("title") or "").strip(),
                        item.get("url"),
                    )
                    if course_id is not None
                    else ""
                )
                first_seen_ts = first_seen_map.get(uid)
                item["assignment_uid"] = uid
                item["first_seen_ts"] = first_seen_ts
                item["is_new"] = bool(
                    first_seen_ts is not None
                    and now_ts - int(first_seen_ts) <= NEW_ASSIGNMENT_WINDOW_SECONDS
                )
                item["new_until_ts"] = (
                    (int(first_seen_ts) + NEW_ASSIGNMENT_WINDOW_SECONDS)
                    if first_seen_ts is not None
                    else None
                )

    def set_assign_cache_for_user(
        username: str, result: Dict[str, Any], excel_data: Optional[str]
    ) -> None:
        if not username:
            return
        existing = load_cache_from_disk(username) or {}
        slim = dict(result)
        slim.pop("debug_files", None)
        slim.pop("login_method", None)
        payload = {
            "result": json_safe(slim),
            "excel_data": excel_data,
            "ts": int(datetime.now(TAIPEI_TZ).timestamp()),
        }
        stored_prefs = _sanitize_preferences(existing.get("preferences"))
        if stored_prefs:
            payload["preferences"] = stored_prefs
        save_cache_to_disk(username, payload)
        if not result.get("errors"):
            try:
                notification_service.observe(username, slim)
            except Exception:
                app.logger.warning("Notification observation will retry on the next worker tick")

    def set_assign_cache(result: Dict[str, Any], excel_data: Optional[str]) -> None:
        user = current_user()
        if not user:
            return
        set_assign_cache_for_user(user["username"], result, excel_data)

    def _generate_excel_data(
        assignments: Optional[List[Dict[str, Any]]],
    ) -> Optional[str]:
        if not assignments:
            return None
        try:
            excel_stream = build_excel(assignments, return_bytes=True)
            return base64.b64encode(excel_stream.getvalue()).decode("ascii")
        except Exception:
            return None

    def clear_assign_cache() -> None:
        user = current_user()
        if not user:
            return
        storage.delete_user_cache(user["username"])

    def _google_ready() -> bool:
        return bool(google_client_id and google_client_secret and google_redirect_uri)

    def _assignment_uid(item: Dict[str, Any]) -> str:
        return f"{item.get('course_id')}|{item.get('title')}|{item.get('url')}"

    def _select_assignments_from_result(
        result: Optional[Dict[str, Any]], selected_uids: List[str]
    ) -> List[Dict[str, Any]]:
        if not isinstance(result, dict) or not selected_uids:
            return []
        selected = set(selected_uids)
        return [
            item
            for item in result.get("all_assignments", [])
            if _assignment_uid(item) in selected
        ]

    def _google_redirect_uri() -> str:
        return google_redirect_uri or url_for("google_callback", _external=True)

    def _google_state_signer() -> URLSafeTimedSerializer:
        return URLSafeTimedSerializer(app.secret_key, salt="google-calendar")

    def _build_google_state() -> str:
        token = secrets.token_urlsafe(16)
        return _google_state_signer().dumps({"nonce": token})

    def _verify_google_state(value: str) -> bool:
        try:
            _google_state_signer().loads(value, max_age=300)
            return True
        except SignatureExpired:
            flash("Google 授權逾時，請再試一次。", "error")
        except BadSignature:
            flash("Google 授權驗證失敗，請重新操作。", "error")
        return False

    def load_google_tokens(username: str) -> Optional[Dict[str, Any]]:
        return storage.load_google_tokens(username)

    def save_google_tokens(username: str, payload: Dict[str, Any]) -> None:
        storage.save_google_tokens(username, dict(payload))

    def clear_google_tokens(username: str) -> None:
        storage.clear_google_tokens(username)

    def _ensure_google_access_token(
        username: str, tokens: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not _google_ready():
            raise RuntimeError("尚未設定 Google OAuth。")
        expires_at = tokens.get("expires_at", 0)
        if time.time() < expires_at - 60:
            return tokens
        refresh_token = tokens.get("refresh_token")
        if not refresh_token:
            raise RuntimeError("Google access token 已過期，請重新授權。")
        refreshed = refresh_google_token(
            refresh_token,
            client_id=google_client_id,
            client_secret=google_client_secret,
        )
        tokens["access_token"] = refreshed.get("access_token")
        tokens["expires_at"] = compute_expiry(refreshed.get("expires_in", 3600))
        save_google_tokens(username, tokens)
        return tokens

    def _escape_ics_text(value: Optional[str]) -> str:
        text = (value or "").replace("\\", "\\\\")
        text = text.replace("\n", "\\n").replace(",", "\\,").replace(";", "\\;")
        return text

    def _build_calendar(assignments: List[Dict[str, Any]]) -> Optional[str]:
        if not assignments:
            return None
        dtstamp = datetime.now(TAIPEI_TZ).strftime("%Y%m%dT%H%M%S")
        lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//NYCU E3//EN",
            "CALSCALE:GREGORIAN",
            "X-WR-TIMEZONE:Asia/Taipei",
        ]
        for idx, entry in enumerate(assignments):
            due_ts = entry.get("due_ts")
            if not due_ts:
                continue
            due_dt = datetime.fromtimestamp(due_ts, tz=TAIPEI_TZ)
            end_dt = due_dt + timedelta(hours=1)
            dt_value = due_dt.strftime("%Y%m%dT%H%M%S")
            dt_end_value = end_dt.strftime("%Y%m%dT%H%M%S")
            summary = entry.get("title", "").strip()
            description = entry.get("url") or ""
            lines += [
                "BEGIN:VEVENT",
                f"UID=e3-{entry.get('course_id', 'unknown')}-{idx}@e3",
                f"DTSTAMP={dtstamp}",
                f"DTSTART;TZID=Asia/Taipei:{dt_value}",
                f"DTEND;TZID=Asia/Taipei:{dt_end_value}",
                f"SUMMARY:{_escape_ics_text(summary)}",
                f"DESCRIPTION:{_escape_ics_text(description)}",
                "END:VEVENT",
            ]
        lines.append("END:VCALENDAR")
        return "\r\n".join(lines)

    def _build_dashboard_context(user: Dict[str, Any]) -> Dict[str, Any]:
        user = dict(user, surname=storage.load_user_surname(user["username"]),
                    student_number=storage.load_student_number(user["username"]))
        admin_view_options: List[Dict[str, Any]] = []
        viewed_username = user["username"]
        if user.get("is_admin"):
            admin_view_options = list_admin_view_options()
            requested_view_username = (_request_view_username() or "").strip()
            if requested_view_username and requested_view_username != user["username"]:
                valid_usernames = {item["username"] for item in admin_view_options}
                if requested_view_username in valid_usernames:
                    viewed_username = requested_view_username
                else:
                    flash("找不到指定帳號的資料快取，已切回目前登入帳號。", "warning")
        is_admin_view = is_admin_viewing_other_user(
            actor=user, viewed_username=viewed_username
        )
        if user["username"] not in {item["username"] for item in admin_view_options}:
            self_cache = load_cache_from_disk(user["username"]) or {}
            admin_view_options.insert(
                0,
                {
                    "username": user["username"],
                    "is_admin": bool(user.get("is_admin")),
                    "fetched_ts": self_cache.get("ts"),
                    "fetched_label": (
                        datetime.fromtimestamp(
                            int(self_cache.get("ts")), TAIPEI_TZ
                        ).strftime("%Y-%m-%d %H:%M")
                        if self_cache.get("ts")
                        else "尚未更新"
                    ),
                    "assignment_count": len(
                        (self_cache.get("result") or {}).get("all_assignments", [])
                    ),
                    "course_count": len(
                        (self_cache.get("result") or {}).get("courses", [])
                    ),
                },
            )
        cache = get_assign_cache(viewed_username)
        result = cache.get("result") if cache else None
        excel_data = cache.get("excel_data") if cache else None
        preferences = get_user_preferences(viewed_username)
        if result:
            preferred_semesters = preferences.get("semester_filter") or None
            annotate_result_semesters(result, selected_keys=preferred_semesters)
        guest_mode = bool(user.get("is_guest"))
        if result and not excel_data:
            excel_data = _generate_excel_data(result.get("all_assignments"))
            if excel_data:
                set_assign_cache_for_user(viewed_username, result, excel_data)
        if not result and not guest_mode and not is_admin_view:
            flash("正在載入資料，請稍候...", "info")
        google_linked = bool(not is_admin_view and load_google_tokens(user["username"]))
        stats = usage_stats()
        stats_version_value = current_stats_version()
        announcements_list = load_announcements(
            None if is_admin_view else user["username"]
        )
        cache_ts_val = cache.get("ts") if cache else None
        now_ts = int(datetime.now(TAIPEI_TZ).timestamp())
        _annotate_new_assignments(
            result,
            username=viewed_username,
            readonly=is_admin_view,
            now_ts=now_ts,
        )
        last_updated_label = None
        if cache_ts_val:
            try:
                last_updated_label = datetime.fromtimestamp(
                    int(cache_ts_val), TAIPEI_TZ
                ).strftime("%Y-%m-%d %H:%M")
            except Exception:
                last_updated_label = None
        return {
            "result": result,
            "excel_data": excel_data,
            "user": user,
            "google_ready": _google_ready(),
            "google_linked": google_linked,
            "guest_mode": guest_mode,
            "stats": stats,
            "stats_version": stats_version_value,
            "now_ts": now_ts,
            "preferences": preferences,
            "cache_ts": cache_ts_val,
            "last_updated_ts": cache_ts_val,
            "last_updated_label": last_updated_label,
            "announcements": announcements_list,
            "announcement_version": (
                announcements_list[0]["id"] if announcements_list else None
            ),
            "viewed_username": viewed_username,
            "viewed_student_number": storage.load_student_number(viewed_username),
            "is_admin_view": is_admin_view,
            "admin_view_options": admin_view_options,
        }

    def fetch_assignments_for(
        user: Dict[str, str],
        *,
        semester_keys: Optional[Sequence[str]] = None,
        include_archived: bool = False,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        selected_semesters = normalize_semester_selection(semester_keys)
        if semester_keys is None:
            stored = _sanitize_preferences(
                storage.load_user_preferences(str(user.get("username") or ""))
            )
            selected_semesters = normalize_semester_selection(
                stored.get("semester_filter")
            )
        if not selected_semesters:
            selected_semesters = [current_semester_key()]
        previous_cache = storage.load_user_cache(str(user.get("username") or "")) or {}
        previous_result = previous_cache.get("result")
        if not isinstance(previous_result, dict):
            previous_result = {}
        annotate_result_semesters(previous_result)
        current_key = current_semester_key()
        cached_semesters = normalize_semester_keys(
            [
                item.get("key")
                for item in (previous_result.get("available_semesters") or [])
                if isinstance(item, dict)
            ]
        )
        cached_archived = [key for key in cached_semesters if key != current_key]
        opts = CollectOptions(
            base_url=base_url,
            scope=default_scope,
            course_id=None,
            include_completed=True,
            all_courses=False,
            # Once historical courses exist locally, refresh only the active
            # semester and merge the archived semesters from the durable cache.
            # This avoids repeatedly requesting removed E3 course endpoints.
            all_courses_all_terms=bool(include_archived and not cached_archived),
            semester_keys=None,
            username=None,
            password=None,
            moodle_session=user.get("moodle_session"),
            insecure=False,
            timeout=default_timeout,
            debug=False,
        )
        refreshed_result = collect_assignments(opts)
        result = merge_current_semester_cache(
            previous_result,
            refreshed_result,
            selected_keys=selected_semesters,
        )
        excel_data = _generate_excel_data(result.get("all_assignments"))
        return result, excel_data

    (
        login,
        api_cache,
        save_preferences,
        guest_login,
        guest_tool,
        guest_tool_source,
        guest_import,
        announcement_vote,
        google_authorize,
        google_callback,
        google_unlink,
        google_sync,
        logout,
        api_assignments,
        calendar_export,
    ) = register_assignments_routes(
        LOGIN_TEMPLATE=LOGIN_TEMPLATE,
        PRIVACY_TEMPLATE=PRIVACY_TEMPLATE,
        ROOT_DIR=ROOT_DIR,
        TERMS_TEMPLATE=TERMS_TEMPLATE,
        _build_calendar=_build_calendar,
        _build_google_state=_build_google_state,
        _ensure_google_access_token=_ensure_google_access_token,
        _generate_excel_data=_generate_excel_data,
        _google_ready=_google_ready,
        _google_redirect_uri=_google_redirect_uri,
        _mark_refresh_job_done=_mark_refresh_job_done,
        _mark_refresh_job_started=_mark_refresh_job_started,
        _refresh_job_state=_refresh_job_state,
        _select_assignments_from_result=_select_assignments_from_result,
        _start_web_session=_start_web_session,
        _verify_google_state=_verify_google_state,
        admin_user_id=admin_user_id,
        app=app,
        app_home_url=app_home_url,
        base_url=base_url,
        canonical_host=canonical_host,
        clear_google_tokens=clear_google_tokens,
        current_stats_version=current_stats_version,
        current_user=current_user,
        default_timeout=default_timeout,
        fetch_assignments_for=fetch_assignments_for,
        get_assign_cache=get_assign_cache,
        get_user_preferences=get_user_preferences,
        get_viewed_username=get_viewed_username,
        google_calendar_id=google_calendar_id,
        google_client_id=google_client_id,
        google_client_secret=google_client_secret,
        is_admin_viewing_other_user=is_admin_viewing_other_user,
        legal_effective_date=legal_effective_date,
        legal_entity_name=legal_entity_name,
        load_announcements=load_announcements,
        load_cache_from_disk=load_cache_from_disk,
        load_google_tokens=load_google_tokens,
        login_required=login_required,
        record_ui_event=record_ui_event,
        save_google_tokens=save_google_tokens,
        set_announcement_vote=set_announcement_vote,
        set_assign_cache=set_assign_cache,
        set_assign_cache_for_user=set_assign_cache_for_user,
        storage=storage,
        support_email=support_email,
        update_user_preferences=update_user_preferences,
        usage_stats=usage_stats,
        session_identity_sync=session_identity_sync,
    )

    register_dashboard_routes(
        app=app,
        current_user=current_user,
        HOME_TEMPLATE=HOME_TEMPLATE,
        WEB_TEMPLATE=WEB_TEMPLATE,
        _build_dashboard_context=_build_dashboard_context,
        usage_stats=usage_stats,
        current_stats_version=current_stats_version,
        app_home_url=app_home_url,
        support_email=support_email,
    )
    register_notification_routes(app, storage, current_user, login_required, notification_service, record_activity)
    course_news = CourseAnnouncementService(storage, base_url, default_timeout)
    app.extensions['e3_course_announcements'] = course_news
    register_course_announcement_routes(app, storage, current_user, login_required,
        load_cache_from_disk, get_user_preferences, course_news, record_activity)
    notification_service.start(app, fetch_assignments_for, set_assign_cache_for_user)
    session_identity_sync.start(app)
    return list_admin_view_options
