import base64
import json
import math
import os
import re
import secrets
import shutil
import threading
import time
import hashlib
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urlsplit, urlunsplit

import requests
from flask import Flask, Response, flash, redirect, render_template_string, request, send_file, session, url_for, has_request_context
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.http import http_date
from werkzeug.utils import secure_filename

from ..services.collector import CollectOptions, collect_assignments
from ..services.google_calendar import (
    GOOGLE_CALENDAR_SCOPE,
    GoogleUnauthorizedError,
    build_google_authorize_url,
    compute_expiry,
    exchange_code_for_google_token,
    refresh_google_token,
    sync_assignments_to_google_calendar,
)
from ..services.http import login_with_password
from ..shared.config import load_env_defaults
from ..shared.constants import TAIPEI_TZ
from ..shared.storage import PersistentStorage
from ..shared.study_plan_data import STUDY_PLAN_VIDEO_INVENTORY
from ..shared.excel import build_excel
from ..shared.utils import json_safe

PASSIVE_TRAFFIC_ACTIONS = {"heartbeat", "refresh_assignments"}

ROOT_DIR = Path(__file__).resolve().parents[3]
FRONTEND_TEMPLATE_DIR = ROOT_DIR / "frontend" / "templates"
TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "web.html"
WEB_TEMPLATE = TEMPLATE_PATH.read_text(encoding="utf-8")
LOGIN_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "login.html"
LOGIN_TEMPLATE = LOGIN_TEMPLATE_PATH.read_text(encoding="utf-8")
TRAFFIC_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "admin_traffic.html"
TRAFFIC_TEMPLATE = TRAFFIC_TEMPLATE_PATH.read_text(encoding="utf-8")
ANNOUNCEMENTS_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "admin_announcements.html"
ANNOUNCEMENTS_TEMPLATE = ANNOUNCEMENTS_TEMPLATE_PATH.read_text(encoding="utf-8")
HOME_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "home.html"
HOME_TEMPLATE = HOME_TEMPLATE_PATH.read_text(encoding="utf-8")
PRIVACY_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "privacy.html"
PRIVACY_TEMPLATE = PRIVACY_TEMPLATE_PATH.read_text(encoding="utf-8")
TERMS_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "terms.html"
TERMS_TEMPLATE = TERMS_TEMPLATE_PATH.read_text(encoding="utf-8")
FEEDBACK_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "feedback.html"
FEEDBACK_TEMPLATE = FEEDBACK_TEMPLATE_PATH.read_text(encoding="utf-8")
ADMIN_FEEDBACK_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "admin_feedback.html"
ADMIN_FEEDBACK_TEMPLATE = ADMIN_FEEDBACK_TEMPLATE_PATH.read_text(encoding="utf-8")
STUDY_PLAN_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "admin_study_plan.html"
STUDY_PLAN_TEMPLATE = STUDY_PLAN_TEMPLATE_PATH.read_text(encoding="utf-8")
STUDY_HOME_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "admin_study_home.html"
STUDY_HOME_TEMPLATE = STUDY_HOME_TEMPLATE_PATH.read_text(encoding="utf-8")
PUBLIC_STUDY_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "public_study_progress.html"
PUBLIC_STUDY_TEMPLATE = PUBLIC_STUDY_TEMPLATE_PATH.read_text(encoding="utf-8")
STUDY_RECALL_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "study_recall.html"
STUDY_RECALL_TEMPLATE = STUDY_RECALL_TEMPLATE_PATH.read_text(encoding="utf-8")
STUDY_UPLOAD_TRACKER_TEMPLATE_PATH = FRONTEND_TEMPLATE_DIR / "_study_upload_tracker.html"
STUDY_UPLOAD_TRACKER_TEMPLATE = STUDY_UPLOAD_TRACKER_TEMPLATE_PATH.read_text(encoding="utf-8")

STUDY_PLAN_BLOCKS = (
    {"subject": "線性代數", "weeks": 4, "total_minutes": 4107.8, "lesson_targets": (11, 22, 32, 42)},
    {"subject": "離散數學", "weeks": 4, "total_minutes": 4770.4, "lesson_targets": (6, 12, 17, 23)},
    {"subject": "資料結構", "weeks": 5, "total_minutes": 6590.0, "lesson_targets": (13, 26, 40, 53, 67)},
    {"subject": "演算法", "weeks": 2, "total_minutes": 1610.5, "lesson_targets": (8, 16)},
    {"subject": "作業系統", "weeks": 3, "total_minutes": 5478.3, "lesson_targets": (19, 39, 58)},
    {"subject": "計算機組織", "weeks": 5, "total_minutes": 8633.8, "lesson_targets": (17, 34, 51, 68, 78)},
)
STUDY_PLAN_START = "2026-06-29"
STUDY_PLAN_END = "2026-12-06"
STUDY_PLAN_SUBJECTS = tuple(block["subject"] for block in STUDY_PLAN_BLOCKS)
STUDY_PLAN_WEEKEND_VIDEO_HOUR_CAP = 4.0
STUDY_PLAN_DAILY_LABELS = (
    "週一",
    "週二",
    "週三",
    "週四",
    "週五",
    "週六",
    "週日",
)
STUDY_PLAN_COMPLETE_TOLERANCE_SECONDS = 5.0
STUDY_PLAN_COMPLETE_RATIO = 0.995
STUDY_PLAN_DAY_CUTOFF_HOUR = 8
STUDY_NOTE_MAX_IMAGE_BYTES = 3 * 1024 * 1024
STUDY_NOTE_MAX_TOTAL_BYTES = 24 * 1024 * 1024
STUDY_NOTE_MAX_REQUEST_BYTES = 28 * 1024 * 1024


def _study_plan_business_date(now: Optional[datetime] = None) -> date:
    current = now or datetime.now(TAIPEI_TZ)
    return (current - timedelta(hours=STUDY_PLAN_DAY_CUTOFF_HOUR)).date()


def _study_plan_business_day_from_timestamp(value: Any) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        # Storage timestamps are UTC even when they do not carry an explicit offset.
        # Normalising first keeps the 08:00 Taipei learning-day cutoff consistent with
        # activity events and daily snapshots.
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        parsed = parsed.astimezone(TAIPEI_TZ).replace(tzinfo=None)
        return (parsed - timedelta(hours=STUDY_PLAN_DAY_CUTOFF_HOUR)).date().isoformat()
    except ValueError:
        if len(raw) >= 10:
            return raw[:10]
        return None


def _study_plan_video_completion(duration_seconds: Any, watched_seconds: Any) -> float:
    try:
        duration = max(0.0, float(duration_seconds or 0))
        watched = max(0.0, float(watched_seconds or 0))
    except (TypeError, ValueError):
        return 0.0
    if duration <= 0:
        return 0.0
    if _study_plan_video_is_complete(duration, watched):
        return 100.0
    return min(100.0, watched / duration * 100)


def _study_plan_video_is_complete(duration_seconds: Any, watched_seconds: Any) -> bool:
    try:
        duration = max(0.0, float(duration_seconds or 0))
        watched = max(0.0, float(watched_seconds or 0))
    except (TypeError, ValueError):
        return False
    if duration <= 0:
        return False
    return watched >= duration - STUDY_PLAN_COMPLETE_TOLERANCE_SECONDS or watched / duration >= STUDY_PLAN_COMPLETE_RATIO


def _study_plan_total_is_complete(target_seconds: Any, watched_seconds: Any) -> bool:
    try:
        target = max(0.0, float(target_seconds or 0))
        watched = max(0.0, float(watched_seconds or 0))
    except (TypeError, ValueError):
        return False
    if target <= 0:
        return False
    return watched >= target - STUDY_PLAN_COMPLETE_TOLERANCE_SECONDS or watched / target >= STUDY_PLAN_COMPLETE_RATIO


def _study_plan_completion_percent(target_seconds: Any, watched_seconds: Any, *, complete_override: bool = False) -> float:
    try:
        target = max(0.0, float(target_seconds or 0))
        watched = max(0.0, float(watched_seconds or 0))
    except (TypeError, ValueError):
        return 0.0
    if target <= 0:
        return 0.0
    if complete_override or _study_plan_total_is_complete(target, watched):
        return 100.0
    return min(100.0, watched / target * 100)


def _study_plan_daily_recommendations(
    subject: str,
    target_seconds: float,
    watched_seconds: float,
    week_start: date,
    today: date,
    *,
    week_is_complete: bool = False,
) -> Tuple[float, float, List[Dict[str, Any]]]:
    video_hours = target_seconds / 3600 if target_seconds else 0.0
    watched_hours = watched_seconds / 3600 if watched_seconds else 0.0
    weekly_hours = video_hours
    average_hours = weekly_hours / 7 if weekly_hours else 0.0
    weekend_hours = min(average_hours, STUDY_PLAN_WEEKEND_VIDEO_HOUR_CAP)
    weekday_hours = max(0.0, (weekly_hours - weekend_hours * 2) / 5) if weekly_hours else 0.0
    daily_targets = [weekday_hours] * 5 + [weekend_hours] * 2
    daily_rows: List[Dict[str, Any]] = []
    remaining_hours = watched_hours
    for index, label in enumerate(STUDY_PLAN_DAILY_LABELS):
        target_hours = daily_targets[index]
        if week_is_complete:
            credited_hours = target_hours
            completion = 100.0 if target_hours else 0.0
        else:
            credited_hours = min(max(remaining_hours, 0.0), target_hours)
            completion = min(100.0, (credited_hours / target_hours * 100) if target_hours else 0.0)
        remaining_hours -= target_hours
        current_day = week_start + timedelta(days=index)
        if completion >= 100:
            if today < current_day:
                state = "early"
                state_label = "提早完成"
            else:
                state = "complete"
                state_label = "完成"
        elif completion > 0:
            if today < current_day:
                state = "early"
                state_label = "超前"
            else:
                state = "partial"
                state_label = "部分"
        elif current_day == today:
            state = "active"
            state_label = "進行中"
        elif today > current_day:
            state = "behind"
            state_label = "待補"
        else:
            state = "upcoming"
            state_label = "未開始"
        daily_rows.append(
            {
                "label": label,
                "date": current_day.isoformat(),
                "short_date": current_day.strftime("%m/%d"),
                "focus": "看影片",
                "hours": round(target_hours, 1),
                "credited_hours": round(credited_hours, 1),
                "completion": round(completion, 1),
                "state": state,
                "state_label": state_label,
            }
        )
    return round(video_hours, 1), round(weekly_hours, 1), daily_rows


class TrafficTracker:
    def __init__(
        self,
        activity_window: int = 60,
        count_interval: int = 3600,
        storage_path: Optional[Path] = None,
        log_path: Optional[Path] = None,
        max_events: int = 200,
        state_loader: Optional[Callable[[], Optional[Dict[str, Any]]]] = None,
        state_saver: Optional[Callable[[Dict[str, Any]], None]] = None,
        event_loader: Optional[Callable[[int], List[Dict[str, Any]]]] = None,
        event_writer: Optional[Callable[[Dict[str, Any]], None]] = None,
        event_clearer: Optional[Callable[[], None]] = None,
    ) -> None:
        self._activity_window = activity_window
        self._count_interval = count_interval
        self._active_ips: Dict[str, float] = {}
        self._last_total_increment: Dict[str, float] = {}
        self._ip_total_hits: Dict[str, int] = {}
        self._ip_users: Dict[str, str] = {}
        self._active_users: Dict[str, float] = {}
        self._user_total_hits: Dict[str, int] = {}
        self._user_last_count: Dict[str, float] = {}
        self._user_last_seen: Dict[str, float] = {}
        self._user_flags: Dict[str, bool] = {}
        self._total_hits = 0
        self._recent_events: List[Dict[str, Any]] = []
        self._max_events = max_events
        self._version = 0
        self._version = 0
        self._lock = threading.Lock()
        self._storage_path = Path(storage_path) if storage_path else None
        self._log_path = Path(log_path) if log_path else None
        self._concurrent_history: List[Dict[str, Any]] = []
        self._concurrent_history: List[Dict[str, Any]] = []
        self._hourly_buckets: Dict[int, Set[str]] = {}
        self._hourly_series: List[Dict[str, Any]] = []
        self._state_loader = state_loader
        self._state_saver = state_saver
        self._event_loader = event_loader
        self._event_writer = event_writer
        self._event_clearer = event_clearer
        if self._storage_path:
            self._storage_path.parent.mkdir(parents=True, exist_ok=True)
        if self._log_path:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
        if self._state_loader:
            self._load_from_backend()
        elif self._storage_path:
            self._load_from_disk()
        if self._event_loader:
            try:
                self._recent_events = self._event_loader(self._max_events) or []
            except Exception:
                self._recent_events = []
        elif self._log_path:
            self._load_recent_events()

    def _purge_expired(self, now: float) -> bool:
        expired_ips = [ip for ip, ts in self._active_ips.items() if now - ts > self._activity_window]
        for ip in expired_ips:
            self._active_ips.pop(ip, None)
        expired_users = [user for user, ts in self._active_users.items() if now - ts > self._activity_window]
        for user in expired_users:
            self._active_users.pop(user, None)
        return bool(expired_ips or expired_users)

    def remove_user_stats(self, username: str) -> bool:
        """Remove all tracked state for a specific username."""
        if not username:
            return False
        changed = False
        with self._lock:
            if username in self._user_total_hits:
                self._user_total_hits.pop(username, None)
                changed = True
            if username in self._user_last_count:
                self._user_last_count.pop(username, None)
                changed = True
            if username in self._user_last_seen:
                self._user_last_seen.pop(username, None)
                changed = True
            if username in self._active_users:
                self._active_users.pop(username, None)
                changed = True
            if username in self._user_flags:
                self._user_flags.pop(username, None)
                changed = True
            # detach IP mappings pointing to this user
            ips_to_clear = [ip for ip, user in self._ip_users.items() if user == username]
            for ip in ips_to_clear:
                self._ip_users.pop(ip, None)
            if ips_to_clear:
                changed = True
            # prune hourly buckets and recalc series counts
            if self._hourly_buckets:
                for ts, members in list(self._hourly_buckets.items()):
                    if username in members:
                        members.discard(username)
                        changed = True
                        self._hourly_buckets[ts] = members
                # rebuild hourly_series counts
                rebuilt = []
                for ts, members in self._hourly_buckets.items():
                    rebuilt.append({"ts": ts, "count": len(members)})
                self._hourly_series = sorted(rebuilt, key=lambda x: x["ts"])
            if changed:
                self._version += 1
                self._save_to_disk()
        return changed

    def _is_guest_user(self, username: Optional[str]) -> bool:
        if not username:
            return False
        normalized = str(username)
        if normalized in self._user_flags:
            return bool(self._user_flags[normalized])
        return normalized.startswith("訪客")

    def record_visit(
        self,
        ip: Optional[str],
        *,
        action: Optional[str] = None,
        status: str = "success",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not action:
            return
        action_lc = str(action).lower()
        now = time.time()
        with self._lock:
            prev_ts = self._active_ips.get(ip) if ip else None
            previously_online_ip = bool(prev_ts and now - prev_ts <= self._activity_window)
            if ip:
                self._active_ips[ip] = now
            username = None
            is_guest_user = False
            if metadata:
                username = metadata.get("username")
                is_guest_user = bool(metadata.get("is_guest"))
            if username:
                username = str(username)
                if ip:
                    self._ip_users[ip] = username
                self._user_flags[username] = is_guest_user
                self._user_last_seen[username] = now
                if not is_guest_user:
                    self._active_users[username] = now
                if not is_guest_user and action_lc not in PASSIVE_TRAFFIC_ACTIONS:
                    self._update_hourly(username, now)
            last_hit = self._last_total_increment.get(ip, 0) if ip else 0
            stats_changed = True
            if ip and now - last_hit >= self._count_interval:
                self._total_hits += 1
                self._last_total_increment[ip] = now
                self._ip_total_hits[ip] = self._ip_total_hits.get(ip, 0) + 1
                if username and not is_guest_user:
                    self._user_total_hits[username] = self._user_total_hits.get(username, 0) + 1
                    self._user_last_count[username] = now
            elif ip and ip not in self._ip_total_hits:
                self._ip_total_hits[ip] = 0
            if action_lc not in PASSIVE_TRAFFIC_ACTIONS:
                self._append_event(
                    {"ts": now, "ip": ip, "action": action, "status": status, "meta": metadata or {}}
                )
            self._purge_old_total_entries(now)
            previously_online_user = False
            if username and not is_guest_user:
                last_seen = self._active_users.get(username)
                previously_online_user = bool(last_seen and now - last_seen <= self._activity_window)
            if self._purge_expired(now) or not previously_online_ip or not previously_online_user:
                stats_changed = True
            if stats_changed:
                self._version += 1
                self._store_concurrent_snapshot(now)
            self._save_to_disk()

    def snapshot(self) -> Dict[str, int]:
        now = time.time()
        with self._lock:
            self._purge_expired(now)
            user_count, _ = self._online_counts(now)
            unique_users: Set[str] = set()
            for username in self._user_total_hits.keys():
                if not self._is_guest_user(username):
                    unique_users.add(username)
            for username in self._user_last_seen.keys():
                if not self._is_guest_user(username):
                    unique_users.add(username)
            for username in self._active_users.keys():
                if not self._is_guest_user(username):
                    unique_users.add(username)
            cutoff = now - 86400
            daily_users: Set[str] = set()
            for ev in self._recent_events:
                ts = ev.get("ts")
                if not ts or ts < cutoff:
                    continue
                meta = ev.get("meta") or {}
                username = meta.get("username")
                if username and not meta.get("is_guest"):
                    daily_users.add(str(username))
            return {
                "online": user_count,
                "total": self._total_hits,
                "total_users": len(unique_users),
                "daily_users": len(daily_users),
                "online_users": user_count,
            }

    def version(self) -> int:
        with self._lock:
            return self._version

    def _load_from_backend(self) -> None:
        if not self._state_loader:
            return
        try:
            data = self._state_loader() or {}
        except Exception:
            data = {}
        self._apply_state_payload(data)

    def _load_from_disk(self) -> None:
        if not self._storage_path or not self._storage_path.exists():
            return
        try:
            data = json.loads(self._storage_path.read_text(encoding="utf-8"))
        except Exception:
            return
        self._apply_state_payload(data)

    def _apply_state_payload(self, data: Optional[Dict[str, Any]]) -> None:
        if not isinstance(data, dict):
            return
        try:
            self._total_hits = int(data.get("total", 0))
        except Exception:
            self._total_hits = 0
        try:
            self._version = int(data.get("version", int(self._total_hits)))
        except Exception:
            self._version = int(self._total_hits)
        active = data.get("active") or {}
        cleaned: Dict[str, float] = {}
        if isinstance(active, dict):
            for ip, ts in active.items():
                try:
                    cleaned[str(ip)] = float(ts)
                except (TypeError, ValueError):
                    continue
        self._active_ips = cleaned
        last_total = data.get("last_total") or {}
        cleaned_total: Dict[str, float] = {}
        if isinstance(last_total, dict):
            for ip, ts in last_total.items():
                try:
                    cleaned_total[str(ip)] = float(ts)
                except (TypeError, ValueError):
                    continue
        self._last_total_increment = cleaned_total
        ip_totals = data.get("ip_totals") or {}
        cleaned_hits: Dict[str, int] = {}
        if isinstance(ip_totals, dict):
            for ip, count in ip_totals.items():
                try:
                    cleaned_hits[str(ip)] = int(count)
                except (TypeError, ValueError):
                    continue
        self._ip_total_hits = cleaned_hits
        ip_users = data.get("ip_users") or {}
        cleaned_users: Dict[str, str] = {}
        if isinstance(ip_users, dict):
            for ip, name in ip_users.items():
                try:
                    cleaned_users[str(ip)] = str(name)
                except Exception:
                    continue
        self._ip_users = cleaned_users
        active_users = data.get("active_users") or {}
        cleaned_active_users: Dict[str, float] = {}
        if isinstance(active_users, dict):
            for username, ts in active_users.items():
                try:
                    cleaned_active_users[str(username)] = float(ts)
                except (TypeError, ValueError):
                    continue
        self._active_users = cleaned_active_users
        user_totals = data.get("user_totals") or {}
        cleaned_user_totals: Dict[str, int] = {}
        if isinstance(user_totals, dict):
            for username, count in user_totals.items():
                try:
                    cleaned_user_totals[str(username)] = int(count)
                except (TypeError, ValueError):
                    continue
        self._user_total_hits = cleaned_user_totals
        user_last_count = data.get("user_last_count") or {}
        cleaned_last_count: Dict[str, float] = {}
        if isinstance(user_last_count, dict):
            for username, ts in user_last_count.items():
                try:
                    cleaned_last_count[str(username)] = float(ts)
                except (TypeError, ValueError):
                    continue
        self._user_last_count = cleaned_last_count
        user_last_seen = data.get("user_last_seen") or {}
        cleaned_last_seen: Dict[str, float] = {}
        if isinstance(user_last_seen, dict):
            for username, ts in user_last_seen.items():
                try:
                    cleaned_last_seen[str(username)] = float(ts)
                except (TypeError, ValueError):
                    continue
        self._user_last_seen = cleaned_last_seen
        user_flags = data.get("user_flags") or {}
        cleaned_flags: Dict[str, bool] = {}
        if isinstance(user_flags, dict):
            for username, flag in user_flags.items():
                try:
                    cleaned_flags[str(username)] = bool(flag)
                except Exception:
                    continue
        self._user_flags = cleaned_flags
        history = data.get("concurrent") or []
        cleaned_history: List[Dict[str, Any]] = []
        if isinstance(history, list):
            for item in history:
                if not isinstance(item, dict):
                    continue
                try:
                    ts = float(item.get("ts"))
                except (TypeError, ValueError):
                    continue
                try:
                    count = int(item.get("count") or 0)
                except (TypeError, ValueError):
                    continue
                cleaned_history.append({"ts": ts, "count": count})
        self._concurrent_history = cleaned_history[-(self._max_events * 3) :]
        hourly_series = data.get("hourly_series") or []
        cleaned_hourly_series: List[Dict[str, Any]] = []
        if isinstance(hourly_series, list):
            for item in hourly_series:
                if not isinstance(item, dict):
                    continue
                try:
                    ts = int(item.get("ts"))
                except (TypeError, ValueError):
                    continue
                try:
                    count = int(item.get("count") or 0)
                except (TypeError, ValueError):
                    count = 0
                cleaned_hourly_series.append({"ts": ts, "count": count})
        cleaned_hourly_series = sorted(cleaned_hourly_series, key=lambda x: x["ts"])
        max_hourly = self._max_events * 24
        if len(cleaned_hourly_series) > max_hourly:
            cleaned_hourly_series = cleaned_hourly_series[-max_hourly:]
        self._hourly_series = cleaned_hourly_series
        hourly_buckets = data.get("hourly_buckets") or {}
        cleaned_buckets: Dict[int, Set[str]] = {}
        if isinstance(hourly_buckets, dict):
            for ts, members in hourly_buckets.items():
                try:
                    bucket_ts = int(ts)
                except (TypeError, ValueError):
                    continue
                bucket_set: Set[str] = set()
                if isinstance(members, (list, set, tuple)):
                    for m in members:
                        if m is None:
                            continue
                        try:
                            bucket_set.add(str(m))
                        except Exception:
                            continue
                cleaned_buckets[bucket_ts] = bucket_set
        self._hourly_buckets = cleaned_buckets
        for entry in list(self._hourly_series):
            ts = entry.get("ts")
            if ts not in self._hourly_buckets:
                self._hourly_buckets[ts] = set()
        self._purge_expired(time.time())

    def _persist_state_payload(self, payload: Dict[str, Any]) -> None:
        if self._state_saver:
            try:
                self._state_saver(payload)
            except Exception:
                pass
            return
        if not self._storage_path:
            return
        try:
            self._storage_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    def _save_to_disk(self) -> None:
        payload = {
            "total": self._total_hits,
            "active": self._active_ips,
            "last_total": self._last_total_increment,
            "ip_totals": self._ip_total_hits,
            "version": self._version,
            "ip_users": self._ip_users,
            "active_users": self._active_users,
            "user_totals": self._user_total_hits,
            "user_last_count": self._user_last_count,
            "user_last_seen": self._user_last_seen,
            "user_flags": self._user_flags,
            "concurrent": self._concurrent_history,
            "hourly_series": self._hourly_series,
            "hourly_buckets": {ts: list(names) for ts, names in self._hourly_buckets.items()},
        }
        self._persist_state_payload(payload)

    def _purge_old_total_entries(self, now: float) -> None:
        expire_after = self._count_interval * 2
        stale = [ip for ip, ts in self._last_total_increment.items() if now - ts > expire_after]
        for ip in stale:
            self._last_total_increment.pop(ip, None)

    def _online_counts(self, now: float) -> Tuple[int, int]:
        active_usernames = [
            user
            for user, ts in self._active_users.items()
            if ts and now - ts <= self._activity_window and not self._is_guest_user(user)
        ]
        active_ips = [
            ip for ip, ts in self._active_ips.items() if ts and now - ts <= self._activity_window
        ]
        return len(active_usernames), len(active_ips)

    def ip_summary(self) -> Dict[str, int]:
        now = time.time()
        with self._lock:
            self._purge_expired(now)
            unique_ips = set(self._ip_total_hits.keys()) | set(self._active_ips.keys())
            online_ips = sum(
                1 for ts in self._active_ips.values() if ts and now - ts <= self._activity_window
            )
            return {
                "unique": len(unique_ips),
                "online": online_ips,
                "total": self._total_hits,
            }

    def reset(self) -> None:
        with self._lock:
            self._active_ips.clear()
            self._last_total_increment.clear()
            self._ip_total_hits.clear()
            self._ip_users.clear()
            self._active_users.clear()
            self._user_total_hits.clear()
            self._user_last_count.clear()
            self._user_last_seen.clear()
            self._user_flags.clear()
            self._recent_events = []
            self._concurrent_history = []
            self._concurrent_history = []
            self._total_hits = 0
            self._version += 1
            self._save_to_disk()
            if self._event_clearer:
                try:
                    self._event_clearer()
                except Exception:
                    pass
            elif self._log_path:
                try:
                    self._log_path.write_text("", encoding="utf-8")
                except Exception:
                    pass

    def _append_event(self, event: Dict[str, Any]) -> None:
        cleaned = {
            "ts": event.get("ts"),
            "ip": event.get("ip"),
            "action": event.get("action"),
            "status": event.get("status") or "info",
            "meta": event.get("meta") or {},
        }
        self._recent_events.append(cleaned)
        if len(self._recent_events) > self._max_events:
            self._recent_events = self._recent_events[-self._max_events :]
        if self._event_writer:
            try:
                self._event_writer(cleaned)
            except Exception:
                pass
            return
        if not self._log_path:
            return
        try:
            with self._log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(cleaned, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def _load_recent_events(self) -> None:
        if self._event_loader:
            try:
                self._recent_events = self._event_loader(self._max_events) or []
            except Exception:
                self._recent_events = []
            return
        if not self._log_path or not self._log_path.exists():
            return
        try:
            lines = self._log_path.read_text(encoding="utf-8").splitlines()
        except Exception:
            return
        events: List[Dict[str, Any]] = []
        for raw in lines[-self._max_events :]:
            try:
                event = json.loads(raw)
            except Exception:
                continue
            if isinstance(event, dict):
                events.append(event)
        self._recent_events = events

    def recent_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            subset = self._recent_events[-limit:]
            return list(subset)

    def concurrent_history(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._concurrent_history)

    def hourly_series(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._hourly_series)

    def hourly_buckets(self) -> Dict[int, Set[str]]:
        with self._lock:
            return {ts: set(names) for ts, names in self._hourly_buckets.items()}

    def _store_concurrent_snapshot(self, now: float) -> None:
        user_count, _ = self._online_counts(now)
        entry = {"ts": float(now), "count": user_count}
        if self._concurrent_history and now - self._concurrent_history[-1]["ts"] < 60:
            self._concurrent_history[-1] = entry
        else:
            self._concurrent_history.append(entry)
        max_len = self._max_events * 3
        if len(self._concurrent_history) > max_len:
            self._concurrent_history = self._concurrent_history[-max_len:]

    def _update_hourly(self, username: str, now: float) -> None:
        if not username or self._is_guest_user(username):
            return
        bucket_dt = datetime.fromtimestamp(now, tz=TAIPEI_TZ).replace(minute=0, second=0, microsecond=0)
        bucket_ts = int(bucket_dt.timestamp())
        bucket = self._hourly_buckets.setdefault(bucket_ts, set())
        before = len(bucket)
        bucket.add(username)
        if len(bucket) != before:
            # update series entry for this bucket
            self._hourly_series = [entry for entry in self._hourly_series if entry.get("ts") != bucket_ts]
            self._hourly_series.append({"ts": bucket_ts, "count": len(bucket)})
            self._hourly_series = sorted(self._hourly_series, key=lambda x: x["ts"])
            max_len = self._max_events * 24
            if len(self._hourly_series) > max_len:
                drop = len(self._hourly_series) - max_len
                old = self._hourly_series[:drop]
                self._hourly_series = self._hourly_series[-max_len:]
                for entry in old:
                    ts = entry.get("ts")
                    if ts in self._hourly_buckets:
                        self._hourly_buckets.pop(ts, None)
    def user_breakdown(self) -> List[Dict[str, Any]]:
        now = time.time()
        with self._lock:
            aggregated: Dict[str, Dict[str, Any]] = {}
            def _entry(username: str) -> Dict[str, Any]:
                return aggregated.setdefault(
                    username,
                    {"username": username, "count": 0, "last_seen": 0.0, "last_counted": None, "online": False},
                )

            for username, count in self._user_total_hits.items():
                if self._is_guest_user(username):
                    continue
                entry = _entry(username)
                entry["count"] = count
                if username in self._user_last_count:
                    entry["last_counted"] = self._user_last_count[username]
                entry["last_seen"] = max(
                    entry.get("last_seen") or 0.0,
                    self._user_last_seen.get(username, 0.0),
                    self._user_last_count.get(username, 0.0) or 0.0,
                )

            for username, last_seen in self._active_users.items():
                if self._is_guest_user(username):
                    continue
                entry = _entry(username)
                entry["last_seen"] = max(
                    entry.get("last_seen") or 0.0,
                    last_seen or 0.0,
                    self._user_last_seen.get(username, 0.0),
                )
                if last_seen and now - last_seen <= self._activity_window:
                    entry["online"] = True
                if username in self._user_last_count:
                    entry["last_counted"] = self._user_last_count[username]

            for username, last_seen in self._user_last_seen.items():
                if self._is_guest_user(username):
                    continue
                entry = _entry(username)
                entry["last_seen"] = max(
                    entry.get("last_seen") or 0.0,
                    last_seen or 0.0,
                    self._user_last_count.get(username, 0.0),
                )
                if username in self._user_total_hits:
                    entry["count"] = self._user_total_hits.get(username, entry.get("count", 0))

            entries = list(aggregated.values())
            entries.sort(key=lambda item: item["count"], reverse=True)
            return entries

    def ip_breakdown(self) -> List[Dict[str, Any]]:
        now = time.time()
        with self._lock:
            entries = []
            for ip, count in self._ip_total_hits.items():
                last_seen = self._active_ips.get(ip)
                entries.append(
                    {
                        "ip": ip,
                        "count": count,
                        "last_seen": last_seen,
                        "last_counted": self._last_total_increment.get(ip),
                        "online": bool(last_seen and now - last_seen <= self._activity_window),
                        "username": self._ip_users.get(ip),
                    }
                )
            entries.sort(key=lambda item: item["count"], reverse=True)
            return entries

    def guest_summary(self) -> Dict[str, int]:
        now = time.time()
        with self._lock:
            self._purge_expired(now)
            total_guests = sum(1 for flag in self._user_flags.values() if flag)
            active_guests: Set[str] = set()
            for ip, ts in self._active_ips.items():
                if not ts or now - ts > self._activity_window:
                    continue
                username = self._ip_users.get(ip)
                if username and self._is_guest_user(username):
                    active_guests.add(username)
            return {"total": total_guests, "online": len(active_guests)}


def _env_flag_truthy(value: Optional[str]) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")



def create_app(*, default_base_url: Optional[str] = None, default_scope: str = "assignment", default_timeout: int = 30) -> Flask:
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
    storage = PersistentStorage(db_location)
    storage.sync_study_plan_videos(STUDY_PLAN_VIDEO_INVENTORY)

    app = Flask(__name__)
    app.secret_key = env_defaults["web_secret"]
    app.jinja_env.globals["study_upload_tracker"] = STUDY_UPLOAD_TRACKER_TEMPLATE
    session_cookie_secure = _env_flag_truthy(env_defaults.get("session_cookie_secure"))
    session_cookie_samesite = env_defaults.get("session_cookie_samesite") or "Lax"
    app.config.update(
        PERMANENT_SESSION_LIFETIME=timedelta(days=1),
        SESSION_COOKIE_SECURE=session_cookie_secure,
        SESSION_COOKIE_SAMESITE=session_cookie_samesite,
        SESSION_COOKIE_HTTPONLY=True,
        PREFERRED_URL_SCHEME="https",
        MAX_CONTENT_LENGTH=STUDY_NOTE_MAX_REQUEST_BYTES,
    )
    base_url = default_base_url or env_defaults["base_url"]
    default_scope = default_scope or env_defaults["scope"]
    default_moodle_session = env_defaults["session"]
    cafile = env_defaults.get("cafile") or None
    insecure_tls = _env_flag_truthy(env_defaults.get("insecure_tls"))
    google_client_id = env_defaults.get("google_client_id")
    google_client_secret = env_defaults.get("google_client_secret")
    google_redirect_uri = env_defaults.get("google_redirect_uri")
    google_calendar_id = env_defaults.get("google_calendar_id") or "primary"
    admin_user_id = (env_defaults.get("admin_user_id") or "112550103").strip()
    canonical_host = (env_defaults.get("canonical_host") or "").strip()
    if canonical_host == "":
        canonical_host = None
    support_email = (env_defaults.get("support_email") or "support@e3hwtool.space").strip()
    if not support_email:
        support_email = "support@e3hwtool.space"
    app_home_url = (env_defaults.get("app_home_url") or "https://e3hwtool.space/").strip()
    if app_home_url and not app_home_url.startswith(("http://", "https://")):
        app_home_url = f"https://{app_home_url.lstrip('/')}"
    if not app_home_url:
        app_home_url = "https://e3hwtool.space/"
    if not app_home_url.endswith("/"):
        app_home_url = f"{app_home_url}/"
    legal_entity_name = env_defaults.get("legal_entity_name") or "E3 Homework Tracker Project"
    openai_api_key = (env_defaults.get("openai_api_key") or "").strip()
    openai_model = (env_defaults.get("openai_model") or "gpt-5-mini").strip()
    configured_upload_dir = (env_defaults.get("study_upload_dir") or "").strip()
    study_upload_root = Path(configured_upload_dir).expanduser() if configured_upload_dir else data_root / "study_note_images"
    _ensure_private_dir(study_upload_root)
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
        event_writer=lambda event: storage.append_traffic_event(event, traffic_event_limit),
        event_clearer=storage.clear_traffic_events,
    )

    def _is_study_upload_request() -> bool:
        return request.headers.get("X-E3-Study-Upload") == "1"

    def _study_upload_error(message: str, status_code: int = 400):
        if _is_study_upload_request():
            return {"ok": False, "error": message}, status_code
        flash(message, "error")
        return redirect(url_for("admin_study_recall"))

    @app.errorhandler(RequestEntityTooLarge)
    def handle_request_entity_too_large(_error: RequestEntityTooLarge):
        if request.path == "/admin/study-recall/upload":
            return _study_upload_error("筆記照片壓縮後仍超過 24MB，請減少張數後再試。", 413)
        return Response("Request Entity Too Large", status=413, mimetype="text/plain")

    DEFAULT_PREFERENCES = {
        "view_mode": "due",
        "status_filter": ["pending"],
        "include_ignored_overdue": False,
        "show_overdue": False,
        "show_completed": False,
        "show_graded": False,
        "ignored_overdue_uids": [],
    }
    NEW_ASSIGNMENT_WINDOW_SECONDS = 5 * 60
    refresh_jobs_lock = threading.Lock()
    refresh_jobs: Dict[str, Dict[str, Any]] = {}
    study_upload_jobs_lock = threading.Lock()
    study_upload_jobs: Dict[str, Dict[str, Any]] = {}
    study_relation_rebuild_lock = threading.Lock()

    def _set_study_upload_job(job_id: str, **changes: Any) -> None:
        with study_upload_jobs_lock:
            job = study_upload_jobs.get(job_id)
            if job is None:
                return
            job.update(changes)
            job["updated_at"] = time.time()

    def _active_study_upload_job(username: str) -> Optional[str]:
        cutoff = time.time() - 24 * 60 * 60
        with study_upload_jobs_lock:
            expired = [job_id for job_id, job in study_upload_jobs.items() if float(job.get("updated_at") or 0) < cutoff]
            for job_id in expired:
                study_upload_jobs.pop(job_id, None)
            for job_id, job in study_upload_jobs.items():
                if job.get("username") == username and job.get("status") == "running":
                    return job_id
        return None

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

    def _mark_refresh_job_done(username: str, *, status: str = "success", error: Optional[str] = None) -> None:
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

    def _start_web_session(username: str, *, moodle_session: Optional[str], is_guest: bool, is_admin: bool, permanent: bool) -> None:
        session.clear()
        session_token = secrets.token_urlsafe(24)
        storage.save_web_session(session_token, username)
        session["username"] = username
        session["session_token"] = session_token
        session["moodle_session"] = moodle_session
        session["is_guest"] = is_guest
        session["is_admin"] = is_admin
        session.permanent = permanent

    def current_user() -> Optional[Dict[str, Any]]:
        username = session.get("username")
        session_token = session.get("session_token")
        if username and session_token and storage.is_valid_web_session(session_token, username):
            return {
                "username": username,
                "moodle_session": session.get("moodle_session"),
                "is_guest": bool(session.get("is_guest")),
                "is_admin": bool(session.get("is_admin")),
            }
        if username or session_token:
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
            if lowered in {"course", "due"}:
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
        ignored_overdue_uids = raw.get("ignored_overdue_uids")
        if ignored_overdue_uids is None:
            ignored_overdue_uids = raw.get("ignoredOverdueUids")
        if isinstance(ignored_overdue_uids, list):
            clean["ignored_overdue_uids"] = [
                str(item).strip()
                for item in ignored_overdue_uids
                if str(item).strip()
            ][:500]
        return clean

    def _selected_view_username(raw_username: Optional[str], *, actor: Optional[Dict[str, Any]] = None) -> Optional[str]:
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

    def is_admin_viewing_other_user(*, actor: Optional[Dict[str, Any]] = None, viewed_username: Optional[str] = None) -> bool:
        user = actor or current_user()
        if not user or not user.get("is_admin"):
            return False
        target_username = (viewed_username or get_viewed_username(actor=user) or "").strip()
        return bool(target_username and target_username != user["username"])

    def list_admin_view_options(limit: int = 500) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        guest_prefix = f"{chr(0x8A2A)}{chr(0x5BA2)}_"
        for raw in storage.list_cached_users(limit=limit):
            username = str(raw.get("username") or "").strip()
            if not username:
                continue
            if username.startswith(guest_prefix) or username.startswith("Session-"):
                continue
            fetched_ts = raw.get("fetched_ts")
            fetched_label = "尚未更新"
            if fetched_ts:
                try:
                    fetched_label = datetime.fromtimestamp(int(fetched_ts), TAIPEI_TZ).strftime("%Y-%m-%d %H:%M")
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
        return prefs

    def update_user_preferences(partial: Dict[str, Any], *, username: Optional[str] = None) -> Dict[str, Any]:
        prefs = get_user_preferences(username)
        sanitized = _sanitize_preferences(partial)
        prefs.update(sanitized)
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
            uid = storage.assignment_uid(course_id, str(item.get("title") or "").strip(), item.get("url"))
            if not uid.strip():
                continue
            item["assignment_uid"] = uid
            assignment_uids.append(uid)
        if not assignment_uids:
            return
        first_seen_map = (
            storage.load_assignment_view_map(username, assignment_uids)
            if readonly
            else storage.mark_assignment_views(username, assignment_uids, seen_ts=now_ts)
        )
        for item in assignments:
            uid = str(item.get("assignment_uid") or "").strip()
            first_seen_ts = first_seen_map.get(uid)
            is_new = bool(first_seen_ts is not None and now_ts - int(first_seen_ts) <= NEW_ASSIGNMENT_WINDOW_SECONDS)
            item["first_seen_ts"] = first_seen_ts
            item["is_new"] = is_new
            item["new_until_ts"] = (int(first_seen_ts) + NEW_ASSIGNMENT_WINDOW_SECONDS) if first_seen_ts is not None else None
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
                uid = storage.assignment_uid(
                    course_id,
                    str(item.get("title") or "").strip(),
                    item.get("url"),
                ) if course_id is not None else ""
                first_seen_ts = first_seen_map.get(uid)
                item["assignment_uid"] = uid
                item["first_seen_ts"] = first_seen_ts
                item["is_new"] = bool(first_seen_ts is not None and now_ts - int(first_seen_ts) <= NEW_ASSIGNMENT_WINDOW_SECONDS)
                item["new_until_ts"] = (int(first_seen_ts) + NEW_ASSIGNMENT_WINDOW_SECONDS) if first_seen_ts is not None else None

    def set_assign_cache_for_user(username: str, result: Dict[str, Any], excel_data: Optional[str]) -> None:
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

    def set_assign_cache(result: Dict[str, Any], excel_data: Optional[str]) -> None:
        user = current_user()
        if not user:
            return
        set_assign_cache_for_user(user["username"], result, excel_data)

    def _generate_excel_data(assignments: Optional[List[Dict[str, Any]]]) -> Optional[str]:
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
                created_label = datetime.fromisoformat(created_at).astimezone(TAIPEI_TZ).strftime("%Y-%m-%d %H:%M")
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
        for raw in storage.list_announcements_with_votes(ANNOUNCEMENT_LIMIT, username=username):
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

    def set_announcement_vote(announcement_id: str, username: str, vote_type: Optional[str]) -> Optional[Dict[str, Any]]:
        updated = storage.set_announcement_vote(announcement_id, username, vote_type)
        if not updated:
            return None
        return _serialize_announcement(updated)

    FEEDBACK_LIMIT = 200
    VALID_FEEDBACK_STATUS = {"open", "resolved"}

    def add_feedback_entry(message: str, email: Optional[str], username: Optional[str]) -> int:
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
                    parsed["created_label"] = dt.astimezone(TAIPEI_TZ).strftime("%Y-%m-%d %H:%M")
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

    def _google_ready() -> bool:
        return bool(google_client_id and google_client_secret and google_redirect_uri)

    def _assignment_uid(item: Dict[str, Any]) -> str:
        return f"{item.get('course_id')}|{item.get('title')}|{item.get('url')}"

    def _select_assignments_from_result(result: Optional[Dict[str, Any]], selected_uids: List[str]) -> List[Dict[str, Any]]:
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

    def _client_ip() -> Optional[str]:
        if not has_request_context():
            return None
        forwarded = request.headers.get("X-Forwarded-For", "")
        if forwarded:
            for part in forwarded.split(","):
                ip = part.strip()
                if ip:
                    return ip
        return request.remote_addr

    def record_ui_event(action: str, status: str = "success", meta: Optional[Dict[str, Any]] = None) -> None:
        if not action:
            return
        details = dict(meta or {})
        user = current_user() if has_request_context() else None
        if user:
            details.setdefault("username", user["username"])
            details.setdefault("is_guest", user.get("is_guest"))
            details.setdefault("is_admin", user.get("is_admin"))
        traffic_tracker.record_visit(_client_ip(), action=action, status=status, metadata=details)

    def usage_stats() -> Dict[str, int]:
        return traffic_tracker.snapshot()

    def current_stats_version() -> int:
        return traffic_tracker.version()

    def _ensure_google_access_token(username: str, tokens: Dict[str, Any]) -> Dict[str, Any]:
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
        is_admin_view = is_admin_viewing_other_user(actor=user, viewed_username=viewed_username)
        if user["username"] not in {item["username"] for item in admin_view_options}:
            self_cache = load_cache_from_disk(user["username"]) or {}
            admin_view_options.insert(
                0,
                {
                    "username": user["username"],
                    "is_admin": bool(user.get("is_admin")),
                    "fetched_ts": self_cache.get("ts"),
                    "fetched_label": datetime.fromtimestamp(
                        int(self_cache.get("ts")), TAIPEI_TZ
                    ).strftime("%Y-%m-%d %H:%M")
                    if self_cache.get("ts")
                    else "尚未更新",
                    "assignment_count": len((self_cache.get("result") or {}).get("all_assignments", [])),
                    "course_count": len((self_cache.get("result") or {}).get("courses", [])),
                },
            )
        cache = get_assign_cache(viewed_username)
        result = cache.get("result") if cache else None
        excel_data = cache.get("excel_data") if cache else None
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
        announcements_list = load_announcements(None if is_admin_view else user["username"])
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
                last_updated_label = datetime.fromtimestamp(int(cache_ts_val), TAIPEI_TZ).strftime("%Y-%m-%d %H:%M")
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
            "preferences": get_user_preferences(viewed_username),
            "cache_ts": cache_ts_val,
            "last_updated_ts": cache_ts_val,
            "last_updated_label": last_updated_label,
            "announcements": announcements_list,
            "announcement_version": announcements_list[0]["id"] if announcements_list else None,
            "viewed_username": viewed_username,
            "is_admin_view": is_admin_view,
            "admin_view_options": admin_view_options,
        }

    def _study_plan_week_rows(videos: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
        start_day = datetime.strptime(STUDY_PLAN_START, "%Y-%m-%d").date()
        end_day = datetime.strptime(STUDY_PLAN_END, "%Y-%m-%d").date()
        today = _study_plan_business_date()
        videos_by_subject: Dict[str, List[Dict[str, Any]]] = {}
        for video in videos:
            subject = str(video.get("subject") or "")
            videos_by_subject.setdefault(subject, []).append(video)
        week_rows: List[Dict[str, Any]] = []
        cursor = start_day
        week_number = 1
        for block in STUDY_PLAN_BLOCKS:
            prior_lesson_target = 0
            for lesson_target in block["lesson_targets"]:
                week_start = cursor
                week_end = cursor + timedelta(days=6)
                weekly_videos = [
                    item
                    for item in videos_by_subject.get(block["subject"], [])
                    if prior_lesson_target < int(item.get("sequence") or 0) <= int(lesson_target)
                ]
                target_seconds = sum(float(item.get("duration_seconds") or 0) for item in weekly_videos)
                watched_seconds = sum(
                    min(float(item.get("watched_seconds") or 0), float(item.get("duration_seconds") or 0))
                    for item in weekly_videos
                )
                completed_videos = sum(
                    1
                    for item in weekly_videos
                    if _study_plan_video_is_complete(item.get("duration_seconds"), item.get("watched_seconds"))
                )
                week_is_complete = bool(weekly_videos) and (
                    completed_videos == len(weekly_videos)
                    or _study_plan_total_is_complete(target_seconds, watched_seconds)
                )
                video_hours, suggested_weekly_hours, daily_recommendations = _study_plan_daily_recommendations(
                    str(block["subject"]),
                    target_seconds,
                    watched_seconds,
                    week_start,
                    today,
                    week_is_complete=week_is_complete,
                )
                completion = _study_plan_completion_percent(
                    target_seconds,
                    watched_seconds,
                    complete_override=week_is_complete,
                )
                if week_is_complete:
                    if today < week_end:
                        state = "early"
                        state_label = "提早完成"
                    else:
                        state = "complete"
                        state_label = "已達標"
                elif week_start <= today <= week_end:
                    state = "active"
                    state_label = "進行中"
                elif today > week_end:
                    state = "behind"
                    state_label = "待補"
                else:
                    state = "upcoming"
                    state_label = "未開始"
                week_rows.append(
                    {
                        "number": week_number,
                        "subject": block["subject"],
                        "start": week_start.isoformat(),
                        "end": week_end.isoformat(),
                        "target_minutes": target_seconds / 60,
                        "target_seconds": target_seconds,
                        "video_hours": video_hours,
                        "suggested_weekly_hours": suggested_weekly_hours,
                        "daily_recommendations": daily_recommendations,
                        "lesson_target": int(lesson_target),
                        "video_count": len(weekly_videos),
                        "completed_videos": completed_videos,
                        "watched_minutes": round(watched_seconds / 60, 1),
                        "completion": completion,
                        "state": state,
                        "state_label": state_label,
                    }
                )
                cursor = week_end + timedelta(days=1)
                week_number += 1
                prior_lesson_target = int(lesson_target)

        active_week = next((row for row in week_rows if row["start"] <= today.isoformat() <= row["end"]), None)
        if active_week is None:
            active_week = week_rows[0] if today < start_day else week_rows[-1]
        total_target_seconds = sum(float(item.get("duration_seconds") or 0) for item in videos)
        total_watched_seconds = sum(
            min(float(item.get("watched_seconds") or 0), float(item.get("duration_seconds") or 0))
            for item in videos
        )
        total_target = total_target_seconds / 60
        total_watched = total_watched_seconds / 60
        completed_videos = sum(
            1
            for item in videos
            if _study_plan_video_is_complete(item.get("duration_seconds"), item.get("watched_seconds"))
        )
        all_videos_complete = bool(videos) and completed_videos == len(videos)
        recorded_videos = sum(
            1
            for item in videos
            if float(item.get("watched_seconds") or 0) > 0 or bool(str(item.get("notes") or "").strip())
        )
        summary = {
            "total_target": total_target,
            "total_watched": total_watched,
            "completion": _study_plan_completion_percent(
                total_target_seconds,
                total_watched_seconds,
                complete_override=all_videos_complete,
            ),
            "completed_videos": completed_videos,
            "recorded_videos": recorded_videos,
            "total_videos": len(videos),
        }
        return week_rows, active_week, summary

    def _build_study_home_context(
        videos: List[Dict[str, Any]],
        week_rows: List[Dict[str, Any]],
        current_week: Dict[str, Any],
        summary: Dict[str, Any],
    ) -> Dict[str, Any]:
        today = _study_plan_business_date()
        plan_start = datetime.strptime(STUDY_PLAN_START, "%Y-%m-%d").date()
        plan_end = datetime.strptime(STUDY_PLAN_END, "%Y-%m-%d").date()
        total_plan_days = max(1, (plan_end - plan_start).days + 1)
        elapsed_days = min(max((today - plan_start).days + 1, 0), total_plan_days)
        elapsed_percent = min(100.0, max(0.0, elapsed_days / total_plan_days * 100))
        completion = float(summary.get("completion") or 0)
        total_target_minutes = float(summary.get("total_target") or 0)
        watched_minutes_total = float(summary.get("total_watched") or 0)
        target_minutes_by_today = 0.0
        today_iso = today.isoformat()
        previous_day_iso = (today - timedelta(days=1)).isoformat()
        previous_day_row: Optional[Dict[str, Any]] = None
        for row in week_rows:
            row_start = str(row.get("start") or "")
            row_end = str(row.get("end") or "")
            if row_start and row_start <= previous_day_iso <= row_end:
                previous_day_row = next(
                    (day for day in row.get("daily_recommendations", []) if str(day.get("date") or "") == previous_day_iso),
                    previous_day_row,
                )
            if row_end and row_end < today_iso:
                target_minutes_by_today += float(row.get("target_minutes") or 0)
            elif row_start and row_start <= today_iso <= row_end:
                for day in row.get("daily_recommendations", []):
                    day_key = str(day.get("date") or "")
                    if day_key <= today_iso:
                        target_minutes_by_today += float(day.get("hours") or 0) * 60
        target_minutes_by_today = min(max(target_minutes_by_today, 0.0), total_target_minutes)
        scheduled_percent = min(100.0, (target_minutes_by_today / total_target_minutes * 100) if total_target_minutes else 0.0)
        pace_delta = completion - scheduled_percent
        pace_minutes = watched_minutes_total - target_minutes_by_today
        previous_day_incomplete = bool(previous_day_row and float(previous_day_row.get("completion") or 0) < 100)
        if previous_day_incomplete:
            pace_state = "behind"
            pace_label = "待補"
            pace_message = f"目前比計畫進度慢 {abs(pace_delta):.1f} 個百分點。"
        elif pace_delta >= 3:
            pace_state = "early"
            pace_label = "提早完成"
            pace_message = f"目前比計畫進度快 {abs(pace_delta):.1f} 個百分點。"
        elif pace_delta <= -3:
            pace_state = "behind"
            pace_label = "待補"
            pace_message = f"目前比計畫進度慢 {abs(pace_delta):.1f} 個百分點。"
        else:
            pace_state = "active"
            pace_label = "穩定推進"
            pace_message = "目前大致貼近計畫進度。"

        subject_rows: List[Dict[str, Any]] = []
        videos_by_subject: Dict[str, List[Dict[str, Any]]] = {subject: [] for subject in STUDY_PLAN_SUBJECTS}
        for video in videos:
            videos_by_subject.setdefault(str(video.get("subject") or ""), []).append(video)
        for subject in STUDY_PLAN_SUBJECTS:
            subject_videos = videos_by_subject.get(subject, [])
            target_seconds = sum(float(item.get("duration_seconds") or 0) for item in subject_videos)
            watched_seconds = sum(
                min(float(item.get("watched_seconds") or 0), float(item.get("duration_seconds") or 0))
                for item in subject_videos
            )
            completed_count = sum(
                1
                for item in subject_videos
                if _study_plan_video_is_complete(item.get("duration_seconds"), item.get("watched_seconds"))
            )
            subject_is_complete = bool(subject_videos) and (
                completed_count == len(subject_videos)
                or _study_plan_total_is_complete(target_seconds, watched_seconds)
            )
            subject_weeks = [row for row in week_rows if row["subject"] == subject]
            active_subject_week = next(
                (row for row in subject_weeks if row["start"] <= today.isoformat() <= row["end"]),
                subject_weeks[0] if subject_weeks else None,
            )
            subject_completion = _study_plan_completion_percent(
                target_seconds,
                watched_seconds,
                complete_override=subject_is_complete,
            )
            subject_rows.append(
                {
                    "name": subject,
                    "completion": round(subject_completion, 1),
                    "target_hours": round(target_seconds / 3600, 1),
                    "watched_hours": round(watched_seconds / 3600, 1),
                    "completed_videos": completed_count,
                    "total_videos": len(subject_videos),
                    "state": "complete" if subject_is_complete else active_subject_week["state"] if active_subject_week else "upcoming",
                    "state_label": "已達標" if subject_is_complete else active_subject_week["state_label"] if active_subject_week else "未開始",
                }
            )
        weak_subjects = sorted(
            [item for item in subject_rows if item["completion"] < 100],
            key=lambda item: (item["completion"], -item["total_videos"]),
        )[:3]

        today_row = next(
            (row for row in current_week.get("daily_recommendations", []) if row.get("date") == today.isoformat()),
            None,
        )
        if today_row is None:
            today_row = next(
                (row for row in current_week.get("daily_recommendations", []) if row.get("state") in {"active", "upcoming", "behind"}),
                (current_week.get("daily_recommendations") or [{}])[0],
            )
        current_subject_videos = videos_by_subject.get(str(current_week.get("subject") or ""), [])
        next_videos: List[Dict[str, Any]] = []
        for video in current_subject_videos:
            duration = float(video.get("duration_seconds") or 0)
            watched = min(float(video.get("watched_seconds") or 0), duration)
            if duration <= 0 or _study_plan_video_is_complete(duration, watched):
                continue
            next_videos.append(
                {
                    "id": int(video.get("id") or 0),
                    "subject": str(video.get("subject") or current_week.get("subject") or ""),
                    "sequence": int(video.get("sequence") or 0),
                    "title": str(video.get("title") or ""),
                    "remaining_minutes": round(max(0.0, duration - watched) / 60, 1),
                    "completion": round(min(100.0, watched / duration * 100), 1),
                }
            )
            if len(next_videos) >= 1:
                break

        last_updated_label = "尚未開始"
        latest_dt: Optional[datetime] = None
        for video in videos:
            updated = str(video.get("updated_at") or "").strip()
            if not updated:
                continue
            try:
                parsed = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                if latest_dt is None or parsed > latest_dt:
                    latest_dt = parsed
                    last_updated_label = updated
            except ValueError:
                last_updated_label = updated
        timeline_nodes = [
            {
                "number": row["number"],
                "subject": row["subject"],
                "completion": round(float(row["completion"]), 1),
                "state": row["state"],
                "state_label": row["state_label"],
            }
            for row in week_rows
        ]
        remaining_hours = max(0.0, (float(summary.get("total_target") or 0) - float(summary.get("total_watched") or 0)) / 60)
        total_hours = max(0.0, float(summary.get("total_target") or 0) / 60)
        watched_hours = max(0.0, float(summary.get("total_watched") or 0) / 60)
        visual_angle = round(completion / 100 * 360, 1)
        pace_hours = pace_minutes / 60
        daily_target_minutes = (float(summary.get("total_target") or 0) / total_plan_days) if total_plan_days else 0.0
        catchup_minutes_per_day = math.ceil(abs(pace_minutes) / 7) if pace_minutes < 0 else 0
        catchup_hours, catchup_remainder_minutes = divmod(catchup_minutes_per_day, 60)
        if catchup_hours and catchup_remainder_minutes:
            catchup_time_label = f"{catchup_hours} 小時 {catchup_remainder_minutes} 分鐘"
        elif catchup_hours:
            catchup_time_label = f"{catchup_hours} 小時"
        else:
            catchup_time_label = f"{catchup_remainder_minutes} 分鐘"
        buffer_days = max(0.0, pace_minutes / daily_target_minutes) if pace_minutes > 0 and daily_target_minutes else 0.0
        pace_meter_position = min(96.0, max(4.0, 50.0 + pace_delta * 2.2))
        if pace_state == "behind":
            pace_action = f"若要 1 週內追完，每天需多看 {catchup_time_label}。"
            pace_primary_value = f"{abs(pace_hours):.1f}"
            pace_primary_unit = "小時待補"
        elif pace_state == "early":
            pace_action = f"已累積約 {buffer_days:.1f} 天緩衝，可休息或提前下一週。"
            pace_primary_value = f"{abs(pace_hours):.1f}"
            pace_primary_unit = "小時領先"
        else:
            pace_action = "維持目前節奏即可貼近計畫進度。"
            pace_primary_value = f"{abs(pace_hours):.1f}"
            pace_primary_unit = "小時差距"

        pace_insight = {
            "state": pace_state,
            "label": pace_label,
            "message": pace_message,
            "action": pace_action,
            "primary_value": pace_primary_value,
            "primary_unit": pace_primary_unit,
            "delta_hours": round(pace_hours, 1),
            "catchup_minutes_per_day": catchup_minutes_per_day,
            "buffer_days": round(buffer_days, 1),
            "meter_position": round(pace_meter_position, 1),
            "target_today_hours": round(target_minutes_by_today / 60, 1),
            "watched_hours": round(watched_minutes_total / 60, 1),
        }

        metric_cards = [
            {
                "label": "影片總時長",
                "value": f"{total_hours:.1f}",
                "unit": "小時",
                "icon": "play",
                "state": "blue",
            },
            {
                "label": "已觀看時長",
                "value": f"{watched_hours:.1f}",
                "unit": "小時",
                "icon": "clock",
                "state": "green",
            },
            {
                "label": "目前進度",
                "value": f"{completion:.1f}",
                "unit": "%",
                "icon": "progress",
                "state": pace_state,
            },
            {
                "label": "預估完成率",
                "value": pace_label,
                "unit": "",
                "icon": "target",
                "state": pace_state,
            },
            {
                "label": "已完成影片",
                "value": str(int(summary.get("completed_videos") or 0)),
                "unit": f"/ {int(summary.get('total_videos') or 0)} 支",
                "icon": "check",
                "state": "purple",
            },
        ]

        chart_days = list(current_week.get("daily_recommendations") or [])
        week_start_date = str(current_week.get("start") or "")
        week_end_date = str(current_week.get("end") or "")
        recent_days = [(today - timedelta(days=offset)).isoformat() for offset in range(6, -1, -1)]
        tracked_start_day = min([day for day in [week_start_date, recent_days[0]] if day])
        activity_events = storage.list_study_plan_activity_events(
            start_day=tracked_start_day,
            end_day=today.isoformat(),
        )
        activity_events_by_day: Dict[str, List[Dict[str, Any]]] = {}
        for event in activity_events:
            event_day = str(event.get("day") or "")
            if event_day:
                activity_events_by_day.setdefault(event_day, []).append(event)
        # Each grouped event stores the first position of its learning day and the
        # final position saved that day. Only a positive difference is new study time.
        activity_seconds_by_day = {
            day: sum(max(0.0, float(item.get("delta_seconds") or 0)) for item in events)
            for day, events in activity_events_by_day.items()
        }
        recorded_days = {day for day, seconds in activity_seconds_by_day.items() if seconds > 0}
        momentum_days = [{"date": day, "active": day in recorded_days, "label": day[5:]} for day in recent_days]
        active_recent_days = sum(1 for item in momentum_days if item["active"])
        momentum_score = min(100, int(round(active_recent_days / 7 * 100)))

        chart_rows: List[Dict[str, Any]] = []
        target_total = 0.0
        actual_total = 0.0
        target_cumulative = 0.0
        actual_cumulative = 0.0
        for day in chart_days:
            day_key = str(day.get("date") or "")
            is_future_day = bool(day_key and day_key > today.isoformat())
            target_hours = float(day.get("hours") or 0)
            if is_future_day:
                actual_hours: Optional[float] = None
            else:
                actual_hours = activity_seconds_by_day.get(day_key, 0.0) / 3600
            target_total += target_hours
            target_cumulative += target_hours
            if actual_hours is not None:
                actual_total += actual_hours
                actual_cumulative += actual_hours
            chart_rows.append(
                {
                    "label": str(day.get("label") or ""),
                    "date": day_key,
                    "short_date": str(day.get("short_date") or ""),
                    "state": str(day.get("state") or ""),
                    "state_label": str(day.get("state_label") or ""),
                    "target_hours": round(target_cumulative, 2),
                    "actual_hours": round(actual_cumulative, 2) if actual_hours is not None else None,
                    "actual_daily_hours": round(actual_hours, 2) if actual_hours is not None else None,
                    "is_future": is_future_day,
                }
            )
        chart_max_candidates = [1.0]
        chart_max_candidates.extend(float(row["target_hours"]) for row in chart_rows)
        chart_max_candidates.extend(float(row["actual_hours"]) for row in chart_rows if row["actual_hours"] is not None)
        chart_max_hours = max(chart_max_candidates)
        chart_max_hours = max(1.0, chart_max_hours * 1.08)

        def _chart_point(index: int, value: float) -> str:
            total_points = max(1, len(chart_rows) - 1)
            x = 48 + (288 * (index / total_points))
            y = 132 - (104 * min(max(value / chart_max_hours, 0.0), 1.0))
            return f"{round(x, 1)},{round(y, 1)}"

        for index, row in enumerate(chart_rows):
            target_y = 132 - (104 * min(max(float(row["target_hours"]) / chart_max_hours, 0.0), 1.0))
            row["target_point"] = _chart_point(index, float(row["target_hours"]))
            if row["actual_hours"] is not None:
                actual_y = 132 - (104 * min(max(float(row["actual_hours"]) / chart_max_hours, 0.0), 1.0))
                row["actual_point"] = _chart_point(index, float(row["actual_hours"]))
                row["actual_y"] = round(actual_y, 1)
                row["actual_label_y"] = round(max(16.0, actual_y - 9), 1)
            else:
                row["actual_point"] = ""
                row["actual_y"] = None
                row["actual_label_y"] = None
            row["x"] = round(48 + (288 * (index / max(1, len(chart_rows) - 1))), 1)
            row["target_y"] = round(target_y, 1)

        y_tick_values = [0.0, chart_max_hours / 2, chart_max_hours]
        y_ticks = []
        for tick_value in y_tick_values:
            tick_y = 132 - (104 * min(max(tick_value / chart_max_hours, 0.0), 1.0))
            y_ticks.append(
                {
                    "value": round(tick_value, 1),
                    "label": f"{tick_value:.1f}h",
                    "y": round(tick_y, 1),
                    "label_y": round(tick_y + 3, 1),
                }
            )

        week_chart = {
            "rows": chart_rows,
            "target_points": " ".join(str(row["target_point"]) for row in chart_rows),
            "actual_points": " ".join(str(row["actual_point"]) for row in chart_rows if row["actual_point"]),
            "y_ticks": y_ticks,
            "target_total": round(target_total, 1),
            "actual_total": round(actual_total, 1),
            "max_hours": round(chart_max_hours, 1),
        }
        today_chart_row = next((row for row in chart_rows if str(row.get("date") or "") == today.isoformat()), None)
        chart_today_hours = float(today_chart_row.get("actual_daily_hours") or 0.0) if today_chart_row else 0.0
        today_study_hours = chart_today_hours
        today_delta_seconds = today_study_hours * 3600
        today_study_minutes = int(round(today_study_hours * 60))
        total_target_seconds = max(0.0, float(summary.get("total_target") or 0) * 60)
        today_progress_delta = (today_delta_seconds / total_target_seconds * 100) if total_target_seconds else 0.0

        before_today_minutes = max(0.0, watched_minutes_total - today_delta_seconds / 60)
        after_today_minutes = watched_minutes_total
        makeup_days: List[Dict[str, Any]] = []
        cumulative_minutes = 0.0
        for week in week_rows:
            for day in week.get("daily_recommendations", []):
                target_minutes = float(day.get("hours") or 0) * 60
                if target_minutes <= 0:
                    continue
                start_minutes = cumulative_minutes
                end_minutes = cumulative_minutes + target_minutes
                overlap_minutes = max(0.0, min(after_today_minutes, end_minutes) - max(before_today_minutes, start_minutes))
                if overlap_minutes > 0:
                    makeup_days.append(
                        {
                            "week_number": int(week.get("number") or 0),
                            "subject": str(week.get("subject") or ""),
                            "label": str(day.get("label") or ""),
                            "date": str(day.get("date") or ""),
                            "minutes": int(round(overlap_minutes)),
                        }
                    )
                cumulative_minutes = end_minutes

        today_videos = []
        for item in activity_events_by_day.get(today.isoformat(), []):
            delta_seconds = max(0.0, float(item.get("delta_seconds") or 0))
            if delta_seconds <= 0:
                continue
            duration_seconds = max(0.0, float(item.get("duration_seconds") or 0))
            watched_seconds = max(0.0, float(item.get("watched_seconds") or 0))
            today_videos.append(
                {
                    "subject": str(item.get("subject") or ""),
                    "sequence": int(item.get("sequence") or 0),
                    "title": str(item.get("title") or ""),
                    "minutes": round(delta_seconds / 60, 1),
                    "completion": round(min(100.0, watched_seconds / duration_seconds * 100) if duration_seconds else 0.0, 1),
                }
            )

        today_study = {
            "day": today.isoformat(),
            "hours": round(today_study_hours, 2),
            "minutes": today_study_minutes,
            "progress_delta": round(today_progress_delta, 2),
            "makeup_days": makeup_days,
            "videos": today_videos,
        }
        return {
            "plan_start": STUDY_PLAN_START,
            "plan_end": STUDY_PLAN_END,
            "plan_total_weeks": len(week_rows),
            "summary": summary,
            "total_hours": round(total_hours, 1),
            "remaining_hours": round(remaining_hours, 1),
            "elapsed_percent": round(elapsed_percent, 1),
            "pace_delta": round(pace_delta, 1),
            "pace_state": pace_state,
            "pace_label": pace_label,
            "pace_message": pace_message,
            "pace_insight": pace_insight,
            "visual_angle": visual_angle,
            "metric_cards": metric_cards,
            "subject_rows": subject_rows,
            "weak_subjects": weak_subjects,
            "current_week": current_week,
            "today_row": today_row,
            "today_study": today_study,
            "next_videos": next_videos,
            "momentum_days": momentum_days,
            "momentum_score": momentum_score,
            "momentum_angle": round(momentum_score * 3.6, 1),
            "active_recent_days": active_recent_days,
            "last_updated_label": last_updated_label,
            "timeline_nodes": timeline_nodes,
            "week_chart": week_chart,
        }

    _RECALL_EXCLUDED_CARD_MARKERS = ("待確認", "已修正", "需修正", "校正", "原筆記", "筆記中")

    def _is_recall_concept_eligible(concept: Any) -> bool:
        if not isinstance(concept, dict):
            return False
        card_text = "\n".join(
            str(concept.get(field) or "").strip()
            for field in ("concept", "explanation", "memory_hint")
        )
        return bool(card_text) and not any(marker in card_text for marker in _RECALL_EXCLUDED_CARD_MARKERS)

    def _build_recall_widget_context() -> Dict[str, Any]:
        today = _study_plan_business_date().isoformat()
        due_cards = storage.list_due_study_recall_cards(today=today, limit=18)
        cards: List[Dict[str, Any]] = []
        session_cache: Dict[int, Dict[str, Any]] = {}
        for due_card in due_cards:
            session_id = int(due_card["session_id"])
            recall_session = session_cache.get(session_id)
            if recall_session is None:
                recall_session = storage.get_study_recall_session(session_id) or {}
                session_cache[session_id] = recall_session
            concept_index = int(due_card["concept_index"])
            concepts = recall_session.get("key_concepts") or []
            if concept_index >= len(concepts) or not _is_recall_concept_eligible(concepts[concept_index]):
                continue
            cards.append({**due_card, "concept_data": concepts[concept_index]})
        return {
            "due_count": len(cards),
            "cards": cards,
        }

    def _study_plan_minutes(value: Any) -> float:
        try:
            parsed = float(str(value or "0").strip())
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, min(parsed, 1_440.0))

    _NOTE_IMAGE_MIME_TYPES = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".webp": "image/webp",
        ".gif": "image/gif",
    }

    def _extract_openai_text(payload: Dict[str, Any]) -> str:
        direct = payload.get("output_text")
        if isinstance(direct, str) and direct.strip():
            return direct.strip()
        for item in payload.get("output") or []:
            if not isinstance(item, dict):
                continue
            for content in item.get("content") or []:
                if isinstance(content, dict) and isinstance(content.get("text"), str):
                    return content["text"].strip()
        return ""

    def _validate_recall_output(payload: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return None
        summary = str(payload.get("summary") or "").strip()
        detected_topic = str(payload.get("detected_topic") or "").strip()
        raw_concepts = payload.get("key_concepts")
        if not summary or not detected_topic or not isinstance(raw_concepts, list):
            return None
        prepared_concepts: List[Dict[str, Any]] = []
        for item in raw_concepts[:15]:
            if not isinstance(item, dict):
                continue
            concept = str(item.get("concept") or "").strip()
            explanation = str(item.get("explanation") or "").strip()
            memory_hint = str(item.get("memory_hint") or "").strip()
            topic = str(item.get("topic") or detected_topic).strip()
            related_concepts = item.get("related_concepts")
            if concept and explanation and _is_recall_concept_eligible(
                {"concept": concept, "explanation": explanation, "memory_hint": memory_hint}
            ):
                prepared_concepts.append(
                    {
                        "concept": concept[:80],
                        "explanation": explanation[:720],
                        "memory_hint": memory_hint[:120],
                        "topic": topic[:48] or detected_topic[:48],
                        "note_topic": detected_topic[:80],
                        "related_concepts": [
                            str(value).strip()[:80]
                            for value in related_concepts[:2]
                            if str(value).strip()
                        ] if isinstance(related_concepts, list) else [],
                    }
                )
        if not prepared_concepts:
            return None
        title_lookup = {item["concept"].casefold(): item["concept"] for item in prepared_concepts}
        concepts_by_title = {item["concept"]: item for item in prepared_concepts}
        for item in prepared_concepts:
            normalized_related: List[str] = []
            for related in item["related_concepts"]:
                matched = title_lookup.get(related.casefold())
                if matched and matched != item["concept"] and matched not in normalized_related:
                    normalized_related.append(matched)
            item["related_concepts"] = normalized_related[:2]
        for item in prepared_concepts:
            for related in list(item["related_concepts"]):
                target = concepts_by_title.get(related)
                if target is not None and item["concept"] not in target["related_concepts"]:
                    target["related_concepts"] = (target["related_concepts"] + [item["concept"]])[:2]
        return {
            "detected_topic": detected_topic[:80],
            "summary": summary[:800],
            "key_concepts": prepared_concepts,
        }

    def _analyze_study_note_images(images: List[Tuple[str, bytes, str]]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        if not openai_api_key:
            return None, "尚未設定 OPENAI_API_KEY，無法分析筆記。"
        content: List[Dict[str, Any]] = [
            {
                "type": "input_text",
                "text": (
                    "你是研究所考試的嚴謹助教。請閱讀上傳的繁體中文筆記，整理成精確、好理解且好記憶的繁體中文重點卡。"
                    "卡片內容必須以影像中實際寫到的資訊為主，只可補上理解原句所必需的最少說明，不得自行擴寫成課本章節。"
                    "不要因為筆記提到某個專有名詞，就額外建立該名詞的定義卡；只有筆記本身正在記錄、定義或解釋該名詞時，才建立對應重點卡。"
                    "每張卡片只保留一個可直接複習的考試概念；concept 是不超過 16 字的短標題。explanation 請用 2 至 5 句說清楚："
                    "先下定義或結論，再說明成立條件、用途、推論原因或容易混淆處；資訊要完整但避免背景敘述、重複語句與空泛提醒。"
                    "memory_hint 是不超過 32 字的口訣或辨識線索。"
                    "影像中每個可辨識、具考試價值的公式、定義式或符號關係，都必須建立至少一張獨立的 key_concepts 公式卡，"
                    "不可只附在其他概念卡的 explanation 中。公式卡須完整列出公式並解釋符號、成立條件或如何使用。"
                    "請先用 detected_topic 為整份筆記命名一個精確的學科主題，不要使用日期或『今日筆記』等泛稱。"
                    "每張卡的 topic 是較細的觀念群組名稱；同一觀念群的卡片必須使用完全相同的 topic，讓頁面能自動分組。"
                    "若兩張卡具有前置知識、推導、比較、互逆或應用關係，請在 related_concepts 填入對方完全相同的 concept 標題，"
                    "最多 2 張且只保留最能幫助理解或記憶的強關聯；關聯需雙向，沒有明確關係時輸出空陣列，不可為了湊數建立關聯。"
                    "所有數學表達式請使用 LaTeX：行內公式一律寫成 \\( ... \\)，獨立公式一律寫成 \\[ ... \\]；"
                    "若公式推導較長，必須在獨立公式中使用 \\begin{aligned} ... \\\\ ... \\end{aligned}，依等號或推導步驟合理換行，避免輸出單一超長公式。"
                    "保留變數、上下標、分數、轉置、向量與條件，不要輸出 Markdown 程式碼區塊或純文字替代公式。"
                    "請用可靠的學科知識檢查筆記：若定義、符號、公式、推論或例子可明確判定為錯誤，先靜默修正為正確版本，再建立一般重點卡。"
                    "不得建立『待確認／已修正／錯誤說明』卡片，也不得在 summary、explanation 或 memory_hint 解釋你做了修正。"
                    "只有數字、符號或公式模糊到無法可靠判定時才略過；不可猜測。"
                    "逐一涵蓋其餘每個獨立且有考試價值的概念，不要把不同概念硬合併；每次最多建立 15 張重點卡。"
                    "若超過 15 項，先保留所有公式卡，再保留最核心、最常考且能涵蓋其他細節的概念。"
                    "輸出繁體中文 JSON：summary 是卡片後方的『重點總結』，以 3 至 5 行整理考前必記結論、公式關係與判斷順序，"
                    "最多 400 字，只摘要實際保留的卡片內容。"
                    "key_concepts 至少 1 項、最多 15 項。不要輸出考題、選項、答案或任何題庫資料。"
                ),
            }
        ]
        for _filename, image_bytes, mime_type in images:
            encoded = base64.b64encode(image_bytes).decode("ascii")
            content.append(
                {
                    "type": "input_image",
                    "image_url": f"data:{mime_type};base64,{encoded}",
                    "detail": "high",
                }
            )
        schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["detected_topic", "summary", "key_concepts"],
            "properties": {
                "detected_topic": {"type": "string", "maxLength": 80},
                "summary": {"type": "string", "maxLength": 800},
                "key_concepts": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 15,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["concept", "explanation", "memory_hint", "topic", "related_concepts"],
                        "properties": {
                            "concept": {"type": "string", "maxLength": 80},
                            "explanation": {"type": "string", "maxLength": 720},
                            "memory_hint": {"type": "string", "maxLength": 120},
                            "topic": {"type": "string", "maxLength": 48},
                            "related_concepts": {
                                "type": "array",
                                "maxItems": 2,
                                "items": {"type": "string", "maxLength": 80},
                            },
                        },
                    },
                },
            },
        }
        request_body = {
            "model": openai_model,
            "store": False,
            "input": [{"role": "user", "content": content}],
            "text": {"format": {"type": "json_schema", "name": "study_recall_note", "strict": True, "schema": schema}},
        }
        try:
            response = requests.post(
                "https://api.openai.com/v1/responses",
                headers={"Authorization": f"Bearer {openai_api_key}", "Content-Type": "application/json"},
                json=request_body,
                timeout=90,
            )
            response.raise_for_status()
            output_text = _extract_openai_text(response.json())
            parsed = json.loads(output_text)
        except (requests.RequestException, ValueError, TypeError):
            return None, "筆記分析暫時失敗，請確認 API 金鑰、模型設定與網路後重試。"
        validated = _validate_recall_output(parsed)
        if not validated:
            return None, "筆記內容不足以產生可靠的重點卡，請上傳更清晰或更多頁筆記。"
        return validated, None

    def _rebuild_all_study_recall_relations() -> Optional[str]:
        sessions = storage.list_study_recall_sessions(limit=None)
        concepts_by_session: Dict[int, List[Dict[str, Any]]] = {}
        card_catalog: List[Dict[str, Any]] = []
        cards_by_id: Dict[str, Dict[str, Any]] = {}
        for recall_session in sessions:
            session_id = int(recall_session["id"])
            concepts = recall_session.get("key_concepts") or []
            concepts_by_session[session_id] = concepts
            for concept_index, concept in enumerate(concepts):
                if not isinstance(concept, dict) or not _is_recall_concept_eligible(concept):
                    continue
                card_id = f"s{session_id}:c{concept_index}"
                catalog_item = {
                    "id": card_id,
                    "note": str(concept.get("note_topic") or recall_session.get("title") or "")[:80],
                    "topic": str(concept.get("topic") or "")[:48],
                    "concept": str(concept.get("concept") or "")[:80],
                    "explanation": str(concept.get("explanation") or "")[:220],
                }
                card_catalog.append(catalog_item)
                cards_by_id[card_id] = {
                    "session_id": session_id,
                    "concept_index": concept_index,
                    "title": catalog_item["concept"],
                }

        if len(card_catalog) < 2:
            for concepts in concepts_by_session.values():
                for concept in concepts:
                    if isinstance(concept, dict):
                        concept["relations"] = []
            storage.replace_study_recall_concepts_bulk(concepts_by_session)
            return None

        schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["relations"],
            "properties": {
                "relations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["source_id", "target_id", "association"],
                        "properties": {
                            "source_id": {"type": "string"},
                            "target_id": {"type": "string"},
                            "association": {"type": "string", "maxLength": 180},
                        },
                    },
                }
            },
        }
        prompt = (
            "你是研究所考試的知識架構助教。以下是使用者目前所有重點卡。每次都必須忽略舊關聯，重新審視全部卡片。"
            "找出具有明確學理關係的卡片配對，包括前置知識、定義與推論、公式推導、互逆、比較、特例或實際應用。"
            "同份與不同份筆記都可連結，但不得只因同科目或關鍵字相似而連結。每張卡最多保留 2 個最有助於理解與記憶的強關聯；"
            "relations 必須依關聯的重要性由高到低輸出；沒有強關聯的卡片可不輸出。每組配對只輸出一次。association 請用精確、好記的繁體中文說明兩件事："
            "它們的觀念關聯在哪，以及複習時可以如何從一張聯想到另一張。說明必須從任一張卡閱讀都成立，形成雙向記憶橋接；"
            "請直接說明具體知識，不可使用『兩者相關』等空泛句子，也不要使用『前者／後者』等依賴輸出順序的代稱。"
            "source_id 與 target_id 只使用清單提供的 id，不得改寫；association 只能使用卡片標題或觀念名稱，絕對不可出現 s6:c6 這類內部 id。\n\n重點卡清單：\n"
            + json.dumps(card_catalog, ensure_ascii=False, separators=(",", ":"))
        )
        request_body = {
            "model": openai_model,
            "store": False,
            "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "study_recall_relations",
                    "strict": True,
                    "schema": schema,
                }
            },
        }
        try:
            response = requests.post(
                "https://api.openai.com/v1/responses",
                headers={"Authorization": f"Bearer {openai_api_key}", "Content-Type": "application/json"},
                json=request_body,
                timeout=120,
            )
            response.raise_for_status()
            parsed = json.loads(_extract_openai_text(response.json()))
        except (requests.RequestException, ValueError, TypeError):
            return "AI 關聯分析暫時失敗，原有關聯已保留。"

        raw_relations = parsed.get("relations") if isinstance(parsed, dict) else None
        if not isinstance(raw_relations, list):
            return "AI 未回傳有效的關聯資料，原有關聯已保留。"

        for concepts in concepts_by_session.values():
            for concept in concepts:
                if isinstance(concept, dict):
                    concept["relations"] = []
        relation_counts = {card_id: 0 for card_id in cards_by_id}
        card_titles_by_id = {
            card_id.casefold(): str(card.get("title") or "").strip()
            for card_id, card in cards_by_id.items()
        }
        seen_pairs = set()
        for relation in raw_relations:
            if not isinstance(relation, dict):
                continue
            source_id = str(relation.get("source_id") or "").strip()
            target_id = str(relation.get("target_id") or "").strip()
            association = " ".join(str(relation.get("association") or "").split())
            association = re.sub(
                r"\bs\d+\s*:\s*c\d+\b",
                lambda match: card_titles_by_id.get(re.sub(r"\s+", "", match.group(0)).casefold(), ""),
                association,
                flags=re.IGNORECASE,
            )
            association = re.sub(r"\s+([，。；：、！？])", r"\1", association).strip(" \t:：,，;；-")[:180]
            pair = tuple(sorted((source_id, target_id)))
            if (
                source_id not in cards_by_id
                or target_id not in cards_by_id
                or source_id == target_id
                or pair in seen_pairs
                or not association
                or relation_counts[source_id] >= 2
                or relation_counts[target_id] >= 2
            ):
                continue
            seen_pairs.add(pair)
            relation_counts[source_id] += 1
            relation_counts[target_id] += 1
            source = cards_by_id[source_id]
            target = cards_by_id[target_id]
            source_concept = concepts_by_session[source["session_id"]][source["concept_index"]]
            target_concept = concepts_by_session[target["session_id"]][target["concept_index"]]
            source_concept["relations"].append(
                {
                    "session_id": target["session_id"],
                    "concept_index": target["concept_index"],
                    "title": target["title"],
                    "association": association,
                }
            )
            target_concept["relations"].append(
                {
                    "session_id": source["session_id"],
                    "concept_index": source["concept_index"],
                    "title": source["title"],
                    "association": association,
                }
            )
        storage.replace_study_recall_concepts_bulk(concepts_by_session)
        return None

    def fetch_assignments_for(user: Dict[str, str]) -> Tuple[Dict[str, Any], Optional[str]]:
        opts = CollectOptions(
            base_url=base_url,
            scope=default_scope,
            course_id=None,
            include_completed=True,
            all_courses=False,
            all_courses_all_terms=False,
            username=None,
            password=None,
            moodle_session=user.get("moodle_session"),
            insecure=False,
            timeout=default_timeout,
            debug=False,
        )
        result = collect_assignments(opts)
        excel_data = _generate_excel_data(result.get("all_assignments"))
        return result, excel_data

    @app.before_request
    def enforce_canonical_host():
        if not canonical_host:
            return
        forwarded_host = request.headers.get("X-Forwarded-Host")
        host = forwarded_host or request.host
        normalized_host = host.lower()
        if normalized_host.startswith("www."):
            normalized_host = normalized_host[4:]
        desired_host = canonical_host.lower()
        proto = request.headers.get("X-Forwarded-Proto", request.scheme)
        needs_host_redirect = normalized_host != desired_host
        needs_proto_redirect = proto != "https"
        if needs_host_redirect or needs_proto_redirect:
            parts = urlsplit(request.url)
            new_url = urlunsplit(
                (
                    "https",
                    desired_host,
                    parts.path,
                    parts.query,
                    parts.fragment,
                )
            )
            return redirect(new_url, code=301)

    @app.after_request
    def add_no_store_headers(resp):
        cache_control = resp.headers.get("Cache-Control", "")
        content_disposition = resp.headers.get("Content-Disposition", "")
        if "attachment" in content_disposition.lower() or cache_control.startswith("public"):
            return resp
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if current_user():
            return redirect(url_for("index"))
        if request.method == "POST":
            login_type = request.form.get("login_type", "password")
            if login_type == "session":
                raw_session = request.form.get("moodle_session", "").strip()
                if not raw_session:
                    flash("請貼上有效的 MoodleSession 值。", "error")
                else:
                    digest = hashlib.sha1(raw_session.encode("utf-8")).hexdigest()[:10]
                    session_label = f"Session-{digest}"
                    existing_cache = load_cache_from_disk(session_label)
                    try:
                        result = None
                        excel_data = None
                        if not existing_cache:
                            result, excel_data = fetch_assignments_for(
                                {"username": session_label, "moodle_session": raw_session}
                            )
                        _start_web_session(
                            session_label,
                            moodle_session=raw_session,
                            is_guest=False,
                            is_admin=bool(admin_user_id and session_label == admin_user_id),
                            permanent=True,
                        )
                        record_ui_event("login_success", meta={"username": session_label})
                        if existing_cache:
                            flash("已載入先前的課程資料，系統將在背景自動更新最新內容。", "info")
                        else:
                            try:
                                set_assign_cache(result, excel_data)
                                flash("已成功透過 E3 Session 取得最新資訊。", "success")
                            except Exception:
                                flash("Session 登入成功，但暫存資料寫入失敗。", "warning")
                        response = redirect(url_for("index"))
                        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
                        response.headers["Pragma"] = "no-cache"
                        response.headers["Expires"] = "0"
                        return response
                    except Exception as exc:
                        flash(f"Session 驗證失敗：{exc}，請確認 MoodleSession 是否正確。", "error")
            else:
                raw_username = request.form.get("username", "").strip()
                raw_password = request.form.get("password", "")
                if not raw_username or not raw_password:
                    flash("請輸入帳號與密碼。", "error")
                else:
                    try:
                        sess = requests.Session()
                        login_with_password(sess, base_url, raw_username, raw_password, timeout=default_timeout)
                        cookie_val = sess.cookies.get("MoodleSession")
                        if not cookie_val:
                            raise RuntimeError("登入成功但未取得 MoodleSession。")
                        _start_web_session(
                            raw_username,
                            moodle_session=cookie_val,
                            is_guest=False,
                            is_admin=bool(admin_user_id and raw_username == admin_user_id),
                            permanent=True,
                        )
                        record_ui_event("login_success", meta={"username": raw_username})
                        existing_cache = load_cache_from_disk(raw_username)
                        if existing_cache:
                            flash("已載入先前的課程資料，系統將在背景自動更新最新內容。", "info")
                        else:
                            try:
                                result, excel_data = fetch_assignments_for(
                                    {"username": raw_username, "moodle_session": cookie_val}
                                )
                                set_assign_cache(result, excel_data)
                                flash("已成功獲取最新資訊。", "success")
                            except Exception as exc:
                                flash(f"登入成功但獲取資料失敗：{exc}，程式將在背景重試。", "warning")
                        response = redirect(url_for("index"))
                        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
                        response.headers["Pragma"] = "no-cache"
                        response.headers["Expires"] = "0"
                        return response
                    except Exception as exc:
                        flash(f"{exc}", "error")
        announcements_list = load_announcements()
        return render_template_string(
            LOGIN_TEMPLATE,
            stats=usage_stats(),
            stats_version=current_stats_version(),
            announcements=announcements_list,
            announcement_version=announcements_list[0]["id"] if announcements_list else None,
            support_email=support_email,
            app_home_url=app_home_url,
        )

    @app.route("/healthz", methods=["GET"])
    def health_check():
        return {"status": "ok"}, 200

    @app.route("/traffic/stats", methods=["GET"])
    def traffic_stats():
        stats = usage_stats()
        payload = {
            "version": current_stats_version(),
            "online": stats["online"],
            "total": stats["total"],
        }
        return payload, 200, {"Cache-Control": "no-store, max-age=0"}

    @app.route("/privacy", methods=["GET"])
    def privacy_policy():
        return render_template_string(
            PRIVACY_TEMPLATE,
            app_home_url=app_home_url,
            support_email=support_email,
            google_scope=GOOGLE_CALENDAR_SCOPE,
            legal_entity_name=legal_entity_name,
            effective_date=legal_effective_date,
        )

    @app.route("/terms", methods=["GET"])
    def terms_of_service():
        return render_template_string(
            TERMS_TEMPLATE,
            app_home_url=app_home_url,
            support_email=support_email,
            google_scope=GOOGLE_CALENDAR_SCOPE,
            legal_entity_name=legal_entity_name,
        )

    @app.post("/ui-event")
    @login_required
    def ui_event():
        payload = request.get_json(silent=True) or {}
        action = str(payload.get("action") or "").strip()
        status = str(payload.get("status") or "info").strip() or "info"
        meta = payload.get("meta")
        if not isinstance(meta, dict):
            meta = None
        if not action:
            return {"ok": False, "error": "action required"}, 400
        record_ui_event(action, status, meta)
        return {"ok": True}

    @app.get("/session/status")
    @login_required
    def session_status():
        user = current_user()
        return {"ok": True, "username": user["username"] if user else None}

    @app.get("/api/cache")
    @login_required
    def api_cache():
        user = current_user()
        viewed_username = get_viewed_username(actor=user)
        cache = get_assign_cache(viewed_username) or {}
        preferences = get_user_preferences(viewed_username)
        include_cache = str(request.args.get("include_cache") or "").lower() in {"1", "true", "yes"}
        refresh_state = _refresh_job_state(viewed_username)
        payload = {
            "ok": True,
            "ts": cache.get("ts"),
            "has_result": bool(cache.get("result")) if cache else False,
            "preferences": preferences,
            "viewed_username": viewed_username,
            "readonly_view": is_admin_viewing_other_user(actor=user, viewed_username=viewed_username),
            "refresh_status": refresh_state.get("status") if refresh_state else None,
            "refresh_error": refresh_state.get("error") if refresh_state else None,
            "refresh_in_progress": bool(refresh_state and refresh_state.get("status") == "running"),
            "refresh_started_at": refresh_state.get("started_at") if refresh_state else None,
            "refresh_finished_at": refresh_state.get("finished_at") if refresh_state else None,
        }
        if include_cache:
            payload["cache"] = cache
        return payload

    @app.post("/preferences")
    @login_required
    def save_preferences():
        user = current_user()
        if is_admin_viewing_other_user(actor=user):
            return {"ok": False, "error": "readonly_view"}, 403
        payload = request.get_json(silent=True) or {}
        updated = update_user_preferences(payload)
        return {"ok": True, "preferences": updated}

    @app.route("/guest-login", methods=["POST"])
    def guest_login():
        guest_name = f"訪客_{secrets.token_hex(3)}"
        _start_web_session(
            guest_name,
            moodle_session=None,
            is_guest=True,
            is_admin=False,
            permanent=False,
        )
        flash("已進入訪客模式：請使用匯出工具生成 JSON 後上傳即可瀏覽作業。", "info")
        record_ui_event("guest_login", meta={"username": guest_name})
        return redirect(url_for("index"))

    @app.route("/guest-tool", methods=["GET"])
    def guest_tool():
        tool_path = ROOT_DIR / "backend" / "tools" / "guest_export.exe"
        if not tool_path.exists():
            flash("找不到匯出工具。", "error")
            return redirect(url_for("login"))
        payload = send_file(
            tool_path,
            as_attachment=True,
            download_name="guest_export.exe",
            conditional=True,
            max_age=604800,
            etag=True,
        )
        stat = tool_path.stat()
        payload.headers["Cache-Control"] = "public, max-age=604800, immutable"
        payload.headers["Last-Modified"] = http_date(stat.st_mtime)
        payload.headers["Content-Length"] = str(stat.st_size)
        return payload

    @app.route("/guest-tool.py", methods=["GET"])
    def guest_tool_source():
        source_path = ROOT_DIR / "backend" / "tools" / "guest_export.py"
        if not source_path.exists():
            flash("找不到匯出工具原始碼。", "error")
            return redirect(url_for("login"))
        payload = send_file(
            source_path,
            as_attachment=True,
            download_name="guest_export.py",
            mimetype="text/x-python",
        )
        stat = source_path.stat()
        payload.headers["Cache-Control"] = "public, max-age=604800, immutable"
        payload.headers["Last-Modified"] = http_date(stat.st_mtime)
        payload.headers["Content-Length"] = str(stat.st_size)
        return payload

    @app.route("/guest/import", methods=["POST"])
    @login_required
    def guest_import():
        user = current_user()
        if not user or not user.get("is_guest"):
            flash("訪客匯入僅限訪客模式使用。", "error")
            record_ui_event("guest_import", "error", {"reason": "not_guest"})
            return redirect(url_for("index"))
        uploaded = request.files.get("guest_file")
        if not uploaded or not uploaded.filename:
            flash("請上傳由 guest_export 匯出工具產生的 JSON 檔。", "warning")
            record_ui_event("guest_import", "error", {"reason": "missing_file"})
            return redirect(url_for("index"))
        try:
            payload = json.load(uploaded.stream)
        except Exception as exc:
            flash(f"解析上傳檔案失敗：{exc}", "error")
            record_ui_event("guest_import", "error", {"reason": "parse_failed"})
            return redirect(url_for("index"))
        if payload.get("mode") != "guest_export_v1":
            flash("檔案格式不支援，請使用 guest_export 匯出工具產生的 JSON。", "warning")
            record_ui_event("guest_import", "error", {"reason": "unsupported_mode"})
            return redirect(url_for("index"))
        result = payload.get("result")
        excel_data = payload.get("excel_data")
        if not result:
            flash("檔案內容缺少作業資料。", "error")
            record_ui_event("guest_import", "error", {"reason": "missing_result"})
            return redirect(url_for("index"))
        if not excel_data:
            excel_data = _generate_excel_data((result or {}).get("all_assignments"))
        set_assign_cache(result, excel_data)
        flash("已匯入訪客資料（檔案）。", "success")
        record_ui_event(
            "guest_import",
            "success",
            {"items": len(result.get("all_assignments", [])), "has_excel": bool(excel_data)},
        )
        return redirect(url_for("index"))

    @app.post("/announcements/<announcement_id>/vote")
    @login_required
    def announcement_vote(announcement_id: str):
        user = current_user()
        if not user:
            return {"ok": False, "error": "not_logged_in"}, 401
        payload = request.get_json(silent=True) or {}
        requested_vote = str(payload.get("vote") or "").strip().lower()
        if requested_vote not in {"up", "down", "clear"}:
            return {"ok": False, "error": "invalid_vote"}, 400
        resolved_vote = None if requested_vote == "clear" else requested_vote
        updated = set_announcement_vote(announcement_id, user["username"], resolved_vote)
        if not updated:
            return {"ok": False, "error": "announcement_not_found"}, 404
        record_ui_event(
            "announcement_vote",
            "success",
            {"announcement_id": announcement_id, "vote": resolved_vote or "clear"},
        )
        return {"ok": True, "announcement": updated}
    @app.route("/google/authorize")
    @login_required
    def google_authorize():
        if not _google_ready():
            flash("尚未設定 Google OAuth，請先在伺服器端提供 Client ID/Secret。", "warning")
            record_ui_event("google_link", "error", {"stage": "authorize", "reason": "not_ready"})
            return redirect(url_for("index"))
        state = _build_google_state()
        session["google_auth_state"] = state
        record_ui_event("google_link", "start", {"stage": "authorize"})
        return redirect(
            build_google_authorize_url(
                google_client_id,
                _google_redirect_uri(),
                scope=GOOGLE_CALENDAR_SCOPE,
                state=state,
            )
        )

    @app.route("/google/callback")
    def google_callback():
        user = current_user()
        if not user:
            flash("請先登入 E3，再進行 Google 授權。", "warning")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": "not_logged_in"})
            return redirect(url_for("login"))
        if not _google_ready():
            flash("尚未設定 Google OAuth。", "warning")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": "not_ready"})
            return redirect(url_for("index"))
        error = request.args.get("error")
        if error:
            flash(f"Google 授權失敗：{error}", "error")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": error})
            return redirect(url_for("index"))
        code = request.args.get("code")
        state = request.args.get("state")
        stored_state = session.get("google_auth_state")
        state_valid = bool(state) and _verify_google_state(state)
        if not code or not state_valid:
            flash("Google 授權資訊錯誤，請重新嘗試。", "error")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": "invalid_state"})
            return redirect(url_for("index"))
        if stored_state and state != stored_state:
            record_ui_event("google_link", "info", {"stage": "callback", "reason": "state_mismatch_but_signed"})
        session.pop("google_auth_state", None)
        try:
            token_resp = exchange_code_for_google_token(
                code,
                client_id=google_client_id,
                client_secret=google_client_secret,
                redirect_uri=_google_redirect_uri(),
            )
        except Exception as exc:  # pragma: no cover
            flash(f"換取 Google Token 失敗：{exc}", "error")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": "token_exchange"})
            return redirect(url_for("index"))
        existing = load_google_tokens(user["username"]) or {}
        refresh_token = token_resp.get("refresh_token") or existing.get("refresh_token")
        if not refresh_token:
            flash("Google 未提供 refresh token，請勾選同意並再次授權。", "error")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": "missing_refresh_token"})
            return redirect(url_for("index"))
        tokens = {
            "access_token": token_resp.get("access_token"),
            "refresh_token": refresh_token,
            "scope": token_resp.get("scope"),
            "token_type": token_resp.get("token_type"),
            "expires_at": compute_expiry(token_resp.get("expires_in", 3600)),
        }
        save_google_tokens(user["username"], tokens)
        flash("已成功連結 Google 日曆，可同步作業。", "success")
        record_ui_event("google_link", "success", {"stage": "callback"})
        return redirect(url_for("index"))

    @app.post("/google/unlink")
    @login_required
    def google_unlink():
        user = current_user()
        if user:
            clear_google_tokens(user["username"])
        flash("已解除 Google 日曆連結。", "info")
        record_ui_event("google_unlink", "success")
        return redirect(url_for("index"))

    @app.post("/google/sync")
    @login_required
    def google_sync():
        if not _google_ready():
            flash("尚未設定 Google OAuth，無法同步日曆。", "warning")
            record_ui_event("google_sync", "error", {"reason": "not_ready"})
            return redirect(url_for("index"))
        user = current_user()
        if not user:
            flash("請先登入後再同步。", "warning")
            record_ui_event("google_sync", "error", {"reason": "not_logged_in"})
            return redirect(url_for("login"))
        raw_selected = request.form.get("selected_uids", "")
        selected_uids: List[str] = []
        if raw_selected:
            try:
                selected_uids = json.loads(raw_selected)
            except Exception:
                selected_uids = []
        if not selected_uids:
            flash("請先選擇要導入的作業。", "warning")
            record_ui_event("google_sync", "error", {"reason": "no_selection"})
            return redirect(url_for("index"))
        tokens = load_google_tokens(user["username"])
        if not tokens:
            flash("尚未連結 Google 日曆。", "warning")
            record_ui_event("google_sync", "error", {"reason": "not_linked"})
            return redirect(url_for("index"))
        try:
            tokens = _ensure_google_access_token(user["username"], tokens)
        except Exception as exc:
            flash(f"無法更新 Google Token：{exc}", "error")
            record_ui_event("google_sync", "error", {"reason": "token_refresh"})
            return redirect(url_for("index"))
        record_ui_event("google_sync", "start", {"count": len(selected_uids)})
        try:
            cache = get_assign_cache() or {}
            result = cache.get("result") or {}
            excel_data = cache.get("excel_data")
            assignments = _select_assignments_from_result(result, selected_uids)
            if user.get("is_guest"):
                if not assignments:
                    raise RuntimeError("找不到訪客匯入的作業資料，請重新匯入後再試。")
            elif not assignments:
                result, excel_data = fetch_assignments_for({"username": user["username"], "moodle_session": session.get("moodle_session")})
                set_assign_cache(result, excel_data)
                assignments = _select_assignments_from_result(result, selected_uids)
            if not assignments:
                flash("找不到選擇的作業，請重新整理後再試。", "warning")
                record_ui_event("google_sync", "error", {"reason": "not_found"})
                return redirect(url_for("index"))
            synced = sync_assignments_to_google_calendar(
                assignments,
                access_token=tokens["access_token"],
                calendar_id=google_calendar_id,
            )
            flash(f"已將 {synced} 筆作業同步到 Google 日曆。", "success")
            record_ui_event("google_sync", "success", {"synced": synced})
        except GoogleUnauthorizedError:
            clear_google_tokens(user["username"])
            flash("Google 授權已失效，請重新連結後再嘗試。", "error")
            record_ui_event("google_sync", "error", {"reason": "unauthorized"})
        except Exception as exc:
            flash(f"同步 Google 日曆失敗：{exc}", "error")
            record_ui_event("google_sync", "error", {"reason": "exception"})
        return redirect(url_for("index"))

    @app.route("/logout")
    def logout():
        old_user = session.get("username")
        session_token = session.get("session_token")
        was_guest = bool(session.get("is_guest"))
        if old_user:
            if was_guest:
                storage.delete_user_cache(old_user)
            clear_google_tokens(old_user)
        if session_token:
            storage.clear_web_session(session_token)
        if old_user:
            record_ui_event("logout", meta={"username": old_user})
        session.clear()
        session.permanent = False
        session.modified = True
        flash("已登出。", "success")
        resp = redirect(url_for("index"))
        session_cookie_name = app.config.get("SESSION_COOKIE_NAME", "session")
        cookie_path = app.config.get("SESSION_COOKIE_PATH", "/")
        cookie_domain = app.config.get("SESSION_COOKIE_DOMAIN")
        resp.delete_cookie(session_cookie_name, path=cookie_path, domain=cookie_domain)
        host_only_domain = (request.host.split(":", 1)[0] or "").strip() or None
        if host_only_domain and host_only_domain != cookie_domain:
            resp.delete_cookie(session_cookie_name, path=cookie_path, domain=host_only_domain)
            if not host_only_domain.startswith("."):
                resp.delete_cookie(session_cookie_name, path=cookie_path, domain=f".{host_only_domain}")
        return resp

    @app.post("/api/assignments")
    @login_required
    def api_assignments():
        user = current_user()
        if is_admin_viewing_other_user(actor=user):
            return {"ok": False, "error": "readonly_view"}, 403
        if user and user.get("is_guest"):
            return {"ok": False, "error": "訪客模式不支援自動更新"}, 400
        username = user["username"]
        moodle_session_val = session.get("moodle_session")
        prev_cache = get_assign_cache() or {}
        prev_ts = prev_cache.get("ts") or 0
        if not _mark_refresh_job_started(username):
            return {
                "ok": True,
                "message": "背景更新已在進行中。",
                "background": True,
                "in_progress": True,
                "ts": prev_ts,
            }

        def _run_background():
            with app.app_context():
                try:
                    result, excel_data = fetch_assignments_for({"username": username, "moodle_session": moodle_session_val})
                    set_assign_cache_for_user(username, result, excel_data)
                    record_ui_event(
                        "refresh_assignments",
                        "success",
                        {"items": len(result.get("all_assignments", [])), "mode": "background"},
                    )
                except Exception as exc:  # pragma: no cover - background logging
                    record_ui_event("refresh_assignments", "error", {"reason": str(exc), "mode": "background"})
                    _mark_refresh_job_done(username, status="error", error=str(exc))
                else:
                    _mark_refresh_job_done(username, status="success")

        threading.Thread(target=_run_background, daemon=True).start()
        return {
            "ok": True,
            "message": "已啟動背景更新，稍後將自動刷新。",
            "background": True,
            "ts": prev_ts,
        }

    @app.route("/calendar.ics")
    @login_required
    def calendar_export():
        cache = get_assign_cache()
        assignments = cache.get("result", {}).get("all_assignments", []) if cache else []
        calendar = _build_calendar(assignments)
        if not calendar:
            flash("尚無可匯出的作業資料。", "info")
            record_ui_event("export_calendar", "error", {"reason": "no_assignments"})
            return redirect(url_for("index"))
        record_ui_event("export_calendar", "success", {"items": len(assignments)})
        return Response(
            calendar,
            mimetype="text/calendar",
            headers={"Content-Disposition": "attachment; filename=pending_assignments.ics"},
        )

    @app.get("/study-progress")
    def public_study_progress():
        user = current_user()
        videos = storage.list_study_plan_videos_with_records()
        week_rows, current_week, summary = _study_plan_week_rows(videos)
        context = _build_study_home_context(videos, week_rows, current_week, summary)
        record_ui_event("public_study_progress_view", meta={"completion": round(float(summary.get("completion") or 0), 1)})
        return render_template_string(
            PUBLIC_STUDY_TEMPLATE,
            **context,
            share_url=request.url,
            is_admin=bool(user and user.get("is_admin")),
            admin_user=user,
        )

    @app.get("/public/study-progress")
    def public_study_progress_alias():
        return redirect(url_for("public_study_progress"), code=301)

    @app.get("/admin/study-home")
    @admin_required
    def admin_study_home():
        user = current_user()
        videos = storage.list_study_plan_videos_with_records()
        week_rows, current_week, summary = _study_plan_week_rows(videos)
        home_context = _build_study_home_context(videos, week_rows, current_week, summary)
        return render_template_string(
            STUDY_HOME_TEMPLATE,
            admin_user=user,
            recall_widget=_build_recall_widget_context(),
            **home_context,
        )

    @app.get("/admin/study-recall")
    @admin_required
    def admin_study_recall():
        def decorate_review_curve(concept: Dict[str, Any]) -> None:
            review = concept.get("review") or {}
            history = review.get("history") or []
            total_points = max(1, len(history))
            curve_points = []
            for index, entry in enumerate(history):
                x = 50 if total_points == 1 else 6 + index * 88 / (total_points - 1)
                y = 36 - max(0, min(int(entry.get("rating") or 1) - 1, 4)) * 7
                curve_points.append(f"{x:.1f},{y:.1f}")
            review["curve_points"] = " ".join(curve_points)
            review["history_label"] = " → ".join(str(entry.get("rating")) for entry in history) or "尚未自評"
            review["latest_curve_y"] = 36 - max(0, min(int(history[-1].get("rating") or 1) - 1, 4)) * 7 if history else 36
            concept["review"] = review

        def organize_session_concepts(session: Dict[str, Any], *, replace_concepts: bool) -> Dict[int, Dict[str, Any]]:
            raw_concepts = session.get("key_concepts") or []
            indexed_concepts = {
                index: concept
                for index, concept in enumerate(raw_concepts)
                if _is_recall_concept_eligible(concept)
            }
            title_indexes = {
                str(concept.get("concept") or "").strip().casefold(): index
                for index, concept in indexed_concepts.items()
            }
            note_topic = next(
                (
                    str(concept.get("note_topic") or "").strip()
                    for concept in indexed_concepts.values()
                    if str(concept.get("note_topic") or "").strip()
                ),
                next(
                    (
                        str(concept.get("topic") or "").strip()
                        for concept in indexed_concepts.values()
                        if str(concept.get("topic") or "").strip()
                    ),
                    str(session.get("title") or "未分類筆記"),
                ),
            )
            topic_groups: Dict[str, List[Dict[str, Any]]] = {}
            for index, concept in indexed_concepts.items():
                concept["display_index"] = index
                concept["topic"] = str(concept.get("topic") or note_topic).strip() or note_topic
                related_cards = []
                stored_relations = concept.get("relations")
                if isinstance(stored_relations, list):
                    for relation in stored_relations:
                        if not isinstance(relation, dict):
                            continue
                        try:
                            related_session_id = int(relation.get("session_id") or 0)
                            related_index = int(relation.get("concept_index"))
                        except (TypeError, ValueError):
                            continue
                        related_title = str(relation.get("title") or "").strip()
                        association = " ".join(str(relation.get("association") or "").split())
                        visible_card_titles = {
                            f"s{int(session['id'])}:c{index}".casefold(): str(concept.get("concept") or "").strip(),
                            f"s{related_session_id}:c{related_index}".casefold(): related_title,
                        }
                        association = re.sub(
                            r"\bs\d+\s*:\s*c\d+\b",
                            lambda match: visible_card_titles.get(
                                re.sub(r"\s+", "", match.group(0)).casefold(),
                                "",
                            ),
                            association,
                            flags=re.IGNORECASE,
                        )
                        association = re.sub(r"\s+([，。；：、！？])", r"\1", association).strip(" \t:：,，;；-")
                        if related_session_id <= 0 or related_index < 0 or not related_title or not association:
                            continue
                        related_cards.append(
                            {
                                "session_id": related_session_id,
                                "title": related_title,
                                "index": related_index,
                                "association": association,
                            }
                        )
                else:
                    for related_title in concept.get("related_concepts") or []:
                        related_index = title_indexes.get(str(related_title or "").strip().casefold())
                        if related_index is None or related_index == index:
                            continue
                        related_concept = indexed_concepts.get(related_index) or {}
                        related_cards.append(
                            {
                                "session_id": int(session["id"]),
                                "title": str(related_concept.get("concept") or related_title),
                                "index": related_index,
                                "association": "這兩張卡屬於同一份筆記中的直接相關觀念；可一起對照複習。",
                            }
                        )
                concept["related_cards"] = related_cards[:2]
                topic_groups.setdefault(concept["topic"], []).append(concept)
            session["note_topic"] = note_topic
            session["concept_groups"] = [
                {"topic": topic, "concepts": concepts}
                for topic, concepts in topic_groups.items()
            ]
            if replace_concepts:
                session["key_concepts"] = list(indexed_concepts.values())
            return indexed_concepts

        user = current_user()
        try:
            selected_id = int(request.args.get("session_id") or 0)
        except (TypeError, ValueError):
            selected_id = 0
        sessions = storage.list_study_recall_sessions(limit=36)
        session_groups_by_topic: Dict[str, List[Dict[str, Any]]] = {}
        for recall_session in sessions:
            concepts = recall_session.get("key_concepts") or []
            topic_label = next(
                (
                    str(concept.get("note_topic") or "").strip()
                    for concept in concepts
                    if isinstance(concept, dict) and str(concept.get("note_topic") or "").strip()
                ),
                next(
                    (
                        str(concept.get("topic") or "").strip()
                        for concept in concepts
                        if isinstance(concept, dict) and str(concept.get("topic") or "").strip()
                    ),
                    str(recall_session.get("subject") or recall_session.get("title") or "未分類筆記"),
                ),
            )
            recall_session["topic_label"] = topic_label
            session_groups_by_topic.setdefault(topic_label, []).append(recall_session)
        session_groups = [
            {"topic": topic, "sessions": grouped_sessions}
            for topic, grouped_sessions in sorted(session_groups_by_topic.items(), key=lambda item: item[0].casefold())
        ]
        selected_session = storage.get_study_recall_session(selected_id) if selected_id else None
        if selected_session is None and sessions:
            selected_session = storage.get_study_recall_session(int(sessions[0]["id"]))
        if selected_session:
            selected_session["image_urls"] = [
                url_for("admin_study_recall_image", session_id=selected_session["id"], filename=filename)
                for filename in selected_session.get("image_filenames") or []
            ]
            organize_session_concepts(selected_session, replace_concepts=True)
            for concept in selected_session["key_concepts"]:
                decorate_review_curve(concept)
        today = _study_plan_business_date().isoformat()
        due_cards = storage.list_due_study_recall_cards(today=today)
        review_cards: List[Dict[str, Any]] = []
        review_sessions: Dict[int, Dict[str, Any]] = {}
        for due_card in due_cards:
            session_id = int(due_card["session_id"])
            review_session = review_sessions.get(session_id)
            if review_session is None:
                review_session = storage.get_study_recall_session(session_id) or {}
                review_sessions[session_id] = review_session
            concept_index = int(due_card["concept_index"])
            concepts_by_index = organize_session_concepts(review_session, replace_concepts=False)
            concept = concepts_by_index.get(concept_index)
            if concept is None:
                continue
            decorate_review_curve(concept)
            review_cards.append({**due_card, "concept_data": concept})
        review_schedule = storage.list_study_recall_schedule(start_date=today)
        return render_template_string(
            STUDY_RECALL_TEMPLATE,
            admin_user=user,
            subjects=STUDY_PLAN_SUBJECTS,
            today=today,
            sessions=sessions,
            session_groups=session_groups,
            due_cards=due_cards,
            review_cards=review_cards,
            review_schedule=review_schedule,
            selected_session=selected_session,
            openai_ready=bool(openai_api_key),
            nav_active="recall",
        )

    @app.get("/admin/study-recall/<int:session_id>/image/<filename>")
    @admin_required
    def admin_study_recall_image(session_id: int, filename: str):
        recall_session = storage.get_study_recall_session(session_id)
        allowed_names = set((recall_session or {}).get("image_filenames") or [])
        if filename not in allowed_names or Path(filename).name != filename:
            return Response("Not Found", status=404, mimetype="text/plain")
        image_path = study_upload_root / str(session_id) / filename
        if not image_path.is_file():
            return Response("Not Found", status=404, mimetype="text/plain")
        return send_file(image_path, conditional=True, max_age=0)

    @app.post("/admin/study-recall/<int:session_id>/cards/<int:concept_index>/ask")
    @admin_required
    def admin_study_recall_ask_card(session_id: int, concept_index: int):
        if not openai_api_key:
            return {"ok": False, "error": "AI 問答尚未啟用，請先設定 OPENAI_API_KEY。"}, 503
        recall_session = storage.get_study_recall_session(session_id)
        concepts = (recall_session or {}).get("key_concepts") or []
        if concept_index < 0 or concept_index >= len(concepts) or not isinstance(concepts[concept_index], dict):
            return {"ok": False, "error": "找不到這張重點卡。"}, 404
        concept = concepts[concept_index]
        if not _is_recall_concept_eligible(concept):
            return {"ok": False, "error": "這張重點卡目前無法使用 AI 問答。"}, 400
        payload = request.get_json(silent=True) or {}
        question = " ".join(str(payload.get("question") or "").split()).strip()
        if not question:
            return {"ok": False, "error": "請輸入想詢問的內容。"}, 400
        if len(question) > 800:
            return {"ok": False, "error": "問題請控制在 800 字以內。"}, 400

        history: List[Dict[str, str]] = []
        raw_history = payload.get("history")
        if isinstance(raw_history, list):
            for entry in raw_history[-6:]:
                if not isinstance(entry, dict):
                    continue
                role = str(entry.get("role") or "").strip().lower()
                content = " ".join(str(entry.get("content") or "").split()).strip()[:1600]
                if role in {"user", "assistant"} and content:
                    history.append({"role": role, "content": content})

        relations = []
        for relation in (concept.get("relations") or [])[:2]:
            if not isinstance(relation, dict):
                continue
            title = str(relation.get("title") or "").strip()
            association = str(relation.get("association") or "").strip()
            if title and association:
                relations.append({"title": title[:80], "association": association[:240]})
        concept_topic = str(concept.get("topic") or "").strip()
        supporting_cards = []
        candidates = [
            (index, other)
            for index, other in enumerate(concepts)
            if index != concept_index and isinstance(other, dict) and _is_recall_concept_eligible(other)
        ]
        candidates.sort(
            key=lambda item: 0
            if concept_topic and str(item[1].get("topic") or "").strip() == concept_topic
            else 1
        )
        for _index, other in candidates[:6]:
            supporting_cards.append(
                {
                    "concept": str(other.get("concept") or "")[:80],
                    "explanation": str(other.get("explanation") or "")[:600],
                    "memory_hint": str(other.get("memory_hint") or "")[:120],
                }
            )
        card_context = {
            "note_title": str((recall_session or {}).get("title") or "")[:120],
            "note_summary": str((recall_session or {}).get("summary") or "")[:1200],
            "subject": str((recall_session or {}).get("subject") or "")[:48],
            "topic": concept_topic[:48],
            "concept": str(concept.get("concept") or "")[:80],
            "explanation": str(concept.get("explanation") or "")[:1200],
            "memory_hint": str(concept.get("memory_hint") or "")[:160],
            "relations": relations,
            "supporting_cards": supporting_cards,
        }
        conversation = "\n".join(
            f"{'學生' if entry['role'] == 'user' else '助教'}：{entry['content']}"
            for entry in history
        )
        prompt = (
            "你是研究所考試筆記的即時 AI 助教。請以提供的重點卡為主要脈絡，回答學生目前不熟悉的觀念。"
            "重點卡、同主題卡與最近對話都可能含有錯誤，只能當作問題背景，不可當成事實依據；必須以可靠學科知識重新驗證。"
            "若卡片有可明確判定的錯誤，必須指出正確內容，不可沿用錯誤"
            "回答使用好理解的繁體中文，控制在 2 至 8 個短段落且完整收尾。數學表達式一律使用 LaTeX："
            "行內公式用 \\( ... \\)，獨立公式用 \\[ ... \\]。不要輸出 Markdown 標題、粗體標記或程式碼區塊。"
            "不得提及資料庫欄位、session_id、concept_index 或 s6:c6 這類內部代碼。\n\n"
            f"重點卡資料：\n{json.dumps(card_context, ensure_ascii=False)}\n\n"
            f"最近對話：\n{conversation or '尚無'}\n\n"
            f"學生這次的問題：{question}"
        )
        def request_answer(request_prompt: str) -> Tuple[str, bool]:
            response = requests.post(
                "https://api.openai.com/v1/responses",
                headers={"Authorization": f"Bearer {openai_api_key}", "Content-Type": "application/json"},
                json={
                    "model": openai_model,
                    "store": False,
                    "input": [{"role": "user", "content": [{"type": "input_text", "text": request_prompt}]}],
                    "max_output_tokens": 3200,
                },
                timeout=90,
            )
            response.raise_for_status()
            response_payload = response.json()
            return _extract_openai_text(response_payload).strip()[:8000], response_payload.get("status") == "incomplete"

        try:
            answer, incomplete = request_answer(prompt)
            if incomplete:
                answer, incomplete = request_answer(
                    prompt
                    + "\n\n上一次回答因長度限制而不完整。請重新從頭回答，不要接續殘句；保留必要公式與條件，"
                    "將完整答案壓縮在 1000 個繁體中文字內，並以完整句子收尾。"
                )
            answer = re.sub(r"\bs\d+\s*:\s*c\d+\b", "", answer, flags=re.IGNORECASE).strip()
        except (requests.RequestException, ValueError, TypeError):
            return {"ok": False, "error": "AI 助教暫時無法回答，請稍後再試。"}, 502
        if incomplete:
            return {"ok": False, "error": "回答內容仍過長，請把問題縮小到一個觀念後再試。"}, 502
        if not answer:
            return {"ok": False, "error": "AI 助教沒有產生有效回答，請換個方式提問。"}, 502
        record_ui_event(
            "study_recall_card_question",
            meta={"session_id": session_id, "concept_index": concept_index, "history_count": len(history)},
        )
        return {"ok": True, "answer": answer}

    @app.post("/admin/study-recall/<int:session_id>/delete")
    @admin_required
    def admin_study_recall_delete(session_id: int):
        recall_session = storage.get_study_recall_session(session_id)
        if not recall_session:
            flash("找不到這份筆記紀錄。", "error")
            return redirect(url_for("admin_study_recall"))
        if not storage.delete_study_recall_session(session_id):
            flash("筆記刪除失敗，請再試一次。", "error")
            return redirect(url_for("admin_study_recall", session_id=session_id))
        upload_root = study_upload_root.resolve()
        image_directory = (upload_root / str(session_id)).resolve()
        if image_directory.parent == upload_root and image_directory.is_dir():
            try:
                shutil.rmtree(image_directory)
            except OSError:
                pass
        record_ui_event("study_recall_note_deleted", meta={"session_id": session_id, "subject": recall_session.get("subject")})
        flash("已刪除筆記、所屬重點卡、複習紀錄與原始圖片。", "success")
        return redirect(url_for("admin_study_recall"))

    @app.post("/admin/study-recall/upload")
    @admin_required
    def admin_study_recall_upload():
        user = current_user() or {}
        username = str(user.get("username") or "")
        active_job_id = _active_study_upload_job(username)
        if active_job_id:
            if _is_study_upload_request():
                return {
                    "ok": True,
                    "background": True,
                    "job_id": active_job_id,
                    "message": "已有一份筆記正在背景整理。",
                }, 202
            flash("已有一份筆記正在背景整理，可先使用其他頁面。", "info")
            return redirect(url_for("admin_study_recall"))
        study_date = (request.form.get("study_date") or _study_plan_business_date().isoformat()).strip()
        try:
            date.fromisoformat(study_date)
        except ValueError:
            return _study_upload_error("請輸入有效的筆記日期。")
        subject = (request.form.get("subject") or "").strip()
        if subject not in STUDY_PLAN_SUBJECTS:
            return _study_upload_error("請選擇科目。")
        requested_title = (request.form.get("title") or "").strip()[:120]
        incoming_files = [item for item in request.files.getlist("note_images") if item and item.filename]
        if not incoming_files or len(incoming_files) > 8:
            return _study_upload_error("請上傳 1 至 8 張筆記照片。")
        images: List[Tuple[str, bytes, str]] = []
        total_image_bytes = 0
        for item in incoming_files:
            filename = secure_filename(item.filename) or "note-image"
            extension = Path(filename).suffix.lower()
            mime_type = _NOTE_IMAGE_MIME_TYPES.get(extension)
            if not mime_type:
                return _study_upload_error("筆記僅支援 JPG、PNG、WEBP 或 GIF 圖片。")
            image_bytes = item.read()
            if not image_bytes or len(image_bytes) > STUDY_NOTE_MAX_IMAGE_BYTES:
                return _study_upload_error("每張筆記照片壓縮後必須小於 3MB。")
            total_image_bytes += len(image_bytes)
            if total_image_bytes > STUDY_NOTE_MAX_TOTAL_BYTES:
                return _study_upload_error("筆記照片壓縮後合計必須小於 24MB。")
            images.append((filename, image_bytes, mime_type))
        job_id = secrets.token_urlsafe(18)
        now = time.time()
        with study_upload_jobs_lock:
            study_upload_jobs[job_id] = {
                "username": username,
                "status": "running",
                "progress": 10,
                "message": "照片已接收，等待 AI 開始整理。",
                "created_at": now,
                "updated_at": now,
            }

        def _run_study_upload() -> None:
            recall_id: Optional[int] = None
            destination: Optional[Path] = None
            try:
                _set_study_upload_job(job_id, progress=25, message="AI 正在閱讀原始筆記並建立重點卡。")
                analysis, error = _analyze_study_note_images(images)
                if error or not analysis:
                    raise RuntimeError(error or "筆記分析失敗。")
                _set_study_upload_job(job_id, progress=62, message="重點卡已產生，正在儲存原始圖片與卡片。")
                stored_names = [
                    f"{index + 1:02d}-{secrets.token_hex(5)}{Path(name).suffix.lower()}"
                    for index, (name, _bytes, _mime) in enumerate(images)
                ]
                title = requested_title or str(analysis.get("detected_topic") or "").strip()[:120] or f"{subject}筆記"
                recall_id = storage.create_study_recall_session(
                    study_date=study_date,
                    subject=subject,
                    title=title,
                    image_filenames=stored_names,
                    summary=analysis["summary"],
                    key_concepts=analysis["key_concepts"],
                )
                destination = _ensure_private_dir(study_upload_root / str(recall_id))
                for stored_name, (_original_name, image_bytes, _mime_type) in zip(stored_names, images):
                    (destination / stored_name).write_bytes(image_bytes)
                _set_study_upload_job(job_id, progress=78, message="正在重新分析所有新舊重點卡的關聯與聯想。")
                with study_relation_rebuild_lock:
                    relation_error = _rebuild_all_study_recall_relations()
                final_message = (
                    f"重點卡已完成；{relation_error}"
                    if relation_error
                    else "重點卡已完成，所有新舊卡片的關聯與聯想也已更新。"
                )
                _set_study_upload_job(
                    job_id,
                    status="success",
                    progress=100,
                    message=final_message,
                    session_id=recall_id,
                )
                record_ui_event(
                    "study_recall_note_analyzed",
                    meta={"username": username, "session_id": recall_id, "subject": subject, "image_count": len(images)},
                )
            except Exception as exc:  # pragma: no cover - guarded by route-level integration tests
                if recall_id is not None:
                    storage.delete_study_recall_session(recall_id)
                if destination is not None and destination.is_dir():
                    try:
                        shutil.rmtree(destination)
                    except OSError:
                        pass
                message = str(exc).strip() or "筆記背景處理失敗，請稍後重試。"
                _set_study_upload_job(job_id, status="error", message=message[:240])
                record_ui_event("study_recall_note_analyzed", "error", {"username": username, "reason": message[:160]})

        threading.Thread(target=_run_study_upload, daemon=True).start()
        if _is_study_upload_request():
            return {
                "ok": True,
                "background": True,
                "job_id": job_id,
                "message": "已開始背景整理，可自由前往其他頁面。",
            }, 202
        flash("已開始背景整理筆記，可自由前往其他頁面並從右下角查看進度。", "success")
        return redirect(url_for("admin_study_recall"))

    @app.get("/admin/study-recall/upload-jobs/<job_id>")
    @admin_required
    def admin_study_recall_upload_job(job_id: str):
        user = current_user() or {}
        with study_upload_jobs_lock:
            stored_job = study_upload_jobs.get(job_id)
            job = dict(stored_job) if stored_job else None
        if not job or job.get("username") != user.get("username"):
            return {"ok": False, "error": "找不到這次筆記處理工作。"}, 404
        payload = {
            "ok": True,
            "status": job.get("status") or "running",
            "progress": int(job.get("progress") or 0),
            "message": str(job.get("message") or "正在處理筆記。"),
        }
        if job.get("status") == "success" and job.get("session_id"):
            payload["redirect_url"] = url_for("admin_study_recall", session_id=int(job["session_id"]))
        return payload

    @app.post("/admin/study-recall/<int:session_id>/rate-cards")
    @admin_required
    def admin_study_recall_rate_cards(session_id: int):
        is_async_rating = request.headers.get("X-E3-Recall-Rating") == "1"
        return_to = (request.form.get("return_to") or "").strip()
        if return_to not in {"admin_study_home", "admin_study_plan", "public_study_progress"}:
            return_to = ""

        def recall_redirect():
            return redirect(url_for(return_to) if return_to else url_for("admin_study_recall", session_id=session_id))

        def rating_error(message: str, status_code: int = 400):
            if is_async_rating:
                return {"ok": False, "error": message}, status_code
            flash(message, "error")
            return recall_redirect()

        recall_session = storage.get_study_recall_session(session_id)
        if not recall_session:
            return rating_error("找不到這份回想紀錄。", 404)
        ratings: Dict[int, int] = {}
        for index, _concept in enumerate(recall_session.get("key_concepts") or []):
            raw_rating = (request.form.get(f"rating_{index}") or "").strip()
            if not raw_rating:
                continue
            try:
                rating = int(raw_rating)
            except (TypeError, ValueError):
                rating = 0
            if rating not in {1, 2, 3, 4, 5}:
                return rating_error("印象分必須是 1 至 5 分。")
            ratings[index] = rating
        if not ratings:
            return rating_error("請至少為一張重點卡填寫印象分。")
        if storage.record_study_recall_card_ratings(
            session_id=session_id,
            ratings=ratings,
            review_date=_study_plan_business_date().isoformat(),
        ):
            next_session = storage.get_study_recall_session(session_id) or {}
            next_review_at = next_session.get("next_review_at") or "待安排"
            record_ui_event(
                "study_recall_cards_rated",
                meta={"session_id": session_id, "card_count": len(ratings), "next_review_at": next_review_at},
            )
            if is_async_rating:
                return {"ok": True, "remaining_due_count": _build_recall_widget_context()["due_count"]}
            flash(f"已記錄每張重點卡的印象分；最早的下一次複習是 {next_review_at}。", "success")
        elif is_async_rating:
            return {"ok": False, "error": "印象分暫時無法儲存，請再試一次。"}, 500
        else:
            flash("印象分暫時無法儲存，請再試一次。", "error")
        return recall_redirect()

    @app.route("/admin/study-plan", methods=["GET", "POST"])
    @admin_required
    def admin_study_plan():
        user = current_user()
        if request.method == "POST":
            action = (request.form.get("action") or "save_video").strip()
            selected_subject = (request.form.get("subject") or "").strip()
            if selected_subject not in STUDY_PLAN_SUBJECTS:
                selected_subject = STUDY_PLAN_SUBJECTS[0]
            try:
                video_id = int(request.form.get("video_id") or 0)
            except (TypeError, ValueError):
                video_id = 0
            if action == "delete_video":
                if video_id and storage.delete_study_plan_video_record(video_id):
                    record_ui_event("study_plan_video_record_deleted", meta={"video_id": video_id})
            else:
                watched_minutes = _study_plan_minutes(request.form.get("watched_minutes"))
                notes = (request.form.get("notes") or "").strip()[:2000]
                if video_id and storage.upsert_study_plan_video_record(
                    video_id=video_id,
                    watched_seconds=watched_minutes * 60,
                    notes=notes,
                ):
                    record_ui_event(
                        "study_plan_video_record_saved",
                        meta={"video_id": video_id, "watched_minutes": watched_minutes},
                    )
            return redirect(url_for("admin_study_plan", subject=selected_subject))

        videos = storage.list_study_plan_videos_with_records()
        week_rows, current_week, summary = _study_plan_week_rows(videos)
        selected_subject = (request.args.get("subject") or current_week["subject"]).strip()
        if selected_subject not in STUDY_PLAN_SUBJECTS:
            selected_subject = current_week["subject"]
        videos_by_subject: Dict[str, List[Dict[str, Any]]] = {subject: [] for subject in STUDY_PLAN_SUBJECTS}
        for video in videos:
            video["duration_minutes"] = round(float(video["duration_seconds"]) / 60, 1)
            video["watched_minutes"] = round(
                min(float(video["watched_seconds"]), float(video["duration_seconds"])) / 60,
                1,
            )
            video["completion"] = _study_plan_video_completion(video["duration_seconds"], video["watched_seconds"])
            videos_by_subject.setdefault(video["subject"], []).append(video)
        visible_videos = videos_by_subject.get(selected_subject, [])
        return render_template_string(
            STUDY_PLAN_TEMPLATE,
            admin_user=user,
            week_rows=week_rows,
            current_week=current_week,
            summary=summary,
            subjects=STUDY_PLAN_SUBJECTS,
            selected_subject=selected_subject,
            videos=visible_videos,
            video_count=len(visible_videos),
            plan_total_weeks=len(week_rows),
            plan_start=STUDY_PLAN_START,
            plan_end=STUDY_PLAN_END,
            recall_widget=_build_recall_widget_context(),
        )

    @app.post("/admin/study-plan/video-progress")
    @admin_required
    def admin_study_plan_video_progress():
        payload = request.get_json(silent=True) or {}
        try:
            video_id = int(payload.get("video_id") or 0)
            watched_seconds = float(payload.get("watched_seconds") or 0)
        except (TypeError, ValueError):
            return {"ok": False, "error": "invalid_payload"}, 400
        if video_id <= 0:
            return {"ok": False, "error": "missing_video"}, 400
        result = storage.update_study_plan_video_progress(video_id=video_id, watched_seconds=watched_seconds)
        if not result:
            return {"ok": False, "error": "video_not_found"}, 404
        videos = storage.list_study_plan_videos_with_records()
        _week_rows, current_week, summary = _study_plan_week_rows(videos)
        record_ui_event(
            "study_plan_youtube_progress_saved",
            meta={"video_id": video_id, "watched_seconds": round(float(result["watched_seconds"]), 1)},
        )
        return {
            "ok": True,
            **result,
            "summary": {
                "total_watched_hours": round(float(summary["total_watched"]) / 60, 1),
                "completion": round(float(summary["completion"]), 1),
                "completed_videos": int(summary["completed_videos"]),
                "total_videos": int(summary["total_videos"]),
            },
            "current_week": {
                "watched_minutes": round(float(current_week["watched_minutes"]), 1),
                "completion": round(float(current_week["completion"]), 1),
                "state": current_week["state"],
                "state_label": current_week["state_label"],
                "daily_recommendations": current_week["daily_recommendations"],
            },
        }

    @app.route("/admin/traffic", methods=["GET"])
    @login_required
    def admin_traffic():
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員瀏覽流量資訊。", "error")
            return redirect(url_for("index"))
        admin_view_options = list_admin_view_options()
        selected_view_username = user["username"]
        requested_view_username = (request.args.get("view_user") or "").strip()
        if requested_view_username:
            valid_usernames = {item["username"] for item in admin_view_options}
            if requested_view_username in valid_usernames:
                selected_view_username = requested_view_username

        def _fmt_ts(ts: Optional[float]) -> str:
            if not ts:
                return "-"
            try:
                dt = datetime.fromtimestamp(ts, tz=TAIPEI_TZ)
            except Exception:
                return "-"
            return dt.strftime("%Y-%m-%d %H:%M:%S")

        ACTION_LABELS = {
            "login_success": "登入成功",
            "logout": "登出",
            "guest_login": "訪客登入",
            "guest_import": "匯入訪客資料",
            "refresh_assignments": "更新作業資料",
            "ui-event": "操作事件",
        }

        def _action_description(action: str) -> str:
            action = action or "-"
            return ACTION_LABELS.get(action, action.replace("_", " "))

        user_breakdown = traffic_tracker.user_breakdown()
        ip_overview = traffic_tracker.ip_summary()
        guest_overview = traffic_tracker.guest_summary()
        formatted_users = [
            {
                "username": entry["username"],
                "count": entry["count"],
                "online": entry["online"],
                "last_seen": _fmt_ts(entry.get("last_seen")),
            }
            for entry in user_breakdown
        ]
        raw_events = traffic_tracker.recent_events(500)
        filtered_events = [
            ev
            for ev in raw_events
            if (ev.get("action") or "").lower() not in PASSIVE_TRAFFIC_ACTIONS
        ]
        formatted_events = []
        for ev in reversed(filtered_events[-200:]):
            meta = ev.get("meta") or {}
            role = "訪客" if meta.get("is_guest") else ("管理員" if meta.get("is_admin") else "一般使用者")
            detail_parts: List[str] = []
            for key in ("info", "course", "message", "target", "action_detail"):
                val = meta.get(key)
                if val:
                    detail_parts.append(f"{key}: {val}")
            extra = {
                key: value
                for key, value in meta.items()
                if key
                not in {"username", "is_guest", "is_admin", "info", "course", "message", "target", "action_detail"}
            }
            if extra:
                try:
                    detail_parts.append(json.dumps(extra, ensure_ascii=False))
                except Exception:
                    detail_parts.append(str(extra))
            formatted_events.append(
                {
                    "ts": _fmt_ts(ev.get("ts")),
                    "ip": ev.get("ip") or "-",
                    "action": ev.get("action") or "-",
                    "status": ev.get("status") or "info",
                    "username": meta.get("username") or "-",
                    "description": _action_description(ev.get("action") or "-"),
                    "details": "；".join(detail_parts),
                }
            )
        trend_window = request.args.get("trend", "hour")
        if trend_window not in {"hour", "day"}:
            trend_window = "hour"
        hourly_series = traffic_tracker.hourly_series()
        if trend_window == "day":
            daily_map: Dict[int, Set[str]] = {}
            for ts, members in traffic_tracker.hourly_buckets().items():
                day_dt = datetime.fromtimestamp(ts, tz=TAIPEI_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
                day_ts = int(day_dt.timestamp())
                day_set = daily_map.setdefault(day_ts, set())
                day_set.update(members)
            
            if daily_map:
                sorted_days_keys = sorted(daily_map.keys())
                min_day_ts = sorted_days_keys[0]
                max_day_ts = sorted_days_keys[-1]
                
                full_daily_series = []
                current_ts = min_day_ts
                while current_ts <= max_day_ts:
                    full_daily_series.append({
                        "ts": current_ts,
                        "count": len(daily_map.get(current_ts, []))
                    })
                    current_ts += 86400

                chart_labels = [datetime.fromtimestamp(item["ts"], tz=TAIPEI_TZ).strftime("%Y-%m-%d") for item in full_daily_series]
                chart_values = [item["count"] for item in full_daily_series]
            else:
                chart_labels = []
                chart_values = []
        else:
            if hourly_series:
                full_series = []
                series_dict = {item["ts"]: item["count"] for item in hourly_series}
                min_ts = hourly_series[0]["ts"]
                max_ts = hourly_series[-1]["ts"]
                
                current_ts = min_ts
                while current_ts <= max_ts:
                    full_series.append({
                        "ts": current_ts,
                        "count": series_dict.get(current_ts, 0)
                    })
                    current_ts += 3600
                
                hourly_series = full_series

            chart_labels = [datetime.fromtimestamp(item["ts"], tz=TAIPEI_TZ).strftime("%m-%d %H:00") for item in hourly_series]
            chart_values = [item["count"] for item in hourly_series]
        if not chart_labels or not chart_values:
            # fallback to on-the-fly aggregation of filtered events to avoid空白圖
            buckets: Dict[datetime, Set[str]] = {}
            for ev in filtered_events:
                ts = ev.get("ts")
                if not ts:
                    continue
                meta = ev.get("meta") or {}
                username = meta.get("username")
                if not username or meta.get("is_guest"):
                    continue
                try:
                    dt = datetime.fromtimestamp(float(ts), tz=TAIPEI_TZ)
                except Exception:
                    continue
                action = (ev.get("action") or "").lower()
                if action in PASSIVE_TRAFFIC_ACTIONS:
                    continue
                if trend_window == "day":
                    bucket = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                else:
                    bucket = dt.replace(minute=0, second=0, microsecond=0)
                bucket_set = buckets.setdefault(bucket, set())
                bucket_set.add(str(username))
            sorted_keys = sorted(buckets.keys())
            chart_labels = [
                key.strftime("%Y-%m-%d") if trend_window == "day" else key.strftime("%m-%d %H:00")
                for key in sorted_keys
            ]
            chart_values = [len(buckets[key]) for key in sorted_keys]
        action_counter: Counter = Counter()
        for ev in filtered_events:
            action_counter[ev.get("action") or "-"] += 1
        top_actions = [{"action": action, "count": count} for action, count in action_counter.most_common(5)]
        recent_unique_keys = set()
        for ev in raw_events:
            meta = ev.get("meta") or {}
            username = meta.get("username")
            if username and not meta.get("is_guest"):
                recent_unique_keys.add(username)
            elif not username and ev.get("ip"):
                recent_unique_keys.add(ev.get("ip"))
        summary = {
            "unique_users": len(formatted_users),
            "online_users": sum(1 for entry in formatted_users if entry["online"]),
            "recent_unique_users": len(recent_unique_keys),
            "last_event": _fmt_ts(filtered_events[-1].get("ts")) if filtered_events else "-",
            "last_action": (filtered_events[-1].get("action") or "-") if filtered_events else "-",
            "event_samples": len(filtered_events),
        }
        if formatted_users:
            summary["top_user"] = formatted_users[0]["username"]
            summary["top_user_count"] = formatted_users[0]["count"]
        else:
            summary["top_user"] = "-"
            summary["top_user_count"] = 0
        summary["ip_total_hits"] = ip_overview["total"]
        summary["unique_ips"] = ip_overview["unique"]
        summary["online_ips"] = ip_overview["online"]
        summary["guest_total"] = guest_overview.get("total", 0)
        summary["guest_online"] = guest_overview.get("online", 0)
        return render_template_string(
            TRAFFIC_TEMPLATE,
            stats=usage_stats(),
            stats_version=current_stats_version(),
            user_rows=formatted_users,
            events=formatted_events,
            generated_at=_fmt_ts(time.time()),
            admin_user=user,
            chart_labels=chart_labels,
            chart_values=chart_values,
            top_actions=top_actions,
            top_users=formatted_users[:5],
            summary=summary,
            trend_window=trend_window,
            trend_label="每小時" if trend_window == "hour" else "每天",
            ip_summary=ip_overview,
            admin_view_options=admin_view_options,
            selected_view_username=selected_view_username,
        )

    @app.route("/admin/traffic/reset", methods=["POST"])
    @login_required
    def admin_traffic_reset():
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員操作。", "error")
            return redirect(url_for("index"))
        traffic_tracker.reset()
        flash("已清除所有流量統計與累積訪問次數。", "success")
        record_ui_event("reset_traffic", "success")
        return redirect(url_for("admin_traffic"))

    @app.post("/admin/traffic/reset-user")
    @login_required
    def admin_traffic_reset_user():
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員操作。", "error")
            return redirect(url_for("index"))
        target = (request.form.get("username") or "").strip()
        if not target:
            flash("請提供要清除統計的帳號名稱。", "warning")
            return redirect(url_for("admin_traffic"))
        removed = traffic_tracker.remove_user_stats(target)
        deleted_events = storage.delete_traffic_events_for_user(target)
        if removed or deleted_events:
            flash(f"已清除 {target} 的統計與事件紀錄（移除 {deleted_events} 筆事件）。", "success")
            record_ui_event("reset_traffic_user", meta={"target": target, "events_removed": deleted_events})
        else:
            flash("找不到對應的統計資料，未執行變更。", "info")
        return redirect(url_for("admin_traffic"))

    @app.route("/admin/feedback", methods=["GET", "POST"])
    @login_required
    def admin_feedback():
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員操作。", "error")
            return redirect(url_for("index"))
        if request.method == "POST":
            feedback_id = request.form.get("id")
            status = request.form.get("status", "open")
            if update_feedback_status_entry(feedback_id, status):
                flash("狀態已更新。", "success")
            else:
                flash("更新失敗，請稍後再試。", "error")
            return redirect(url_for("admin_feedback"))
        feedback_items = list_feedback_entries()
        open_count = sum(1 for item in feedback_items if (item.get("status") or "open") == "open")
        return render_template_string(
            ADMIN_FEEDBACK_TEMPLATE,
            admin_user=user,
            feedback_entries=feedback_items,
            open_count=open_count,
        )

    @app.route("/admin/announcements", methods=["GET", "POST"])
    @login_required
    def admin_announcements():
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員操作。", "error")
            return redirect(url_for("index"))
        if request.method == "POST":
            title = request.form.get("title", "").strip()
            content = request.form.get("content", "").strip()
            if not title or not content:
                flash("請輸入公告標題與內容。", "error")
            else:
                add_announcement(title, content, user["username"])
                flash("公告已發布。", "success")
                return redirect(url_for("admin_announcements"))
        return render_template_string(
            ANNOUNCEMENTS_TEMPLATE,
            admin_user=user,
            announcements=load_announcements(),
        )

    @app.post("/admin/announcements/<announcement_id>/delete")
    @login_required
    def delete_announcement(announcement_id: str):
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員操作。", "error")
            return redirect(url_for("index"))
        if delete_announcement_entry(announcement_id):
            flash("公告已刪除。", "info")
        else:
            flash("找不到指定的公告。", "error")
        return redirect(url_for("admin_announcements"))

    @app.route("/feedback", methods=["GET", "POST"])
    def feedback():
        user = current_user()
        if request.method == "POST":
            message = request.form.get("message", "")
            email = request.form.get("email", "")
            name = request.form.get("name", "")
            username = user["username"] if user else name
            feedback_id = add_feedback_entry(message, email, username)
            if feedback_id:
                flash("已收到回報，感謝你的意見！", "success")
                record_ui_event("feedback_submitted", "success", {"feedback_id": feedback_id})
                return redirect(url_for("feedback"))
            flash("請輸入回報內容。", "error")
        return render_template_string(
            FEEDBACK_TEMPLATE,
            admin_user=user,
            support_email=support_email,
            stats=usage_stats(),
            stats_version=current_stats_version(),
        )

    @app.route("/", methods=["GET"])
    def index():
        user = current_user()
        if not user:
            return render_template_string(
                HOME_TEMPLATE,
                stats=usage_stats(),
                stats_version=current_stats_version(),
                google_scope=GOOGLE_CALENDAR_SCOPE,
                app_home_url=app_home_url,
                support_email=support_email,
            )
        context = _build_dashboard_context(user)
        return render_template_string(
            WEB_TEMPLATE,
            **context,
            app_home_url=app_home_url,
            support_email=support_email,
        )

    return app
