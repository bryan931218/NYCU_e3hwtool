import base64
import json
import os
import re
import secrets
import threading
import time
import hashlib
from collections import Counter
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urlsplit, urlunsplit
import tempfile

import requests
from flask import Flask, Response, flash, redirect, render_template_string, request, send_file, session, url_for, has_request_context
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from werkzeug.http import http_date

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
        data_root = Path(tempfile.gettempdir()) / "e3_tracker_cache"
    _ensure_private_dir(data_root)
    database_url = env_defaults.get("database_url") or ""
    if database_url:
        db_location = database_url
    else:
        db_location = str((data_root / "e3_tracker.sqlite3").resolve())
    storage = PersistentStorage(db_location)

    app = Flask(__name__)
    app.secret_key = env_defaults["web_secret"]
    session_cookie_secure = _env_flag_truthy(env_defaults.get("session_cookie_secure"))
    session_cookie_samesite = env_defaults.get("session_cookie_samesite") or "Lax"
    app.config.update(
        PERMANENT_SESSION_LIFETIME=timedelta(days=1),
        SESSION_COOKIE_SECURE=session_cookie_secure,
        SESSION_COOKIE_SAMESITE=session_cookie_samesite,
        SESSION_COOKIE_HTTPONLY=True,
        PREFERRED_URL_SCHEME="https",
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

    DEFAULT_PREFERENCES = {
        "view_mode": "due",
        "show_overdue": False,
        "show_completed": False,
    }
    def load_cache_from_disk(username: str) -> Optional[Dict[str, Any]]:
        return storage.load_user_cache(username)

    def save_cache_to_disk(username: str, payload: Dict[str, Any]) -> None:
        storage.save_user_cache(username, payload)

    def current_user() -> Optional[Dict[str, Any]]:
        if "username" in session:
            return {
                "username": session["username"],
                "moodle_session": session.get("moodle_session"),
                "is_guest": bool(session.get("is_guest")),
                "is_admin": bool(session.get("is_admin")),
            }
        return None

    def login_required(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not current_user():
                return redirect(url_for("login"))
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
        for key, alias in (("show_overdue", "showOverdue"), ("show_completed", "showCompleted")):
            value = raw.get(key)
            if value is None and alias:
                value = raw.get(alias)
            coerced = _coerce_bool(value)
            if coerced is not None:
                clean[key] = coerced
        return clean

    def get_user_preferences() -> Dict[str, Any]:
        prefs = dict(DEFAULT_PREFERENCES)
        user = current_user()
        if not user:
            return prefs
        stored = storage.load_user_preferences(user["username"])
        prefs.update(_sanitize_preferences(stored))
        return prefs

    def update_user_preferences(partial: Dict[str, Any]) -> Dict[str, Any]:
        prefs = get_user_preferences()
        sanitized = _sanitize_preferences(partial)
        prefs.update(sanitized)
        user = current_user()
        if not user:
            return prefs
        storage.save_user_preferences(user["username"], prefs)
        return prefs

    def get_assign_cache() -> Optional[Dict[str, Any]]:
        user = current_user()
        if not user:
            return None
        return load_cache_from_disk(user["username"])

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
        return {
            "id": ident,
            "title": title,
            "content": content,
            "created_at": created_at,
            "created_label": created_label,
            "author": author,
        }

    def load_announcements() -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for raw in storage.list_announcements(ANNOUNCEMENT_LIMIT):
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
                        session["username"] = session_label
                        session["moodle_session"] = raw_session
                        session["is_guest"] = False
                        session["is_admin"] = bool(admin_user_id and session_label == admin_user_id)
                        session.permanent = True
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
                        session["username"] = raw_username
                        session["moodle_session"] = cookie_val
                        session["is_guest"] = False
                        session["is_admin"] = bool(admin_user_id and raw_username == admin_user_id)
                        session.permanent = True
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
        cache = get_assign_cache() or {}
        preferences = get_user_preferences()
        payload = {
            "ok": True,
            "cache": cache,
            "ts": cache.get("ts"),
            "has_result": bool(cache.get("result")) if cache else False,
            "preferences": preferences,
        }
        return payload

    @app.post("/preferences")
    @login_required
    def save_preferences():
        payload = request.get_json(silent=True) or {}
        updated = update_user_preferences(payload)
        return {"ok": True, "preferences": updated}

    @app.route("/guest-login", methods=["POST"])
    def guest_login():
        session.clear()
        guest_name = f"訪客_{secrets.token_hex(3)}"
        session["username"] = guest_name
        session["is_guest"] = True
        session["is_admin"] = False
        session.permanent = False
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
            if user.get("is_guest"):
                cache = get_assign_cache()
                if not cache:
                    raise RuntimeError("找不到訪客匯入的作業資料，請重新匯入後再試。")
                result = cache.get("result") or {}
                excel_data = cache.get("excel_data")
            else:
                result, excel_data = fetch_assignments_for({"username": user["username"], "moodle_session": session.get("moodle_session")})
                set_assign_cache(result, excel_data)
            assignments = [
                item
                for item in result.get("all_assignments", [])
                if _assignment_uid(item) in selected_uids
            ]
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
        was_guest = bool(session.get("is_guest"))
        if old_user:
            if was_guest:
                storage.delete_user_cache(old_user)
            clear_google_tokens(old_user)
        if old_user:
            record_ui_event("logout", meta={"username": old_user})
        session.clear()
        session.modified = True
        flash("已登出。", "success")
        resp = redirect(url_for("index"))
        session_cookie_name = app.config.get("SESSION_COOKIE_NAME", "session")
        resp.delete_cookie(
            session_cookie_name,
            path=app.config.get("SESSION_COOKIE_PATH", "/"),
            domain=app.config.get("SESSION_COOKIE_DOMAIN"),
        )
        return resp

    @app.post("/api/assignments")
    @login_required
    def api_assignments():
        user = current_user()
        if user and user.get("is_guest"):
            return {"ok": False, "error": "訪客模式不支援自動更新"}, 400
        username = user["username"]
        moodle_session_val = session.get("moodle_session")
        prev_cache = get_assign_cache() or {}
        prev_ts = prev_cache.get("ts") or 0

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

    @app.route("/admin/traffic", methods=["GET"])
    @login_required
    def admin_traffic():
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員瀏覽流量資訊。", "error")
            return redirect(url_for("index"))

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
        cache = get_assign_cache()
        result = cache.get("result") if cache else None
        excel_data = cache.get("excel_data") if cache else None
        guest_mode = bool(user and user.get("is_guest"))
        if result and not excel_data:
            excel_data = _generate_excel_data(result.get("all_assignments"))
            if excel_data:
                set_assign_cache(result, excel_data)
        if not result and user and not guest_mode:
            flash("正在載入資料，請稍候...", "info")
        google_linked = bool(user and load_google_tokens(user["username"]))
        stats = usage_stats()
        stats_version_value = current_stats_version()
        announcements_list = load_announcements()
        cache_ts_val = cache.get("ts") if cache else None
        last_updated_label = None
        if cache_ts_val:
            try:
                last_updated_label = datetime.fromtimestamp(int(cache_ts_val), TAIPEI_TZ).strftime("%Y-%m-%d %H:%M")
            except Exception:
                last_updated_label = None
        return render_template_string(
            WEB_TEMPLATE,
            result=result,
            excel_data=excel_data,
            user=user,
            google_ready=_google_ready(),
            google_linked=google_linked,
            guest_mode=guest_mode,
            stats=stats,
            stats_version=stats_version_value,
            now_ts=int(datetime.now(TAIPEI_TZ).timestamp()),
            preferences=get_user_preferences(),
            cache_ts=cache_ts_val,
            last_updated_ts=cache_ts_val,
            last_updated_label=last_updated_label,
            announcements=announcements_list,
            announcement_version=(announcements_list[0]["id"] if announcements_list else None),
            app_home_url=app_home_url,
            support_email=support_email,
        )

    return app
