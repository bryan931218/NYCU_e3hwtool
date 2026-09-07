"""Traffic counters and persistent visitor statistics."""

import json
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from ..shared.constants import TAIPEI_TZ

PASSIVE_TRAFFIC_ACTIONS = {"heartbeat", "refresh_assignments"}


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
