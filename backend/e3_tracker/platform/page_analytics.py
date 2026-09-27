"""First-party page analytics for the public E3 site.

This module intentionally stores only coarse request metadata:
- path without query string
- referrer hostname (not the full referrer URL)
- coarse device/browser family
- a keyed visitor hash (or account identifier key) instead of raw IP

It is separate from the existing traffic event stream so page-view history is
not truncated by the shorter operational-event retention window.
"""

from __future__ import annotations

import hashlib
import hmac
import math
import time
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

from flask import redirect, render_template, request, session, url_for
from sqlalchemy import (
    Column,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    delete,
    func,
    select,
)

from e3_tracker.platform.constants import TAIPEI_TZ


RETENTION_DAYS = 90
MAX_ROWS = 50000
DEFAULT_WINDOW_DAYS = 30
ALLOWED_WINDOWS = {1, 7, 30, 90}

_metadata = MetaData()
page_analytics_table = Table(
    "page_analytics",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ts", Float, nullable=False),
    Column("path", String(512), nullable=False),
    Column("source", String(255), nullable=False, default="direct"),
    Column("device", String(32), nullable=False, default="Unknown"),
    Column("browser", String(32), nullable=False, default="Unknown"),
    Column("visitor_key", String(96), nullable=False),
    Index("ix_page_analytics_ts", "ts"),
    Index("ix_page_analytics_path", "path"),
    Index("ix_page_analytics_visitor", "visitor_key"),
)


def _current_user(storage) -> Optional[Dict[str, Any]]:
    token = session.get("session_token")
    if not token:
        return None
    try:
        return storage.load_web_session(token)
    except Exception:
        return None


def _visitor_key(app, storage) -> str:
    user = _current_user(storage)
    if user and not user.get("is_guest"):
        username = str(user.get("username") or "").strip()
        if username:
            return f"user:{username}"

    ip = str(request.remote_addr or "unknown")
    secret = app.secret_key
    if isinstance(secret, str):
        secret_bytes = secret.encode("utf-8", errors="ignore")
    elif isinstance(secret, bytes):
        secret_bytes = secret
    else:
        secret_bytes = str(secret or "e3-analytics").encode("utf-8")
    digest = hmac.new(secret_bytes, ip.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"anon:{digest[:32]}"


def _browser_family(user_agent: str) -> str:
    ua = (user_agent or "").lower()
    if any(token in ua for token in ("bot", "crawler", "spider", "slurp")):
        return "Bot"
    if "edg/" in ua or "edge/" in ua:
        return "Edge"
    if "opr/" in ua or "opera" in ua:
        return "Opera"
    if "firefox/" in ua or "fxios/" in ua:
        return "Firefox"
    if "chrome/" in ua or "crios/" in ua:
        return "Chrome"
    if "safari/" in ua:
        return "Safari"
    return "Other"


def _device_family(user_agent: str) -> str:
    ua = (user_agent or "").lower()
    if any(token in ua for token in ("bot", "crawler", "spider", "slurp")):
        return "Bot"
    if "ipad" in ua or "tablet" in ua or ("android" in ua and "mobile" not in ua):
        return "Tablet"
    if any(token in ua for token in ("iphone", "ipod", "android", "mobile")):
        return "Mobile"
    if ua:
        return "Desktop"
    return "Unknown"


def _source_label() -> str:
    raw = str(request.referrer or "").strip()
    if not raw:
        return "直接開啟 / 書籤"
    try:
        parsed = urlparse(raw)
        hostname = (parsed.hostname or "").lower().strip(".")
    except Exception:
        return "其他來源"
    if not hostname:
        return "其他來源"
    current_host = (request.host.split(":", 1)[0] or "").lower().strip(".")
    if hostname == current_host or hostname.endswith(f".{current_host}"):
        return "站內導覽"
    if hostname.startswith("www."):
        hostname = hostname[4:]
    return hostname[:255]


def _should_record(response) -> bool:
    if request.method not in {"GET", "HEAD"}:
        return False
    if response.status_code >= 400:
        return False
    if (response.mimetype or "").lower() != "text/html":
        return False
    path = request.path or "/"
    if path.startswith(("/assets/", "/static/", "/admin/")):
        return False
    if path in {"/healthz", "/favicon.ico", "/traffic-stats"}:
        return False
    return True


def _ensure_table(storage) -> None:
    _metadata.create_all(storage._engine, tables=[page_analytics_table], checkfirst=True)


def _trim(storage, now: float) -> None:
    cutoff = now - RETENTION_DAYS * 86400
    with storage._lock, storage._engine.begin() as conn:
        conn.execute(delete(page_analytics_table).where(page_analytics_table.c.ts < cutoff))
        total = int(conn.execute(select(func.count()).select_from(page_analytics_table)).scalar_one())
        if total > MAX_ROWS:
            excess = total - MAX_ROWS
            old_ids = (
                conn.execute(
                    select(page_analytics_table.c.id)
                    .order_by(page_analytics_table.c.id.asc())
                    .limit(excess)
                )
                .scalars()
                .all()
            )
            if old_ids:
                conn.execute(
                    delete(page_analytics_table).where(page_analytics_table.c.id.in_(old_ids))
                )


def _record_pageview(app, storage) -> None:
    user_agent = request.headers.get("User-Agent", "")
    browser = _browser_family(user_agent)
    device = _device_family(user_agent)
    now = time.time()
    row = {
        "ts": now,
        "path": (request.path or "/")[:512],
        "source": _source_label(),
        "device": device,
        "browser": browser,
        "visitor_key": _visitor_key(app, storage),
    }
    with storage._lock, storage._engine.begin() as conn:
        conn.execute(page_analytics_table.insert().values(**row))

    if int(now) % 97 == 0:
        _trim(storage, now)


def _window_days() -> int:
    try:
        value = int(request.args.get("days", DEFAULT_WINDOW_DAYS))
    except (TypeError, ValueError):
        value = DEFAULT_WINDOW_DAYS
    return value if value in ALLOWED_WINDOWS else DEFAULT_WINDOW_DAYS


def _fmt_ts(ts: float) -> str:
    try:
        return datetime.fromtimestamp(float(ts), tz=TAIPEI_TZ).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "-"


def _friendly_path(path: str) -> str:
    labels = {
        "/": "首頁 / 作業總覽",
        "/login": "登入頁",
        "/guest-login": "訪客登入",
        "/privacy": "隱私權政策",
        "/terms": "服務條款",
        "/feedback": "意見回饋",
        "/study": "讀書專區",
        "/study/": "讀書專區",
    }
    return labels.get(path, path)


def _rows(storage, since: float) -> List[Dict[str, Any]]:
    with storage._lock, storage._engine.connect() as conn:
        records = conn.execute(
            select(
                page_analytics_table.c.ts,
                page_analytics_table.c.path,
                page_analytics_table.c.source,
                page_analytics_table.c.device,
                page_analytics_table.c.browser,
                page_analytics_table.c.visitor_key,
            )
            .where(page_analytics_table.c.ts >= since)
            .order_by(page_analytics_table.c.ts.asc())
        ).fetchall()
    return [dict(row._mapping) for row in records]


def _counter_rows(counter: Counter, limit: int = 8) -> List[Dict[str, Any]]:
    return [{"label": str(label), "count": int(count)} for label, count in counter.most_common(limit)]


def _daily_series(rows: Iterable[Dict[str, Any]], days: int) -> List[Dict[str, Any]]:
    now = datetime.now(TAIPEI_TZ)
    start = (now - timedelta(days=days - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
    counts: Counter = Counter()
    visitors: Dict[str, set] = {}
    for row in rows:
        dt = datetime.fromtimestamp(float(row["ts"]), tz=TAIPEI_TZ)
        key = dt.strftime("%Y-%m-%d")
        counts[key] += 1
        visitors.setdefault(key, set()).add(row["visitor_key"])

    series: List[Dict[str, Any]] = []
    max_count = 1
    for offset in range(days):
        day = start + timedelta(days=offset)
        key = day.strftime("%Y-%m-%d")
        count = int(counts.get(key, 0))
        max_count = max(max_count, count)
        series.append(
            {
                "date": key,
                "label": day.strftime("%m/%d"),
                "count": count,
                "visitors": len(visitors.get(key, set())),
            }
        )
    for item in series:
        item["width"] = max(2, int(math.ceil(item["count"] / max_count * 100))) if item["count"] else 0
    return series


def register_page_analytics(app) -> None:
    """Register first-party page tracking and the admin analytics dashboard."""

    storage = app.extensions.get("e3_storage")
    if storage is None:
        raise RuntimeError("E3 storage must be configured before page analytics")
    _ensure_table(storage)

    @app.after_request
    def capture_page_view(response):
        try:
            if _should_record(response):
                _record_pageview(app, storage)
        except Exception:
            pass
        return response

    @app.get("/admin/analytics")
    def admin_page_analytics():
        user = _current_user(storage)
        if not user:
            return redirect(url_for("login"))
        if not user.get("is_admin"):
            return redirect(url_for("index"))

        days = _window_days()
        now = time.time()
        since = now - days * 86400
        rows = _rows(storage, since)
        human_rows = [row for row in rows if row.get("device") != "Bot"]
        bot_views = len(rows) - len(human_rows)

        page_counter = Counter(row["path"] for row in human_rows)
        source_counter = Counter(row["source"] for row in human_rows)
        device_counter = Counter(row["device"] for row in human_rows)
        browser_counter = Counter(row["browser"] for row in human_rows)
        visitor_count = len({row["visitor_key"] for row in human_rows})

        top_pages = [
            {
                "label": _friendly_path(path),
                "path": path,
                "count": count,
            }
            for path, count in page_counter.most_common(10)
        ]
        top_sources = _counter_rows(source_counter, 10)
        devices = _counter_rows(device_counter, 8)
        browsers = _counter_rows(browser_counter, 8)
        series = _daily_series(human_rows, days)

        recent = [
            {
                "ts": _fmt_ts(row["ts"]),
                "path": _friendly_path(row["path"]),
                "source": row["source"],
                "device": row["device"],
                "browser": row["browser"],
            }
            for row in reversed(human_rows[-100:])
        ]

        total_views = len(human_rows)
        avg_views = round(total_views / visitor_count, 1) if visitor_count else 0
        top_page = top_pages[0] if top_pages else None
        top_source = top_sources[0] if top_sources else None

        return render_template(
            "shared/admin_page_analytics.html",
            admin_user=user,
            days=days,
            windows=sorted(ALLOWED_WINDOWS),
            total_views=total_views,
            unique_visitors=visitor_count,
            avg_views=avg_views,
            bot_views=bot_views,
            top_page=top_page,
            top_source=top_source,
            top_pages=top_pages,
            top_sources=top_sources,
            devices=devices,
            browsers=browsers,
            series=series,
            recent=recent,
            generated_at=_fmt_ts(now),
            retention_days=RETENTION_DAYS,
        )
