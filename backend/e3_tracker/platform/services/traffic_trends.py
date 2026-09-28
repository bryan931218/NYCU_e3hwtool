"""Bounded, Taipei-time views of distinct active-account traffic buckets."""

from datetime import date, datetime, time, timedelta

from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.platform.services.traffic import PASSIVE_TRAFFIC_ACTIONS


def build_traffic_trend(hourly_series, hourly_buckets, events, params, *, now=None):
    now = (now or datetime.now(TAIPEI_TZ)).astimezone(TAIPEI_TZ)
    today = now.astimezone(TAIPEI_TZ).date()
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    hourly_counts = {int(item["ts"]): int(item["count"]) for item in hourly_series}
    daily_members = {}
    for timestamp, members in hourly_buckets.items():
        day = datetime.fromtimestamp(timestamp, tz=TAIPEI_TZ).date()
        daily_members.setdefault(day, set()).update(members)
        hourly_counts.setdefault(int(timestamp), len(members))

    # Legacy state without account buckets can still use the retained events.
    fallback_hours = {}
    fallback_days = {}
    if not hourly_counts or not daily_members:
        for event in events:
            meta = event.get("meta") or {}
            username = meta.get("username")
            if not username or meta.get("is_guest"):
                continue
            if str(event.get("action") or "").lower() in PASSIVE_TRAFFIC_ACTIONS:
                continue
            try:
                moment = datetime.fromtimestamp(float(event["ts"]), tz=TAIPEI_TZ)
            except (KeyError, TypeError, ValueError, OverflowError, OSError):
                continue
            hour = int(moment.replace(minute=0, second=0, microsecond=0).timestamp())
            fallback_hours.setdefault(hour, set()).add(str(username))
            fallback_days.setdefault(moment.date(), set()).add(str(username))
    if not hourly_counts:
        hourly_counts = {timestamp: len(names) for timestamp, names in fallback_hours.items()}
    if not daily_members:
        daily_members = fallback_days

    history_days = [datetime.fromtimestamp(ts, tz=TAIPEI_TZ).date() for ts in hourly_counts]
    history_days.extend(daily_members)
    first_day = min(history_days, default=today)
    selection = params.get("range", "7d")
    if selection not in {"today", "7d", "30d", "all", "custom"}:
        selection = "7d"
    end = today
    start = {"today": today, "7d": today - timedelta(days=6),
             "30d": today - timedelta(days=29), "all": first_day}.get(selection)
    notice = ""
    if selection == "custom":
        try:
            start = date.fromisoformat(params.get("start", ""))
            end = date.fromisoformat(params.get("end", ""))
            if start > end or end > today or (end - start).days > 3650:
                raise ValueError
        except (TypeError, ValueError):
            selection = "7d"
            start, end = today - timedelta(days=6), today
            notice = "請選擇有效的日期區間，結束日期不可晚於今天，最多可查看 10 年。已恢復近 7 天。"

    resolution = params.get("trend")
    if resolution not in {"hour", "day"}:
        resolution = "hour" if selection == "today" else "day"
    days = (end - start).days + 1
    if resolution == "hour" and days > 31:
        resolution = "day"
        notice = "超過 31 天的區間以每天顯示；縮小日期區間即可查看每小時資料。"

    labels, values, rows = [], [], []
    cursor = TAIPEI_TZ.localize(datetime.combine(start, time.min))
    last = TAIPEI_TZ.localize(datetime.combine(end, time.min))
    if resolution == "hour":
        last = min(last + timedelta(hours=23), current_hour)
    step = timedelta(hours=1) if resolution == "hour" else timedelta(days=1)
    while cursor <= last:
        count = (hourly_counts.get(int(cursor.timestamp()), 0) if resolution == "hour"
                 else len(daily_members.get(cursor.date(), set())))
        label = cursor.strftime("%Y-%m-%d %H:00" if resolution == "hour" else "%Y-%m-%d")
        labels.append(label)
        values.append(count)
        rows.append({"label": label, "count": count})
        cursor += step

    query = {"range": selection}
    if selection == "custom":
        query.update(start=start.isoformat(), end=end.isoformat())
    previous_end = start - timedelta(days=1)
    next_start = end + timedelta(days=1)
    peak = max(values, default=0)
    return {
        "range": selection, "resolution": resolution, "query": query,
        "start": start.isoformat(), "end": end.isoformat(), "today": today.isoformat(),
        "labels": labels, "values": values, "rows": rows, "notice": notice,
        "has_data": any(values), "has_history": bool(history_days),
        "unit": "小時" if resolution == "hour" else "天",
        "peak": peak, "peak_label": labels[values.index(peak)] if peak else "—",
        "active_periods": sum(value > 0 for value in values),
        "previous": ({"start": (start - timedelta(days=days)).isoformat(),
                      "end": previous_end.isoformat()}
                     if selection != "all" and previous_end >= first_day else None),
        "next": ({"start": next_start.isoformat(),
                  "end": min(end + timedelta(days=days), today).isoformat()}
                 if selection != "all" and next_start <= today else None),
    }
