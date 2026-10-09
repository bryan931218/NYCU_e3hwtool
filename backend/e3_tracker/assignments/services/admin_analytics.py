"""Deduplicated adoption and student-code distributions for authorized administrators."""

import json
from collections import Counter
from datetime import datetime, timedelta
from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.assignments.domain.usage import FEATURE_LABELS, student_identity


def analytics_window(params, *, now=None):
    today = (now or datetime.now(TAIPEI_TZ)).astimezone(TAIPEI_TZ).date()
    value = params.get("usage_range", "30d")
    if value not in {"7d", "30d", "90d", "all"}:
        value = "30d"
    days = {"7d": 7, "30d": 30, "90d": 90, "all": 730}[value]
    return {"range": value, "start": (today - timedelta(days=days - 1)).isoformat(), "end": today.isoformat()}


def build_assignment_analytics(snapshot, window):
    accounts = snapshot["accounts"]
    identities = {row["id"]: student_identity(row) or f"account:{row['id']}" for row in accounts}
    students = {student_identity(row) for row in accounts} - {""}
    total = len(set(identities.values()))
    linked = {key: {identities[uid] for uid in ids if uid in identities} for key, ids in snapshot["linked"].items()}
    enabled = {"line": set(), "browser": set(), "new_assignment": set(), "due_reminder": set()}
    for row in snapshot["settings"]:
        identity = identities.get(row["user_id"])
        try:
            prefs = json.loads(row["preferences"])
        except (ValueError, TypeError):
            continue
        if not identity or not isinstance(prefs, dict):
            continue
        channels = {key for key in ("line", "browser") if prefs.get(f"{key}_enabled") is True and row["user_id"] in snapshot["linked"][key]}
        for channel in channels:
            enabled[channel].add(identity)
        if channels and prefs.get("new_assignment") is True:
            enabled["new_assignment"].add(identity)
        if channels and prefs.get("due_reminder") is True:
            enabled["due_reminder"].add(identity)
    feature_users = {key: set() for key in FEATURE_LABELS}
    feature_counts = Counter()
    for row in snapshot["usage"]:
        identity = identities.get(row["user_id"])
        if identity and row["feature"] in FEATURE_LABELS:
            feature_users[row["feature"]].add(identity)
            feature_counts[row["feature"]] += row["count"]
    features = [{"key": key, "label": label, "users": len(feature_users[key]), "count": feature_counts[key],
                 "percent": round(len(feature_users[key]) * 100 / total, 1) if total else 0}
                for key, label in FEATURE_LABELS.items()]
    def distribution(slice_):
        counts = Counter(number[slice_] for number in students)
        return [{"code": code, "count": count, "percent": round(count * 100 / len(students), 1)}
                for code, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))]
    return {
        **window, "total": total, "identified": len(students), "unidentified": total - len(students),
        "line": len(linked["line"]), "browser": len(linked["browser"]), "google": len(linked["google"]),
        "enabled": {key: len(value) for key, value in enabled.items()},
        "features": features, "cohorts": distribution(slice(0, 3)), "departments": distribution(slice(3, 6)),
        "started_at": datetime.fromtimestamp(snapshot["state"]["started_at"], TAIPEI_TZ).strftime("%Y-%m-%d %H:%M"),
        "legacy_samples": snapshot["state"]["legacy_samples"],
    }


def build_account_engagement(snapshot, memberships, window, *, now=None):
    """First-party adoption and fully observed seven-calendar-day new-user cohorts."""
    today = (now or datetime.now(TAIPEI_TZ)).astimezone(TAIPEI_TZ).date()
    identities = {row["id"]: student_identity(row) or f"account:{row['id']}" for row in snapshot["accounts"]}
    member_rows = {}
    for account in snapshot["accounts"]:
        member = memberships.get(account["username"])
        key = identities[account["id"]]
        if member and (key not in member_rows or member["joined_at"] < member_rows[key]["joined_at"]):
            member_rows[key] = member
    days = {}
    for row in snapshot.get("daily", []):
        key = identities.get(row["user_id"])
        if key:
            days.setdefault(key, set()).add(datetime.fromisoformat(row["day"]).date())
    interval_days = {key: {day for day in values if window["start"] <= day.isoformat() <= window["end"]}
                     for key, values in days.items()}
    tracking_day = datetime.fromtimestamp(snapshot["state"]["started_at"], TAIPEI_TZ).date()
    eligible, retained, pending = set(), set(), set()
    for key, member in member_rows.items():
        joined = datetime.fromtimestamp(member["joined_at"], TAIPEI_TZ).date()
        if (not member["is_new"] or member["joined_at"] < snapshot["state"]["started_at"]
                or not max(window["start"], tracking_day.isoformat()) <= joined.isoformat() <= window["end"]):
            continue
        if joined + timedelta(days=7) >= today:
            pending.add(key)
            continue
        eligible.add(key)
        if any(1 <= (day - joined).days <= 7 for day in days.get(key, set())):
            retained.add(key)
    adoption = build_assignment_analytics(snapshot, window)
    total = adoption["total"]
    return {
        "active": sum(bool(value) for value in interval_days.values()),
        "repeat": sum(len(value) > 1 for value in interval_days.values()),
        "eligible": len(eligible), "retained": len(retained), "pending": len(pending),
        "retention": round(100 * len(retained) / len(eligible), 1) if eligible else None,
        "line_steps": [{"label": label, "count": count,
                        "percent": round(count * 100 / total, 1) if total else 0}
                       for label, count in [("已建立帳號", total), ("目前綁定 LINE", adoption["line"]),
                                            ("LINE 通知已啟用", adoption["enabled"]["line"])]]}
