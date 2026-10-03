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
