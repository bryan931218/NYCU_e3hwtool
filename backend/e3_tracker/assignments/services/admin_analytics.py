"""Deduplicated adoption and student-code distributions for authorized administrators."""

import json
from collections import Counter
from datetime import datetime, timedelta
from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.assignments.domain.usage import FEATURE_LABELS, student_identity
from e3_tracker.assignments.domain.student_directory import academic_year, build_student_directory


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
    directory = build_student_directory(accounts)
    grades, departments = {}, {}
    for number in sorted(students):
        info = directory[number]
        for grouped, key, name in ((grades, info['grade_key'], info['grade'].replace('（推估）', '')),
                                   (departments, (info['department_code'], info['department']), info['department'])):
            row = grouped.setdefault(key, {'code':info['department_code'] if grouped is departments else info['grade_key'],
                                           'name':name, 'count':0, 'inferred_count':0, 'e3_count':0})
            row['count'] += 1
            source = info['department_source'] if grouped is departments else info['grade_source']
            row['inferred_count'] += source == 'student_number'
            row['e3_count'] += source == 'e3'
    def named_rows(grouped):
        return [{**row, 'percent':round(row['count'] * 100 / len(students), 1)}
                for row in sorted(grouped.values(), key=lambda item:(-item['count'], item['code'], item['name']))]
    return {
        **window, "total": total, "identified": len(students), "unidentified": total - len(students),
        "line": len(linked["line"]), "browser": len(linked["browser"]), "google": len(linked["google"]),
        "enabled": {key: len(value) for key, value in enabled.items()},
        "features": features, "cohorts": distribution(slice(0, 3)), "departments": named_rows(departments),
        "grades": named_rows(grades), "academic_year": academic_year(),
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
    # Union identities across days; summing daily active counts would overcount return visits.
    daily_identities = {}
    for key, values in interval_days.items():
        for day in values:
            daily_identities.setdefault(day, set()).add(key)
    chart_rows, seen = [], set()
    cursor = datetime.fromisoformat(window['start']).date()
    end = min(datetime.fromisoformat(window['end']).date(), today)
    while cursor <= end:
        seen.update(daily_identities.get(cursor, set()))
        chart_rows.append({'day': cursor.isoformat(), 'count': len(seen)})
        cursor += timedelta(days=1)
    ceiling = max(4, ((len(seen) + 3) // 4) * 4)
    for index, row in enumerate(chart_rows):
        row.update(x=round(8 + 704 * index / max(1, len(chart_rows) - 1), 2),
                   y=round(192 - 176 * row['count'] / ceiling, 2))
    chart = {'rows': chart_rows, 'ceiling': ceiling,
             'points': ' '.join(f"{row['x']},{row['y']}" for row in chart_rows),
             'ticks': [{'value': ceiling * i // 4, 'y': 192 - 44 * i} for i in range(5)]}
    return {
        'chart': chart,
        "active": sum(bool(value) for value in interval_days.values()),
        "repeat": sum(len(value) > 1 for value in interval_days.values()),
        "eligible": len(eligible), "retained": len(retained), "pending": len(pending),
        "retention": round(100 * len(retained) / len(eligible), 1) if eligible else None,
        "line_steps": [{"label": label, "count": count,
                        "percent": round(count * 100 / total, 1) if total else 0}
                       for label, count in [("已建立帳號", total), ("目前綁定 LINE", adoption["line"]),
                                            ("LINE 通知已啟用", adoption["enabled"]["line"])]]}
