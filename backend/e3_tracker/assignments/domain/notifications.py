"""Validate settings and build assignment events without transport side effects."""

import base64
import hashlib
from datetime import datetime
from urllib.parse import urlsplit

from cryptography.hazmat.primitives.asymmetric import ec
from e3_tracker.platform.constants import TAIPEI_TZ

DEFAULT_NOTIFICATION_PREFERENCES = {
    "new_assignment": False,
    "due_reminder": False,
    "days_before": [1],
    "browser_enabled": False,
    "line_enabled": False,
}


def digest(value):
    return hashlib.sha256(str(value).encode()).hexdigest()


def validate_preferences(raw):
    if not isinstance(raw, dict):
        raise ValueError("設定格式不正確")
    result = dict(DEFAULT_NOTIFICATION_PREFERENCES)
    for key in ("new_assignment", "due_reminder", "browser_enabled", "line_enabled"):
        value = raw.get(key, result[key])
        if not isinstance(value, bool):
            raise ValueError("通知開關格式不正確")
        result[key] = value
    days = raw.get("days_before", [1])
    if (
        not isinstance(days, list)
        or not 1 <= len(days) <= 5
        or any(type(day) is not int or not 1 <= day <= 30 for day in days)
    ):
        raise ValueError("請設定 1 至 30 天，最多 5 個提醒時間")
    result["days_before"] = sorted(set(days), reverse=True)
    return result


def validate_subscription(raw):
    if not isinstance(raw, dict):
        raise ValueError("推播訂閱格式不正確")
    endpoint = str(raw.get("endpoint") or "")
    parsed = urlsplit(endpoint)
    host = parsed.hostname or ""
    # Only browser push providers are allowed; never fetch arbitrary client URLs.
    allowed = (
        host
        in {
            "fcm.googleapis.com",
            "updates.push.services.mozilla.com",
            "web.push.apple.com",
        }
        or host.endswith(".notify.windows.com")
        or host.endswith(".push.apple.com")
    )
    if (
        not allowed
        or parsed.scheme != "https"
        or parsed.port not in (None, 443)
        or parsed.username
        or parsed.password
        or parsed.fragment
        or len(endpoint) > 2048
    ):
        raise ValueError("不支援此推播服務網址")
    keys = raw.get("keys")
    if not isinstance(keys, dict):
        raise ValueError("推播金鑰格式不正確")
    clean_keys = {}
    for key, size in (("p256dh", 65), ("auth", 16)):
        value = keys.get(key)
        if not isinstance(value, str) or len(value) > 100:
            raise ValueError("推播金鑰格式不正確")
        try:
            decoded = base64.b64decode(
                value + "=" * (-len(value) % 4), altchars=b"-_", validate=True
            )
            if len(decoded) != size:
                raise ValueError()
            if key == "p256dh":
                ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), decoded)
        except (ValueError, TypeError):
            raise ValueError("推播金鑰格式不正確") from None
        clean_keys[key] = value
    return {"endpoint": endpoint, "keys": clean_keys}


def active_assignments(result, semester_key, uid_for, ignored=()):
    course_keys = {
        str(c.get("id")): c.get("semester_key") for c in result.get("courses", [])
    }
    items = {}
    for item in result.get("all_assignments", []):
        if not isinstance(item, dict):
            continue
        key = item.get("semester_key") or course_keys.get(str(item.get("course_id")))
        if key != semester_key:
            continue
        uid = uid_for(
            item.get("course_id"), str(item.get("title") or ""), item.get("url")
        )
        if uid in ignored or item.get("completed") or item.get("grade_text"):
            continue
        items[digest(uid)] = item
    return items


def notification_payload(item, kind, days=None):
    title = "新作業" if kind == "new" else f"作業到期提醒 · {days} 天前"
    body = f"{str(item.get('course_title') or '')[:160]}\n{str(item.get('title') or '')[:160]}".strip()
    if item.get("due_ts"):
        due = datetime.fromtimestamp(int(item["due_ts"]), TAIPEI_TZ).strftime(
            "%m/%d %H:%M"
        )
        body += f"\n截止：{due}"
    return {"title": title, "body": body, "url": "/"}
