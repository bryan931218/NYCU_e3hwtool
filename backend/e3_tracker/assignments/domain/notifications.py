"""Validate settings and build assignment events without transport side effects."""

import base64
import hashlib
from datetime import datetime
from urllib.parse import urlsplit

from cryptography.hazmat.primitives.asymmetric import ec
from e3_tracker.platform.constants import TAIPEI_TZ

DEFAULT_NOTIFICATION_PREFERENCES = {
    "new_assignment": False,
    "assignment_graded": False,
    "new_announcement": False,
    "new_mail": False,
    "deadline_changes": False,
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
    for key in ("new_assignment", "assignment_graded", "new_announcement", "new_mail", "deadline_changes", "due_reminder", "browser_enabled", "line_enabled"):
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


def published_grade_text(item):
    grade = item.get("grade_text")
    text = " ".join(str(grade if grade is not None else "").split())
    return text if text.casefold() not in {"", "-", "—", "n/a", "not graded", "ungraded", "尚未評分", "未評分"} else None


def is_assignment_graded(item):
    return bool(published_grade_text(item) or item.get("feedback_text")) or str(
        item.get("raw_status_text") or item.get("raw_status") or ""
    ).strip().casefold() in {"graded", "已評分"}


def semester_assignments(result, semester_key, uid_for, ignored=()):
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
        if uid in ignored:
            continue
        items[digest(uid)] = item
    return items


def active_assignments(result, semester_key, uid_for, ignored=()):
    return {
        key: item for key, item in semester_assignments(result, semester_key, uid_for, ignored).items()
        if not item.get("completed") and not is_assignment_graded(item)
    }


def notification_payload(item, kind, days=None, *, include_grading_details=False):
    title = {"new": "E3｜新作業", "graded": "E3｜作業已評分"}.get(kind, "E3｜作業到期提醒")
    body = f"課程：{notification_text(item.get('course_title'), 100) or '未分類'}\n作業：{notification_text(item.get('title'), 160)}"
    if kind == "graded":
        grade = published_grade_text(item)
        feedback = notification_text(item.get("feedback_text"), 800)
        if include_grading_details and (grade or feedback):
            if grade:
                body += f"\n分數：{notification_text(grade, 120)}"
            if feedback:
                body += f"\n評語：{feedback}"
        else:
            body += "\n評分已公布，請至 E3 查看成績與回饋。"
    elif item.get("due_ts"):
        body += f"\n截止：{notification_time(item['due_ts'])}"
    else:
        body += "\n截止：未設定"
    return {"title": title, "body": body, "url": "/"}


def notification_text(value, limit):
    value = ' '.join(str(value or '').split())
    return value if len(value) <= limit else value[:limit - 1] + '…'


def notification_time(timestamp):
    date = datetime.fromtimestamp(int(timestamp), TAIPEI_TZ)
    weekday = '一二三四五六日'[date.weekday()]
    return f"{date:%m/%d}（{weekday}）{date:%H:%M}"


def course_message_notification_payload(item, kind):
    noun = '信件' if kind == 'mail' else '公告'
    body = f"課程：{notification_text(item.get('course_title'), 100) or '未分類'}\n標題：{notification_text(item.get('title'), 160)}"
    if item.get('updated_ts'):
        body += f"\n時間：{notification_time(item['updated_ts'])}"
    return {'title': f'E3｜新課程{noun}', 'body': body}
