"""Study Discord presence formatting and signed tokens."""

import hashlib
import hmac
import re
import secrets
from datetime import datetime, timezone
from typing import Any, Dict

DISCORD_PRESENCE_IDLE_SECONDS = 5 * 60 + 30

DISCORD_APPLICATION_ID_PATTERN = re.compile(r"^[0-9]{17,20}$")

DISCORD_TOKEN_NONCE_PATTERN = re.compile(r"^[A-Za-z0-9_-]{12,32}$")


def issue_discord_presence_token(application_id: str, signing_secret: Any) -> str:
    normalized_application_id = str(application_id or "").strip()
    if not DISCORD_APPLICATION_ID_PATTERN.fullmatch(normalized_application_id):
        raise ValueError("Invalid Discord Application ID")
    nonce = secrets.token_urlsafe(12)
    message = f"e3-discord-presence:{normalized_application_id}:{nonce}".encode("utf-8")
    key = (
        signing_secret
        if isinstance(signing_secret, bytes)
        else str(signing_secret or "").encode("utf-8")
    )
    signature = hmac.new(key, message, hashlib.sha256).hexdigest()
    return f"dp1.{normalized_application_id}.{nonce}.{signature}"


def verify_discord_presence_signed_token(token: str, signing_secret: Any) -> str:
    parts = str(token or "").strip().split(".")
    if len(parts) != 4 or parts[0] != "dp1":
        return ""
    _version, application_id, nonce, signature = parts
    if not DISCORD_APPLICATION_ID_PATTERN.fullmatch(application_id):
        return ""
    if not DISCORD_TOKEN_NONCE_PATTERN.fullmatch(nonce) or not re.fullmatch(
        r"[0-9a-f]{64}", signature
    ):
        return ""
    message = f"e3-discord-presence:{application_id}:{nonce}".encode("utf-8")
    key = (
        signing_secret
        if isinstance(signing_secret, bytes)
        else str(signing_secret or "").encode("utf-8")
    )
    expected = hmac.new(key, message, hashlib.sha256).hexdigest()
    return application_id if hmac.compare_digest(expected, signature) else ""


def format_discord_study_duration(seconds: Any) -> str:
    total_minutes = max(0, int(float(seconds or 0) // 60))
    hours, minutes = divmod(total_minutes, 60)
    if hours and minutes:
        return f"{hours} 小時 {minutes} 分"
    if hours:
        return f"{hours} 小時"
    return f"{minutes} 分"


def _discord_utc_datetime(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_discord_presence_payload(
    storage: Any,
    *,
    now: datetime | None = None,
    public_url: str = "",
    cover_image_url: str = "",
) -> Dict[str, Any]:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    else:
        current = current.astimezone(timezone.utc)
    day = storage._study_plan_business_day_from_timestamp(current.isoformat())
    summary = storage.get_study_time_summary(day=day)
    sessions = storage.list_study_time_sessions(day=day, limit=100)
    active_session = None
    for candidate in sessions:
        if bool(candidate.get("completed")):
            continue
        updated_at = _discord_utc_datetime(candidate.get("updated_at"))
        if updated_at is None:
            continue
        age_seconds = max(0.0, (current - updated_at).total_seconds())
        if age_seconds <= DISCORD_PRESENCE_IDLE_SECONDS:
            active_session = candidate
            break

    total_seconds = max(0.0, float(summary.get("total_seconds") or 0))
    payload: Dict[str, Any] = {
        "ok": True,
        "day": day,
        "active": bool(active_session) or total_seconds > 0,
        "studying_now": bool(active_session),
        "today_total_seconds": round(total_seconds, 1),
        "today_video_seconds": round(
            max(0.0, float(summary.get("video_seconds") or 0)), 1
        ),
        "today_practice_seconds": round(
            max(0.0, float(summary.get("practice_seconds") or 0)), 1
        ),
        "today_label": format_discord_study_duration(total_seconds),
        "public_url": str(public_url or "").strip(),
        "cover_image_url": str(cover_image_url or "").strip(),
        "checked_at": current.isoformat(),
    }
    if active_session:
        kind = str(active_session.get("kind") or "")
        subject = str(active_session.get("subject") or "").strip()
        label = str(active_session.get("label") or "").strip()
        started_at = _discord_utc_datetime(active_session.get("started_at"))
        details = (
            f"正在讀{subject}"
            if kind == "video" and subject
            else ("正在看課程影片" if kind == "video" else "正在刷題")
        )
        payload.update(
            {
                "kind": kind,
                "subject": subject,
                "label": label,
                "details": details[:128],
                "state": f"今日實際學習 {format_discord_study_duration(total_seconds)}"[
                    :128
                ],
                "session_started_at": (
                    int(started_at.timestamp()) if started_at else None
                ),
                "session_updated_at": str(active_session.get("updated_at") or ""),
            }
        )
    elif total_seconds > 0:
        payload.update(
            {
                "kind": "daily_summary",
                "subject": "",
                "label": "",
                "details": "今日學習紀錄",
                "state": f"實際學習 {format_discord_study_duration(total_seconds)}"[
                    :128
                ],
                "session_started_at": None,
                "session_updated_at": "",
            }
        )
    return payload
