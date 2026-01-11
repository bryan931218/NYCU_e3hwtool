import hashlib
import time
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Tuple
from urllib.parse import quote, urlencode

import requests

from ..shared.constants import TAIPEI_TZ

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_CAL_BASE = "https://www.googleapis.com/calendar/v3"
GOOGLE_CALENDAR_SCOPE = "https://www.googleapis.com/auth/calendar.events"


class GoogleUnauthorizedError(RuntimeError):
    """Raised when Google API returns 401/403 and token refresh is required."""


def build_google_authorize_url(
    client_id: str,
    redirect_uri: str,
    *,
    scope: str = GOOGLE_CALENDAR_SCOPE,
    state: str,
) -> str:
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": scope,
        "state": state,
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
    }
    return f"{GOOGLE_AUTH_URL}?{urlencode(params)}"


def exchange_code_for_google_token(
    code: str,
    *,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    timeout: int = 10,
) -> Dict[str, str]:
    payload = {
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    }
    resp = requests.post(GOOGLE_TOKEN_URL, data=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def refresh_google_token(
    refresh_token: str,
    *,
    client_id: str,
    client_secret: str,
    timeout: int = 10,
) -> Dict[str, str]:
    payload = {
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
    }
    resp = requests.post(GOOGLE_TOKEN_URL, data=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _event_id_for(item: Dict[str, str]) -> str:
    raw = f"{item.get('course_id','na')}|{item.get('title','') or ''}|{item.get('url','') or ''}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return f"e3-{digest}"


def _event_body_for(item: Dict[str, str]) -> Tuple[str, Dict[str, object]]:
    due_ts = item.get("due_ts")
    if not due_ts:
        raise ValueError("missing due timestamp")
    due_dt = datetime.fromtimestamp(due_ts, tz=TAIPEI_TZ)
    end_dt = due_dt + timedelta(hours=1)
    summary = (item.get("title") or "").strip() or "E3 Assignment"
    description_parts: List[str] = []
    if item.get("url"):
        description_parts.append(item["url"])
    description = "\n".join(description_parts).strip()
    location = item.get("course_title") or ""
    event_id = _event_id_for(item)
    body: Dict[str, object] = {
        "summary": summary[:250] or "E3 Assignment",
        "description": description,
        "start": {"dateTime": due_dt.isoformat(), "timeZone": "Asia/Taipei"},
        "end": {"dateTime": end_dt.isoformat(), "timeZone": "Asia/Taipei"},
        "reminders": {"useDefault": False, "overrides": [{"method": "popup", "minutes": 2880}]},
        "source": {"title": "NYCU E3", "url": item.get("url")},
        "location": location,
        "colorId": "4",  # Flamingo / 桃紅色
    }
    return event_id, body


def _iter_event_payloads(assignments: Iterable[Dict[str, object]]) -> Iterable[Tuple[str, Dict[str, object]]]:
    for item in assignments:
        try:
            yield _event_body_for(item)
        except ValueError:
            continue


def sync_assignments_to_google_calendar(
    assignments: Iterable[Dict[str, object]],
    *,
    access_token: str,
    calendar_id: str,
    timeout: int = 15,
) -> int:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    encoded_calendar = quote(calendar_id, safe="")
    updated = 0
    for event_id, body in _iter_event_payloads(assignments):
        payload = dict(body)
        payload["iCalUID"] = f"{event_id}@e3.hwtool"
        ext = payload.setdefault("extendedProperties", {}).setdefault("private", {})
        ext["e3_uid"] = event_id
        ext["category"] = "作業"
        url = f"{GOOGLE_CAL_BASE}/calendars/{encoded_calendar}/events/import"
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
        if resp.status_code in (401, 403):
            raise GoogleUnauthorizedError("Google API authorization required")
        if resp.status_code >= 400:
            raise RuntimeError(f"Google API error: {resp.status_code} {resp.text}")
        updated += 1
    return updated


def compute_expiry(expires_in: int) -> float:
    return time.time() + max(expires_in - 30, 0)
