import hashlib
import time
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Tuple
from urllib.parse import quote, urlencode

import requests

from e3_tracker.platform.constants import TAIPEI_TZ

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
    updated = 0
    for item in assignments:
        if not item.get('due_ts'):
            continue
        upsert_assignment_action(item, access_token=access_token, calendar_id=calendar_id, timeout=timeout)
        updated += 1
    return updated


def compute_expiry(expires_in: int) -> float:
    return time.time() + max(expires_in - 30, 0)


def upsert_assignment_action(item, *, access_token, calendar_id, plan=None, timeout=10):
    """Update only an E3-owned event; work blocks have separate stable identities."""
    identity, body = _event_body_for(item if item.get('due_ts') else {**item, 'due_ts': plan['start_ts']})
    if plan:
        identity = 'e3-plan-' + plan['id']
        start = datetime.fromtimestamp(plan['start_ts'], TAIPEI_TZ)
        body.update(summary=f"處理作業｜{item['title']}"[:250], colorId='7',
                    start={'dateTime': start.isoformat(), 'timeZone': 'Asia/Taipei'},
                    end={'dateTime': (start+timedelta(minutes=plan['minutes'])).isoformat(), 'timeZone': 'Asia/Taipei'},
                    reminders={'useDefault': False, 'overrides': [{'method': 'popup', 'minutes': 10}]})
    body['extendedProperties'] = {'private': {'e3_uid': identity, 'category': '處理作業' if plan else '作業'}}
    base = f'{GOOGLE_CAL_BASE}/calendars/{quote(calendar_id, safe="")}/events'
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
    def checked(response):
        if response.status_code in (401, 403):
            raise GoogleUnauthorizedError('Google authorization required')
        response.raise_for_status()
        return response.json()
    listed = checked(requests.get(base, headers=headers, params={'privateExtendedProperty': f'e3_uid={identity}', 'maxResults': 250},
                                  timeout=timeout, allow_redirects=False))
    if listed.get('nextPageToken'):
        raise ValueError('Too many matching calendar events; no event was created')
    existing = [event for event in listed.get('items', []) if event.get('status') != 'cancelled'
                and event.get('extendedProperties', {}).get('private', {}).get('e3_uid') == identity]
    if existing:
        for event in existing:
            checked(requests.patch(f"{base}/{quote(event['id'], safe='')}", headers=headers,
                json={'start': body['start'], 'end': body['end']}, timeout=timeout, allow_redirects=False))
    else:
        event_id = 'e3' + hashlib.sha256(identity.encode()).hexdigest()
        response = requests.post(base, headers=headers, json={**body, 'id': event_id}, timeout=timeout, allow_redirects=False)
        if response.status_code == 409:
            event = checked(requests.get(f'{base}/{event_id}', headers=headers, timeout=timeout, allow_redirects=False))
            if event.get('extendedProperties', {}).get('private', {}).get('e3_uid') != identity:
                raise ValueError('Calendar event identity collision')
            checked(requests.patch(f'{base}/{event_id}', headers=headers, json={'start': body['start'], 'end': body['end']},
                                   timeout=timeout, allow_redirects=False))
        else:
            checked(response)
