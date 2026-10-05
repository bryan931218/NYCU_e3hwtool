"""Conservative deadline suggestions and personal, non-destructive date overlays."""

import copy
import re
from datetime import datetime, timedelta
from urllib.parse import urlsplit

from e3_tracker.platform.constants import TAIPEI_TZ
from .notifications import digest


def assignment_hash(item, uid_for):
    return digest(uid_for(item.get('course_id'), str(item.get('title') or ''), item.get('url')))


def effective_result(result, overrides, uid_for, *, now=None):
    result = copy.deepcopy(result or {})
    now = datetime.now(TAIPEI_TZ).timestamp() if now is None else now
    def apply(item):
        value = overrides.get(assignment_hash(item, uid_for))
        if not value:
            return
        if not item.get('personal_due'):
            item['original_due_ts'] = item.get('due_ts')
            item['original_due_at'] = item.get('due_at')
        item['due_ts'] = value['due_ts']
        item['due_at'] = datetime.fromtimestamp(value['due_ts'], TAIPEI_TZ).strftime('%Y-%m-%d %H:%M')
        item['personal_due'] = True
        item['overdue'] = value['due_ts'] < now and not item.get('completed')
    for item in result.get('all_assignments', []):
        apply(item)
    for course in result.get('courses', []):
        for item in course.get('assignments', []):
            apply(item)
    return result


def source_version(item):
    return digest(f"{item.get('title')}|{item.get('updated_ts')}|{item.get('content', '')}")


def deadline_dates(item, *, now=None):
    """Only suggest explicit future dates near deadline/change language; never apply them."""
    now = datetime.now(TAIPEI_TZ) if now is None else datetime.fromtimestamp(now, TAIPEI_TZ)
    reference = datetime.fromtimestamp(item.get('updated_ts') or now.timestamp(), TAIPEI_TZ)
    text = f"{item.get('title', '')}\n{item.get('content', '')}"[:20000]
    change = re.compile(r'截止|期限|繳交|延期|延後|延長|延至|改為|改至|改到|deadline|due\b|extend|postpon', re.I)
    date = re.compile(r'(?:(20\d{2}|1\d{2})\s*[/年.-]\s*)?(\d{1,2})\s*[/月.-]\s*(\d{1,2})\s*(?:日)?(?:\s*[（(][^\n）)]{0,10}[）)])?(?:\s*(\d{1,2}):(\d{2}))?')
    suggestions = []
    for match in date.finditer(text):
        excerpt = text[max(0, match.start()-100):min(len(text), match.end()+60)]
        if not change.search(excerpt):
            continue
        before = text[max(0, match.start()-30):match.start()]
        if re.search(r'(?:原(?:訂|本|先)?|從|由|original(?:ly)?|from)\s*(?:截止|期限)?\s*[:：]?\s*$', before, re.I):
            continue
        year = int(match[1] or reference.year)
        if year < 1911:
            year += 1911
        try:
            value = TAIPEI_TZ.localize(datetime(year, int(match[2]), int(match[3]), int(match[4] or 23), int(match[5] or 59)))
            if not match[1] and reference.month == 12 and value.month == 1:
                value = value.replace(year=year+1)
        except ValueError:
            continue
        if now < value <= now + timedelta(days=370) and value.timestamp() not in {entry['due_ts'] for entry in suggestions}:
            suggestions.append({'due_ts': int(value.timestamp()), 'evidence': excerpt.strip(), 'time_explicit': bool(match[4])})
    return suggestions[-3:]


def match_assignment(message, candidates):
    text = re.sub(r'\s+', '', f"{message.get('title', '')} {message.get('content', '')}").lower()
    matches = []
    numbers = set(re.findall(r'(?:hw|homework|assignment|作業)\s*#?\s*0*(\d+)(?!\d)', text, re.I))
    for item in candidates:
        title = re.sub(r'\s+', '', item.get('title', '')).lower()
        ids = set(re.findall(r'(?:hw|homework|assignment|作業)\s*#?\s*0*(\d+)(?!\d)', title, re.I))
        if (len(title) >= 3 and title in text) or numbers.intersection(ids):
            matches.append(item)
    return matches[0] if len(matches) == 1 else None


def tonight_time(now, due):
    date = datetime.fromtimestamp(now, TAIPEI_TZ)
    tonight = date.replace(hour=20, minute=0, second=0, microsecond=0)
    if tonight.timestamp() <= now:
        tonight += timedelta(days=1)
    target = int(tonight.timestamp())
    if due and target >= due:
        target = int(now + 3600)
    if due and target >= due:
        raise ValueError('作業即將截止，請直接開啟作業。')
    return target


def safe_assignment_url(url):
    try:
        parsed = urlsplit(url or '')
        if (parsed.scheme == 'https' and parsed.hostname in {'e3.nycu.edu.tw', 'e3p.nycu.edu.tw'}
                and parsed.port in (None, 443) and not parsed.username and not parsed.password):
            return url
    except ValueError:
        pass
    return ''
