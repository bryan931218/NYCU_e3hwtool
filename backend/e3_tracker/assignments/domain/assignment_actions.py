"""Conservative deadline suggestions and personal, non-destructive date overlays."""

import copy
import re
import unicodedata
from datetime import datetime, timedelta
from urllib.parse import urlsplit

from e3_tracker.platform.constants import TAIPEI_TZ
from .notifications import digest
from .deadline_parser import deadline_dates, chinese_number


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


def match_assignment(message, candidates):
    raw = unicodedata.normalize('NFKC', f"{message.get('title', '')} {message.get('content', '')}"[:20000]).lower()
    text = re.sub(r'\s+', '', raw)
    matches = []
    def numbers(value):
        found = re.findall(r'(?:hw|homework|assignment|problem\s+set|lab|作業|作业|實驗)\s*(?:#|第)?\s*([\d一二兩三四五六七八九十]+)', value, re.I)
        found += re.findall(r'第([\d一二兩三四五六七八九十]+)(?:份|次)?(?:作業|作业)', value)
        normalized = set()
        for word in found:
            if word.isdigit():
                normalized.add(str(int(word)))
            elif not any(character.isdigit() for character in word):
                normalized.add(str(int(chinese_number(word))))
        return normalized
    identifiers = numbers(raw)
    if re.search(r'(?:hw|homework|assignment|作業|作业)\s*#?\s*\d+\s*[-~～至]\s*\d+', raw, re.I):
        return None
    for item in candidates:
        raw_title = unicodedata.normalize('NFKC', item.get('title', '')).lower()
        title = re.sub(r'\s+', '', raw_title)
        ids = numbers(raw_title)
        if (not ids and len(title) >= 3 and title in text) or identifiers.intersection(ids):
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
