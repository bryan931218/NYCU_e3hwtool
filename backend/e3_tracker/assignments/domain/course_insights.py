"""Validate AI evidence and reconcile official course schedule updates."""

import re
import unicodedata
from datetime import datetime, timedelta

from dateutil.parser import parse

from e3_tracker.platform.constants import TAIPEI_TZ
from .deadline_parser import ENGLISH, NUMERIC, ORIGINAL, RELATIVE, WEEKDAYS, _clock, _normalize, chinese_number


EXAMS = {
    'midterm': r'期中|mid[\s-]*term',
    'final': r'期末|final',
    'quiz': r'小考|quiz',
    'exam': r'考試|測驗|段考|\b(?:exam|test|assessment)\b',
}
CHANGE = re.compile(r'改期|改為|改到|改至|更改|調整|延後|延長|延至|提前|順延|\b(?:reschedul\w*|mov(?:ed|ing)|postpon\w*|extend\w*|chang(?:ed|es)|new deadline)\b', re.I)
TIME_CHANGE = re.compile(r'(?:同(?:一)?天|當天|時間|時段|鐘點|same day|(?:start|exam|due) time|time)', re.I)
UNCERTAIN = re.compile(r'暫定|可能|預計|另行公告|日期未定|尚未確定|待確認|不(?:會)?(?:延期|延後|延長|變更)|沒有(?:延期|變更)|\b(?:tentative|possibly|maybe|TBD|unchanged|not (?:be )?(?:changed|extended|postponed))\b', re.I)
CANCEL = re.compile(r'取消|cancel(?:led|ed|lation)?\b', re.I)
ASSIGNMENT = re.compile(r'作業|習題|報告|繳交|提交|deadline|homework|\bhw\b|hw\s*\d|assignment|project|report|submission|\bdue\b', re.I)
EXAM_CONTEXT = re.compile(r'考試|考核|小考|測驗|口試|期中考|期末考|mid[\s-]*term|\b(?:quiz|exam|test|assessment)\b', re.I)
HISTORICAL = re.compile(r'歷屆|考古題|範例|去年|\b(?:past exam|sample schedule|last year|previous semester)\b', re.I)


def normalized(value):
    return re.sub(r'[\W_]+', '', unicodedata.normalize('NFKC', str(value or '')).casefold())


def compact_text(value, limit=20000):
    raw = str(value or '')[:limit * 2]
    raw = re.split(r'(?im)^\s*(?:-{2,}\s*(?:original|forwarded) message|轉寄的郵件|原始郵件)', raw)[0]
    raw = '\n'.join(line for line in raw.splitlines() if not line.lstrip().startswith('>'))
    raw = re.sub(r'https?://\S+|[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}|\b\d{9}\b', '[已隱藏]', raw)
    raw = re.sub(r'(?i)\b(?:moodlesession|sesskey|wstoken|api[_-]?key|access[_-]?token)\s*[:=]\s*[^\s;]+',
                 '[已隱藏]', raw)
    return ' '.join(raw.split())[:limit]


def _term_year(semester, month):
    match = re.fullmatch(r'(\d{3})-(1|2|summer|X)', semester)
    if not match:
        return None
    year, term = int(match[1]) + 1911, match[2]
    if term == '1':
        return year if month >= 8 else year + 1 if month == 1 else None
    if term == '2':
        return year + 1 if 1 <= month <= 7 else None
    return year + 1 if month in (7, 8) else None


def _literal_date(phrase, source, semester):
    """Require a literal numeric/named date or a relative date anchored to publication."""
    value = _normalize(phrase).strip()
    if not value or len(value) > 100:
        return None
    numeric, english, relative = NUMERIC.match(value), ENGLISH.match(value), RELATIVE.match(value)
    try:
        if numeric:
            month, day = int(numeric['a']), int(numeric['b'])
            year_text = numeric['year'] or numeric['endyear']
            if not year_text and month > 12:
                month, day = day, month
            if not year_text and '月' not in numeric[0] and max(month, day) <= 12 and month != day:
                return None
            year = int(year_text) if year_text else _term_year(semester, month)
            if year is None:
                return None
            if year < 1911:
                year += 1911
            date, end = datetime(year, month, day), numeric.end()
        elif english:
            date = parse(english[0], default=datetime(2000, 1, 1))
            if not re.search(r'\b20\d{2}\b', english[0]):
                year = _term_year(semester, date.month)
                if year is None:
                    return None
                date = date.replace(year=year)
            end = english.end()
        elif relative and source['kind'] in {'announcements', 'mail'} and source.get('published'):
            ref = datetime.fromtimestamp(source['published'], TAIPEI_TZ).replace(tzinfo=None)
            if relative['relative']:
                delta = {'今天': 0, '明天': 1, '後天': 2, 'today': 0, 'tomorrow': 1,
                         'day after tomorrow': 2}[relative['relative'].lower()]
            else:
                label = relative['day']
                weekday = (WEEKDAYS.index(relative['enday'].lower()) if not label else
                           int(label) - 1 if label.isdigit() else '一二三四五六日'.index(label.replace('天', '日')))
                next_week = relative['week'] == '下' or (relative['enweek'] or '').lower() == 'next'
                delta = weekday - ref.weekday() + (7 if next_week else 0)
            date, end = ref + timedelta(days=delta), relative.end()
        else:
            return None
        result, timed, warnings, consumed = _clock(value[end:], date)
        remainder = value[end+consumed:].strip()
        if warnings or (remainder and not re.fullmatch(r'\((?:週|星期)?[一二三四五六日天A-Za-z]{1,10}\)', remainder)):
            return None
        if timed:
            return result.isoformat()
        return date.date().isoformat()
    except (ValueError, OverflowError):
        return None


def _text_field(item, key, limit):
    return str(item.get(key) or '').strip()[:limit] if isinstance(item.get(key), str) else ''


def _clock_change(phrase, date):
    if not date:
        return None
    try:
        day = datetime.fromisoformat(date[:10])
        clock = _normalize(phrase).strip()
        value, timed, warnings, consumed = _clock(clock, day)
        return value.isoformat() if timed and not warnings and consumed == len(clock) else None
    except ValueError:
        return None


def _evidence(item, sources):
    sid = _text_field(item, 'source', 40)
    source = sources.get(sid)
    quote = ' '.join(_text_field(item, 'quote', 650).split())
    if not source or len(quote) < 5 or quote not in source['text']:
        return None, ''
    return source, quote


def _exam_key(title, kind, evidence):
    if not EXAM_CONTEXT.search(evidence):
        return ''
    number = re.search(r'(?:第\s*([一二三四五六七八九十\d]+)\s*次|(?:quiz|exam|midterm|test)\s*#?\s*(\d+)|(?:考試|小考|測驗|期中考|期末考)\s*#?\s*(\d+))', title, re.I)
    if number:
        value = next(group for group in number.groups() if group)
        if not value.isdigit():
            value = chinese_number(value)
        if not re.search(rf'(?:quiz|exam|midterm|test|考試|小考|測驗|期中考|期末考)\s*#?\s*0*{int(value)}(?![\d/.-])|第\s*{re.escape(next(group for group in number.groups() if group))}\s*次', evidence, re.I):
            return ''
        return f'{kind}:{int(value)}'
    if kind in {'midterm', 'final'} and re.search(EXAMS[kind], evidence, re.I):
        return kind
    if kind == 'quiz' and re.search(EXAMS['quiz'], evidence, re.I):
        if normalized(title) in {'小考', 'quiz', '課堂小考'}:
            return 'quiz'
    key = normalized(title)
    return f'{kind}:{key[:90]}' if len(key) >= 2 and key in normalized(evidence) else ''


def _assignment(assignment_refs, ref, evidence):
    selected = assignment_refs.get(ref) if isinstance(ref, str) else None
    if not selected:
        return None
    mentioned = normalized(evidence)
    wanted = normalized(selected['title'])
    matches = [item for item in assignment_refs.values()
               if normalized(item['title']) and normalized(item['title']) in mentioned]
    if len(wanted) >= 2 and len(matches) == 1 and matches[0] == selected:
        return selected
    initials = re.match(r'^(?:hw|homework|assignment|作業)\s*#?\s*(\d+)(?![A-Za-z0-9])', selected['title'], re.I)
    if len(matches) > 1 or not initials:
        return None
    number = initials[1]
    pattern = rf'(?:\bhw|\bhomework|\bassignment|作業)\s*#?\s*{number}(?![A-Za-z0-9])'
    same_number = [item for item in assignment_refs.values() if re.search(pattern, item['title'], re.I)]
    return selected if re.search(pattern, evidence, re.I) and same_number == [selected] else None


def _origin(source, quote):
    return {'title': source['title'], 'url': source['url'], 'location': source.get('location', ''),
            'kind': source['kind'], 'quote': quote}


def reconcile_analysis(raw, sources, prior_events, assignment_refs, semester):
    lookup = {source['id']: source for source in sources}
    events = dict(prior_events)
    grading = []
    for item in raw.get('grading', [])[:16]:
        source, quote = _evidence(item, lookup)
        label = _text_field(item, 'label', 110)
        detail = _text_field(item, 'detail', 350)
        percentages = r'(\d+(?:\.\d+)?)\s*[%％]'
        if source and label and detail and set(re.findall(percentages, detail)) <= set(re.findall(percentages, quote)):
            grading.append({'label': label, 'detail': detail, 'origin': _origin(source, quote)})
    candidates = {}
    for item in raw.get('events', [])[:60]:
        source, quote = _evidence(item, lookup)
        title = _text_field(item, 'title', 150)
        kind = _text_field(item, 'kind', 16)
        phrase = _text_field(item, 'date_phrase', 100)
        if not source or not title:
            continue
        context = f"{source['title']}\n{quote}"
        if UNCERTAIN.search(context) or HISTORICAL.search(context):
            continue
        is_message = source['kind'] in {'announcements', 'mail'}
        if kind == 'deadline':
            assignment = _assignment(assignment_refs, item.get('assignment'), context)
            if not assignment or not ASSIGNMENT.search(context):
                continue
            key, title = f"deadline:{assignment['ref']}", assignment['title']
        elif kind in EXAMS:
            key = _exam_key(title, kind, context)
            if not key:
                continue
        else:
            continue
        cancelled = (item.get('cancelled') is True and is_message and CANCEL.search(context)
                     and not re.search(r'取消(?:延期|延後|延長)|cancel(?:led|ed)? (?:the )?(?:extension|rescheduling)', context, re.I))
        clock_only = ''
        if not cancelled:
            position = quote.find(phrase) if phrase else -1
            if position < 0 or ORIGINAL.search(quote[:position]):
                continue
            date = _literal_date(phrase, source, semester)
            if not date:
                surrounding_text = _normalize(quote[:position] + quote[position+len(phrase):])
                if (is_message and CHANGE.search(context) and TIME_CHANGE.search(context)
                        and not re.search(r'下[週月]|next (?:week|month)', context, re.I)
                        and not any(pattern.search(surrounding_text) for pattern in (NUMERIC, ENGLISH, RELATIVE))
                        and _clock_change(phrase, '2000-01-01')):
                    clock_only = phrase
                else:
                    continue
        else:
            date = ''
        timestamp = source.get('version', 0)
        if is_message and not timestamp:
            continue
        rank = [2 if is_message and (cancelled or CHANGE.search(context)) else 1 if is_message else 0, timestamp]
        event = {'id': key, 'kind': kind, 'title': title, 'date': date, 'cancelled': bool(cancelled),
                 'rank': rank, 'source_key': source['key'], 'origin': _origin(source, quote), 'clock_only': clock_only}
        candidates.setdefault(key, []).append(event)
    for key, values in candidates.items():
        old = events.get(key)
        for rank in sorted({tuple(event['rank']) for event in values}):
            if old and rank < tuple(old['rank']):
                continue
            matches = []
            for event in values:
                if tuple(event['rank']) != rank:
                    continue
                candidate = {**event}
                clock = candidate.pop('clock_only')
                if clock:
                    candidate['date'] = _clock_change(clock, old.get('date') if old else None)
                    if not candidate['date']:
                        continue
                matches.append(candidate)
            if not matches or len({(event['date'], event['cancelled']) for event in matches}) != 1:
                continue
            winner = matches[0]
            if old and rank == tuple(old['rank']) and (winner['date'], winner['cancelled']) != (old['date'], old['cancelled']):
                continue
            if len(events) < 160 or old:
                events[key] = old = winner
    return {'summary': _text_field(raw, 'summary', 850), 'grading': grading, 'events': events}
