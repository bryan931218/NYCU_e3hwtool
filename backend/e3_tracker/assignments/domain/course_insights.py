"""Validate AI evidence and reconcile official course schedule updates."""

import re
import unicodedata
from datetime import datetime, timedelta
from decimal import Decimal

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
EXAM_CONTEXT = re.compile(r'考試|考核|小考|測驗|口試|筆試|實作考|實機考|期中考|期末考|mid[\s-]*term|\b(?:quiz|exam|test|assessment)\b', re.I)
HISTORICAL = re.compile(r'歷屆|考古題|範例|去年|\b(?:past exam|sample schedule|last year|previous semester)\b', re.I)
GRADE_AMOUNT = re.compile(r'(?<![\d.,])([+-]?\d{1,4}(?:[.,]\d{1,2})?)\s*(%|percent\b|分(?!\s*[鐘別])|points?\b|pts?\b)', re.I)
CHINESE_PERCENT = re.compile(r'百分之(\d{1,3}|[零〇○一二兩三四五六七八九十]{1,3})')
EXAM_VARIANTS = {
    'group': r'(?:分組|小組|團體|團隊|group|team).{0,12}(?:實作|實機|上機|practical|lab)',
    'individual': r'(?:個人|個別|individual).{0,12}(?:實作|實機|上機|practical|lab)',
    'written': r'筆試|\bwritten\b',
    'practical': r'上機|實機|實作(?:考|測驗)|\bpractical\b',
}
TENTATIVE_GRADING = re.compile(r'暫定|僅供參考|可能調整|\b(?:tentative|provisional|subject to change)\b', re.I)
RUBRIC_HEADING = re.compile(r'(?:分數配比|成績配比|總成績(?:配分|計算方式)|(?:final|overall) grade breakdown)\s*[:：]', re.I)
FINAL_PROJECT = re.compile(r'期末\s*(?:專題|專案)|\bfinal\s+project\b', re.I)
PROJECT_DEADLINE = re.compile(r'截止|期限|最晚|due|deadline', re.I)
EXAM_SCHEDULE_LINE = re.compile(
    r'(?P<label>(?:(?:團體|分組|小組|個人|個別)\s*)?(?:期末\s*)?'
    r'(?:(?:上機|實機|實作)(?:考試|考|測驗)|筆試))(?:日期|時間)\s*[:：]\s*')


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
                options = [(month, day), (day, month)]
                weekday = re.match(r'\s*\((?:週|星期)?([一二三四五六日天])\)', value[numeric.end():])
                if weekday:
                    day_index = '一二三四五六日'.index(weekday[1].replace('天', '日'))
                    options = [(m, d) for m, d in options if (y := _term_year(semester, m))
                               and datetime(y, m, d).weekday() == day_index]
                else:
                    options = [(m, d) for m, d in options if _term_year(semester, m)]
                if len(options) > 1:
                    formats = {int(match['a']) > int(match['b']) for match in NUMERIC.finditer(_normalize(source['text']))
                               if not (match['year'] or match['endyear']) and '月' not in match[0]
                               and max(int(match['a']), int(match['b'])) > 12
                               and min(int(match['a']), int(match['b'])) <= 12}
                    if len(formats) == 1:
                        options = [options[int(next(iter(formats)))]]
                if len(options) != 1:
                    return None
                month, day = options[0]
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
    variant = next((name for name, pattern in EXAM_VARIANTS.items()
                    if re.search(pattern, title, re.I) and re.search(pattern, evidence, re.I)), '')
    if not EXAM_CONTEXT.search(evidence) and not variant:
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
        return f'{kind}:{variant}' if variant else kind
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


def _schedule_rows(sources):
    for source in sources:
        if source['kind'] != 'announcements' or not source.get('version'):
            continue
        if not re.search(EXAMS['final'], source['title'], re.I):
            continue
        text = source['text']
        if UNCERTAIN.search(text) or HISTORICAL.search(source['title']):
            continue
        for match in EXAM_SCHEDULE_LINE.finditer(text):
            date = NUMERIC.match(text, match.end()) or ENGLISH.match(text, match.end())
            if date:
                yield {'kind': 'final', 'title': f"期末 {match['label'].removeprefix('期末').strip()}",
                       'source': source['id'], 'quote': text[match.start():date.end()], 'date_phrase': date[0]}


def _origin(source, quote):
    return {'title': source['title'], 'url': source['url'], 'location': source.get('location', ''),
            'kind': source['kind'], 'quote': quote}


def _grade_amounts(value):
    text = unicodedata.normalize('NFKC', value)
    amounts = set()
    for match in GRADE_AMOUNT.finditer(text):
        number = format(Decimal(match[1].replace(',', '.')).normalize(), 'f')
        unit = '%' if match[2].lower() in {'%', 'percent'} else ' 分'
        amounts.add(f'{number}{unit}')
    for match in CHINESE_PERCENT.finditer(text):
        amounts.add((match[1] if match[1].isdigit() else chinese_number(match[1])) + '%')
    return amounts


def grading_fields(item, evidence):
    detail = _text_field(item, 'detail', 350)
    proposal = _text_field(item, 'weight', 60)
    detail_amounts = _grade_amounts(detail)
    evidence_amounts = _grade_amounts(evidence)
    if not detail_amounts <= evidence_amounts:
        detail = ''
    proposed_amounts = _grade_amounts(proposal) if proposal else detail_amounts
    weight = next(iter(proposed_amounts)) if len(proposed_amounts) == 1 and proposed_amounts <= evidence_amounts else ''
    if weight and not CHANGE.search(evidence):
        same_unit = [amount for amount in evidence_amounts if amount.endswith(weight[-1])]
        if len(same_unit) > 1 and weight not in _bound_weights(item.get('label', ''), evidence):
            weight = ''
    if weight and (_grade_amounts(detail) == {weight} and
                   (GRADE_AMOUNT.fullmatch(unicodedata.normalize('NFKC', detail)) or CHINESE_PERCENT.fullmatch(detail))):
        detail = ''
    if weight and not _bound_weights(item.get('label', ''), evidence):
        if re.search(r'滿分|最高|最多|門檻|\b(?:maximum|out of|up to)\b', evidence, re.I):
            weight = ''
    return {'weight': weight, 'detail': detail}


def _bound_weights(label, evidence):
    """Accept the number attached to this label in a multi-item source."""
    label = normalized(label)
    if not label:
        return set()
    value = unicodedata.normalize('NFKC', evidence)
    previous = 0
    amounts = set()
    for match in GRADE_AMOUNT.finditer(value):
        prefix = normalized(value[previous:match.start()])
        if prefix.endswith(label) or prefix.endswith(label + '佔') or prefix.endswith(label + '占'):
            amounts.update(_grade_amounts(match[0]))
        previous = match.end()
    return amounts


def grading_key(label):
    for kind in ('midterm', 'final'):
        if re.search(EXAMS[kind], label, re.I):
            exam = _exam_key(label, kind, label)
            if exam.startswith(kind + ':'):
                return exam
    return normalized(label)


def _published_rubric(source):
    text = source['text']
    if source['kind'] == 'syllabus' and source.get('topic') == 'grading':
        if TENTATIVE_GRADING.search(text):
            return []
        lines = source.get('lines') or [text]
    elif source['kind'] in {'announcements', 'mail'} and source.get('version'):
        heading = RUBRIC_HEADING.search(text)
        if not heading or TENTATIVE_GRADING.search(text[max(0, heading.start() - 30):heading.end() + 30]):
            return []
        lines = re.split(r'[。；;]', text[heading.end():], maxsplit=1)[:1]
    else:
        return []
    rows = []
    for line in lines:
        value = unicodedata.normalize('NFKC', line)
        split_exam = re.fullmatch(r'(期末考?|final)\s*:\s*(\d{1,3})\s*%\s*\((筆試)\)\s*\+\s*(\d{1,3})\s*%\s*\((上機|實機|實作)\)', value, re.I)
        if split_exam:
            for weight, variant in ((split_exam[2], split_exam[3]), (split_exam[4], split_exam[5])):
                rows.append({'label': f'期末{variant}', 'weight': f'{weight}%', 'detail': '',
                             'origin': _origin(source, line)})
            continue
        previous = 0
        for match in GRADE_AMOUNT.finditer(value):
            start = previous
            label = value[previous:match.start()].strip(' ：:,，、.·\t').removesuffix('(').rstrip()
            label = re.sub(r'^\s*\d+[.)、]\s*', '', label)
            previous = match.end()
            amounts = _grade_amounts(match[0])
            weight = next(iter(amounts), '')
            if not weight.endswith('%') or not 2 <= len(label) <= 85 or not normalized(label):
                break
            if re.search(r'[=+×*]|每次|範例|最高|加分|\b(?:each|bonus)\b', label, re.I):
                break
            tail = re.match(r'^\)?\s*\(([^()]{1,180})\)', value[match.end():])
            detail = tail[1] if tail and source['kind'] == 'syllabus' else ''
            end = match.end() + (tail.end() if detail else 0)
            rows.append({'label': label, 'weight': weight, 'detail': detail,
                         'origin': _origin(source, value[start:end].strip())})
            if detail:
                break
    if len(rows) < 2 or len({grading_key(row['label']) for row in rows}) != len(rows):
        return []
    if sum(Decimal(row['weight'][:-1]) for row in rows) != 100:
        return []
    return rows


def reconcile_analysis(raw, sources, prior_events, assignment_refs, semester):
    lookup = {source['id']: source for source in sources}
    events = dict(prior_events)
    grading = {}
    for item in raw.get('grading', [])[:30]:
        source, quote = _evidence(item, lookup)
        label = _text_field(item, 'label', 110)
        if (not source or not label or HISTORICAL.search(quote) or TENTATIVE_GRADING.search(quote)
                or (source.get('topic') == 'grading' and TENTATIVE_GRADING.search(source['text']))):
            continue
        display = grading_fields(item, quote)
        if not (display['weight'] or display['detail']):
            continue
        rank = (bool(display['weight']), source.get('version') or 0)
        key = grading_key(label)
        if key not in grading or rank > grading[key][0]:
            grading[key] = (rank, {'label': label, **display, 'origin': _origin(source, quote)})
    candidates = {}
    for item in [*raw.get('events', [])[:60], *_schedule_rows(sources)]:
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
            if assignment and ASSIGNMENT.search(context):
                key, title = f"deadline:{assignment['ref']}", assignment['title']
            elif is_message and FINAL_PROJECT.search(title) and FINAL_PROJECT.search(context) and PROJECT_DEADLINE.search(quote):
                key, title = 'deadline:final-project', '期末專題繳交'
            else:
                continue
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
            if not matches or len({(event['date'][:10], event['cancelled']) for event in matches}) != 1:
                continue
            timed = [event for event in matches if len(event['date']) > 10]
            if timed and len({event['date'] for event in timed}) != 1:
                continue
            if timed:
                matches = timed
            winner = matches[0]
            if old and rank == tuple(old['rank']) and (winner['date'], winner['cancelled']) != (old['date'], old['cancelled']):
                if old['date'] != winner['date'][:10] or not timed or old['cancelled'] != winner['cancelled']:
                    continue
            if len(events) < 160 or old:
                events[key] = old = winner
    for kind in ('midterm', 'final'):
        old = events.get(kind)
        if old:
            variant = _exam_key(old['title'], kind, old['origin']['quote'])
            if variant != kind and variant in events and events[variant]['rank'] >= old['rank']:
                del events[kind]
    published = sorted(sources, key=lambda row: row.get('version') or 0, reverse=True)
    rubric = next((rows for source in published if (rows := _published_rubric(source))), [])
    grading_outline = next((row for row in sources if row['kind'] == 'syllabus' and row.get('topic') == 'grading'), None)
    grading_note = _origin(grading_outline, '\n'.join(grading_outline.get('lines', []))[:1300]) if grading_outline and not rubric else None
    summary_request = {'source': raw.get('summary_source'), 'quote': raw.get('summary_quote')}
    _, summary_quote = _evidence(summary_request, lookup)
    if not summary_quote:
        summary_quote = next((row['text'] for row in sources if row['kind'] == 'syllabus'
                              and row.get('topic') == 'overview'), '')
    return {'summary': summary_quote[:850], 'grading': rubric or [row[1] for row in grading.values()], 'grading_note': grading_note,
            'published_grading': bool(rubric), 'events': events}
