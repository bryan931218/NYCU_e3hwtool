"""Bounded bilingual date recognition; ambiguity is surfaced, not silently applied."""

import re
import unicodedata
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse
from bs4 import BeautifulSoup
from e3_tracker.platform.constants import TAIPEI_TZ

CHANGE = re.compile(r'截止|期限|繳交|提交|延期|延後|延長|延至|改為|改至|改到|順延|deadline|due\b|extend|postpon|submission|submit\s+by', re.I)
UNCHANGED = re.compile(r'不(?:會)?(?:延期|延後|延長|變)|沒有(?:延期|變更)|取消(?:期限|截止)|無截止|\b(?:unchanged|no\s+(?:extension|deadline)|not\s+(?:extended|postponed))\b', re.I)
ORIGINAL = re.compile(r'(?:原(?:訂|本|先)?(?:的)?(?:截止(?:時間)?|期限|繳交時間)?|從|由|(?:original(?:ly)?|old|previous|prior)(?:\s+(?:deadline|due(?:\s+date)?))?|from)\s*(?:was|is|為|是|定於)?\s*[:：]?\s*$', re.I)
NUMERIC = re.compile(r'(?<![A-Za-z\d./-])(?:(?P<year>20\d{2}|1\d{2})\s*[/年.-]\s*)?(?P<a>\d{1,2})\s*[/月.-]\s*(?P<b>\d{1,2})\s*(?:日|[/.-]\s*(?P<endyear>20\d{2}))?(?![\d/.-])')
MONTH = r'(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?'
ENGLISH = re.compile(rf'\b(?:{MONTH}\s+\d{{1,2}}(?:st|nd|rd|th)?|\d{{1,2}}(?:st|nd|rd|th)?\s+{MONTH})(?:,?\s+20\d{{2}})?\b', re.I)
WEEKDAYS = 'monday tuesday wednesday thursday friday saturday sunday'.split()
RELATIVE = re.compile(r'(?P<relative>day after tomorrow|tomorrow|today|明天|後天|今天)|(?P<week>本|這|下)(?:週|星期)(?P<day>[一二三四五六日天1-7])|\b(?P<enweek>next|this)\s+(?P<enday>'+'|'.join(WEEKDAYS)+r')\b', re.I)
SHIFT = re.compile(r'(?:延後|延期|延長|順延)\s*(?P<cn>\d{1,3})\s*(?P<unit>天|週)|(?:extend(?:ed)?|postpone(?:d)?)\s+by\s+(?P<en>\d{1,3})\s*(?P<enunit>days?|weeks?)\b', re.I)
PREFIX = r'\s*(?:[,(]\s*(?:星期|週)?[一二三四五六日天0-9A-Za-z]{1,10}\s*\)?\s*)?(?:T|at\s+|by\s+|before\s+|於\s*)?'
CLOCK = re.compile(PREFIX + r'(?P<period>上午|下午|晚上|中午|凌晨|早上)?\s*(?P<hour>\d{1,2})(?:(?P<colon>:)(?P<minute>\d{2})(?::(?P<second>\d{2}))?|(?P<unit>[點时時])(?P<cnminute>\d{1,2}|半)?分?)?\s*(?P<ampm>[ap]\.?m\.?)?', re.I)
ZONE = re.compile(r'\s*(?P<zone>Z\b|(?:UTC|GMT)(?:\s*[+-]\s*\d{1,2}(?::?\d{2})?)?|[+-]\d{2}:?\d{2}|台灣時間|臺灣時間|Taipei(?:\s+time)?|(?:CST|PST|PDT|EST|EDT|JST|KST|HKT)\b)(?![A-Za-z0-9:+-])', re.I)
NAMED_CLOCK = re.compile(PREFIX+r'(?P<name>noon\b|midnight\b|中午|午夜)', re.I)


def chinese_number(text):
    digits = {character: value for value, character in enumerate('零一二三四五六七八九')}
    digits.update({'〇': 0, '○': 0, '兩': 2})
    if '十' in text:
        left, right = text.split('十', 1)
        return str((digits.get(left, 1) if left else 1)*10 + (digits.get(right, 0) if right else 0))
    return ''.join(str(digits[character]) for character in text)


def _normalize(value):
    text = unicodedata.normalize('NFKC', str(value or '')[:20000])
    text = text.translate(str.maketrans('后长为顺这周点时两', '後長為順這週點時兩'))
    if re.search(r'</?(?:p|div|br|blockquote)\b', text, re.I):
        soup = BeautifulSoup(text, 'html.parser')
        for quote in soup.select('blockquote, script, style'):
            quote.decompose()
        text = soup.get_text(' ', strip=True)
    text = '\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('>'))
    return re.sub(r'[零〇○一二兩三四五六七八九十]+(?=[年月日點时時分天週])', lambda match: chinese_number(match[0]), text)


def _clock(tail, date):
    """A malformed explicit clock invalidates a date instead of defaulting to 23:59."""
    match = CLOCK.match(tail)
    explicit = bool(match and any(match[key] for key in ('period', 'colon', 'unit', 'ampm')))
    warnings = []
    weekday = re.match(r'\s*\((?:星期|週)?([一二三四五六日天A-Za-z]{1,10})\)', tail)
    if weekday:
        label = weekday[1].lower()
        expected = '一二三四五六日'.find(label.replace('天', '日')) if len(label) == 1 else next(
            (index for index, day in enumerate(WEEKDAYS) if label in {day, day[:3]}), -1)
        if expected >= 0 and date.weekday() != expected:
            warnings.append('來源日期與星期不一致；暫依數字日期顯示，請核對。')
    consumed = 0
    if explicit:
        hour = int(match['hour'])
        minute = 30 if match['cnminute'] == '半' else int(match['minute'] or match['cnminute'] or 0)
        second = int(match['second'] or 0)
        period = match['period'] or (match['ampm'] or '').lower().replace('.', '')
        if period:
            if not 1 <= hour <= 12:
                raise ValueError('invalid 12-hour time')
            hour %= 12
            if period in {'下午', '晚上', '中午', 'pm'}:
                hour += 12
        rollover = hour == 24 and minute == second == 0
        date = date.replace(hour=0 if rollover else hour, minute=minute, second=second)
        if rollover:
            date += timedelta(days=1)
        consumed = match.end()
    else:
        named = NAMED_CLOCK.match(tail)
        if named:
            date = date.replace(hour=12 if named['name'].lower() in {'noon', '中午'} else 0, minute=0, second=0)
            explicit, consumed = True, named.end()
            if named['name'].lower() in {'midnight', '午夜'}:
                warnings.append('午夜可能指當天開始或結束；暫以當天 00:00 顯示，請核對。')
        else:
            date = date.replace(hour=23, minute=59, second=0)
    zone = ZONE.match(tail[consumed:])
    if not zone and re.match(r'\s*(?:UTC|GMT|[+-]\d)', tail[consumed:], re.I):
        raise ValueError('invalid time zone')
    unknown_zone = re.match(r'\s*([A-Z]{2,5})\b', tail[consumed:]) if explicit and not zone else None
    if unknown_zone:
        warnings.append(f'無法確定 {unknown_zone[1]} 時區；暫以台灣時間顯示，請核對。')
    tz = TAIPEI_TZ
    if zone:
        name = zone['zone'].replace(' ', '').upper()
        if name in {'CST', 'PST', 'PDT', 'EST', 'EDT'}:
            warnings.append(f'{name} 時區縮寫可能有歧義；暫以台灣時間顯示，請核對。')
        elif name in {'JST', 'KST', 'HKT'}:
            tz = timezone(timedelta(hours=8 if name == 'HKT' else 9))
        elif name in {'Z', 'UTC', 'GMT'}:
            tz = timezone.utc
        elif name.startswith(('+', '-')) or re.match(r'(UTC|GMT)[+-]', name):
            offset = re.sub(r'^(UTC|GMT)', '', name)
            sign = -1 if offset[0] == '-' else 1
            parts = offset[1:].split(':')
            hour = int(parts[0][:-2]) if len(parts) == 1 and len(parts[0]) > 2 else int(parts[0])
            minute = int(parts[0][-2:]) if len(parts) == 1 and len(parts[0]) > 2 else int(parts[1]) if len(parts) > 1 else 0
            if hour > 14 or minute > 59 or (hour == 14 and minute):
                raise ValueError('invalid time zone')
            tz = timezone(sign*timedelta(hours=hour, minutes=minute))
        consumed += zone.end()
    value = TAIPEI_TZ.localize(date) if tz is TAIPEI_TZ else date.replace(tzinfo=tz).astimezone(TAIPEI_TZ)
    if value.second:
        warnings.append('來源含秒數，建議期限以分鐘顯示，請核對。')
        value = value.replace(second=0)
    return value, explicit, warnings, consumed


def deadline_dates(item, *, now=None, baseline_due=None):
    current = datetime.now(TAIPEI_TZ) if now is None else datetime.fromtimestamp(now, TAIPEI_TZ)
    reference = datetime.fromtimestamp(item.get('updated_ts') or current.timestamp(), TAIPEI_TZ)
    title = _normalize(item.get('title'))
    text = (title + '\n' + _normalize(item.get('content')))[:20000]
    found, occupied = [], []

    def context(start, end):
        excerpt = text[max(0, start-100):min(len(text), end+70)]
        clause_start = max(text.rfind(char, 0, start) for char in '\n;；。')+1
        clause_end = min([p for char in '\n;；。' if (p := text.find(char, end)) >= 0] or [len(text)])
        clause = text[clause_start:clause_end]
        before = text[max(clause_start, start-80):start]
        if not (CHANGE.search(clause) or CHANGE.search(title)) or UNCHANGED.search(clause) or ORIGINAL.search(before):
            return None
        cues = list(re.finditer(r'上課|考試|補課|會議|lecture|meeting|exam|class\b', before, re.I))
        if cues and not CHANGE.search(before[cues[-1].end():]):
            return None
        return excerpt.strip()

    def add(start, end, date, notes=(), *, preserve_clock=False):
        evidence = context(start, end)
        if not evidence:
            return
        try:
            if preserve_clock:
                value, explicit, warnings, consumed = date, True, [], 0
            else:
                value, explicit, warnings, consumed = _clock(text[end:end+100], date)
                if not explicit:
                    leading = re.search(r'(?:at|by|before)\s+(.{1,35}?)\s+on\s*$', text[max(0, start-50):start], re.I)
                    if leading:
                        value, explicit, leading_notes, _ = _clock(leading[1], date)
                        warnings += leading_notes
            if current < value <= current+timedelta(days=370):
                found.append((start, {'due_ts': int(value.timestamp()), 'evidence': evidence,
                    'time_explicit': explicit, 'warnings': list(notes)+warnings}))
                occupied.append((start, end+consumed))
        except (ValueError, OverflowError):
            occupied.append((start, end))

    for match in NUMERIC.finditer(text):
        if not match['year'] and not match['endyear'] and (re.search(r'(?:hw|assignment|作業|作业)\s*#?\s*$', text[max(0,match.start()-20):match.start()], re.I)
                or re.match(r'\s*(?:points?\b|pts\b|分|題)', text[match.end():], re.I)):
            continue
        a, b = int(match['a']), int(match['b'])
        year = int(match['year'] or match['endyear'] or reference.year)
        notes = []
        if year < 1911:
            year += 1911
        if not match['year'] and a > 12:
            a, b = b, a
        elif not match['year'] and a <= 12 and b <= 12 and a != b and '月' not in match[0]:
            notes.append('數字日期可能為月／日或日／月；暫以月／日顯示，請核對。')
        if not (match['year'] or match['endyear']):
            notes.append(f'來源未寫年份，以訊息日期的 {reference.year} 年判讀。')
            if reference.month >= 10 and a <= 3:
                year += 1
                notes[-1] = f'來源未寫年份，依跨年日期判讀為 {year} 年，請核對。'
        try:
            add(match.start(), match.end(), datetime(year, a, b), notes)
        except ValueError:
            occupied.append(match.span())
    for match in ENGLISH.finditer(text):
        try:
            date = parse(match[0], default=reference.replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0), fuzzy=False)
            notes = []
            if not re.search(r'20\d{2}', match[0]):
                notes.append('來源未寫年份，依訊息日期判讀，請核對。')
                if reference.month >= 10 and date.month <= 3:
                    date = date.replace(year=date.year+1)
            add(match.start(), match.end(), date, notes)
        except (ValueError, OverflowError):
            occupied.append(match.span())
    for match in RELATIVE.finditer(text):
        word = (match['relative'] or '').lower()
        if word:
            days = {'today': 0, '今天': 0, 'tomorrow': 1, '明天': 1, 'day after tomorrow': 2, '後天': 2}[word]
            date = reference.replace(tzinfo=None)+timedelta(days=days)
        else:
            weekday = WEEKDAYS.index(match['enday'].lower()) if match['enday'] else '一二三四五六日'.index(match['day'].replace('天', '日')) if not match['day'].isdigit() else int(match['day'])-1
            weeks = 1 if match['week'] == '下' or (match['enweek'] or '').lower() == 'next' else 0
            date = reference.replace(tzinfo=None)+timedelta(days=weekday-reference.weekday()+weeks*7)
        add(match.start(), match.end(), date, [f'相對日期以訊息日期 {reference:%Y/%m/%d} 判讀，請核對。'])
    if baseline_due:
        baseline = datetime.fromtimestamp(baseline_due, TAIPEI_TZ)
        for match in SHIFT.finditer(text):
            days = int(match['cn'] or match['en']) * (7 if match['unit'] == '週' or (match['enunit'] or '').lower().startswith('week') else 1)
            if 0 < days <= 370:
                add(match.start(), match.end(), baseline+timedelta(days=days), ['依 E3 原始期限推算延期天數，請核對。'], preserve_clock=True)
        # A clock-only change requires one uniquely matched assignment with a known date.
        if not found:
            for match in re.finditer(r'(?:(?:deadline|due(?:\s+date)?)(?:\s+(?:is|has been|was))?\s+(?:changed|moved|extended|postponed)\s+(?:from\s+.{1,40}?\s+)?to|改為|改至|改到|延至|deadline(?:\s+(?:is|now))?|due(?:\s+(?:at|by))?)\s*[:：]?\s*', text, re.I):
                if any(start <= match.end() < end for start, end in occupied):
                    continue
                try:
                    value, explicit, notes, consumed = _clock(text[match.end():match.end()+100], baseline.replace(tzinfo=None))
                    if explicit:
                        add(match.start(), match.end()+consumed, value, ['來源只寫時間，沿用 E3 原始期限的日期，請核對。']+notes, preserve_clock=True)
                except ValueError:
                    continue
    unique = {}
    for position, value in sorted(found):
        unique[value['due_ts']] = value
    return list(unique.values())[-3:]
