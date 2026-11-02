"""Readable, byte-bounded LINE cards. Full bodies are never put into browser alerts."""
import json
import re
from datetime import datetime
from urllib.parse import parse_qs, urlsplit

from .course_files import safe_file_link
from .notifications import notification_time
from e3_tracker.platform.constants import TAIPEI_TZ


def clean(value):
    text = str(value or '').encode('utf-8', 'replace').decode('utf-8')
    return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text).strip()


def chunks(value, maximum=1200):
    """Split by UTF-16 units without cutting emoji; keep paragraph boundaries where possible."""
    value = clean(value)
    while value:
        units, end = 0, 0
        for char in value:
            size = 2 if ord(char) > 0xffff else 1
            if units + size > maximum:
                break
            units += size
            end += 1
        if end < len(value):
            boundary = value.rfind('\n', end // 2, end)
            if boundary >= 0:
                end = boundary + 1
        yield value[:end]
        value = value[end:]


def short(value, maximum):
    return next(chunks(value, maximum), '')


def safe_url(value):
    value = clean(value)
    try:
        url = urlsplit(value)
        tokens = {'sesskey', 'token', 'wstoken', 'moodlesession', 'access_token', 'auth', 'key'}
        if (url.scheme == 'https' and url.netloc and not url.username and not url.password
                and len(value) <= 2000 and not re.search(r'\s', value)
                and not tokens.intersection(key.casefold() for key in parse_qs(url.query, keep_blank_values=True))):
            return url._replace(fragment='').geturl()
    except ValueError:
        pass
    return ''


def text(value, **options):
    return {'type': 'text', 'text': clean(value) or '—', 'wrap': True, 'size': 'sm',
            'color': '#263440', 'scaling': True, **options}


def full_time(timestamp):
    return f'{datetime.fromtimestamp(int(timestamp), TAIPEI_TZ):%Y}/' + notification_time(timestamp)


def line_detail_messages(payload, quick_reply=None, *, _page_size=1000):
    detail = payload['line_detail']
    kind = payload.get('kind')
    label = {'new': '新作業', 'due': '到期提醒', 'scheduled': '作業提醒', 'graded': '評分通知',
             'new_announcement': '課程公告', 'new_mail': '課程信件'}.get(kind, '課程通知')
    accent = '#AA6426' if kind == 'due' else '#15786D'
    title = clean(detail.get('title')) or label
    course = clean(detail.get('course_title')) or '課程'
    content = clean(detail.get('content'))
    content = re.sub(r'\n[ \t]*\n(?:[ \t]*\n)+', '\n\n', content)
    if detail.get('truncated'):
        content += '\n\n（原文超過同步上限，尚有內容或附件請查看 E3 原文。）'
    if not content:
        content = ('暫時無法取得內文。E3 登入失效或連線異常時，請重新登入後同步。'
                   if detail.get('unavailable') else '本則內容沒有文字說明，請查看下方附件或原文。')
    pages = [[part] for part in chunks(content, _page_size)]
    original = safe_url(detail.get('url'))
    links, seen = [], set()
    for entry in detail.get('links') or []:
        url = safe_file_link(entry.get('url'), original or 'https://e3p.nycu.edu.tw/')
        if url and url not in seen:
            seen.add(url)
            links.append((clean(entry.get('title')) or '附件／連結', url))
    cards = []
    for index, parts in enumerate(pages):
        body = []
        if index == 0:
            body.append(text(short(course, 1000), size='xs', color='#637780'))
            for name, value in [('發布者', detail.get('author')),
                                ('截止時間', full_time(detail['due_ts']) if detail.get('due_ts') else ('未設定' if kind in {'new', 'due', 'scheduled'} else '')),
                                ('發布時間', full_time(detail['updated_ts']) if detail.get('updated_ts') else '')]:
                if value:
                    body.append(text(f'{name}　{short(value, 300)}', size='xs', color='#637780'))
            body.append({'type': 'separator', 'margin': 'lg', 'color': '#E2EAE8'})
        body.extend(text(part, margin='lg', lineSpacing='6px') for part in parts if part.strip())
        cards.append(_card(label, title if index == 0 else short(title, 160), body, accent))
    for index in range(0, len(links), 5):
        body = []
        for name, url in links[index:index + 5]:
            body.extend([text(short(name, 600), weight='bold', margin='md'),
                         text(urlsplit(url).hostname, size='xs', color='#637780'),
                         {'type': 'button', 'style': 'secondary', 'height': 'sm',
                          'action': {'type': 'uri', 'label': '開啟附件／連結', 'uri': url}}])
        cards.append(_card('附件與連結', short(title, 160), body, accent))
    for index, card in enumerate(cards):
        card['header']['contents'][0]['text'] += f'  ·  {index + 1}/{len(cards)}' if len(cards) > 1 else ''
    if original:
        cards[-1]['footer'] = {'type': 'box', 'layout': 'vertical', 'paddingAll': '16px', 'contents': [
            {'type': 'button', 'style': 'link', 'height': 'sm', 'color': accent,
             'action': {'type': 'uri', 'label': '查看評分' if kind == 'graded' else ('開啟作業' if kind in {'new', 'due', 'scheduled'} else '開啟 E3 原文'), 'uri': original}}]}
    if len(cards) > 1:
        cards[0]['header']['contents'].append(text('左滑續讀內文與附件', size='xs', color='#637780'))
    if quick_reply:
        quick_reply = {'items': [entry for entry in quick_reply.get('items', []) if
            entry.get('action', {}).get('type') != 'uri' or safe_url(entry['action'].get('uri'))]}
        picker = next((entry['action'] for entry in quick_reply['items'] if entry['action']['type'] == 'datetimepicker'), None)
        if picker:
            footer = cards[-1].setdefault('footer', {'type': 'box', 'layout': 'vertical', 'paddingAll': '16px', 'spacing': 'sm', 'contents': []})
            footer['contents'].insert(0, {'type': 'button', 'style': 'primary', 'height': 'sm', 'color': accent, 'action': picker})
    # LINE: <=30 KB/bubble, <=50 KB/carousel and <=5 messages/push.
    # The source parsers bound content to 20,000 characters and 20 outbound links.
    groups, group = [], []
    for card in cards:
        if _bytes(card) > 29000:
            raise ValueError('line_bubble_too_large')
        if group and (len(group) == 12 or _bytes({'type': 'carousel', 'contents': group + [card]}) > 48000):
            groups.append(group)
            group = []
        group.append(card)
    if group:
        groups.append(group)
    if len(groups) > 5:
        if _page_size < 2000:
            return line_detail_messages(payload, quick_reply, _page_size=2000)
        raise ValueError('line_content_exceeds_source_limits')
    messages = [{'type': 'flex', 'altText': short(f'{label}｜{course}｜{title}', 380),
                 'contents': group[0] if len(group) == 1 else {'type': 'carousel', 'contents': group}}
                for group in groups]
    if quick_reply:
        messages[-1]['quickReply'] = quick_reply
    return messages


def _bytes(value):
    return len(json.dumps(value, ensure_ascii=False).encode('utf-8'))


def _card(label, title, body, accent):
    return {'type': 'bubble', 'size': 'giga',
            'styles': {'body': {'backgroundColor': '#FFFFFF'}, 'footer': {'backgroundColor': '#FFFFFF'}},
            'header': {'type': 'box', 'layout': 'vertical', 'spacing': 'sm', 'paddingAll': '20px',
                       'backgroundColor': '#F0F7F5', 'contents': [
                           text(label, size='xs', color=accent, weight='bold'),
                           text(short(title, 1000), size='lg', weight='bold', color='#183D38')]},
            'body': {'type': 'box', 'layout': 'vertical', 'paddingAll': '20px', 'contents': body}}
