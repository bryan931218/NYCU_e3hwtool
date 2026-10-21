"""Private LINE attachment browsing over the same cached library as the website."""

import hashlib
import hmac
import json
import re
import time
import unicodedata
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlsplit, urlunsplit

from e3_tracker.assignments.domain.course_files import safe_file_link, search_files
from e3_tracker.assignments.domain.notifications import digest, notification_text


def line_text(value, units):
    text = notification_text(value, units)
    while len(text.encode('utf-16-le', errors='replace')) // 2 > units:
        text = text[:-1]
    return text


class LineFileMenu:
    COMMAND = re.compile(r'^(?:找附件|找檔案|搜尋附件|課程附件)(?:[\s:：]*(.*))?$', re.S)
    PAGE_SIZE = 5
    COURSE_PAGE_SIZE = 8

    def __init__(self, storage, service, files, app):
        self.storage, self.service, self.files, self.app = storage, service, files, app

    def matches(self, text):
        return bool(self.COMMAND.fullmatch(unicodedata.normalize('NFKC', text).strip()))

    def owner(self, target):
        owner = self.storage.line_reminder_owner(digest(target))
        if not owner:
            raise ValueError('請先綁定 E3 帳號，再搜尋自己的附件。')
        return owner

    def query(self, value):
        if not isinstance(value, str) or len(value.encode('utf-16-le', errors='replace')) // 2 > 60:
            raise ValueError('搜尋關鍵字請控制在 60 字內。')
        value = ' '.join(unicodedata.normalize('NFKC', value).split())
        if re.search(r'(?:moodlesession|password|密碼)\s*[=:：]', value, re.I):
            raise ValueError('請只輸入課程或檔名，不要傳送密碼或登入憑證。')
        return value

    def token(self, owner, target, action, semester, course='', query='', offset=0, *, ttl=900):
        context = {'a': action, 's': semester, 'c': str(course), 'q': self.query(query), 'o': offset, 't': int(time.time()) + ttl}
        value = 'lf:' + json.dumps(context, ensure_ascii=False, separators=(',', ':'))
        signature = hmac.new(self.service.line_secret.encode(), f'{value}:{digest(target)}:{owner}'.encode(), hashlib.sha256).hexdigest()
        data = f'{value}:{signature}'
        if len(data.encode('utf-16-le')) // 2 > 300:
            raise ValueError('搜尋關鍵字太長，請縮短後再試。')
        return data

    def action(self, owner, target, label, action, semester, course='', query='', offset=0):
        result = {'type': 'postback', 'label': line_text(label, 20), 'displayText': line_text(label, 100),
            'data': self.token(owner, target, action, semester, course, query, offset)}
        if action == 'q':
            result.update(inputOption='openKeyboard', fillInText='找附件 ')
        return result

    def web_url(self, semester='', course='', query=''):
        params = {key: value for key, value in {'semester': semester, 'course': course, 'q': query}.items() if value}
        params['openExternalBrowser'] = '1'
        return urljoin(self.service.home_url, '/courses/files') + '?' + urlencode(params)

    def error(self, error):
        return {'type': 'text', 'text': str(error), 'quickReply': {'items': [
            {'type': 'action', 'action': {'type': 'message', 'label': '重新找附件', 'text': '找附件'}},
            {'type': 'action', 'action': {'type': 'uri', 'label': '綁定與登入', 'uri': urljoin(self.service.home_url, '/settings/notifications')}},
        ]}}

    def _snapshot(self, owner, target, semester=None):
        if not self.storage.consume_security_limit(f'line-file-search:{owner}', 60, 600):
            raise ValueError('搜尋過於頻繁，請稍後再試。')
        data = self.files.library(owner, semester, prefer_current=True)
        if self.owner(target) != owner:
            raise ValueError('帳號綁定已變更，請重新找附件。')
        return data

    def _bubble(self, title, subtitle, rows, actions):
        return {'type': 'flex', 'altText': line_text(f'{title}｜{subtitle}', 300), 'contents': {
            'type': 'bubble', 'size': 'mega',
            'header': {'type': 'box', 'layout': 'vertical', 'spacing': 'sm', 'contents': [
                {'type': 'text', 'text': line_text(title, 100), 'weight': 'bold', 'size': 'lg', 'color': '#202A35', 'wrap': True},
                {'type': 'text', 'text': line_text(subtitle, 200) or '附件收納', 'size': 'xs', 'color': '#5B6774', 'wrap': True}]},
            'body': {'type': 'box', 'layout': 'vertical', 'spacing': 'md', 'contents': rows},
            'footer': {'type': 'box', 'layout': 'vertical', 'spacing': 'xs', 'contents': [
                {'type': 'box', 'layout': 'horizontal', 'contents': [
                    {'type': 'button', 'style': 'link', 'height': 'sm', 'action': action} for action in actions[index:index + 2]]}
                for index in range(0, len(actions), 2)]},
        }}

    def _footer(self, owner, target, semester, course='', query=''):
        return [self.action(owner, target, '搜尋附件', 'q', semester, course, query),
            self.action(owner, target, '更新附件', 'r', semester, course, query),
            {'type': 'uri', 'label': '網頁收納', 'uri': self.web_url(semester, course, query)}]

    def courses(self, owner, target, data, query='', offset=0):
        semester = data['semester']
        courses = data['courses']
        offset = offset if offset < len(courses) else 0
        counts = {}
        for item in search_files(data['items'], query=query):
            counts[item['course_id']] = counts.get(item['course_id'], 0) + 1
        rows = []
        for index, course in enumerate(courses[offset:offset + self.COURSE_PAGE_SIZE]):
            if index:
                rows.append({'type': 'separator', 'color': '#E1E6EB'})
            rows.append({'type': 'box', 'layout': 'horizontal', 'paddingTop': 'md', 'paddingBottom': 'md',
                'action': self.action(owner, target, course.get('name') or course['title'], 'f', semester, str(course['id']), query),
                'contents': [{'type': 'text', 'text': line_text(course.get('name') or course['title'], 90), 'size': 'sm', 'wrap': True},
                    {'type': 'text', 'text': str(counts.get(int(course['id']), 0)), 'size': 'sm', 'color': '#08786B', 'flex': 0}]})
        actions = []
        if offset:
            actions.append(self.action(owner, target, '上一頁課程', 'c', semester, query=query, offset=max(0, offset - self.COURSE_PAGE_SIZE)))
        if offset + self.COURSE_PAGE_SIZE < len(courses):
            actions.append(self.action(owner, target, '下一頁課程', 'c', semester, query=query, offset=offset + self.COURSE_PAGE_SIZE))
        actions.append(self.action(owner, target, '全部附件', 'f', semester, query=query))
        actions.extend(self._footer(owner, target, semester, query=query))
        label = next((entry['label'] for entry in data['semesters'] if entry['key'] == semester), semester)
        subtitle = f'{label} · {len(data["items"])} 項附件'
        if not data['initialized']:
            subtitle += ' · 尚未整理，可點「更新附件」'
        elif data['running']:
            subtitle += ' · 更新中'
        elif data.get('error'):
            subtitle += ' · 需回網站重新登入 E3' if '登入已失效' in data['error'] else ' · 部分來源尚待更新'
        return self._bubble('找附件', subtitle, rows or [{'type': 'text', 'text': '尚無課程資料', 'wrap': True}], actions)

    def results(self, owner, target, data, course='', query='', offset=0):
        semester = data['semester']
        if course and course not in {str(entry['id']) for entry in data['courses']}:
            raise ValueError('找不到該課程，請重新選擇。')
        items = search_files(data['items'], course=course, query=query)
        items.sort(key=lambda item: (item['course_title'], item['title'].casefold(), item['id']))
        offset = offset if offset < len(items) else 0
        rows = []
        for index, item in enumerate(items[offset:offset + self.PAGE_SIZE]):
            url = safe_file_link(item['url'], self.files.base_url)
            if not url:
                continue
            target_url = urlsplit(url)
            if target_url.netloc == urlsplit(self.files.base_url).netloc:
                url = urlunsplit(target_url._replace(query=urlencode([*parse_qsl(target_url.query), ('openExternalBrowser', '1')])))
            url = quote(url, safe=":/?#[]@!$&'()*+,;=%-._~")
            in_library = len(url) > 1000
            if in_library:
                url = self.web_url(semester, str(item['course_id']), (item.get('filename') or item['title'])[:40])
            if index:
                rows.append({'type': 'separator', 'color': '#E1E6EB'})
            title = (item.get('filename') or item['title']) if item['type'] != 'link' else item['title']
            rows.append({'type': 'box', 'layout': 'vertical', 'spacing': 'sm', 'paddingTop': 'sm', 'paddingBottom': 'sm',
                'action': {'type': 'uri', 'label': '網頁開啟' if in_library else '開啟附件', 'uri': url}, 'contents': [
                    {'type': 'text', 'text': line_text(title, 160), 'weight': 'bold', 'size': 'sm', 'wrap': True, 'maxLines': 3, 'color': '#08786B'},
                    {'type': 'text', 'text': line_text(item.get('course_name') or item['course_title'], 80), 'size': 'xs', 'color': '#5B6774', 'wrap': True},
                    {'type': 'text', 'text': f"{item.get('extension') or ('連結' if item['type'] == 'link' else '檔案')} · {'至網頁開啟' if in_library else '點選開啟'}", 'size': 'xs', 'color': '#5B6774'}]})
        if not rows:
            rows.append({'type': 'text', 'text': '尚無符合的附件，可換關鍵字或更新附件。', 'wrap': True, 'size': 'sm', 'color': '#5B6774'})
        else:
            rows.append({'type': 'text', 'text': 'E3 附件需在瀏覽器登入 E3。', 'size': 'xs', 'color': '#5B6774', 'wrap': True})
        actions = []
        if offset:
            actions.append(self.action(owner, target, '上一頁', 'f', semester, course, query, max(0, offset - self.PAGE_SIZE)))
        if offset + self.PAGE_SIZE < len(items):
            actions.append(self.action(owner, target, '下一頁', 'f', semester, course, query, offset + self.PAGE_SIZE))
        actions.append(self.action(owner, target, '選擇課程', 'c', semester))
        actions.extend(self._footer(owner, target, semester, course, query))
        title = next((entry.get('name') or entry['title'] for entry in data['courses'] if str(entry['id']) == course), '全部附件')
        subtitle = f'{offset + 1}–{min(offset + self.PAGE_SIZE, len(items))}／{len(items)} 項' if items else '0 項附件'
        if query:
            subtitle = f'「{query}」 · {subtitle}'
        if data['running']:
            subtitle += ' · 更新中，稍後可重查'
        elif data.get('error'):
            subtitle += ' · 需回網站重新登入 E3' if '登入已失效' in data['error'] else ' · 部分來源尚待更新'
        elif data.get('limited'):
            subtitle += ' · 附件較多，目前為部分結果'
        return self._bubble(title, subtitle, rows, actions)

    def handle_text(self, text, target):
        match = self.COMMAND.fullmatch(unicodedata.normalize('NFKC', text).strip())
        if not match:
            return None
        query = self.query(match[1] or '')
        owner = self.owner(target)
        data = self._snapshot(owner, target)
        return self.results(owner, target, data, query=query) if query else self.courses(owner, target, data)

    def handle_postback(self, data, target):
        owner = self.owner(target)
        if not isinstance(data, str) or not data.startswith('lf:') or len(data.encode('utf-16-le', errors='replace')) // 2 > 300:
            raise ValueError('附件選單無效，請重新找附件。')
        value, _, signature = data.rpartition(':')
        expected = hmac.new(self.service.line_secret.encode(), f'{value}:{digest(target)}:{owner}'.encode(), hashlib.sha256).hexdigest()
        if not re.fullmatch(r'[0-9a-f]{64}', signature) or not hmac.compare_digest(signature, expected):
            raise ValueError('附件選單已失效，請重新找附件。')
        try:
            context = json.loads(value[3:])
            if (set(context) != {'a', 's', 'c', 'q', 'o', 't'} or context['a'] not in {'c', 'f', 'q', 'r'}
                or type(context['t']) is not int or not time.time() <= context['t'] <= time.time() + 960
                or type(context['o']) is not int or not 0 <= context['o'] <= 3000
                or not isinstance(context['s'], str) or not re.fullmatch(r'(?:\d{2,3}-(?:1|2|summer)|other)', context['s'])
                or not isinstance(context['c'], str) or not re.fullmatch(r'(?:[1-9][0-9]{0,9})?', context['c'])):
                raise ValueError()
        except (ValueError, TypeError, KeyError):
            raise ValueError('附件選單已過期，請重新找附件。') from None
        query = self.query(context['q'])
        snapshot = self._snapshot(owner, target, context['s'])
        course = context['c']
        if course and course not in {str(entry['id']) for entry in snapshot['courses']}:
            raise ValueError('找不到該課程，請重新選擇。')
        if context['a'] == 'q':
            return {'type': 'text', 'text': '請輸入「找附件 關鍵字」，例如：找附件 HW2。',
                'quickReply': {'items': [{'type': 'action', 'action': self.action(owner, target, '輸入關鍵字', 'q', context['s'])}]}}
        if context['a'] == 'r':
            if not self.storage.consume_security_limit(f'course-files:{owner}', 12, 600):
                raise ValueError('更新過於頻繁，請稍後再試。')
            user = self.files.storage.line_user(owner, digest(target))
            if not user:
                raise ValueError('E3 登入已失效，請先回網站登入；已收納的附件仍可搜尋。')
            semester, courses, _ = self.files.scope(owner, context['s'])
            status = self.files.start(self.app, user, semester, courses, self.files.messages(owner, semester, courses))
            if status == 'busy':
                raise ValueError('目前整理人數較多，請稍後再試。')
            if status == 'session_expired':
                raise ValueError('E3 登入已失效，請先回網站重新登入。')
            return {'type': 'text', 'text': '附件正在更新，稍後點「查看結果」。' if status == 'running' else '稍後再試，或點「查看結果」讀取已收納的附件。',
                'quickReply': {'items': [{'type': 'action', 'action': self.action(owner, target, '查看結果', 'f', semester, course, query)},
                    {'type': 'action', 'action': {'type': 'uri', 'label': '網頁收納', 'uri': self.web_url(semester, course, query)}}]}}
        if context['a'] == 'c':
            return self.courses(owner, target, snapshot, query, context['o'])
        return self.results(owner, target, snapshot, course, query, context['o'])
