"""Account-bound LINE assignment picker; no browser or prior push is required."""

import hashlib
import hmac
import re
import time
from datetime import datetime

from e3_tracker.assignments.domain.assignment_actions import safe_assignment_url
from e3_tracker.assignments.domain.notifications import digest, notification_text, notification_time
from e3_tracker.assignments.persistence.assignment_actions import work_plans
from e3_tracker.platform.constants import TAIPEI_TZ


class LineReminderMenu:
    COMMANDS = {'設定作業提醒', '設定提醒', '安排提醒'}
    PAGE_SIZE = 8

    def __init__(self, storage, service):
        self.storage, self.service = storage, service

    def token(self, owner, target, action, key='0', *, ttl=900):
        value = f'lr:{action}:{key}:{int(time.time()) + ttl}'
        signature = hmac.new(self.service.line_secret.encode(),
            f'{value}:{digest(target)}:{owner}'.encode(), hashlib.sha256).hexdigest()
        return f'{value}:{signature}'

    def owner(self, target):
        owner = self.storage.line_reminder_owner(digest(target))
        if not owner:
            raise ValueError('請先綁定 E3 帳號。點下方「綁定教學」查看步驟。')
        return owner

    def reply(self, text, choices=()):
        message = {'type': 'text', 'text': text}
        if choices:
            message['quickReply'] = {'items': [{'type': 'action', 'action': action} for action in choices]}
        return message

    def postback(self, owner, target, label, action, key='0'):
        return {'type': 'postback', 'label': label, 'displayText': label,
                'data': self.token(owner, target, action, key)}

    def error(self, error):
        return self.reply(str(error), [{'type': 'message', 'label': '重新選作業', 'text': '設定作業提醒'},
                                     {'type': 'message', 'label': '綁定教學', 'text': '綁定教學'}])

    def handle_text(self, text, target):
        if text.strip() not in self.COMMANDS:
            return None
        return self.list(self.owner(target), target)

    def list(self, owner, target, offset=0):
        if not self.storage.consume_security_limit(f'line-reminder-list:{owner}', 30, 600):
            raise ValueError('操作過於頻繁，請稍後再試。')
        if not self.storage.notification_preferences(owner)['preferences']['line_enabled']:
            return self.reply('LINE 通知目前關閉。開啟後即可在這裡選作業與提醒時間；其他提醒條件不會更動。',
                [self.postback(owner, target, '開啟 LINE 通知', 'enable')])
        now = time.time()
        items = [(key, item) for key, item in self.service.actions.items(owner).items()
                 if not item.get('due_ts') or item['due_ts'] > now + 90]
        items.sort(key=lambda entry: (entry[1].get('due_ts') or float('inf'),
                                    str(entry[1].get('course_title', '')), str(entry[1].get('title', '')), entry[0]))
        if not items:
            return self.reply('目前沒有可安排提醒的當期作業。已完成、已評分、忽略與已截止的作業不會列出。')
        if offset >= len(items):
            offset = 0
        page = items[offset:offset + self.PAGE_SIZE]
        text = [f'選擇要提醒的作業（{offset + 1}–{offset + len(page)}／{len(items)}）']
        choices = []
        for index, (key, item) in enumerate(page, offset + 1):
            due = notification_time(item['due_ts']) if item.get('due_ts') else '未設定截止日期'
            text.append(f"{index}. {notification_text(item['title'], 100)}\n{notification_text(item.get('course_title'), 70)}\n截止：{due}")
            choices.append(self.postback(owner, target, notification_text(f"{index}. {item['title']}", 20), 'pick', key))
        if offset:
            choices.append(self.postback(owner, target, '上一頁', 'list', str(max(0, offset - self.PAGE_SIZE))))
        if offset + len(page) < len(items):
            choices.append(self.postback(owner, target, '下一頁', 'list', str(offset + self.PAGE_SIZE)))
        text.append('時間皆以台灣時間為準。')
        return self.reply('\n\n'.join(text), choices)

    def handle_postback(self, data, target, params=None):
        match = re.fullmatch(r'lr:(list|pick|set|cancel|enable):([0-9a-f]{64}|\d{1,6}):(\d{10}):([0-9a-f]{64})', data) if isinstance(data, str) else None
        owner = self.owner(target)
        if not match or not time.time() <= int(match[3]) <= time.time() + 7 * 86400 + 60:
            raise ValueError('這個操作已過期，請重新選擇作業。')
        expected = hmac.new(self.service.line_secret.encode(),
            f"{data.rsplit(':', 1)[0]}:{digest(target)}:{owner}".encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, match[4]):
            raise ValueError('無法驗證操作，請重新選擇作業。')
        action, key = match[1], match[2]
        if action == 'enable' and key == '0':
            self.storage.enable_line_reminders(owner, digest(target))
            return self.list(owner, target)
        if action == 'list' and key.isdecimal():
            return self.list(owner, target, int(key))
        if not re.fullmatch('[0-9a-f]{64}', key):
            raise ValueError('作業資料無效。')
        if action == 'cancel':
            if not self.storage.cancel_assignment_plan(owner, key):
                raise ValueError('找不到這筆提醒。')
            return self.reply('這筆提醒已取消。', [self.postback(owner, target, '設定其他提醒', 'list')])
        if not self.storage.notification_preferences(owner)['preferences']['line_enabled']:
            return self.list(owner, target)
        item = self.service.actions.item(owner, key)
        if action == 'pick':
            now = time.time()
            minimum = int((now + 90) // 60) * 60
            maximum = min(int(now + 370 * 86400), int(item['due_ts']) - 1 if item.get('due_ts') else int(now + 370 * 86400))
            maximum = maximum // 60 * 60
            if maximum <= minimum:
                raise ValueError('作業即將截止，已沒有可安排的提醒時間。')
            stamp = lambda value: datetime.fromtimestamp(value, TAIPEI_TZ).strftime('%Y-%m-%dT%H:%M')
            picker = {'type': 'datetimepicker', 'label': '選擇日期與時間', 'mode': 'datetime',
                'data': self.token(owner, target, 'set', key), 'min': stamp(minimum), 'max': stamp(maximum),
                'initial': stamp(min(now + 3600, maximum))}
            return self.reply(f"{notification_text(item['title'], 160)}\n{notification_text(item.get('course_title'), 100)}\n\n請點下方「選擇日期與時間」。以台灣時間設定，且須早於作業截止。",
                [picker, self.postback(owner, target, '重選作業', 'list')])
        value = params.get('datetime') if isinstance(params, dict) else None
        if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}', value):
            raise ValueError('請使用日期時間選擇器重新設定提醒。')
        try:
            start = int(TAIPEI_TZ.localize(datetime.strptime(value.upper(), '%Y-%m-%dT%H:%M')).timestamp())
        except ValueError:
            raise ValueError('日期與時間無效，請重新選擇。') from None
        # Same account, assignment and minute share one plan across retries and fresh menus.
        request_id = f'line-menu:{start}'
        plan_id = digest(f'{key}:{request_id}')
        old = self.storage.assignment_action_records(owner, work_plans).get(plan_id)
        if not old:
            if not self.storage.consume_security_limit(f'assignment-plan:{owner}', 30, 600):
                raise ValueError('操作過於頻繁，請稍後再試。')
            self.service.actions.schedule(owner, key, start, request_id=request_id, line_target_hash=digest(target))
        elif old['state'] == 'cancelled':
            raise ValueError('這筆提醒已取消，請選擇其他時間。')
        choices = []
        link = safe_assignment_url(item.get('url'))
        if link:
            choices.append({'type': 'uri', 'label': '開啟作業', 'uri': link})
        choices.extend([{'type': 'postback', 'label': '取消提醒', 'displayText': '取消這筆提醒',
                         'data': self.token(owner, target, 'cancel', plan_id, ttl=7 * 86400)},
                        self.postback(owner, target, '設定其他提醒', 'list')])
        stamp = datetime.fromtimestamp(start, TAIPEI_TZ).strftime('%Y/%m/%d %H:%M')
        return self.reply(f"{notification_text(item['title'], 160)}\n{'已設定' if not old else '已安排過'} {stamp}（台灣時間）的提醒。" + (f'\n\n開啟作業\n{link}' if link else ''), choices)
