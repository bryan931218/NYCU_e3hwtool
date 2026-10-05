"""Own-account work planning and explicit confirmation of course-message suggestions."""

import hashlib
import hmac
import re
import time
from datetime import datetime
from urllib.parse import urlencode
from e3_tracker.platform.constants import TAIPEI_TZ

from e3_tracker.assignments.domain.assignment_actions import (
    assignment_hash, deadline_dates, effective_result, match_assignment, source_version, tonight_time, safe_assignment_url,
)
from e3_tracker.assignments.domain.notifications import active_assignments, digest
from e3_tracker.assignments.services.collector import annotate_result_semesters, current_semester_key
from e3_tracker.assignments.persistence.assignment_actions import deadline_proposals, work_plans


class AssignmentActions:
    def __init__(self, storage, notifications, calendar_sync):
        self.storage, self.notifications, self.calendar_sync = storage, notifications, calendar_sync

    def result(self, username):
        result = (self.storage.load_user_cache(username) or {}).get('result') or {}
        annotate_result_semesters(result)
        return effective_result(result, self.storage.personal_deadline_overrides(username), self.storage.assignment_uid)

    def items(self, username, semester=None):
        return active_assignments(self.result(username), semester or current_semester_key(), self.storage.assignment_uid,
            self.storage.load_user_preferences(username).get('ignored_assignment_uids', []))

    def item(self, username, key):
        if not isinstance(key, str) or not re.fullmatch('[0-9a-f]{64}', key):
            raise ValueError('作業資料無效。')
        item = self.items(username).get(key)
        if not item or (item.get('due_ts') and item['due_ts'] <= time.time()):
            raise ValueError('作業已完成、忽略或截止，無法安排提醒。')
        return item

    def proposals(self, username, kind, semester, message, *, notify=False):
        candidates = [item for item in self.items(username, semester).values() if str(item['course_id']) == str(message['course_id'])]
        if not candidates or 'content' not in message:
            return []
        version = source_version(message)
        matched = match_assignment(message, candidates)
        dates = deadline_dates(message, baseline_due=matched.get('original_due_ts', matched.get('due_ts')) if matched else None)
        if not dates:
            return []
        for suggestion in dates:
            if matched and suggestion['due_ts'] == matched.get('due_ts'):
                continue
            key = digest(f"{kind}:{semester}:{message['key']}:{version}:{suggestion['due_ts']}")
            payload = {**suggestion, 'source_version': version, 'kind': kind, 'semester': semester, 'message_key': message['key'],
                'course_id': message['course_id'], 'course_title': message['course_title'], 'source_title': message['title'],
                'matched_uid': assignment_hash(matched, self.storage.assignment_uid) if matched else '',
                'source_path': '/courses/messages?' + urlencode({'tab': kind, 'semester': semester, 'item': message['key']}),
                'created_at': time.time()}
            recent = semester == current_semester_key() and message.get('updated_ts') and time.time()-86400 < message['updated_ts'] <= time.time()+300
            self.storage.save_deadline_proposal(username, key, payload, notify=notify and bool(recent))
        records = self.storage.assignment_action_records(username, deadline_proposals)
        return [{**proposal, 'id': key, 'candidates': [{'uid': assignment_hash(item, self.storage.assignment_uid), 'title': item['title'],
            'due_ts': item.get('due_ts')} for item in candidates]} for key, proposal in records.items()
            if proposal['kind'] == kind and proposal['semester'] == semester and proposal['message_key'] == message['key']
            and proposal['source_version'] == version and proposal['state'] == 'pending']

    def confirm(self, username, key, item_key, due, original, *, google=False):
        proposal = self.storage.assignment_action_records(username, deadline_proposals).get(key)
        if not proposal or proposal['state'] != 'pending' or time.time()-proposal['created_at'] > 30*86400:
            raise ValueError('期限異動已失效或處理完成。')
        source = self.notifications.course_message_services[proposal['kind']]
        message = next((item for item in source.storage.load_course_announcements(username, proposal['semester'])['items']
                        if item['key'] == proposal['message_key']), None)
        if not message or source_version(message) != proposal['source_version']:
            raise ValueError('來源訊息已更新，請重新讀取。')
        item = self.items(username, proposal['semester']).get(item_key)
        if not item or str(item['course_id']) != str(proposal['course_id']) or item.get('due_ts') != original:
            raise ValueError('作業期限已變更，請重新讀取後確認。')
        validate_future_time(due)
        self.storage.confirm_personal_deadline(username, key, item_key, due, expected_source_version=proposal['source_version'], original_due=original)
        self.notifications.observe(username, self.result(username))
        calendar_error = self.sync_calendar(username, {**item, 'due_ts': due}) if google else ''
        return {'ok': True, 'calendar_error': calendar_error, 'message': '已更新個人期限。' + ('Google 日曆同步失敗，可重新嘗試。' if calendar_error else '')}

    def sync_calendar(self, username, item, *, plan=None):
        try:
            self.calendar_sync(username, item, plan)
        except Exception:
            return '請確認 Google 日曆已連結，或稍後重試同步。'
        return ''

    def schedule(self, username, key, start, minutes, *, request_id, google=False, line_job=None):
        item = self.item(username, key)
        validate_future_time(start)
        if type(minutes) is not int or minutes not in (15, 30, 45, 60, 90, 120):
            raise ValueError('請選擇有效的處理時間。')
        if item.get('due_ts') and start+minutes*60 > item['due_ts']:
            raise ValueError('安排的時間必須在截止時間之前。')
        plan_id = digest(f'{key}:{request_id}')
        plan = self.storage.schedule_assignment_plan(username, plan_id, item, key, start, minutes, line_job=line_job)
        if plan.get('duplicate') and (plan['start_ts'] != start or plan['minutes'] != minutes):
            raise ValueError('這次安排已提交，請重新開啟頁面。')
        error = self.sync_calendar(username, item, plan={**plan, 'id': plan_id}) if google else ''
        if google and not error:
            self.storage.mark_plan_calendar_synced(username, plan_id)
        return {'ok': True, 'id': plan_id, 'calendar_error': error, 'message': '提醒已安排。' + ('Google 日曆同步失敗，可重新嘗試同步。' if error else '')}

    def action_data(self, job, action='tonight', *, expires=None, start=None, minutes=None):
        expires = int(time.time()+7*86400) if expires is None else expires
        value = f"{action}:{job['id']}:{expires}"
        if action == 'plan':
            value += f':{start}:{minutes}'
        signature = hmac.new(self.notifications.line_secret.encode(), f"{value}:{job['target_hash']}".encode(), hashlib.sha256).hexdigest()
        return f'{value}:{signature}'

    def line_picker(self, job, due=None):
        now = time.time()
        minimum = int((now+90)//60)*60
        maximum = min(int(now+370*86400), int(due)-900 if due else int(now+370*86400))
        if maximum <= minimum:
            return None
        stamp = lambda value: datetime.fromtimestamp(value, TAIPEI_TZ).strftime('%Y-%m-%dT%H:%M')
        return {'type': 'datetimepicker', 'label': '安排處理時間', 'data': self.action_data(job, 'schedule'),
                'mode': 'datetime', 'min': stamp(minimum), 'max': stamp(maximum), 'initial': stamp(min(now+3600, maximum))}

    def handle_postback(self, data, target):
        return self.handle_line_postback(data, target)['text']

    def handle_line_postback(self, data, target, params=None):
        match = re.fullmatch(r'(tonight|schedule|plan):([0-9a-f]{64}):(\d{10})(?::(\d{10}):(15|30|45|60|90|120))?:([0-9a-f]{64})', data) if isinstance(data, str) else None
        if (not match or (match[1] == 'plan') != bool(match[4]) or int(match[3]) < time.time()
                or int(match[3]) > time.time()+7*86400+60):
            raise ValueError('此通知操作已失效，請回到網站安排提醒。')
        value = data.rsplit(':', 1)[0]
        target_hash = digest(target)
        expected = hmac.new(self.notifications.line_secret.encode(), f'{value}:{target_hash}'.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, match[6]):
            raise ValueError('無法驗證此通知。')
        found = self.storage.line_action_job(match[2], target_hash)
        if not found:
            raise ValueError('帳號綁定或通知已失效。')
        username, job = found
        delivery = self.storage.notification_delivery(job)
        if not delivery or delivery[2].get('kind') not in {'new', 'due', 'scheduled'} or delivery[2].get('custom_todo'):
            raise ValueError('此通知無法安排提醒。')
        key = delivery[2]['uid_hash']
        item = self.item(username, key)
        from e3_tracker.assignments.domain.notifications import notification_time, notification_text
        link = safe_assignment_url(item.get('url'))
        suffix = f'\n\n開啟作業\n{link}' if link else '\n\n作業列表\n' + self.notifications.home_url
        if match[1] == 'schedule':
            value = params.get('datetime') if isinstance(params, dict) else None
            if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}', value):
                raise ValueError('請重新點選安排處理時間，選擇日期與時間。')
            try:
                start = int(TAIPEI_TZ.localize(datetime.strptime(value.upper(), '%Y-%m-%dT%H:%M')).timestamp())
            except ValueError:
                raise ValueError('日期與時間無效，請重新選擇。') from None
            validate_future_time(start)
            durations = [minute for minute in (15, 30, 45, 60, 90, 120)
                         if not item.get('due_ts') or start+minute*60 <= item['due_ts']]
            if not durations:
                raise ValueError('預留時間至少 15 分鐘，且必須在截止前結束。請重新選擇。')
            choices = [{'type': 'action', 'action': {'type': 'postback', 'label': f'{minute} 分鐘',
                'displayText': f'預留 {minute} 分鐘', 'data': self.action_data(job, 'plan', expires=int(match[3]), start=start, minutes=minute)}}
                for minute in durations]
            return {'type': 'text', 'text': f"{notification_text(item['title'], 160)}\n開始：{notification_time(start)}（台灣時間）\n請選擇預留時間，點選後建立提醒。" + suffix,
                    'quickReply': {'items': choices}}
        start = int(match[4]) if match[4] else tonight_time(time.time(), item.get('due_ts'))
        minutes = int(match[5]) if match[5] else 15
        request_id = f'line:{job["id"]}' + (f':{start}' if match[4] else '')
        plan_id = digest(f'{key}:{request_id}')
        old = self.storage.assignment_action_records(username, work_plans).get(plan_id)
        if old:
            text = (f"已安排過 {notification_time(old['start_ts'])} 的提醒，預留 {old['minutes']} 分鐘。"
                    if old['state'] != 'cancelled' else '這則提醒已取消。')
        else:
            if not self.storage.consume_security_limit(f'assignment-plan:{username}', 30, 600):
                raise ValueError('操作過於頻繁，請稍後再試。')
            self.schedule(username, key, start, minutes, request_id=request_id, line_job=job)
            text = f'已安排 {notification_time(start)}（台灣時間）再提醒你，預留 {minutes} 分鐘。'
        response = {'type': 'text', 'text': text + suffix}
        choices = []
        if link:
            choices.append({'type': 'action', 'action': {'type': 'uri', 'label': '開啟作業', 'uri': link}})
        if choices:
            response['quickReply'] = {'items': choices}
        return response


def validate_future_time(value):
    if type(value) is not int or not time.time()+30 <= value <= time.time()+370*86400:
        raise ValueError('請選擇未來一年內的日期與時間。')
