"""LINE menu scheduling without a prior push; signed, scoped and idempotent."""
import base64
import hashlib
import hmac
import json
import unittest
from datetime import datetime
from unittest.mock import patch

from sqlalchemy import select
from tests import test_assignment_actions as actions, test_assignment_notifications as fixtures
from e3_tracker.assignments.domain.notifications import digest, validate_preferences
from e3_tracker.assignments.persistence.assignment_actions import work_plans
from e3_tracker.assignments.persistence.notification_schema import notification_jobs as jobs
from e3_tracker.assignments.services.line_reminder_menu import LineReminderMenu
from e3_tracker.platform.constants import TAIPEI_TZ


class LineReminderMenuTests(unittest.TestCase):
    setUp = fixtures.NotificationTests.setUp
    tearDown = fixtures.NotificationTests.tearDown
    item = fixtures.NotificationTests.item
    result = fixtures.NotificationTests.result
    seed = actions.AssignmentActionTests.seed

    def menu(self, enabled=True):
        item, key = self.seed()
        target = 'U' + 'a' * 32
        self.storage.consume_line_link_code(self.storage.create_line_link_code('student'), target)
        self.storage.save_notification_preferences('student', validate_preferences({
            'line_enabled': enabled, 'new_assignment': False, 'days_before': [7, 2],
        }))
        return LineReminderMenu(self.storage, self.service), item, key, target

    def choices(self, response):
        return [entry['action'] for entry in response['quickReply']['items']]

    def schedule(self, menu, key, target, start=None):
        start = ((self.now + 3600) // 60) * 60 if start is None else start
        value = datetime.fromtimestamp(start, TAIPEI_TZ).strftime('%Y-%m-%dT%H:%M')
        return menu.handle_postback(menu.token('student', target, 'set', key), target, {'datetime': value})

    def webhook(self, event):
        body = json.dumps({'events': [event]}).encode()
        signature = base64.b64encode(hmac.new(self.service.line_secret.encode(), body, hashlib.sha256).digest()).decode()
        return self.client.post('/api/notifications/line/webhook', data=body, content_type='application/json',
                                headers={'X-Line-Signature': signature})

    def test_menu_picker_schedule_and_delivery_without_prior_notification(self):
        menu, item, key, target = self.menu()
        response = menu.handle_text('設定作業提醒', target)
        self.assertIn('HW1', response['text'])
        self.assertIn('台灣時間', response['text'])
        picked = menu.handle_postback(self.choices(response)[0]['data'], target)
        picker = self.choices(picked)[0]
        self.assertEqual(picker['type'], 'datetimepicker')
        self.assertLess(picker['min'], picker['max'])
        self.assertTrue(picker['min'] <= picker['initial'] <= picker['max'])
        self.assertLess(len(picker['data']), 300)
        response = self.schedule(menu, key, target)
        self.assertIn('已設定', response['text'])
        self.assertIn(item['url'], response['text'])
        self.assertNotIn('預留', response['text'])
        with self.storage._engine.connect() as conn:
            self.assertEqual(conn.execute(select(jobs)).mappings().all()[0]['channel'], 'line')
        start = ((self.now + 3600) // 60) * 60
        with patch.object(self.service, 'line_request') as send:
            self.service.dispatch(now=start + 1)
            self.assertEqual(send.call_count, 1)
            self.assertEqual(send.call_args.args[1]['to'], target)
            self.assertIn(item['url'], send.call_args.args[1]['messages'][0]['text'])

    def test_retries_fresh_menus_and_cancel_are_idempotent(self):
        menu, _, key, target = self.menu()
        response = self.schedule(menu, key, target)
        self.assertIn('已安排過', self.schedule(menu, key, target)['text'])
        self.assertEqual(len(self.storage.assignment_action_records('student', work_plans)), 1)
        cancel = next(c['data'] for c in self.choices(response) if c['label'] == '取消提醒')
        for _ in range(2):
            self.assertIn('已取消', menu.handle_postback(cancel, target)['text'])
        with self.assertRaises(ValueError):
            self.schedule(menu, key, target)
        with patch.object(self.service, 'deliver') as send:
            self.service.dispatch(now=self.now + 3601)
            send.assert_not_called()

    def test_line_enable_is_explicit_and_preserves_other_preferences(self):
        menu, _, _, target = self.menu(enabled=False)
        before = self.storage.notification_preferences('student')['preferences']
        response = menu.handle_text('設定提醒', target)
        self.assertIn('目前關閉', response['text'])
        self.assertFalse(self.storage.notification_preferences('student')['preferences']['line_enabled'])
        response = menu.handle_postback(self.choices(response)[0]['data'], target)
        after = self.storage.notification_preferences('student')['preferences']
        self.assertEqual(after, {**before, 'line_enabled': True})
        self.assertIn('HW1', response['text'])

    def test_invalid_signature_expiry_unlinked_and_other_recipient(self):
        menu, _, key, target = self.menu()
        token = menu.token('student', target, 'pick', key)
        for invalid in (None, token[:-1] + ('0' if token[-1] != '0' else '1'),
                        menu.token('student', target, 'pick', key, ttl=-1)):
            with self.subTest(data=invalid), self.assertRaises(ValueError):
                menu.handle_postback(invalid, target)
        with self.assertRaises(ValueError):
            menu.handle_postback(token, 'U' + 'b' * 32)
        self.storage.unlink_line(username='student')
        with self.assertRaises(ValueError):
            menu.handle_postback(token, target)
        self.storage.save_web_session('other-session', 'other', moodle_session='synthetic')
        self.storage.consume_line_link_code(self.storage.create_line_link_code('other'), target)
        with self.assertRaises(ValueError):
            menu.handle_postback(token, target)

    def test_atomic_schedule_rechecks_binding_and_channel(self):
        menu, _, key, target = self.menu()
        original = self.storage.schedule_assignment_plan
        def changed(*args, **kwargs):
            self.storage.unlink_line(username='student')
            return original(*args, **kwargs)
        with patch.object(self.storage, 'schedule_assignment_plan', side_effect=changed):
            with self.assertRaises(ValueError):
                self.schedule(menu, key, target)
        self.assertEqual(self.storage.assignment_action_records('student', work_plans), {})

    def test_dates_deadline_changes_and_completed_assignments_are_rechecked(self):
        menu, item, key, target = self.menu()
        token = menu.token('student', target, 'set', key)
        for value in ('2026-13-01T12:00', '2026-02-30T12:00', '2026-10-09T12:00+08:00', None, 123):
            with self.subTest(value=value), self.assertRaises(ValueError):
                menu.handle_postback(token, target, {'datetime': value})
        with self.assertRaises(ValueError):
            self.schedule(menu, key, target, self.now - 3600)
        self.storage.set_personal_deadline('student', key, self.now + 1800)
        with self.assertRaises(ValueError):
            self.schedule(menu, key, target)
        self.storage.save_user_cache('student', {'ts': self.now, 'result': self.result({**item, 'completed': True})})
        self.assertIn('沒有可安排', menu.handle_text('設定提醒', target)['text'])
        with self.assertRaises(ValueError):
            menu.handle_postback(token, target, {'datetime': '2027-01-01T12:00'})

    def test_list_pages_and_line_payload_limits(self):
        menu, _, _, target = self.menu()
        items = [self.item(f'HW{index} ' + 'x' * 180) for index in range(12)]
        self.storage.save_user_cache('student', {'ts': self.now, 'result': self.result(*items)})
        first = menu.handle_text('安排提醒', target)
        choices = self.choices(first)
        self.assertEqual(len(choices), 9)
        self.assertTrue(all(len(c['label']) <= 20 and len(c['data']) < 300 for c in choices))
        self.assertLess(len(first['text']), 5000)
        second = menu.handle_postback(choices[-1]['data'], target)
        self.assertIn('9–12／12', second['text'])
        self.assertEqual(self.choices(second)[-1]['label'], '上一頁')

    def test_list_excludes_graded_ignored_past_and_old_semester(self):
        menu, _, _, target = self.menu()
        ignored = self.item('Ignored')
        items = [self.item('Pending'), self.item('Graded', grade_text='85'),
                 self.item('Completed', completed=True), self.item('Past', days=-1),
                 self.item('Old semester', course_id=2, course_title='1141.Other', semester_key='1141'), ignored]
        result = self.result(*items)
        old = items[-2]
        result['courses'][0]['assignments'] = [item for item in items if item is not old]
        result['courses'].append({'id': 2, 'title': '1141.Other', 'semester_key': '1141', 'assignments': [old]})
        self.storage.save_user_cache('student', {'ts': self.now, 'result': result})
        self.storage.save_user_preferences('student', {'ignored_assignment_uids': [
            self.storage.assignment_uid(ignored['course_id'], ignored['title'], ignored['url'])]})
        response = menu.handle_text('設定提醒', target)
        self.assertIn('1–1／1', response['text'])
        self.assertIn('Pending', response['text'])
        self.assertEqual(len(self.choices(response)), 1)

    def test_near_deadline_picker_never_has_equal_min_and_max(self):
        menu, _, key, target = self.menu()
        with patch('e3_tracker.assignments.services.line_reminder_menu.time.time', return_value=self.now):
            self.storage.set_personal_deadline('student', key, ((self.now + 90) // 60) * 60 + 20)
            with self.assertRaises(ValueError):
                menu.handle_postback(menu.token('student', target, 'pick', key), target)

    def test_webhook_commands_postbacks_redelivery_and_group_isolation(self):
        menu, _, key, target = self.menu()
        base = {'source': {'type': 'user', 'userId': target}, 'replyToken': 'synthetic'}
        with patch.object(self.service, 'line_request') as reply:
            self.assertEqual(self.webhook({**base, 'type': 'message',
                'message': {'type': 'text', 'text': '設定作業提醒'}}).status_code, 200)
            self.assertIn('HW1', reply.call_args.args[1]['messages'][0]['text'])
            selected = datetime.fromtimestamp(self.now + 3600, TAIPEI_TZ).strftime('%Y-%m-%dT%H:%M')
            event = {**base, 'type': 'postback', 'postback': {
                'data': menu.token('student', target, 'set', key), 'params': {'datetime': selected}}}
            self.assertEqual(self.webhook(event).status_code, 200)
            self.assertIn('已設定', reply.call_args.args[1]['messages'][0]['text'])
            self.assertEqual(self.webhook({**event, 'deliveryContext': {'isRedelivery': True}}).status_code, 200)
            self.assertEqual(reply.call_count, 2)
            self.webhook({**event, 'source': {'type': 'group', 'userId': target}})
            self.assertEqual(reply.call_count, 2)
        self.assertEqual(len(self.storage.assignment_action_records('student', work_plans)), 1)

    def test_disabled_channel_and_completion_stop_delivery(self):
        menu, item, key, target = self.menu()
        self.schedule(menu, key, target)
        self.storage.save_user_cache('student', {'ts': self.now, 'result': self.result({**item, 'completed': True})})
        with patch.object(self.service, 'line_request') as send:
            self.service.dispatch(now=self.now + 3601)
            send.assert_not_called()


if __name__ == '__main__':
    unittest.main()
