"""LINE private attachment lookup, signed browsing, safe refresh and payload limits."""

import base64
import hashlib
import hmac
import json
import time
import unittest
from unittest.mock import patch

from sqlalchemy import update
from e3_tracker.platform.persistence.core_schema import web_sessions_table
from e3_tracker.assignments.domain.notifications import digest, validate_preferences
from e3_tracker.assignments.services.line_file_menu import LineFileMenu
from tests import test_assignment_notifications as fixtures


BASE = 'https://e3p.nycu.edu.tw'


def actions(value):
    result = []
    if isinstance(value, dict):
        if isinstance(value.get('action'), dict):
            result.append(value['action'])
        for child in value.values():
            result.extend(actions(child))
    elif isinstance(value, list):
        for child in value:
            result.extend(actions(child))
    return result


class LineFileTests(unittest.TestCase):
    setUp = fixtures.NotificationTests.setUp
    tearDown = fixtures.NotificationTests.tearDown

    def seed(self, username='student', count=8):
        files = self.app.extensions['e3_course_files']
        self.service.home_url = 'https://www.e3hwtool.space/'
        self.courses = [{'id': 42, 'title': '【115上】535650視訊壓縮 Video Compression', 'semester_key': '115-1', 'assignments': []},
            {'id': 43, 'title': '【115上】515612網路規劃', 'semester_key': '115-1', 'assignments': []},
            {'id': 99, 'title': '【114下】515612舊學期', 'semester_key': '114-2', 'assignments': []}]
        self.storage.save_user_cache(username, {'ts': time.time(), 'result': {'courses': self.courses, 'all_assignments': []}})
        attempt = files.storage.claim(username, '115-1', now=time.time() - 120)
        files.storage.finish(username, '115-1', attempt, {'course:42': {
            'kind': 'course', 'course_id': 42, 'title': '講義', 'url': BASE + '/course/view.php?id=42',
            'links': [{'title': f'HW {index} 規格', 'url': BASE + f'/pluginfile.php/42/HW_{index}.pdf'} for index in range(count)]}}, [])
        target = 'U' + 'a' * 32
        self.storage.consume_line_link_code(self.storage.create_line_link_code(username), target)
        self.storage.save_notification_preferences(username, validate_preferences({'line_enabled': False, 'new_assignment': False}))
        return LineFileMenu(self.storage, self.service, files, self.app), target

    def webhook(self, event, signature=True):
        body = json.dumps({'events': [event]}).encode()
        value = base64.b64encode(hmac.new(self.service.line_secret.encode(), body, hashlib.sha256).digest()).decode()
        return self.client.post('/api/notifications/line/webhook', data=body, content_type='application/json',
            headers={'X-Line-Signature': value if signature else 'invalid'})

    def test_course_picker_and_keyword_search_without_enabling_notifications(self):
        menu, target = self.seed()
        before = self.storage.notification_preferences('student')['preferences']
        response = menu.handle_text('找附件', target)
        self.assertEqual(response['type'], 'flex')
        self.assertIn('視訊壓縮', json.dumps(response, ensure_ascii=False))
        course = next(action for action in actions(response) if action['type'] == 'postback' and '視訊' in action['label'])
        response = menu.handle_postback(course['data'], target)
        self.assertIn('HW_0.pdf', json.dumps(response))
        self.assertNotIn('HW_7.pdf', json.dumps(response))
        response = menu.handle_text('找附件：ＨＷ－２', target)
        self.assertIn('HW_2.pdf', json.dumps(response))
        self.assertNotIn('HW_1.pdf', json.dumps(response))
        self.assertEqual(self.storage.notification_preferences('student')['preferences'], before)

    def test_pages_requery_current_account_and_keep_keyword_scope(self):
        menu, target = self.seed(count=12)
        first = menu.handle_text('找附件 規格', target)
        next_page = next(action for action in actions(first) if action['label'] == '下一頁')
        second = menu.handle_postback(next_page['data'], target)
        self.assertIn('下一頁', json.dumps(second, ensure_ascii=False))
        previous = next(action for action in actions(second) if action['label'] == '上一頁')
        self.assertEqual(menu.handle_postback(previous['data'], target)['altText'], first['altText'])
        self.assertIn('規格', second['altText'])

    def test_unlinked_guests_other_recipient_rebinding_tamper_and_expiry(self):
        menu, target = self.seed()
        token = menu.token('student', target, 'f', '115-1', '42')
        for invalid in [token + 'x', menu.token('student', target, 'f', '115-1', ttl=-1), 'lf:bad', 'lf:' + 'x' * 400]:
            with self.subTest(invalid=invalid[:20]), self.assertRaises(ValueError):
                menu.handle_postback(invalid, target)
        with self.assertRaises(ValueError): menu.handle_postback(token, 'U' + 'b' * 32)
        self.storage.unlink_line(username='student')
        with self.assertRaises(ValueError): menu.handle_text('找附件', target)
        self.storage.save_web_session('other-session', 'other', moodle_session='synthetic')
        self.storage.consume_line_link_code(self.storage.create_line_link_code('other'), target)
        with self.assertRaises(ValueError): menu.handle_postback(token, target)

    def test_only_the_bound_account_semester_and_courses_are_read(self):
        menu, target = self.seed()
        with self.assertRaises(ValueError):
            menu.handle_postback(menu.token('student', target, 'f', '113-1'), target)
        with self.assertRaises(ValueError):
            menu.handle_postback(menu.token('student', target, 'f', '115-1', '99'), target)
        self.storage.save_web_session('other-session', 'other', moodle_session='other-secret')
        self.storage.consume_line_link_code(self.storage.create_line_link_code('other'), 'U' + 'b' * 32)
        with patch.object(menu.files, 'library', wraps=menu.files.library) as library:
            menu.handle_text('找附件 規格', target)
        self.assertEqual(library.call_args.args[0], 'student')
        self.assertNotIn('other-secret', json.dumps(menu.handle_text('找附件', target)))

    def test_lookup_never_fetches_e3_or_marks_messages_read(self):
        menu, target = self.seed()
        with patch.object(menu.files, 'start', side_effect=AssertionError('Lookup must not sync')), \
             patch('e3_tracker.assignments.services.course_announcements.MoodleNewsClient.get', side_effect=AssertionError('No school calls')):
            response = menu.handle_text('找附件 視訊 HW2', target)
        encoded = json.dumps(response)
        self.assertIn('HW_2.pdf', encoded)
        self.assertNotIn('test-moodle', encoded)
        links = [action['uri'] for action in actions(response) if action['type'] == 'uri' and action['label'] == '開啟附件']
        self.assertEqual(len(links), 1)
        self.assertIn('openExternalBrowser=1', links[0])

    def test_refresh_uses_only_current_bound_session_and_runs_in_background(self):
        menu, target = self.seed()
        token = menu.token('student', target, 'r', '115-1', '42', 'HW2')
        with patch.object(menu.files, 'start', return_value='running') as start:
            response = menu.handle_postback(token, target)
        self.assertEqual(start.call_args.args[1], {'username': 'student', 'moodle_session': 'test-moodle'})
        self.assertEqual(start.call_args.args[2], '115-1')
        self.assertIn('正在更新', response['text'])
        self.assertNotIn('test-moodle', json.dumps(response))
        self.assertEqual(menu.files.storage.line_user('student', digest('U' + 'b' * 32)), None)

    def test_cached_files_still_work_after_login_expires_but_refresh_requires_login(self):
        menu, target = self.seed()
        with patch.object(menu.files.storage, 'line_user', return_value=None):
            self.assertIn('HW_2.pdf', json.dumps(menu.handle_text('找附件 HW2', target)))
            with self.assertRaisesRegex(ValueError, '登入已失效'):
                menu.handle_postback(menu.token('student', target, 'r', '115-1'), target)

    def test_refresh_rejects_expired_or_corrupt_session_credentials(self):
        menu, target = self.seed()
        for values in [{'expires_at': 1}, {'expires_at': time.time() + 3600, 'moodle_credential': 'enc:v1:invalid'}]:
            with self.subTest(values=values):
                with self.storage._engine.begin() as conn:
                    conn.execute(update(web_sessions_table).where(web_sessions_table.c.username == 'student').values(**values))
                self.assertIsNone(menu.files.storage.line_user('student', digest(target)))
                with self.assertRaisesRegex(ValueError, '登入已失效'):
                    menu.handle_postback(menu.token('student', target, 'r', '115-1'), target)

    def test_uri_limits_unicode_and_external_signatures(self):
        menu, target = self.seed()
        snapshot = menu.files.library('student', '115-1')
        item = snapshot['items'][0]
        for url in [BASE + '/pluginfile.php/42/講義.pdf',
                    'https://drive.google.com/file/view?signature=a%2Bb%3D&file=42',
                    'https://example.test/' + 'a' * 1100 + '.pdf']:
            item['url'] = url
            with self.subTest(url=url[:60]):
                response = menu.results('student', target, {**snapshot, 'items': [item]})
                uri = actions(response)[0]['uri']
                self.assertLessEqual(len(uri), 1000)
                self.assertTrue(uri.isascii())
                if 'signature=' in url:
                    self.assertEqual(uri, url, 'Signed external query must not be rewritten')
                if len(url) > 1000:
                    self.assertIn('/courses/files?', uri)

    def test_payload_bounds_unicode_queries_sensitive_input_and_arbitrary_commands(self):
        menu, target = self.seed()
        for query in ['講' * 60, '"' * 60, '\\' * 60, '😀' * 30]:
            data = menu.token('student', target, 'f', '115-1', '42', query, 2999)
            self.assertLessEqual(len(data.encode('utf-16-le')) // 2, 300)
            response = menu.handle_postback(data, target)
            self.assertLess(len(json.dumps(response).encode()), 30000)
        for text in ['找附件 ' + '😀' * 31, '找附件 MoodleSession=private']:
            with self.assertRaises(ValueError): menu.handle_text(text, target)
        self.assertIsNone(menu.handle_text('E3 synthetic-link-code', target))
        self.assertFalse(menu.matches('設定提醒'))
        self.assertFalse(menu.matches('綁定教學'))
        prompt = menu.handle_postback(menu.token('student', target, 'q', '115-1'), target)
        self.assertEqual(actions(prompt)[0]['inputOption'], 'openKeyboard')

    def test_limit_and_invalid_signed_contexts_do_not_leak_or_trigger_updates(self):
        menu, target = self.seed()
        for offset in [-1, True, 99999]:
            with self.subTest(offset=offset), self.assertRaises(ValueError):
                menu.handle_postback(menu.token('student', target, 'f', '115-1', offset=offset), target)
        with patch.object(self.storage, 'consume_security_limit', return_value=False):
            with self.assertRaisesRegex(ValueError, '頻繁'): menu.handle_text('找附件 HW2', target)

    def test_webhook_signatures_groups_redelivery_and_existing_reminder_shortcut(self):
        menu, target = self.seed()
        base = {'source': {'type': 'user', 'userId': target}, 'replyToken': 'synthetic'}
        event = {**base, 'type': 'message', 'message': {'type': 'text', 'text': '找附件 HW2'}}
        with patch.object(self.service, 'line_request') as reply:
            self.assertEqual(self.webhook(event).status_code, 200)
            self.assertEqual(reply.call_args.args[1]['messages'][0]['type'], 'flex')
            for extra in [{'source': {'type': 'group', 'userId': target}}, {'deliveryContext': {'isRedelivery': True}}]:
                self.assertEqual(self.webhook({**event, **extra}).status_code, 200)
            self.assertEqual(reply.call_count, 1)
            self.assertEqual(self.webhook(event, signature=False).status_code, 403)
            postback = {**base, 'type': 'postback', 'postback': {'data': menu.token('student', target, 'f', '115-1', '42')}}
            self.assertEqual(self.webhook(postback).status_code, 200)
            self.webhook({**postback, 'deliveryContext': {'isRedelivery': True}})
            self.assertEqual(reply.call_count, 2)
            self.assertEqual(self.webhook({**base, 'type': 'message', 'message': {'type': 'text', 'text': '設定提醒'}}).status_code, 200)
            self.assertTrue(any(action.get('text') == '找附件' for action in actions(reply.call_args.args[1]['messages'][0])))


if __name__ == '__main__':
    unittest.main()
