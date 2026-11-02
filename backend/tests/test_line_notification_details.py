import json
import time
import unittest
from unittest.mock import patch

import requests

from e3_tracker.assignments.domain.line_notifications import line_detail_messages, chunks
from e3_tracker.assignments.domain.parsing import assignment_description
from tests import test_course_message_notifications as course_fixtures


def bubbles(messages):
    return [bubble for message in messages for bubble in
            (message['contents']['contents'] if message['contents']['type'] == 'carousel' else [message['contents']])]


class LineNotificationCardTests(unittest.TestCase):
    def payload(self, **detail):
        return {'kind': 'new', 'line_detail': {'title': '作業一：資料分析', 'course_title': '資料科學',
            'url': 'https://e3p.nycu.edu.tw/mod/assign/view.php?id=3',
            'content': '請完成以下項目：\n\n• 整理資料\n• 說明分析結果', **detail}}

    def test_description_uses_teacher_intro_not_private_submission_or_grade(self):
        detail = assignment_description('<div id="intro"><p>第一段</p><ul><li>條件一</li><li>條件二</li></ul>'
            '<a href="/pluginfile.php/2/mod_assign/introattachment/0/hw.pdf">題目.pdf</a>'
            '<script>secret-script</script></div><table><td>Private submission</td></table>',
            'https://e3p.nycu.edu.tw/mod/assign/view.php?id=3')
        self.assertIn('• 條件一', detail['content'])
        self.assertNotIn('Private submission', detail['content'])
        self.assertNotIn('secret-script', detail['content'])
        self.assertTrue(detail['links'][0]['url'].endswith('hw.pdf'))

    def test_readable_card_does_not_clip_text_and_has_direct_links(self):
        messages = line_detail_messages(self.payload(links=[{
            'title': '題目.pdf', 'url': 'https://e3p.nycu.edu.tw/pluginfile.php/2/mod_assign/introattachment/0/hw.pdf'}]))
        cards = bubbles(messages)
        self.assertEqual(messages[0]['type'], 'flex')
        self.assertIn('整理資料', json.dumps(cards, ensure_ascii=False))
        for card in cards:
            for node in card['body']['contents']:
                if node['type'] == 'text':
                    self.assertTrue(node['wrap'])
                    self.assertNotIn('maxLines', node)
        self.assertEqual(cards[-1]['footer']['contents'][0]['action']['uri'], self.payload()['line_detail']['url'])

    def test_long_emoji_body_and_twenty_long_links_fit_one_push_without_losing_body(self):
        content = '🧑' * 20000
        links = [{'title': '附件' * 100, 'url': 'https://files.example.test/' + 'a' * 1900 + str(i)} for i in range(20)]
        messages = line_detail_messages(self.payload(content=content, links=links, title='題目' * 250))
        self.assertLessEqual(len(messages), 5)
        cards = bubbles(messages)
        actual = ''.join(node['text'] for card in cards for node in card['body']['contents'] if 'lineSpacing' in node)
        self.assertEqual(actual, content)
        for message in messages:
            self.assertLess(len(json.dumps(message['contents'], ensure_ascii=False).encode()), 50000)
        for card in cards:
            self.assertLess(len(json.dumps(card, ensure_ascii=False).encode()), 30000)
            for node in card['body']['contents']:
                if node.get('type') == 'text':
                    self.assertLessEqual(len(node['text'].encode('utf-16-le')) // 2, 2000)

    def test_secret_urls_and_submission_files_are_not_exposed_and_quick_reply_is_last(self):
        urls = ['javascript:alert(1)', 'https://evil.test/?access_token=secret',
                'https://e3p.nycu.edu.tw/pluginfile.php/2/assignsubmission_file/submission/1/private.pdf']
        messages = line_detail_messages(self.payload(content='很長的內容' * 1000,
            links=[{'title': 'secret', 'url': url} for url in urls]), {'items': [
                {'type': 'action', 'action': {'type': 'uri', 'label': 'unsafe', 'uri': urls[1]}},
                {'type': 'action', 'action': {'type': 'postback', 'label': '提醒', 'data': 'signed-test'}}]})
        encoded = json.dumps(messages)
        self.assertNotIn('secret', encoded)
        self.assertNotIn('assignsubmission', encoded)
        self.assertTrue(all('quickReply' not in message for message in messages[:-1]))
        self.assertEqual(len(messages[-1]['quickReply']['items']), 1)


class LineDetailDeliveryTests(unittest.TestCase):
    setUp = course_fixtures.CourseMessageNotificationTests.setUp
    tearDown = course_fixtures.CourseMessageNotificationTests.tearDown
    enable = course_fixtures.CourseMessageNotificationTests.enable
    refresh = course_fixtures.CourseMessageNotificationTests.refresh
    item = course_fixtures.CourseMessageNotificationTests.item
    job_rows = course_fixtures.CourseMessageNotificationTests.job_rows

    def test_line_fetches_full_message_once_browser_stays_summary_only(self):
        self.enable()
        self.refresh('mail', [])
        item = self.item()
        del item['content']
        self.refresh('mail', [item])
        source = self.service.course_message_services['mail']
        with patch.object(source, 'content', return_value={'content': '完整內文\n第二段', 'links': []}) as fetch, \
                patch.object(self.service, 'line_request') as line, patch('pywebpush.webpush') as browser:
            self.service.dispatch(now=time.time() + 1)
        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(fetch.call_args.args[0]['username'], 'student')
        self.assertIn('完整內文', json.dumps(line.call_args.args[1], ensure_ascii=False))
        self.assertNotIn('完整內文', browser.call_args.kwargs['data'])
        self.assertNotIn('line_detail', browser.call_args.kwargs['data'])
        self.assertEqual({row['state'] for row in self.job_rows()}, {'sent'})
        self.assertTrue(all('完整內文' not in row['payload'] for row in self.job_rows()))

    def test_cached_full_body_survives_expired_session(self):
        self.enable()
        self.refresh('announcements', [])
        self.refresh('announcements', [self.item(content='已快取的完整內容')])
        with patch.object(self.storage, 'notification_content_user', return_value=None), \
                patch.object(self.service, 'line_request') as line, patch('pywebpush.webpush'):
            self.service.dispatch(now=time.time() + 1)
        self.assertIn('已快取的完整內容', json.dumps(line.call_args.args[1], ensure_ascii=False))

    def test_transient_body_failure_retries_and_does_not_mark_line_job_sent(self):
        self.enable()
        self.refresh('mail', [])
        item = self.item()
        del item['content']
        self.refresh('mail', [item])
        source = self.service.course_message_services['mail']
        with patch.object(source, 'content', side_effect=requests.Timeout), \
                patch.object(self.service, 'line_request') as line, patch('pywebpush.webpush'):
            self.service.dispatch(now=time.time() + 1)
            line.assert_not_called()
        self.assertEqual(next(row['state'] for row in self.job_rows() if row['channel'] == 'line'), 'pending')
        with patch.object(source, 'content', side_effect=requests.Timeout), \
                patch.object(self.service, 'line_request') as line, patch('pywebpush.webpush'):
            for _ in range(2):
                job = next(row for row in self.job_rows() if row['channel'] == 'line')
                self.service.dispatch(now=job['retry_at'] + 1)
        self.assertEqual(line.call_count, 1)
        self.assertIn('暫時無法取得內文', json.dumps(line.call_args.args[1], ensure_ascii=False))

    def test_content_lookup_is_owner_scoped_without_consuming_polling_cooldown(self):
        self.enable()
        now = time.time()
        owner = self.storage.notification_content_user('student', now)
        self.assertEqual(owner['username'], 'student')
        self.assertIsNone(self.storage.notification_content_user('unknown', now))
        self.assertIsNotNone(self.storage.notification_sync_user('student', now))
        self.assertIsNotNone(self.storage.notification_content_user('student', now))

    def test_unlink_during_body_fetch_cancels_delivery(self):
        self.enable()
        self.refresh('mail', [])
        item = self.item()
        del item['content']
        self.refresh('mail', [item])
        def content(*args):
            self.storage.unlink_line(username='student')
            return {'content': '不得送出的內容', 'links': []}
        with patch.object(self.service.course_message_services['mail'], 'content', side_effect=content), \
                patch.object(self.service, 'line_request') as send, patch('pywebpush.webpush'):
            self.service.dispatch(now=time.time() + 1)
        send.assert_not_called()

    def test_legacy_assignment_body_uses_only_validated_e3_page_and_owners_session(self):
        with patch('e3_tracker.assignments.services.course_announcements.MoodleNewsClient') as client:
            client.return_value.get.return_value = '<div id="intro">補抓作業說明</div>'
            result = self.service._assignment_description('student', {'url': 'https://e3p.nycu.edu.tw/mod/assign/view.php?id=12&sesskey=secret'}, time.time())
        self.assertIn('補抓作業說明', result['content'])
        self.assertEqual(client.return_value.get.call_args.args[0], 'https://e3p.nycu.edu.tw/mod/assign/view.php?id=12')
        client.return_value.close.assert_called_once()
