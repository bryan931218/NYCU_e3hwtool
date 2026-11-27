"""Password/API accounts retain the existing message, file, and notification flows."""
import json
import os
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

import requests

from e3_tracker.assignments.services.collector import current_semester_key
from e3_tracker.assignments.services.course_announcements import (
    AnnouncementSessionExpired, collect_course_announcements, news_client,
)
from e3_tracker.assignments.services.course_mail import collect_course_mail
from e3_tracker.assignments.services.course_files import CourseFileService
from e3_tracker.assignments.services.course_insight_sources import course_sources
from e3_tracker.assignments.services.notifications import NotificationService
from e3_tracker.assignments.domain.course_announcements import discussion_content, moodle_url
from e3_tracker.assignments.domain.course_files import file_index
from e3_tracker.assignments.domain.course_mail import mail_content
from e3_tracker.platform.application import create_app
from tests.security_helpers import csrf_client

BASE, USER, TOKEN = 'https://e3p.nycu.edu.tw', '114550001', 't' * 32
FILE = BASE + '/webservice/pluginfile.php/100/mod_folder/content/0/slides.pdf'


class ImmediateThread:
    def __init__(self, *, target, args, **kwargs):
        self.target, self.args = target, args

    def start(self):
        self.target(*self.args)


class MobileCourseFeaturesTests(unittest.TestCase):
    def setUp(self):
        key = current_semester_key()
        year, term = key.split('-')
        self.course = {'id': 10, 'title': f'【{year}{"上" if term == "1" else "下"}】課程', 'semester_key': key,
            'assignments': [{'title': 'HW', 'url': BASE + '/mod/assign/view.php?id=456', 'course_id': 10}]}
        self.user = {'username': USER, 'moodle_token': TOKEN, 'moodle_user_id': 42}
        self.calls = []
        self.api_failure = None
        self.foreign = False
        self.modules = [{'id': 101, 'name': '資料夾', 'modname': 'folder', 'contents': [{'fileurl': FILE, 'filename': 'slides.pdf'}]},
            {'id': 456, 'name': 'HW', 'modname': 'assign'},
            {'id': 105, 'name': '公告', 'modname': 'forum'}, {'id': 106, 'name': '課程討論區', 'modname': 'forum'}]

    def respond(self, sess, method, url, **kwargs):
        self.assertEqual(method, 'POST')
        self.assertEqual(url, BASE + '/webservice/rest/server.php')
        self.assertFalse(kwargs['allow_redirects'])
        data = kwargs['data']
        function = data['wsfunction']
        self.calls.append(function)
        self.assertNotIn(TOKEN, url)
        if function == 'core_webservice_get_site_info':
            result = {'userid': 42, 'username': USER, 'siteurl': BASE, 'fullname': '王小明'}
        elif function == 'core_course_get_contents':
            self.assertEqual(data['courseid'], 10)
            result = [{'modules': self.modules}]
        elif function == 'mod_forum_get_forums_by_courses':
            result = [{'id': 50, 'cmid': 105, 'course': 10, 'type': 'news'},
                      {'id': 51, 'cmid': 106, 'course': 10, 'type': 'general'}]
        elif function == 'mod_forum_get_forum_discussions':
            self.assertEqual(data['forumid'], 50)
            result = {'discussions': [{'id': 8, 'discussion': 7, 'name': '期中公告', 'userfullname': '老師', 'timemodified': 1700000000}]}
        elif function == 'mod_forum_get_discussion_posts':
            result = {'courseid': 11 if self.foreign else 10, 'posts': [
                {'id': 8, 'discussionid': 7, 'hasparent': False, 'author': {'fullname': '老師'},
                 'message': '<p>公告內文</p><script>stolen()</script>', 'attachments': [{'fileurl': FILE, 'filename': 'slides.pdf'}]},
                {'id': 9, 'discussionid': 7, 'hasparent': True, 'message': 'private reply must stay private'}]}
        elif function == 'local_dcpcmail_get_index':
            self.assertEqual((data['type'], data['itemid'], data['limit']), ('course', 10, 30))
            result = {'totalcount': 1, 'messages': [{'id': 9, 'course': {'id': 10}, 'subject': '信件主旨',
                'sender': {'fullname': '老師'}, 'unread': True, 'time': 1700000000}]}
        elif function == 'local_dcpcmail_get_message':
            self.assertEqual(data['id'], 9)
            result = {'id': 9, 'course': {'id': 11 if self.foreign else 10}, 'content': '<p>信件內文</p>',
                      'attachments': [{'fileurl': FILE, 'filename': 'slides.pdf'}], 'references': [{'content': 'private reference'}]}
        elif function == 'mod_assign_get_assignments':
            result = {'courses': [{'id': 10, 'assignments': [{'id': 123, 'cmid': 456, 'intro': '<p>作業說明</p>',
                'introattachments': [{'fileurl': FILE, 'filename': 'slides.pdf'}]}]}]}
        elif function == 'mod_page_get_pages_by_courses':
            self.assertEqual(data['courseids[0]'], 10)
            result = {'pages': [
                {'course': 10, 'coursemodule': 118, 'content': '<p>Final project: 40%.</p>'},
                {'course': 11, 'coursemodule': 118, 'content': '<p>Unrelated content.</p>'},
            ]}
        else:
            self.fail('Unapproved read function: ' + function)
        if self.api_failure:
            result = self.api_failure
        response = requests.Response()
        response.status_code, response.url = 200, url
        response.json = lambda: result
        return response

    def mocked(self):
        return patch('e3_tracker.assignments.services.mobile_auth.safe_request', side_effect=self.respond)

    def client(self):
        return news_client(BASE, self.user, courses=[self.course])

    def test_existing_parsers_read_messages_and_safe_attachments_without_a_web_session(self):
        with self.mocked():
            client = self.client()
            try:
                news, successful, failed = collect_course_announcements(client, [self.course])
                self.assertEqual((len(news), successful, failed), (1, [10], []))
                self.assertEqual(news[0]['key'], '10:7')
                detail = discussion_content(client.get(news[0]['url']), BASE)
                self.assertEqual(detail['content'], '公告內文')
                self.assertEqual(detail['links'][0]['url'], FILE.replace('/webservice/', '/'))
                mail, successful, failed = collect_course_mail(client, [self.course])
                self.assertEqual((len(mail), successful, failed), (1, [10], []))
                self.assertFalse(mail[0]['e3_read'])
                body = mail_content(client.get(mail[0]['url']), BASE)
                self.assertEqual(body['content'], '信件內文')
                self.assertNotIn('private reference', json.dumps(body))
                self.assertEqual(len(body['links']), 1)
                self.assertNotIn(TOKEN, json.dumps([news, detail, mail, body]))
                self.assertEqual(self.calls.count('core_webservice_get_site_info'), 1)
                self.assertFalse(any('set_' in f or 'view_' in f or 'send_' in f for f in self.calls))
            finally:
                client.close()

    def test_course_insights_can_read_module_pages_without_forum_access(self):
        self.modules = [{'id': 118, 'name': '評分標準', 'modname': 'page'}]
        with self.mocked():
            client = self.client()
            try:
                sources, partial, document = course_sources(client, self.user, self.course, {}, [])
            finally:
                client.close()
        self.assertTrue(document)
        self.assertFalse(partial)
        self.assertEqual(sources[0]['text'], 'Final project: 40%.')
        self.assertIn('mod_page_get_pages_by_courses', self.calls)
        self.assertNotIn('mod_forum_get_forums_by_courses', self.calls)

    def test_file_collection_keeps_all_origins_and_does_not_open_unread_mail(self):
        with self.mocked():
            client = self.client()
            news_service = Mock()
            news_service.item_url = lambda item: moodle_url(item['url'], BASE, '/mod/forum/discuss.php', 'd')
            news_service.parse_content = lambda html: discussion_content(html, BASE)
            service = CourseFileService(Mock(), BASE, news_service)
            try:
                news, _, _ = collect_course_announcements(client, [self.course])
                mail, _, _ = collect_course_mail(client, [self.course])
                messages = {'announcements': news, 'mail': mail}
                sources, pending, error, successful = service.collect(client, [self.course], messages, {'sources': {}, 'pending': []})
                self.assertEqual(pending, [])
                self.assertFalse(error)
                self.assertGreater(successful, 0)
                files = file_index(sources, [self.course], messages, BASE)
                self.assertEqual(len(files), 1)
                self.assertEqual({o['kind'] for o in files[0]['origins']}, {'course', 'assignment', 'announcements'})
                self.assertNotIn('local_dcpcmail_get_message', self.calls)
            finally:
                client.close()

    def test_cross_course_messages_foreign_hosts_and_expired_credentials_fail_closed(self):
        with self.mocked():
            client = self.client()
            try:
                self.foreign = True
                for url in [BASE + '/mod/forum/discuss.php?d=7', BASE + '/local/dcpcmail/view.php?c=10&m=9',
                            'https://foreign.example/course/view.php?id=10', BASE + '/local/dcpcmail/view.php?c=11&m=9',
                            BASE + '/mod/assign/view.php?id=1&id=456']:
                    with self.assertRaises(ValueError):
                        client.get(url)
                self.api_failure = {'exception': 'secret', 'errorcode': 'invalidtoken', 'message': 'secret'}
                with self.assertRaises(AnnouncementSessionExpired):
                    client.get(BASE + '/mod/forum/discuss.php?d=7')
            finally:
                client.close()

    def test_existing_routes_read_mark_and_index_api_data_and_notifications_fetch_assignment_content(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
            'E3_ENV': 'development', 'E3_CACHE_DIR': directory, 'E3_DATABASE_URL': '', 'DATABASE_URL': '',
            'E3_CANONICAL_HOST': '', 'E3_SESSION_COOKIE_SECURE': '0', 'E3_BASE_URL': BASE,
            'E3_NOTIFICATIONS_WORKER': '0', 'E3_SESSION_PROFILE_WORKER': '0', 'E3_YOUTUBE_AUTO_SYNC_ENABLED': '0',
        }):
            app = create_app()
            storage = app.extensions['e3_storage']
            try:
                storage.save_web_session('test-api', USER, moodle_token=TOKEN, moodle_user_id=42)
                storage.save_user_cache(USER, {'ts': time.time(), 'result': {'courses': [self.course], 'all_assignments': []}})
                client = csrf_client(app)
                with client.session_transaction() as session:
                    session['session_token'] = 'test-api'
                semester = current_semester_key()
                with self.mocked(), patch('e3_tracker.assignments.services.course_announcements.threading.Thread', ImmediateThread):
                    for endpoint, key, text in [('course-announcements', '10:7', '公告內文'), ('course-mail', '10:9', '信件內文')]:
                        response = client.post(f'/api/{endpoint}/refresh', json={'semester': semester})
                        self.assertEqual(response.status_code, 202)
                        items = client.get(f'/api/{endpoint}?semester={semester}').json['items']
                        self.assertEqual(len(items), 1)
                        payload = {'semester': semester, 'key': key, 'read': True}
                        detail = client.post(f'/api/{endpoint}/item', json=payload)
                        self.assertEqual(detail.status_code, 200)
                        self.assertEqual(detail.json['item']['content'], text)
                        calls = len(self.calls)
                        self.assertEqual(client.post(f'/api/{endpoint}/item', json={**payload, 'read': False}).status_code, 200)
                        self.assertEqual(calls, len(self.calls))
                    response = client.post('/api/course-files/refresh', json={'semester': semester})
                    self.assertEqual(response.status_code, 202)
                    files = client.get(f'/api/course-files?semester={semester}').json
                    self.assertTrue(files['items'])
                    self.assertFalse(files['error'])
                    notifications = NotificationService(storage, app_home_url='https://tracker.example/')
                    notifications.course_message_services = {'announcements': app.extensions['e3_course_announcements']}
                    description = notifications._assignment_description(USER, self.course['assignments'][0], time.time())
                    self.assertIn('作業說明', json.dumps(description, ensure_ascii=False))
                    self.assertNotIn(TOKEN, json.dumps([files, description]))
                    self.assertEqual(client.get('/api/profile?refresh=1').status_code, 200)
            finally:
                storage._engine.dispose()
