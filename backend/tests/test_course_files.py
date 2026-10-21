"""Attachment discovery, privacy boundaries, resumable indexing and cache lifecycle."""

import os
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from sqlalchemy import delete, select

from e3_tracker.assignments.domain.course_files import (
    assignment_files, course_display_name, course_materials, file_index, file_metadata, folder_files, safe_file_link,
)
from e3_tracker.assignments.persistence.cleanup import delete_assignment_account_data
from e3_tracker.assignments.persistence.course_files import course_file_cache
from e3_tracker.assignments.services.course_announcements import AnnouncementSessionExpired, MoodleNewsClient
from e3_tracker.platform.application import create_app
from e3_tracker.platform.persistence.core_schema import users_table
from tests.security_helpers import csrf_client


BASE = 'https://e3p.nycu.edu.tw'
COURSE = {'id': 42, 'title': '【115上】作業系統', 'semester_key': '115-1', 'assignments': []}
PDF = {'url': BASE + '/pluginfile.php/42/mod_resource/content/1/lecture%201.pdf', 'title': '課程講義'}
NEWS = {'key': '42:7', 'id': '7', 'course_id': 42, 'semester': '115-1', 'course_title': COURSE['title'],
        'title': '期中報告格式', 'url': BASE + '/mod/forum/discuss.php?d=7', 'updated_ts': 100, 'read_at': 0}
SOURCE = {'kind': 'course', 'course_id': 42, 'title': COURSE['title'], 'url': BASE + '/course/view.php?id=42', 'links': [PDF]}
EMPTY = {'sources': {}, 'pending': []}


class FileParsingTests(unittest.TestCase):
    def test_only_safe_links_and_readonly_moodle_resource_urls(self):
        for url in ['javascript:alert(1)', 'data:text/html,hi', 'http://unsafe.test/f.pdf',
                    'https://user:secret@example.test/f', 'https://[invalid',
                    BASE + '/draftfile.php/1/f.pdf', BASE + '/login/logout.php',
                    BASE + '/pluginfile.php/4/assignsubmission_file/submission_files/1/private.pdf',
                    BASE + '/pluginfile.php/4/assignfeedback_file/feedback/1/private.pdf',
                    BASE + '/pluginfile.php/4/f.pdf?SeSsKeY=', 'https://example.test/f?token=private']:
            with self.subTest(url=url):
                self.assertIsNone(safe_file_link(url, BASE))
        self.assertEqual(safe_file_link('/mod/resource/view.php?id=5&redirect=1', BASE), BASE + '/mod/resource/view.php?id=5')
        self.assertEqual(safe_file_link('/pluginfile.php/42/slides.pdf#page=2', BASE), BASE + '/pluginfile.php/42/slides.pdf')
        self.assertEqual(safe_file_link('https://drive.google.com/file/d/example/view', BASE), 'https://drive.google.com/file/d/example/view')

    def test_filename_and_type_handle_encoded_unicode_uppercase_and_resource_names(self):
        item = file_metadata({'url': BASE + '/pluginfile.php/1/%E8%AC%9B%E7%BE%A9.PDF', 'title': '附件／圖片'})
        self.assertEqual((item['title'], item['filename'], item['type']), ('講義.PDF', '講義.PDF', 'pdf'))
        item = file_metadata({'url': BASE + '/mod/resource/view.php?id=5', 'title': '期末報告.docx'})
        self.assertEqual((item['type'], item['extension']), ('document', 'DOCX'))
        item = file_metadata({'url': BASE + '/mod/resource/view.php?id=6', 'title': '第一週講義'})
        self.assertEqual(item['type'], 'other', 'Moodle file resources must not be classified as general links')
        item = file_metadata({'url': BASE + '/pluginfile.php/1/README', 'title': 'README'})
        self.assertEqual(item['type'], 'other')

    def test_readable_course_names_remove_codes_without_merging_course_ids(self):
        for title, expected in [
            ('【115上】514028娛樂敘事概論 Introduction to Entertainment Narratives', '娛樂敘事概論 Introduction to Entertainment Narratives'),
            ('1151.535650.視訊壓縮 Video Compression', '視訊壓縮 Video Compression'),
            ('114X.010029.生成式AI輔助的Python物理模擬實驗', '生成式AI輔助的Python物理模擬實驗'),
            ('【114下學期】535521 視覺辨識', '視覺辨識'),
            ('Computer Networks', 'Computer Networks'),
        ]:
            with self.subTest(title=title):
                self.assertEqual(course_display_name(title), expected)

    def test_course_materials_exclude_navigation_and_discover_only_assignment_folder_children(self):
        html = '''<nav><a href="https://outside.test/private.pdf">導覽</a></nav><div class="course-content">
        <a href="/mod/resource/view.php?id=5">投影片.pdf<span class="accesshide">File</span></a>
        <a href="/mod/assign/view.php?id=9&amp;sesskey=drop"><span class="instancename">HW 1<span class="accesshide">Assignment</span></span></a>
        <a href="/mod/folder/view.php?id=8">教材資料夾</a><a href="https://drive.google.com/example">共用資料</a>
        <a href="/login/logout.php?sesskey=secret">登出</a></div>'''
        links, children = course_materials(html, BASE, COURSE)
        self.assertEqual(len(links), 2)
        self.assertEqual(links[0]['title'], '投影片.pdf')
        self.assertEqual(set(children), {'assignment:9', 'folder:8'})
        self.assertEqual(children['assignment:9']['title'], 'HW 1')
        self.assertNotIn('sesskey', children['assignment:9']['url'])
        with self.assertRaises(ValueError): course_materials('<p>Login</p>', BASE, COURSE)

    def test_assignment_never_collects_student_submissions_or_feedback(self):
        html = '<main id="region-main"><div id="intro"><a href="/pluginfile.php/1/intro/spec.pdf">規格</a></div><div class="submissionstatustable"><a href="/pluginfile.php/1/report.pdf">學生報告</a></div></main>'
        links = assignment_files(html, BASE + '/mod/assign/view.php?id=9')
        self.assertEqual([item['title'] for item in links], ['規格'])
        self.assertEqual(assignment_files('<main id="region-main"></main>', BASE), [])
        with self.assertRaises(ValueError): assignment_files('<p>Unknown</p>', BASE)
        self.assertEqual(folder_files('<div class="foldertree"><a href="/pluginfile.php/1/lecture.pdf">Lecture</a></div>', BASE)[0]['title'], 'Lecture')

    def test_index_deduplicates_download_variants_and_preserves_source_links(self):
        mail = {**NEWS, 'links': [{**PDF, 'url': PDF['url'] + '?forcedownload=1'}]}
        items = file_index({'course:42': SOURCE}, [COURSE], {'mail': [mail]}, BASE)
        self.assertEqual(len(items), 1)
        self.assertEqual([item['kind'] for item in items[0]['origins']], ['course', 'mail'])
        self.assertIn('/courses/messages?tab=mail&semester=115-1&item=42:7', items[0]['origins'][1]['url'])
        self.assertEqual(file_index({'course:42': SOURCE}, [], {}, BASE), [])

    def test_live_message_content_replaces_stale_index_and_same_file_in_other_courses_is_separate(self):
        old = {**NEWS, 'kind': 'announcements', 'links': [PDF]}
        self.assertEqual(file_index({'announcements:42:7': old}, [COURSE], {'announcements': [{**NEWS, 'links': []}]}, BASE), [])
        items = file_index({'course:42': SOURCE, 'course:43': {**SOURCE, 'course_id': 43}}, [COURSE, {**COURSE, 'id': 43}], {}, BASE)
        self.assertEqual(len(items), 2)
        self.assertEqual(len({item['id'] for item in items}), 2)


class FileAccountTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(os.environ, {'E3_ENV': 'development', 'E3_CACHE_DIR': self.directory.name,
            'E3_DATABASE_URL': '', 'DATABASE_URL': '', 'E3_CANONICAL_HOST': '', 'E3_SESSION_COOKIE_SECURE': '0',
            'E3_NOTIFICATIONS_WORKER': '0', 'E3_SESSION_PROFILE_WORKER': '0', 'E3_YOUTUBE_AUTO_SYNC_ENABLED': '0'})
        self.environment.start()
        self.app = create_app(); self.storage = self.app.extensions['e3_storage']
        self.service = self.app.extensions['e3_course_files']
        self.client = csrf_client(self.app)
        self.login('112550103')

    def tearDown(self):
        self.storage._engine.dispose(); self.environment.stop(); self.directory.cleanup()

    def login(self, username, guest=False):
        self.storage.save_web_session('files-' + username, username, moodle_session='synthetic-cookie', is_guest=guest)
        self.storage.save_user_cache(username, {'ts': time.time(), 'result': {'courses': [COURSE], 'all_assignments': [],
            'available_semesters': [{'key': '115-1', 'label': '115 上學期'}], 'selected_semesters': ['115-1']}, 'excel_data': ''})
        with self.client.session_transaction() as session: session['session_token'] = 'files-' + username

    def seed(self, sources=None, pending=None):
        attempt = self.service.storage.claim('112550103', '115-1', now=time.time() - 120)
        self.assertIsNotNone(attempt)
        self.service.storage.finish('112550103', '115-1', attempt, sources or {'course:42': SOURCE}, pending or [])
        return attempt

    def test_page_entries_and_csp(self):
        response = self.client.get('/courses/files')
        self.assertEqual(response.status_code, 200)
        self.assertIn('附件收納', response.get_data(as_text=True))
        self.assertIn("script-src", response.headers['Content-Security-Policy'])
        self.assertIn('/courses/files', self.client.get('/courses/messages').get_data(as_text=True))
        self.assertIn('/courses/files', self.client.get('/').get_data(as_text=True))
        from bs4 import BeautifulSoup
        import json
        for query, course in [('course=42&q=HW2', '42'), ('course=evil%22', '')]:
            page = BeautifulSoup(self.client.get('/courses/files?' + query).get_data(as_text=True), 'html.parser')
            config = json.loads(page.select_one('#course-files-config').string)
            self.assertEqual(config['course'], course)
            self.assertEqual(config['query'], 'HW2' if course else '')

    def test_own_account_semester_only_and_get_does_not_fetch_remote(self):
        self.seed()
        with patch.object(MoodleNewsClient, 'get', side_effect=AssertionError('GET must never fetch E3')):
            response = self.client.get('/api/course-files?semester=115-1&username=other')
        self.assertIn('no-store', response.headers['Cache-Control'])
        self.assertEqual(len(response.get_json()['items']), 1)
        self.assertNotIn('sources', response.get_json())
        self.login('Session-synthetic')
        self.assertEqual(self.client.get('/api/course-files?semester=115-1&username=112550103').get_json()['items'], [])
        self.assertEqual(self.client.get('/api/course-files?semester=114-2').status_code, 400)

    def test_logged_out_guests_invalid_payload_and_csrf(self):
        self.assertEqual(self.client.post('/api/course-files/refresh', json=[]).status_code, 400)
        self.assertEqual(self.client.post('/api/course-files/refresh', json={'semester': []}).status_code, 400)
        self.assertEqual(self.client.post('/api/course-files/refresh', json={'semester': '114-2'}).status_code, 400)
        raw = self.app.test_client()
        with raw.session_transaction() as session: session['session_token'] = 'files-112550103'
        self.assertEqual(raw.post('/api/course-files/refresh', json={'semester': '115-1'}).status_code, 400)
        self.login('訪客_files', guest=True)
        self.assertEqual(self.client.get('/api/course-files').status_code, 403)
        self.assertEqual(self.client.post('/api/course-files/refresh', json={'semester': '115-1'}).status_code, 403)
        with self.client.session_transaction() as session: session.clear()
        self.assertEqual(self.client.get('/courses/files').status_code, 302)

    def test_refresh_uses_current_session_and_never_browser_url_or_account(self):
        self.login('Session-synthetic')
        with patch.object(self.service, 'start', return_value='running') as start:
            response = self.client.post('/api/course-files/refresh', json={'semester': '115-1', 'username': 'other', 'url': 'https://evil.test'})
        self.assertEqual(response.status_code, 202)
        self.assertEqual(start.call_args.args[1]['username'], 'Session-synthetic')
        self.assertEqual(start.call_args.args[1]['moodle_session'], 'synthetic-cookie')
        self.assertEqual(start.call_args.args[3][0]['id'], 42)

    def test_new_message_attachments_appear_without_marking_messages_read(self):
        mail = self.app.extensions['e3_course_mail'].storage
        attempt = mail.claim_course_announcement_refresh('112550103', '115-1', now=time.time() - 120)
        mail.finish_course_announcement_refresh('112550103', '115-1', attempt, [NEWS], [42])
        mail.update_course_announcement('112550103', '115-1', NEWS['key'], content={'content': 'private body', 'links': [PDF]})
        with patch.object(MoodleNewsClient, 'get', side_effect=AssertionError('Never open unread mail in background')):
            data = self.client.get('/api/course-files?semester=115-1').get_json()
        self.assertEqual(len(data['items']), 1)
        saved = mail.load_course_announcements('112550103', '115-1')['items'][0]
        self.assertEqual(saved['read_at'], 0)
        self.assertNotIn('private body', str(data))

    def test_encrypted_cache_atomic_claim_stale_worker_and_account_cleanup(self):
        attempt = self.seed()
        with self.storage._engine.connect() as conn:
            payload = conn.execute(select(course_file_cache.c.payload)).scalar()
        self.assertNotIn('課程講義', payload)
        self.assertNotIn('pluginfile', payload)
        now = time.time()
        claimed = self.service.storage.claim('112550103', '115-1', now=now)
        self.assertIsNotNone(claimed)
        self.assertIsNone(self.service.storage.claim('112550103', '115-1', now=now + 1))
        self.assertFalse(self.service.storage.finish('112550103', '115-1', attempt, {}, []))
        with self.storage._engine.begin() as conn:
            user_id = conn.execute(select(users_table.c.id).where(users_table.c.username == '112550103')).scalar()
            delete_assignment_account_data(conn, [user_id])
            conn.execute(delete(users_table).where(users_table.c.id == user_id))
        self.assertFalse(self.service.storage.finish('112550103', '115-1', claimed, {}, []))
        self.assertIsNone(self.service.storage.claim('112550103', '115-1'))
        with self.storage._engine.connect() as conn:
            self.assertEqual(conn.execute(select(course_file_cache)).all(), [])

    def client_mock(self, pages):
        client = Mock(base_url=BASE, deadline=time.monotonic() + 60)
        client.get.side_effect = lambda url: pages[url] if not isinstance(pages[url], Exception) else (_ for _ in ()).throw(pages[url])
        return client

    def test_bounded_discovery_resumes_without_refetching_completed_sources(self):
        course_url = BASE + '/course/view.php?id=42'
        assignment_url = BASE + '/mod/assign/view.php?id=9'
        client = self.client_mock({course_url: '<div class="course-content"><a href="/mod/assign/view.php?id=9">HW1</a></div>',
            assignment_url: '<div id="intro"><a href="/pluginfile.php/42/spec.pdf">規格</a></div>'})
        sources, pending, error, success = self.service.collect(client, [COURSE], {}, EMPTY, max_pages=1)
        self.assertEqual((pending, success), (['assignment:9'], 1))
        self.assertTrue(error)
        sources, pending, error, success = self.service.collect(client, [COURSE], {}, {'sources': sources, 'pending': pending})
        self.assertEqual((pending, error, success), ([], '', 1))
        self.assertEqual(client.get.call_count, 2)
        self.assertEqual(sources['assignment:9']['links'][0]['title'], '規格')

    def test_partial_failure_retains_previous_files_and_deleted_materials_are_removed(self):
        old = {'sources': {'course:42': SOURCE, 'folder:8': {**SOURCE, 'kind': 'folder', 'parent': 'course:42'}}, 'pending': []}
        client = self.client_mock({SOURCE['url']: ValueError('offline'), BASE + '/mod/folder/view.php?id=8': ValueError('offline')})
        # A failed course page must not erase its previously indexed links.
        sources, pending, _, _ = self.service.collect(client, [COURSE], {}, old, max_pages=1)
        self.assertEqual(sources['course:42']['links'], [PDF])
        self.assertIn('course:42', pending)
        client = self.client_mock({SOURCE['url']: '<div class="course-content"></div>'})
        sources, pending, _, _ = self.service.collect(client, [COURSE], {}, old)
        self.assertNotIn('folder:8', sources)
        self.assertEqual(sources['course:42']['links'], [])

    def test_session_expiry_keeps_previous_files_and_worker_releases_slot(self):
        self.seed()
        attempt = self.service.storage.claim('112550103', '115-1')
        self.service.slots.acquire()
        with patch.object(MoodleNewsClient, 'get', side_effect=AnnouncementSessionExpired()):
            self.service._refresh(self.app, {'username': '112550103', 'moodle_session': 'synthetic'}, '115-1', [COURSE], {}, attempt)
        saved = self.service.storage.load('112550103', '115-1')
        self.assertEqual(saved['sources']['course:42']['links'], [PDF])
        self.assertIn('登入已失效', saved['error'])
        self.assertTrue(self.service.slots.acquire(blocking=False)); self.service.slots.release()

    def test_announcements_can_be_indexed_without_marking_read_and_mail_is_never_fetched(self):
        client = self.client_mock({SOURCE['url']: '<div class="course-content"></div>',
            NEWS['url']: '<div class="forumpost"><div class="posting"><a href="/pluginfile.php/42/spec.pdf">規格</a></div></div>'})
        sources, pending, _, _ = self.service.collect(client, [COURSE], {'announcements': [NEWS], 'mail': [NEWS]}, EMPTY)
        self.assertEqual(pending, [])
        self.assertEqual(sources['announcements:42:7']['read_at'], 0)
        self.assertEqual(client.get.call_count, 2)
        self.assertEqual(self.service.start(self.app, {}, '115-1', [COURSE], {}), 'session_expired')

    def test_uncached_announcements_are_discovered_from_course_news_forum(self):
        forum = BASE + '/mod/forum/view.php?id=3'
        client = self.client_mock({SOURCE['url']: '<div class="course-content"><a href="/mod/forum/view.php?id=3">公告</a></div>',
            forum: '<table><tr><td><a href="/mod/forum/discuss.php?d=7">報告格式</a></td></tr></table>',
            NEWS['url']: '<div class="forumpost"><div class="posting"><a href="/pluginfile.php/42/spec.pdf">格式</a></div></div>'})
        sources, pending, _, _ = self.service.collect(client, [COURSE], {}, EMPTY)
        self.assertEqual(pending, [])
        self.assertEqual(file_index(sources, [COURSE], {}, BASE)[0]['origins'][0]['kind'], 'announcements')
        self.assertEqual(client.get.call_count, 3)

    def test_storage_failure_still_releases_worker_slot(self):
        self.service.slots.acquire()
        with patch.object(self.service.storage, 'load', side_effect=ValueError('corrupted cache')):
            self.service._refresh(self.app, {'username': '112550103', 'moodle_session': 'test'}, '115-1', [COURSE], {}, 1)
        self.assertTrue(self.service.slots.acquire(blocking=False)); self.service.slots.release()

    def test_large_source_is_bounded_and_not_misrepresented_as_complete(self):
        html = '<div class="course-content">' + ''.join(
            f'<a href="/pluginfile.php/42/file-{index}.pdf">File {index}</a>' for index in range(105)) + '</div>'
        client = self.client_mock({SOURCE['url']: html})
        sources, pending, error, _ = self.service.collect(client, [COURSE], {}, EMPTY)
        self.assertEqual(len(sources['course:42']['links']), 100)
        self.assertTrue(sources['course:42']['limited'])
        self.assertEqual(pending, [])
        self.assertIn('部分資料', error)


if __name__ == '__main__':
    unittest.main()
