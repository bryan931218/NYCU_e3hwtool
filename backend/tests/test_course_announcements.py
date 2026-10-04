"""Course news parsing, private caches, bounded synchronization and own-account routes."""

import os
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from sqlalchemy import delete, select

from e3_tracker.platform.application import create_app
from e3_tracker.platform.persistence.core_schema import users_table
from e3_tracker.assignments.persistence.course_announcements import course_announcement_cache
from e3_tracker.assignments.domain.course_announcements import announcement_forums, discussion_summaries, discussion_content, moodle_url
from e3_tracker.assignments.services.course_announcements import MoodleNewsClient, collect_course_announcements, AnnouncementSessionExpired
from tests.security_helpers import csrf_client


BASE = 'https://e3p.nycu.edu.tw'
COURSE = {'id': 42, 'title': '【115上】作業系統', 'semester_key': '115-1', 'assignments': []}
NEWS = {'id': '7', 'key': '42:7', 'course_id': 42, 'course_title': COURSE['title'], 'semester': '115-1',
        'title': '期中考時間', 'url': BASE + '/mod/forum/discuss.php?d=7', 'updated_ts': 1791079200, 'author': '老師'}


class NewsParsingTests(unittest.TestCase):
    def test_only_announcement_forums_and_official_readonly_urls(self):
        html = '''<a href="/mod/forum/view.php?id=3&amp;sesskey=secret">課程公告</a>
        <a href="/mod/forum/view.php?id=4">課程討論區</a><a href="https://evil.test/mod/forum/view.php?id=5">公告</a>
        <a href="/mod/forum/view.php?id=3">Announcements</a>
        <div class="block_news_items"><a href="/mod/forum/view.php?id=6">Older topics</a></div>'''
        self.assertEqual(announcement_forums(html, BASE), [BASE + '/mod/forum/view.php?id=3', BASE + '/mod/forum/view.php?id=6'])
        for link in ['javascript:alert(1)', '//evil.test/mod/forum/discuss.php?d=7', '/mod/forum/discuss.php?d=7&d=8', '/mod/forum/discuss.php?d=-1']:
            self.assertIsNone(moodle_url(link, BASE, '/mod/forum/discuss.php', 'd'))

    def test_modern_discussion_table_and_chinese_dates(self):
        html = '''<table class="discussion-list"><tr class="discussion"><th class="topic"><a href="discuss.php?d=7">期中考時間</a></th>
        <td class="author"><a>老師</a></td><td class="lastpost">2026年10月4日 星期日 09:30</td></tr></table>'''
        item = discussion_summaries(html, BASE)[0]
        self.assertEqual(item['title'], '期中考時間')
        self.assertEqual(item['author'], '老師')
        self.assertIsInstance(item['updated_ts'], int)

    def test_inline_news_uses_subject_not_read_more_label(self):
        html = '<div class="forumpost"><h3 class="subject">調課通知</h3><div class="author"><time datetime="2026-10-04T09:30:00+08:00"></time></div><a href="/mod/forum/discuss.php?d=9">Discuss this topic</a></div>'
        self.assertEqual(discussion_summaries(html, BASE)[0]['title'], '調課通知')
        self.assertEqual(len(discussion_summaries(html + html, BASE)), 1)

    def test_content_is_plain_text_and_drops_active_markup_tokens_and_reply_bodies(self):
        html = '''<article class="forumpost"><div data-region="post-content"><p>公告內容</p><script>stolen()</script><iframe src="https://evil.test"></iframe>
        <a href="javascript:alert(1)">bad</a><a href="/pluginfile.php/42/notice.pdf">投影片</a>
        <a href="/login/index.php?sesskey=secret">token</a></div></article><article class="forumpost"><div data-region="post-content">同學回覆</div></article>'''
        result = discussion_content(html, BASE)
        self.assertIn('公告內容', result['content'])
        self.assertNotIn('stolen', result['content'])
        self.assertNotIn('同學回覆', result['content'])
        self.assertEqual(result['links'], [{'url': BASE + '/pluginfile.php/42/notice.pdf', 'title': '投影片'}])

    def test_collect_empty_courses_partial_failures_and_login_expiry(self):
        client = Mock(base_url=BASE)
        client.get.side_effect = ['<a href="/mod/forum/view.php?id=3">公告</a>',
            '<table><tr><td><a href="/mod/forum/discuss.php?d=7">期中考</a></td></tr></table>', ValueError('offline'), '<p>沒有公告論壇</p>']
        items, successful, failed = collect_course_announcements(client, [COURSE, {**COURSE, 'id': 43}, {**COURSE, 'id': 44}])
        self.assertEqual(successful, [42, 44]); self.assertEqual(failed, [43])
        self.assertEqual(items[0]['key'], '42:7')
        client.get.side_effect = AnnouncementSessionExpired()
        with self.assertRaises(AnnouncementSessionExpired):
            collect_course_announcements(client, [COURSE])

    def test_http_client_never_sends_cookie_to_untrusted_redirects(self):
        client = MoodleNewsClient(BASE, 'synthetic-test-cookie')
        response = Mock(status_code=302, headers={'Location': 'https://evil.test/login'})
        response.__enter__ = Mock(return_value=response); response.__exit__ = Mock(return_value=False)
        with patch.object(client.session, 'get', return_value=response) as get:
            with self.assertRaises(AnnouncementSessionExpired): client.get(BASE + '/course/view.php?id=42')
            self.assertEqual(get.call_count, 1)
            self.assertFalse(get.call_args.kwargs['allow_redirects'])
            with self.assertRaises(ValueError): client.get('https://evil.test/mod/forum/discuss.php?d=7')
            self.assertEqual(get.call_count, 1)
        client.close()

    def test_http_client_limits_response_size_and_detects_login_forms(self):
        client = MoodleNewsClient(BASE, 'synthetic-test-cookie')
        response = Mock(status_code=200)
        response.__enter__ = Mock(return_value=response); response.__exit__ = Mock(return_value=False)
        response.iter_content.return_value = [b'x' * (2 * 1024 * 1024 + 1)]
        with patch.object(client.session, 'get', return_value=response):
            with self.assertRaises(ValueError): client.get(BASE + '/course/view.php?id=42')
            response.iter_content.return_value = [b'<input name="logintoken">']
            with self.assertRaises(AnnouncementSessionExpired): client.get(BASE + '/course/view.php?id=42')
        client.close()


class NewsAccountTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(os.environ, {'E3_ENV': 'development', 'E3_CACHE_DIR': self.directory.name,
            'E3_DATABASE_URL': '', 'DATABASE_URL': '', 'E3_CANONICAL_HOST': '', 'E3_SESSION_COOKIE_SECURE': '0',
            'E3_NOTIFICATIONS_WORKER': '0', 'E3_SESSION_PROFILE_WORKER': '0', 'E3_YOUTUBE_AUTO_SYNC_ENABLED': '0'})
        self.environment.start()
        self.app = create_app(); self.storage = self.app.extensions['e3_storage']
        self.service = self.app.extensions['e3_course_announcements']
        self.client = csrf_client(self.app)
        self.login('112550103')

    def tearDown(self):
        self.storage._engine.dispose(); self.environment.stop(); self.directory.cleanup()

    def login(self, username, guest=False):
        self.storage.save_web_session('test-' + username, username, moodle_session='synthetic-test-cookie', is_guest=guest)
        self.storage.save_user_cache(username, {'ts': time.time(), 'result': {'courses': [COURSE], 'all_assignments': [],
            'available_semesters': [{'key': '115-1', 'label': '115 上學期'}], 'selected_semesters': ['115-1']}, 'excel_data': ''})
        with self.client.session_transaction() as session: session['session_token'] = 'test-' + username

    def seed(self, username='112550103', items=None):
        attempt = self.storage.claim_course_announcement_refresh(username, '115-1', now=time.time() - 120)
        self.assertIsNotNone(attempt)
        self.storage.finish_course_announcement_refresh(username, '115-1', attempt, items or [NEWS], [42])

    def test_page_navigation_and_guest_login_protection(self):
        html = self.client.get('/').get_data(as_text=True)
        self.assertIn('/courses/announcements', html)
        self.assertEqual(self.client.get('/courses/announcements').status_code, 200)
        with self.client.session_transaction() as session: session.clear()
        self.assertEqual(self.client.get('/api/course-announcements').status_code, 302)
        self.login('訪客_demo', guest=True)
        self.assertEqual(self.client.get('/api/course-announcements').status_code, 403)
        self.assertEqual(self.client.post('/api/course-announcements/refresh', json={'semester':'115-1'}).status_code, 403)

    def test_private_cache_cannot_be_selected_by_username_or_course_id(self):
        self.seed()
        self.login('Session-synthetic')
        data = self.client.get('/api/course-announcements?semester=115-1&username=112550103').get_json()
        self.assertEqual(data['items'], [])
        self.assertEqual(self.client.post('/api/course-announcements/item', json={'semester':'115-1', 'key':'42:7', 'read':True, 'username':'112550103'}).status_code, 404)
        self.assertEqual(self.client.get('/api/course-announcements?semester=114-2').status_code, 400)

    def test_session_accounts_use_their_own_session_and_courses(self):
        self.login('Session-synthetic')
        with patch.object(self.service, 'start', return_value='running') as start:
            response = self.client.post('/api/course-announcements/refresh', json={'semester':'115-1', 'username':'other', 'url':'https://evil.test'})
        self.assertEqual(response.status_code, 202)
        user = start.call_args.args[1]
        self.assertEqual(user['username'], 'Session-synthetic')
        self.assertEqual(user['moodle_session'], 'synthetic-test-cookie')
        self.assertEqual(start.call_args.args[3][0]['id'], 42)

    def test_read_and_restore_are_durable_and_cached_content_avoids_another_remote_fetch(self):
        self.seed()
        with patch.object(self.service, 'content', return_value={'content':'公告內容', 'links':[]}) as fetch:
            payload = {'semester':'115-1', 'key':'42:7', 'read':True}
            first = self.client.post('/api/course-announcements/item', json=payload)
            self.assertEqual(first.status_code, 200)
            self.assertGreater(first.get_json()['item']['read_at'], 0)
            self.client.post('/api/course-announcements/item', json={**payload, 'read':False})
            self.client.post('/api/course-announcements/item', json=payload)
            self.assertEqual(fetch.call_count, 1)
        stored = self.storage.load_course_announcements('112550103', '115-1')['items'][0]
        self.assertEqual(stored['content'], '公告內容')
        self.assertGreater(stored['read_at'], 0)

    def test_invalid_requests_and_csrf_do_not_start_network_calls(self):
        self.assertEqual(self.client.post('/api/course-announcements/refresh', json=[]).status_code, 400)
        self.assertEqual(self.client.post('/api/course-announcements/item', json={'key':'42:7', 'read':'true'}).status_code, 400)
        raw = self.app.test_client()
        with raw.session_transaction() as session: session['session_token'] = 'test-112550103'
        self.assertEqual(raw.post('/api/course-announcements/refresh', json={'semester':'115-1'}).status_code, 400)

    def test_refresh_cooldown_atomic_claims_and_deleted_accounts_are_not_recreated(self):
        now = time.time()
        attempt = self.storage.claim_course_announcement_refresh('112550103', '115-1', now=now)
        self.assertIsNotNone(attempt)
        self.assertIsNone(self.storage.claim_course_announcement_refresh('112550103', '115-1', now=now+1))
        self.assertFalse(self.storage.finish_course_announcement_refresh('112550103','115-1',attempt-1,[NEWS],[42]))
        with self.storage._engine.begin() as conn: conn.execute(delete(users_table).where(users_table.c.username == '112550103'))
        self.assertFalse(self.storage.finish_course_announcement_refresh('112550103','115-1',attempt,[NEWS],[42]))
        self.assertIsNone(self.storage.claim_course_announcement_refresh('112550103','115-1'))

    def test_partial_refresh_preserves_failed_courses_and_read_state(self):
        extra = {**NEWS, 'key':'43:8', 'course_id':43, 'id':'8'}
        self.seed(items=[NEWS,extra])
        self.storage.update_course_announcement('112550103','115-1','42:7',content={'content':'cached', 'links':[]},read=True)
        attempt = self.storage.claim_course_announcement_refresh('112550103','115-1')
        self.storage.finish_course_announcement_refresh('112550103','115-1',attempt,[NEWS],[42],'部分失敗')
        data = self.storage.load_course_announcements('112550103','115-1')
        self.assertEqual(data['status'],'partial')
        self.assertEqual(len(data['items']),2)
        first = next(item for item in data['items'] if item['key']=='42:7')
        self.assertEqual(first['content'],'cached'); self.assertGreater(first['read_at'],0)

    def test_worker_failure_keeps_existing_cache_and_releases_slot(self):
        self.seed()
        attempt = self.storage.claim_course_announcement_refresh('112550103','115-1')
        self.service.slots.acquire()
        with patch('e3_tracker.assignments.services.course_announcements.collect_course_announcements', side_effect=AnnouncementSessionExpired()):
            self.service._refresh(self.app, {'username':'112550103','moodle_session':'test'}, '115-1', [COURSE], attempt)
        data = self.storage.load_course_announcements('112550103','115-1')
        self.assertEqual(data['status'],'error'); self.assertEqual(len(data['items']),1)
        self.assertTrue(self.service.slots.acquire(blocking=False)); self.service.slots.release()

    def test_cleanup_removes_announcement_cache(self):
        self.seed()
        from e3_tracker.assignments.persistence.cleanup import delete_assignment_account_data
        with self.storage._engine.begin() as conn:
            user_id = conn.execute(select(users_table.c.id).where(users_table.c.username=='112550103')).scalar_one()
            delete_assignment_account_data(conn, [user_id])
            self.assertIsNone(conn.execute(select(course_announcement_cache)).first())

    def test_separate_workers_cannot_claim_the_same_refresh(self):
        from concurrent.futures import ThreadPoolExecutor
        from e3_tracker.platform.storage import PersistentStorage
        second = PersistentStorage(str(self.storage._engine.url))
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                claims = list(pool.map(lambda storage: storage.claim_course_announcement_refresh('112550103','115-1'), [self.storage, second]))
            self.assertEqual(sum(value is not None for value in claims), 1)
        finally: second._engine.dispose()

    def test_busy_service_refuses_more_threads_without_starting_a_job(self):
        self.service.slots.acquire(); self.service.slots.acquire()
        try:
            response = self.client.post('/api/course-announcements/refresh', json={'semester':'115-1'})
            self.assertEqual(response.status_code, 429)
            self.assertEqual(self.storage.load_course_announcements('112550103','115-1')['status'], 'idle')
        finally: self.service.slots.release(); self.service.slots.release()

    def test_updated_announcement_invalidates_old_body_but_keeps_read_state(self):
        self.seed()
        self.storage.update_course_announcement('112550103','115-1','42:7',content={'content':'old', 'links':[]},read=True)
        attempt = self.storage.claim_course_announcement_refresh('112550103','115-1')
        self.storage.finish_course_announcement_refresh('112550103','115-1',attempt,[{**NEWS,'updated_ts':NEWS['updated_ts']+10}],[42])
        item = self.storage.load_course_announcements('112550103','115-1')['items'][0]
        self.assertNotIn('content',item)
        self.assertGreater(item['read_at'],0)


if __name__ == '__main__': unittest.main()
