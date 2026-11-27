"""Exercise course AI through Flask, Moodle parsers, and the encrypted store."""

import io
import json
import os
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import requests
from bs4 import BeautifulSoup
from pypdf import PdfWriter
from pypdf.generic import DecodedStreamObject, DictionaryObject, NameObject
from sqlalchemy import update

from e3_tracker.assignments.persistence.course_insights import CourseInsightStorage, insight_courses, insight_settings
from e3_tracker.assignments.services.course_insights import CourseInsightService, ENDPOINT
from e3_tracker.platform.application import create_app
from tests.security_helpers import csrf_client


BASE = 'https://e3p.nycu.edu.tw'
TERM = '115-1'
USERNAME = '987654321'
KEY = 'AQ.synthetic-course-insight-testing'
COURSE = {'id': 42, 'title': '【115上】資料結構', 'semester_key': TERM, 'assignments': [
    {'title': 'HW4 樹與圖', 'url': BASE + '/mod/assign/view.php?id=49'},
]}
FIRST_FILE = '/pluginfile.php/42/mod_resource/content/1/01.pdf'
SECOND_FILE = '/pluginfile.php/42/mod_resource/content/1/course-outline.txt'
OUTLINE_QUOTE = 'Final exam: December 3, 2026 at 10:10.'
GRADE_QUOTE = 'Homework: 40%.'
MAIL_QUOTE = '期末考改至 12 月 8 日 14:30。HW4 繳交期限延至 12 月 10 日 23:59。'
NEWS_QUOTE = '期末考改至 12 月 5 日 09:30。'
SESSION = 'synthetic-moodle-session'


def make_pdf(*, text=True):
    writer = PdfWriter()
    writer.add_blank_page(width=400, height=300)
    page = writer.add_blank_page(width=400, height=300)
    if text:
        font = DictionaryObject({NameObject('/Type'): NameObject('/Font'), NameObject('/Subtype'): NameObject('/Type1'),
                                 NameObject('/BaseFont'): NameObject('/Helvetica')})
        page[NameObject('/Resources')] = DictionaryObject({NameObject('/Font'): DictionaryObject({NameObject('/F1'): font})})
        stream = DecodedStreamObject()
        stream.set_data(f'BT /F1 12 Tf 20 240 Td ({OUTLINE_QUOTE} {GRADE_QUOTE}) Tj ET'.encode())
        page[NameObject('/Contents')] = stream
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def http_response(body, status=200, content_type='text/html', **headers):
    response = requests.Response()
    response.status_code = status
    response._content = body.encode() if isinstance(body, str) else body
    response._content_consumed = True
    response.headers.update({'Content-Type': content_type, **headers})
    return response


def model_response(value):
    return http_response(json.dumps({'candidates': [{'content': {'parts': [
        {'thought': True, 'text': 'This part is not an answer.'},
        {'text': json.dumps(value, ensure_ascii=False)},
    ]}}]}), content_type='application/json')


class CourseInsightPipelineTests(unittest.TestCase):
    def setUp(self):
        folder = self.enterContext(tempfile.TemporaryDirectory())
        self.enterContext(patch.dict(os.environ, {
            'E3_CACHE_DIR': folder, 'E3_DATABASE_URL': '', 'DATABASE_URL': '', 'E3_CANONICAL_HOST': '',
            'E3_ENV': 'development', 'E3_NOTIFICATIONS_WORKER': '0', 'E3_SESSION_PROFILE_WORKER': '0',
            'E3_COURSE_AI_WORKER': '0', 'E3_YOUTUBE_AUTO_SYNC_ENABLED': '0', 'E3_SESSION_COOKIE_SECURE': '0',
        }))
        self.app = create_app(default_base_url=BASE)
        self.db = self.app.extensions['e3_storage']
        self.addCleanup(self.db._engine.dispose)
        self.store = CourseInsightStorage(self.db)
        self.client = self.account(USERNAME, [COURSE])
        self.assertEqual(self.client.post('/settings/course-ai',
            data={'action': 'connect', 'api_key': KEY, 'consent': 'yes'}).status_code, 302)
        self.finished = threading.Event()
        self.has_pending_refresh = False
        refresh = CourseInsightService._refresh

        def observe_refresh(service, *args, **kwargs):
            try:
                return refresh(service, *args, **kwargs)
            finally:
                self.finished.set()

        self.enterContext(patch.object(CourseInsightService, '_refresh', observe_refresh))
        self.pages = {
            '/course/view.php?id=42': '<div class="course-content">'
                '<a href="https://elsewhere.example/announcement.pdf">外部檔案</a>'
                '<a href="/mod/resource/view.php?id=15">第一份教材</a>'
                f'<a href="{SECOND_FILE}">課程大綱</a>'
                '<a href="/mod/forum/view.php?id=3">課程公告</a></div>',
            '/mod/resource/view.php?id=15': http_response('', 303, Location=FIRST_FILE),
            FIRST_FILE: http_response(make_pdf(), content_type='application/pdf'),
            SECOND_FILE: http_response('Course description.', content_type='text/plain'),
            '/mod/forum/view.php?id=3': self.news_listing('2026-11-23T10:00:00+08:00'),
            '/mod/forum/discuss.php?d=7': '<article class="forumpost firstpost">'
                '<header><time datetime="2026-11-23T10:00:00+08:00"></time></header>'
                f'<div class="post-content-container">{NEWS_QUOTE}</div></article>',
            '/local/dcpcmail/view.php?c=42&t=inbox': '<div class="mail_list">沒有郵件</div>',
        }
        self.moodle = self.enterContext(patch('requests.sessions.Session.get', autospec=True, side_effect=self.moodle_get))
        self.provider = self.enterContext(patch('e3_tracker.assignments.services.course_insights.requests.post',
            side_effect=self.model))
        self.addCleanup(self.wait_for_worker)

    def account(self, name, courses):
        token = f'course-insight-session-{name}'
        self.db.save_web_session(token, name, moodle_session=SESSION)
        self.db.save_user_cache(name, {'ts': time.time(), 'result': {'courses': courses,
            'all_assignments': [], 'selected_semesters': [TERM]}, 'excel_data': ''})
        client = csrf_client(self.app)
        with client.session_transaction() as session:
            session['session_token'] = token
        return client

    def test_course_pages_share_header_and_keep_the_selected_semester(self):
        for path, title in (
            ('/courses/overview', '課程中心'),
            ('/courses/messages', '課程訊息'),
            ('/courses/messages?tab=mail', '課程訊息'),
            ('/courses/files', '附件收納'),
        ):
            with self.subTest(path=path):
                response = self.client.get(path, follow_redirects=True)
                self.assertEqual(response.status_code, 200)
                page = BeautifulSoup(response.data, 'html.parser')
                header = page.select_one('.course-workspace-header')
                self.assertIn(title, header.h1.get_text())
                self.assertEqual(header.select_one('.course-workspace-back')['href'], '/')
                self.assertIsNotNone(header.select_one('[data-course-theme]'))
                links = header.select('.course-workspace-nav a')
                self.assertEqual([link.get_text(strip=True) for link in links], ['課程中心', '課程訊息', '附件收納'])
                self.assertEqual([link.get_text(strip=True) for link in links if link.get('aria-current') == 'page'], [title])
                self.assertTrue(all(f'semester={TERM}' in link['href'] for link in links))
                if title == '課程訊息':
                    self.assertEqual(page.select_one('.news-kinds [aria-current="page"]').get_text(strip=True),
                                     '信件' if '?tab=mail' in path else '公告')

    def test_refresh_semesters_keeps_existing_assignments_and_recovers_old_courses(self):
        other = self.account('987654322', [COURSE])
        self.pages['/my/'] = '<script>M.cfg = {"sesskey":"synthetic-sesskey"}</script>'
        for path in ('/my/courses.php', '/my/courses.php?classification=all',
                     '/my/courses.php?classification=past', '/my/courses.php?classification=hidden',
                     '/grade/report/overview/index.php'):
            self.pages[path] = '<main>我的課程</main>'

        def timeline(session, method, url, **kwargs):
            if method == 'GET':
                return self.moodle_get(session, url, **{**kwargs, 'allow_redirects': False})
            self.assertEqual(url.split('?')[0], BASE + '/lib/ajax/service.php')
            self.assertEqual(kwargs['json'][0]['methodname'], 'core_course_get_enrolled_courses_by_timeline_classification')
            classification = kwargs['json'][0]['args']['classification']
            if classification == 'allincludinghidden':
                course = {'id': 42, 'fullname': COURSE['title']}
            else:
                self.assertEqual(classification, 'past')
                course = {'id': 73, 'fullname': '基礎物理', 'shortname': '1142.560001'}
            return http_response(json.dumps([{'error': False, 'data': {'courses': [course]}}]))

        with patch('requests.sessions.Session.request', autospec=True, side_effect=timeline), \
                patch('e3_tracker.assignments.services.collector.current_semester_key', return_value=TERM):
            response = self.client.post('/courses/overview/semesters',
                                        data={'semester': TERM, 'course': '42'}, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        page = BeautifulSoup(response.data, 'html.parser')
        self.assertEqual(page.select_one('#overviewSemester option[selected]')['value'], TERM)
        self.assertIsNotNone(page.select_one('#overviewSemester option[value="114-2"]'))
        self.assertIn('新增 1 個學期', page.select_one('.insight-semester-notice').get_text())
        self.assertIn('基礎物理', self.client.get('/courses/overview?semester=114-2').get_data(as_text=True))
        for path in ('/courses/messages', '/courses/files'):
            page = BeautifulSoup(self.client.get(path + '?semester=114-2').data, 'html.parser')
            self.assertIsNotNone(page.select_one('select[data-course-semester] option[value="114-2"][selected]'))
        self.assertNotIn('基礎物理', other.get('/courses/overview').get_data(as_text=True))
        cached = self.db.load_user_cache(USERNAME)['result']
        self.assertEqual([course['id'] for course in cached['courses']], [42])
        self.assertEqual(cached['all_assignments'][0]['title'], COURSE['assignments'][0]['title'])
        self.provider.assert_not_called()

    def news_listing(self, latest_post):
        return ('<table><tr><th><a href="/mod/forum/discuss.php?d=7">考試異動</a></th>'
                f'<td class="lastpost"><time datetime="{latest_post}"></time></td></tr></table>')

    def add_mail(self):
        self.pages['/local/dcpcmail/view.php?c=42&t=inbox'] = (
            '<div class="mail_list"><div class="mail_item mail_unread">'
            '<a class="mail_link" href="view.php?t=inbox&amp;c=42&amp;m=8">'
            '<span class="mail_summary">課程期限更新</span>'
            '<span class="mail_date"><time datetime="2026-11-25T09:30:00+08:00"></time></span>'
            '</a></div></div>')
        self.pages['/local/dcpcmail/view.php?c=42&t=inbox&m=8'] = (
            f'<div class="mail_content"><p>{MAIL_QUOTE}</p><p>助教 learner@example.test</p>'
            '<p>課堂帳號 987654321</p><p>MoodleSession=synthetic-mail-secret</p></div>')

    def moodle_get(self, session, url, **kwargs):
        self.assertTrue(url.startswith(BASE + '/'), url)
        self.assertIs(kwargs['allow_redirects'], False)
        self.assertEqual(session.cookies.get('MoodleSession'), SESSION)
        value = self.pages[url.removeprefix(BASE)]
        return http_response(value) if isinstance(value, str) else value

    def model(self, endpoint, **kwargs):
        self.assertEqual(endpoint, ENDPOINT)
        self.assertEqual(kwargs['headers']['x-goog-api-key'], KEY)
        self.assertIs(kwargs['allow_redirects'], False)
        serialized = kwargs['json']['contents'][0]['parts'][0]['text']
        for secret in (KEY, SESSION, USERNAME, BASE, 'learner@example.test', 'synthetic-mail-secret'):
            self.assertNotIn(secret, serialized)
        inputs = json.loads(serialized)
        sources = inputs['sources']
        self.assertEqual(sources[0]['location'], '第 2 頁', 'First readable file takes priority over named syllabus')
        events = []
        for source in sources:
            if OUTLINE_QUOTE in source['text']:
                events.append(self.event(source, OUTLINE_QUOTE, 'December 3, 2026 at 10:10'))
            if NEWS_QUOTE in source['text']:
                events.append(self.event(source, NEWS_QUOTE, '12 月 5 日 09:30'))
            if MAIL_QUOTE in source['text']:
                events.extend([self.event(source, MAIL_QUOTE, '12 月 8 日 14:30'),
                    self.event(source, MAIL_QUOTE, '12 月 10 日 23:59', kind='deadline', assignment='49')])
        return model_response({'summary': '這是合成課程概要。', 'grading': [
            {'label': '作業', 'detail': '40%', 'source': sources[0]['id'], 'quote': GRADE_QUOTE},
        ], 'events': events})

    def event(self, source, quote, date, *, kind='final', assignment=''):
        return {'kind': kind, 'title': '期末考' if kind == 'final' else 'HW4',
                'source': source['id'], 'quote': quote, 'date_phrase': date, 'assignment': assignment}

    def refresh(self):
        # Age the cache instead of waiting for the production refresh window.
        with self.db._engine.begin() as connection:
            connection.execute(update(insight_settings).values(retry_at=0))
            connection.execute(update(insight_courses).values(checked=0))
        self.finished.clear()
        response = self.client.post('/api/course-overview/refresh', json={'semester': TERM})
        self.has_pending_refresh = response.status_code == 202
        self.assertEqual(response.status_code, 202)

    def wait_for_worker(self):
        if self.has_pending_refresh:
            self.assertTrue(self.finished.wait(8), 'Background job did not finish before timeout')
            self.has_pending_refresh = False

    def overview(self, semester=TERM):
        return self.client.get(f'/api/course-overview?semester={semester}&include=overview').get_json()

    def test_first_pdf_and_new_mail_reconcile_through_background_job(self):
        self.refresh()
        self.wait_for_worker()
        initial = self.overview()
        self.assertEqual([event['date'] for event in initial['events']], ['2026-12-05T09:30:00+08:00'])
        self.assertIn('40%', initial['overview'])
        self.assertIn('第 2 頁', initial['overview'])
        downloaded = [call.args[1].removeprefix(BASE) for call in self.moodle.call_args_list]
        self.assertLess(downloaded.index(FIRST_FILE), downloaded.index(SECOND_FILE))

        self.add_mail()
        self.refresh()
        self.wait_for_worker()
        updated = self.overview()
        self.assertEqual([(event['id'], event['date']) for event in updated['events']],
            [('final', '2026-12-08T14:30:00+08:00'), ('deadline:49', '2026-12-10T23:59:00+08:00')])
        self.assertEqual(updated['events'][0]['origin']['kind'], 'mail')

        # A reply moves the old thread to the top of the forum without editing its original post.
        self.pages['/mod/forum/view.php?id=3'] = self.news_listing('2026-11-27T11:00:00+08:00')
        self.refresh()
        self.wait_for_worker()
        self.assertEqual(self.overview()['events'], updated['events'])
        self.assertEqual(self.provider.call_count, 2, 'An unchanged reply must not consume more Gemini quota')

        other = self.account('987654322', [COURSE])
        self.assertEqual(other.get(f'/api/course-overview?semester={TERM}').get_json(), {'configured': False})
        self.assertNotIn('40%', other.get(f'/courses/overview?semester={TERM}&course=42').get_data(as_text=True))
        self.assertEqual(self.provider.call_count, 2)

    def test_quota_failure_keeps_schedule_and_applies_retry_cooldown(self):
        self.refresh()
        self.wait_for_worker()
        original = self.overview()['events']
        self.add_mail()
        self.provider.side_effect = lambda *args, **kwargs: http_response('', 429, content_type='application/json')
        self.refresh()
        self.wait_for_worker()
        data = self.overview()
        self.assertEqual(data['events'], original)
        self.assertEqual(data['ready'], 1)
        self.assertIn('用量已達上限', data['error'])
        self.assertGreater(data['retry_at'], time.time())
        retry = self.client.post('/api/course-overview/refresh', json={'semester': TERM})
        self.assertEqual(retry.get_json()['status'], 'waiting')
        self.assertEqual(self.provider.call_count, 2)

    def test_manual_refresh_can_check_for_changes_before_the_normal_update_interval(self):
        self.refresh()
        self.wait_for_worker()
        self.add_mail()
        with self.db._engine.begin() as connection:
            connection.execute(update(insight_settings).values(retry_at=0))
            connection.execute(update(insight_courses).values(checked=time.time() - 240))
        self.finished.clear()
        response = self.client.post('/api/course-overview/refresh', json={'semester': TERM, 'manual': True})
        self.has_pending_refresh = response.status_code == 202
        self.assertEqual(response.status_code, 202)
        self.wait_for_worker()
        self.assertEqual(self.overview()['events'][0]['date'], '2026-12-08T14:30:00+08:00')
        self.assertEqual(self.provider.call_count, 2)

    def test_unreadable_first_document_does_not_look_like_complete_coverage(self):
        self.pages[FIRST_FILE] = http_response(make_pdf(text=False), content_type='application/pdf')
        self.provider.side_effect = lambda *args, **kwargs: model_response({
            'summary': '只能讀取課程大綱的部分資料。', 'grading': [], 'events': [],
        })
        self.refresh()
        self.wait_for_worker()
        self.assertIn('部分來源尚未讀取', self.overview()['overview'])
        indexed = self.store.snapshots(USERNAME, TERM, [42], include_sources=True)[42]
        self.assertTrue(indexed['document'], 'The later text file still parses')
        self.assertTrue(indexed['partial'], 'The first PDF had no extractable text')

    def test_late_syllabus_and_moodle_page_produce_separate_verified_grading_rows(self):
        links = []
        for index in range(9):
            path = f'/pluginfile.php/42/mod_resource/content/1/week-{index}.txt'
            links.append(f'<a href="{path}">第 {index + 1} 週教材</a>')
            text = '平時測驗 10%' if index == 8 else '課堂練習及閱讀重點。' * 1600
            self.pages[path] = http_response(text, content_type='text/plain')
        links.append(f'<a href="{SECOND_FILE}">評分與課程大綱</a>')
        links.append('<a href="/mod/page/view.php?id=77">評量補充</a>')
        self.pages['/course/view.php?id=42'] = '<div class="course-content">' + ''.join(links) + '</div>'
        self.pages[SECOND_FILE] = http_response('作業：４０％。期末專題：30 percent。', content_type='text/plain')
        self.pages['/mod/page/view.php?id=77'] = ('<div class="mod_page-content">出席採簽到。課堂討論：三十分鐘。'
            '<table><tr><th>項目</th><th>配分 (%)</th></tr><tr><td>實作</td><td>15</td></tr></table></div>')

        def model(_, **kwargs):
            sources = json.loads(kwargs['json']['contents'][0]['parts'][0]['text'])['sources']
            outline = next(source for source in sources if '作業：４０％' in source['text'])
            page = next(source for source in sources if '出席採簽到' in source['text'])
            late = next(source for source in sources if '平時測驗 10%' in source['text'])
            return model_response({'summary': '', 'events': [], 'grading': [
                {'label': '作業', 'weight': '40.0%', 'source': outline['id'], 'quote': '作業：４０％。'},
                {'label': '期末專題', 'weight': '30 percent', 'source': outline['id'], 'quote': '期末專題：30 percent。'},
                {'label': '出席', 'detail': '採簽到', 'source': page['id'], 'quote': '出席採簽到。'},
                {'label': '實作', 'weight': '15%', 'source': page['id'], 'quote': '實作 15%'},
                {'label': '平時測驗', 'weight': '10%', 'source': late['id'], 'quote': '平時測驗 10%'},
                {'label': '課堂討論', 'weight': '30 分', 'source': page['id'], 'quote': '課堂討論：三十分鐘。'},
                {'label': '期中考', 'weight': '30%', 'source': outline['id'], 'quote': '作業：４０％。期末專題：30 percent。'},
            ]})

        self.provider.side_effect = model
        self.refresh()
        self.wait_for_worker()
        self.assertTrue(self.overview()['needs_scan'])
        self.assertIn('正在接續讀取教材', self.overview()['overview'])
        self.provider.assert_not_called()
        self.refresh()
        self.wait_for_worker()
        self.provider.assert_called_once()
        self.assertFalse(self.overview()['needs_scan'])
        markup = BeautifulSoup(self.overview()['overview'], 'html.parser')
        grades = markup.select('.insight-grades tbody tr')
        self.assertEqual([(row.strong.get_text(), row.select('td')[-1].get_text(strip=True)) for row in grades],
                         [('作業', '40%'), ('期末專題', '30%'), ('出席', '未列'), ('實作', '15%'), ('平時測驗', '10%')])
        self.assertIn('採簽到', grades[2].get_text())
        self.assertTrue(grades[2].find('a')['href'].endswith('/mod/page/view.php?id=77'))

    def test_worker_resumes_selected_archived_semester_for_all_nine_courses(self):
        archive = '110-1'
        archived = [{'id': cid, 'title': f'【110上】歷史課程 {cid}', 'semester_key': archive, 'assignments': []}
                    for cid in range(50, 59)]
        self.client = self.account(USERNAME, [COURSE, *archived])
        for entry in archived:
            course_id = entry['id']
            self.pages[f'/course/view.php?id={course_id}'] = '<div class="course-content">期中考</div>'
            self.pages[f'/local/dcpcmail/view.php?c={course_id}&t=inbox'] = '<div class="mail_list">沒有郵件</div>'
        links = []
        for index in range(6):
            path = f'/pluginfile.php/50/mod_resource/content/1/week-{index}.txt'
            self.pages[path] = http_response('本週課程講義', content_type='text/plain')
            links.append(f'<a href="{path}">教材 {index}</a>')
        self.pages['/course/view.php?id=50'] = '<div class="course-content">' + ''.join(links) + '</div>'
        self.provider.side_effect = lambda *args, **kwargs: model_response({'summary': '課程摘要', 'grading': [], 'events': []})

        response = self.client.post('/api/course-overview/refresh', json={'semester': archive, 'queue': True})
        self.has_pending_refresh = response.status_code == 202
        self.assertEqual(response.status_code, 202)
        self.wait_for_worker()
        first = self.overview(archive)
        self.assertEqual(first['ready'], 8)
        self.assertTrue(first['queued_here'])
        self.assertTrue(first['needs_scan'])
        page = BeautifulSoup(self.client.get(f'/courses/overview?semester={archive}').data, 'html.parser')
        self.assertEqual(page.select_one('#courseOverviewPage')['data-historical'], '1')
        self.assertEqual(self.client.post('/api/course-overview/refresh', json={'semester': TERM}).get_json()['status'], 'waiting')
        with self.db._engine.begin() as connection:
            connection.execute(update(insight_settings).values(retry_at=0))

        self.finished.clear()
        service = CourseInsightService(self.db, self.app.extensions['e3_course_files'])
        self.has_pending_refresh = True
        service.scan_enabled_accounts(self.app)
        self.wait_for_worker()
        final = self.overview(archive)
        self.assertEqual(final['ready'], 9)
        self.assertFalse(final['queued_here'])
        self.assertFalse(final['needs_scan'])
        self.assertEqual(self.overview()['ready'], 0)
        self.assertEqual(self.provider.call_count, 9)

    def test_per_course_errors_do_not_block_other_courses(self):
        courses = [COURSE, {**COURSE, 'id': 43}, {**COURSE, 'id': 44}]
        self.client = self.account(USERNAME, courses)
        self.pages['/course/view.php?id=43'] = '<body id="page-enrol-index">無法選課</body>'
        self.pages['/course/view.php?id=44'] = '<div class="course-content">Participation accounts for 20%.</div>'
        self.pages['/local/dcpcmail/view.php?c=44&t=inbox'] = '<div class="mail_list">沒有郵件</div>'
        self.provider.side_effect = [model_response({'grading': None}), model_response({
            'summary': '課堂參與', 'events': [],
            'grading': [{'label': '課堂參與', 'weight': '20%', 'source': '1', 'quote': 'Participation accounts for 20%.'}],
        })]
        self.refresh()
        self.wait_for_worker()
        result = self.overview()
        self.assertEqual(result['course_states'], {'42': 'partial', '43': 'partial', '44': 'ready'})
        self.assertEqual(result['ready'], 1)
        self.assertIn('20%', result['overview'])

    def test_provider_failure_preserves_sources_and_continues_with_other_courses(self):
        self.client = self.account(USERNAME, [COURSE, {**COURSE, 'id': 43}])
        self.pages['/course/view.php?id=43'] = '<div class="course-content">另一門課</div>'
        self.pages['/local/dcpcmail/view.php?c=43&t=inbox'] = '<div class="mail_list">沒有郵件</div>'
        response = model_response({'summary': '課程摘要', 'grading': [], 'events': []})
        self.provider.side_effect = [http_response('', 503), response, response]
        self.refresh()
        self.wait_for_worker()
        first = self.overview()
        self.assertEqual(first['ready'], 1)
        self.assertIn('教材已讀取，等候分析', first['overview'])
        read_count = len(self.moodle.call_args_list)
        with self.db._engine.begin() as connection:
            connection.execute(update(insight_settings).values(retry_at=0))
        self.finished.clear()
        with patch('e3_tracker.assignments.services.course_insights.time.time', return_value=time.time() + 45):
            retry = self.client.post('/api/course-overview/refresh', json={'semester': TERM, 'queue': True})
            self.has_pending_refresh = retry.status_code == 202
            self.assertEqual(retry.status_code, 202)
            self.wait_for_worker()
        self.assertEqual(self.overview()['ready'], 2)
        self.assertEqual(len(self.moodle.call_args_list), read_count)
        self.assertEqual(self.provider.call_count, 3)

    def test_grading_in_a_course_section_and_announcement_attachment_is_read(self):
        attachment = '/pluginfile.php/42/mod_forum/attachment/7/grading.txt'
        self.pages['/course/view.php?id=42'] = ('<div class="course-content"><li class="section">'
            '<a href="/course/section.php?id=12">課程大綱</a></li>'
            '<div class="block_news_items"><a href="/mod/forum/view.php?id=3">課程公告</a></div></div>')
        self.pages['/course/section.php?id=12'] = '<div id="region-main">報告配分 30%</div>'
        self.pages['/mod/forum/view.php?id=3'] = self.news_listing('2026-11-23T10:00:00+08:00').replace('考試異動', '評分方式')
        self.pages['/mod/forum/discuss.php?d=7'] = (
            '<article class="forumpost firstpost"><div class="post-content-container">詳見附件</div>'
            f'<div class="attachments"><a href="{attachment}">配分表</a></div></article>')
        self.pages[attachment] = http_response('作業配分 70%', content_type='text/plain')

        def model(_, **kwargs):
            sources = json.loads(kwargs['json']['contents'][0]['parts'][0]['text'])['sources']
            section = next(source for source in sources if '報告配分 30%' in source['text'])
            file = next(source for source in sources if '作業配分 70%' in source['text'])
            return model_response({'summary': '', 'events': [], 'grading': [
                {'label': '報告', 'weight': '30%', 'source': section['id'], 'quote': '報告配分 30%'},
                {'label': '作業', 'weight': '70%', 'source': file['id'], 'quote': '作業配分 70%'},
            ]})

        self.provider.side_effect = model
        self.refresh()
        self.wait_for_worker()
        result = self.overview()
        self.assertEqual(result['ready'], 1)
        self.assertIn('作業配分 70%', result['overview'])
        self.assertIn(attachment, result['overview'])

    def test_expired_moodle_session_keeps_schedule_without_spending_gemini_quota(self):
        self.refresh()
        self.wait_for_worker()
        original = self.overview()['events']
        self.pages['/course/view.php?id=42'] = http_response('', 303, Location='/login/index.php')
        self.refresh()
        self.wait_for_worker()
        data = self.overview()
        self.assertEqual(data['events'], original)
        self.assertIn('請重新登入 E3', data['error'])
        self.assertEqual(self.provider.call_count, 1)

    def test_course_selection_uses_one_timeline_with_source_links_and_no_ai_search(self):
        other_course = {**COURSE, 'id': 43, 'title': '【115上】離散數學', 'assignments': []}
        self.client = self.account(USERNAME, [COURSE, other_course])
        self.pages['/course/view.php?id=43'] = (
            f'<div class="course-content"><a href="{FIRST_FILE}">導論</a></div>')
        self.pages['/local/dcpcmail/view.php?c=43&t=inbox'] = '<div class="mail_list">沒有郵件</div>'
        self.refresh()
        self.wait_for_worker()
        self.assertEqual(self.overview()['ready'], 2)
        all_courses = BeautifulSoup(self.client.get(f'/courses/overview?semester={TERM}').get_data(as_text=True), 'html.parser')
        selected = BeautifulSoup(self.client.get(f'/courses/overview?semester={TERM}&course=43').get_data(as_text=True), 'html.parser')
        for page in (all_courses, selected):
            self.assertIsNone(page.select_one('input[type=search]'))
            self.assertEqual(len(page.select('.insight-schedule-list')), 1)
            self.assertEqual(len(page.select('#insight-event-43-final')), 1)
            self.assertIn('第 2 頁', page.select_one('#insight-event-43-final details').get_text())
        self.assertFalse(all_courses.select_one('#insight-event-42-final').has_attr('hidden'))
        self.assertTrue(selected.select_one('#insight-event-42-final').has_attr('hidden'))
        self.assertFalse(selected.select_one('[data-course-info="43"]').has_attr('hidden'))
        self.assertTrue(selected.select_one('[data-course-info="42"]').has_attr('hidden'))
        self.assertEqual(self.client.post('/api/course-overview/search', json={}).status_code, 404)
        self.assertEqual(self.provider.call_count, 2)

    def test_disconnect_while_gemini_runs_discards_pending_write(self):
        entered = threading.Event()
        release = threading.Event()
        self.addCleanup(release.set)

        def pending_provider(*args, **kwargs):
            entered.set()
            if not release.wait(8):
                raise AssertionError('Test did not release the Gemini response')
            return self.model(*args, **kwargs)

        self.provider.side_effect = pending_provider
        self.refresh()
        self.assertTrue(entered.wait(8))
        self.assertTrue(self.overview()['running'])
        duplicate = self.client.post('/api/course-overview/refresh', json={'semester': TERM})
        self.assertEqual(duplicate.get_json()['status'], 'waiting')
        try:
            self.assertEqual(self.client.post('/settings/course-ai', data={'action': 'disconnect'}).status_code, 302)
        finally:
            release.set()
            self.wait_for_worker()
        self.assertEqual(self.overview(), {'configured': False})
        self.assertEqual(self.store.snapshots(USERNAME, TERM, [42], include_sources=True), {})
        self.assertEqual(self.provider.call_count, 1)
