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

        def observe_refresh(service, *args):
            try:
                return refresh(service, *args)
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
        if 'question' in inputs:
            document = next(source for source in sources if OUTLINE_QUOTE in source['text'])
            return model_response({'points': [
                {'text': '課綱列出期末考時間。', 'source': document['id'], 'quote': OUTLINE_QUOTE},
                {'text': '虛構資料', 'source': document['id'], 'quote': '這段文字不存在於文件'},
            ]})
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

    def overview(self):
        return self.client.get(f'/api/course-overview?semester={TERM}&include=overview').get_json()

    def test_first_pdf_and_new_mail_reconcile_through_background_job_and_search(self):
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

        result = self.client.post('/api/course-overview/search', json={
            'semester': TERM, 'course_id': 42, 'query': 'Final exam time'}).get_json()
        self.assertEqual(len(result['points']), 1)
        self.assertEqual(result['points'][0]['source']['location'], '第 2 頁')
        self.assertNotIn('text', result['points'][0]['source'])
        other = self.account('987654322', [COURSE])
        self.assertEqual(other.get(f'/api/course-overview?semester={TERM}').get_json(), {'configured': False})
        self.assertEqual(other.post('/api/course-overview/search', json={
            'semester': TERM, 'course_id': 42, 'query': 'Final exam time'}).status_code, 409)
        self.assertEqual(self.provider.call_count, 3)

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
        result = self.client.post('/api/course-overview/search', json={
            'semester': TERM, 'course_id': 42, 'query': '參考書目'}).get_json()
        self.assertEqual(result, {'points': [], 'partial': True})

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

    def test_ambiguous_search_asks_for_course_before_calling_gemini(self):
        other_course = {**COURSE, 'id': 43, 'title': '【115上】離散數學', 'assignments': []}
        self.client = self.account(USERNAME, [COURSE, other_course])
        self.pages['/course/view.php?id=43'] = (
            f'<div class="course-content"><a href="{FIRST_FILE}">導論</a></div>')
        self.pages['/local/dcpcmail/view.php?c=43&t=inbox'] = '<div class="mail_list">沒有郵件</div>'
        self.refresh()
        self.wait_for_worker()
        self.assertEqual(self.overview()['ready'], 2)
        self.provider.reset_mock()

        query = {'semester': TERM, 'query': 'Final exam time'}
        choices = self.client.post('/api/course-overview/search', json=query).get_json()
        self.assertCountEqual(choices['choices'], [{'id': 42, 'title': '資料結構'}, {'id': 43, 'title': '離散數學'}])
        self.assertFalse(choices['partial'])
        self.assertEqual(self.client.post('/api/course-overview/search', json={**query, 'course_id': 99}).status_code, 400)
        self.provider.assert_not_called()

        result = self.client.post('/api/course-overview/search', json={**query, 'course_id': 43}).get_json()
        self.assertEqual([point['course'] for point in result['points']], ['離散數學'])
        self.provider.assert_called_once()
        sent = self.provider.call_args.kwargs['json']['contents'][0]['parts'][0]['text']
        self.assertNotIn(NEWS_QUOTE, sent)

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
