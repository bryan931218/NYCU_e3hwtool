import io
import os
import tempfile
import time
import unittest
import zipfile
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import select

from e3_tracker.assignments.domain.course_insights import reconcile_analysis
from e3_tracker.assignments.persistence.course_insights import CourseInsightStorage, insight_settings
from e3_tracker.assignments.services.course_insight_sources import _file_url, document_sections
from e3_tracker.assignments.services.course_insights import CourseAIError, gemini
from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.platform.application import create_app
from e3_tracker.platform.storage import PersistentStorage
from tests.security_helpers import csrf_client


TERM = '115-1'
BASE = 'https://e3p.nycu.edu.tw'


def source(sid, text, *, kind='file', version=0, title='課程說明'):
    return {'id': sid, 'key': sid, 'kind': kind, 'title': title, 'text': text,
            'url': BASE, 'version': version, 'published': version}


def event(row, phrase, *, kind='midterm', title='期中考', cancelled=False, assignment=''):
    return {'kind': kind, 'title': title, 'source': row['id'], 'quote': row['text'],
            'date_phrase': phrase, 'cancelled': cancelled, 'assignment': assignment}


def apply(entries, sources, previous=None, assignments=None):
    return reconcile_analysis({'events': entries, 'grading': []}, sources, previous or {}, assignments or {}, TERM)['events']


class CourseInsightTests(unittest.TestCase):
    def test_later_mail_changes_exam_and_old_sources_cannot_restore_previous_date(self):
        syllabus = source('file-1', '期中考：11 月 26 日 10:10')
        news = source('announcement-1', '期中考改至 2026/12/03 14:10', kind='announcements', version=100)
        mail = source('mail-1', '期中考延後至 12 月 10 日 下午 2:30', kind='mail', version=200)
        original = event(syllabus, '11 月 26 日 10:10')
        older = event(news, '2026/12/03 14:10')
        newer = event(mail, '12 月 10 日 下午 2:30')
        self.assertEqual(apply([original], [syllabus])['midterm']['date'], '2026-11-26T10:10:00+08:00')
        changed = apply([newer, older, original], [mail, news, syllabus])
        self.assertEqual(changed['midterm']['date'], '2026-12-10T14:30:00+08:00')
        self.assertEqual(changed, apply([original, older], [syllabus, news], changed))
        canceled = source('announcement-2', '期中考取消', kind='announcements', version=300)
        tombstone = apply([event(canceled, '', cancelled=True)], [canceled], changed)
        self.assertTrue(tombstone['midterm']['cancelled'])
        self.assertEqual(tombstone, apply([newer], [mail], tombstone))

    def test_ambiguity_hallucinated_evidence_and_report_cannot_schedule(self):
        tentative = source('a', '期中考暫定 11 月 26 日')
        report = source('b', '期末報告請於 12 月 10 日繳交')
        vague = source('c', '期中考日期 11/10')
        self.assertEqual(apply([event(tentative, '11 月 26 日'),
            event(report, '12 月 10 日', kind='final', title='期末考'),
            event(vague, '11/10'), event(vague, '2026/11/10')], [tentative, report, vague]), {})

    def test_time_only_correction_requires_a_known_day(self):
        file = source('file', '期中考 11 月 26 日 10:10')
        mail = source('mail', '期中考時間改為 14:30', kind='mail', version=200)
        original, correction = event(file, '11 月 26 日 10:10'), event(mail, '14:30')
        self.assertEqual(apply([correction], [mail]), {})
        result = apply([correction, original], [mail, file])
        self.assertEqual(result['midterm']['date'], '2026-11-26T14:30:00+08:00')
        self.assertEqual(result, apply([original], [file], result))

    def test_conflicting_same_version_updates_preserve_last_confirmed_date(self):
        confirmed = source('original', '期中考：11 月 26 日 10:10')
        previous = apply([event(confirmed, '11 月 26 日 10:10')], [confirmed])
        news = source('a', '期中考改至 12 月 3 日', kind='announcements', version=300)
        mail = source('b', '期中考改至 12 月 4 日', kind='mail', version=300)
        conflicting = [event(news, '12 月 3 日'), event(mail, '12 月 4 日')]
        for order in (conflicting, list(reversed(conflicting))):
            self.assertEqual(apply(order, [news, mail], previous), previous)
        misleading = source('c', '期中考改至 12 月 5 日，時間改為 15:00', kind='mail', version=600)
        self.assertEqual(apply([event(misleading, '15:00')], [misleading], previous), previous)

    def test_exact_assignment_and_all_day_or_relative_time(self):
        now = TAIPEI_TZ.localize(datetime(2026, 11, 26, 10)).timestamp()
        mail = source('a', 'HW4 期限延至下週三 23:59', kind='mail', version=now)
        exam = source('b', 'Final exam: December 19, 2026.')
        refs = {'49': {'ref': '49', 'title': 'HW4 Motion Estimation'}, '55': {'ref': '55', 'title': 'HW40 Other'}}
        items = apply([event(mail, '下週三 23:59', kind='deadline', assignment='49'),
            event(exam, 'December 19, 2026', kind='final', title='Final exam')], [mail, exam], assignments=refs)
        self.assertEqual(items['deadline:49']['date'], '2026-12-02T23:59:00+08:00')
        self.assertEqual(items['final']['date'], '2026-12-19')
        self.assertEqual(apply([event(mail, '下週三 23:59', kind='deadline', assignment='55')], [mail], assignments=refs), {})

    def test_archived_exam_notice_keeps_three_assessments_and_later_time(self):
        notice = source('a', '團體上機考時間：5/29（10:00～22:00） 筆試時間：6/5 個人上機考時間：6/12',
                        kind='announcements', version=100, title='期末考試相關說明')
        update = source('b', '個人期末上機考 6/12 10:10', kind='announcements', version=200)
        corrected = event(update, '6/12 10:10', kind='final', title='個人期末上機考')
        result = reconcile_analysis({'events': [corrected]}, [notice, update], {}, {}, '114-2')['events']
        self.assertEqual({key: item['date'] for key, item in result.items()}, {
            'final:group': '2026-05-29', 'final:written': '2026-06-05',
            'final:individual': '2026-06-12T10:10:00+08:00',
        })

    def test_published_weights_recover_nested_percentages_and_newer_teacher_rubrics(self):
        lines = ['Four homework assignments 64% (=16% x 4)', 'Final project 36%']
        outline = {**source('syllabus', ' '.join(lines), kind='syllabus'), 'topic': 'grading', 'lines': lines}
        partial_model = {'summary': 'Unsourced grading information', 'grading': [
            {'source': 'syllabus', 'label': 'Final project', 'weight': '36%', 'quote': lines[1]},
        ]}
        result = reconcile_analysis(partial_model, [outline], {}, {}, '114-2')
        self.assertEqual([(row['weight'], row['detail']) for row in result['grading']], [('64%', '=16% x 4'), ('36%', '')])
        self.assertEqual(result['summary'], '')
        teacher = source('teacher', '分數配比：平時作業25% 期末省思15% 出席15% 期中6% 期末9% 課中提問30%。',
                         kind='mail', version=200)
        result = reconcile_analysis(partial_model, [outline, teacher], {}, {}, '114-2')
        self.assertEqual([row['weight'] for row in result['grading']], ['25%', '15%', '15%', '6%', '9%', '30%'])
        self.assertTrue(all(row['origin']['kind'] == 'mail' for row in result['grading']))

    def test_tentative_rubric_and_wrong_item_weight_are_rejected(self):
        outline = {**source('syllabus', 'Tentative grading Assignment 1: 25%', kind='syllabus'), 'topic': 'grading'}
        file = source('file', '學期成績：期末筆試 20%；個人上機 30%')
        raw = {'grading': [
            self.grade(outline, 'Assignment 1', '25%', 'Assignment 1: 25%'),
            self.grade(file, '期末筆試', '30%', file['text']),
            self.grade(file, '期末筆試', '20%', file['text']),
            self.grade(file, 'Final (筆試)', '20%', '期末筆試 20%', scope_quote='學期成績', source_label='期末筆試'),
        ]}
        result = reconcile_analysis(raw, [file, outline], {}, {}, '114-2')
        self.assertEqual([(row['label'], row['weight']) for row in result['grading']], [('期末筆試', '20%')])

    def grade(self, row, label, weight, quote, **context):
        return {'scope': 'course', 'source': row['id'], 'label': label, 'weight': weight, 'quote': quote, **context}

    def test_video_compression_uses_course_weights_instead_of_assignment_subscores(self):
        lines = [
            'Programming projects x4 (80%) - includes both individual assignments and team-based projects.',
            'Project presentation x2 (20%) - Students will present two of the four programming projects.',
        ]
        outline = {**source('outline', ' '.join(lines), kind='syllabus'), 'topic': 'grading', 'lines': lines}
        first_file = source('first', 'Grading Programming Assignments (4-5): 100%', version=100, title='CourseOutline.pdf')
        project = source('project', 'Submission Requirements Slides 70% Files 20% Report 10%',
                         version=300, title='FinalProject.pdf')
        claims = [
            self.grade(first_file, 'Programming Assignments', '100%', first_file['text']),
            self.grade(project, 'Slides', '70%', 'Slides 70%', scope_quote='Submission Requirements'),
            self.grade(project, 'Files', '20%', 'Files 20%', scope_quote='Submission Requirements'),
            self.grade(project, 'Report', '10%', 'Report 10%', scope_quote='Submission Requirements'),
        ]
        result = reconcile_analysis({'grading': claims}, [outline, first_file, project], {}, {}, TERM)
        self.assertEqual([(row['label'], row['weight']) for row in result['grading']],
                         [('Programming projects x4', '80%'), ('Project presentation x2', '20%')])
        self.assertEqual(reconcile_analysis({'grading': claims}, [project], {}, {}, TERM)['grading'], [])

    def test_later_course_file_updates_only_named_items_and_old_files_cannot_restore_them(self):
        lines = ['Homework: 50%', 'Midterm: 20%', 'Final: 30%']
        outline = {**source('outline', ' '.join(lines), kind='syllabus'), 'topic': 'grading', 'lines': lines}
        later = source('later', '學期成績：程式練習配分由 50% 調整為 60%。', version=300, title='課程配分調整')
        earlier = source('earlier', '學期成績：Homework 55%。', version=200)
        updated = self.grade(later, '程式練習', '60%', later['text'], replaces='Homework')
        stale_value = self.grade(later, '程式練習', '50%', later['text'], replaces='Homework')
        older = self.grade(earlier, 'Homework', '55%', earlier['text'])
        revised = reconcile_analysis({'grading': [older, updated, stale_value]}, [outline, earlier, later], {}, {}, TERM)
        self.assertEqual([row['weight'] for row in revised['grading']], ['60%', '20%', '30%'])
        self.assertTrue(revised['grading_conflict'])
        replay = reconcile_analysis({'grading': [older]}, [outline, earlier], {}, {}, TERM, prior_grading=revised['grading_state'])
        self.assertEqual(replay['grading'], revised['grading'])
        full = source('complete', '學期總成績配分：作業 60%、期中考 15%、期末考 25%。', version=400)
        replacement = reconcile_analysis({}, [outline, full], {}, {}, TERM, prior_grading=revised['grading_state'])
        self.assertEqual([row['weight'] for row in replacement['grading']], ['60%', '15%', '25%'])
        self.assertFalse(replacement['grading_conflict'])

    def test_syllabus_exam_is_found_and_dated_revised_file_can_reschedule_it(self):
        outline = source('outline', 'Final exam: December 17, 2026 at 10:10.', kind='syllabus')
        revision = source('revision', 'Final exam is rescheduled to 12/21 at 14:30.', version=200)
        base = apply([], [outline])
        self.assertEqual(base['final']['date'], '2026-12-17T10:10:00+08:00')
        revised = apply([event(revision, '12/21 at 14:30', kind='final', title='Final exam')], [revision], base)
        self.assertEqual(revised['final']['date'], '2026-12-21T14:30:00+08:00')
        self.assertEqual(apply([], [outline], revised), revised)
        class_note = source('mail', '期末考週取消上課。', kind='mail', version=300)
        self.assertEqual(apply([event(class_note, '', kind='final', title='期末考', cancelled=True)], [class_note], revised), revised)

    def test_course_wide_teacher_weights_do_not_mix_with_conditional_formulas(self):
        text = ('特殊情形總成績＝10（保底分數）+ 0.2 * 期末報告成績 + 0.7 * 指導教授評分。'
                '本專題由指導教授給70%分數，這門課開課老師評定30%分數。')
        announcement = source('notice', text, kind='announcements', version=200)
        result = reconcile_analysis({}, [announcement], {}, {}, '114-2')
        self.assertEqual([(row['label'], row['weight']) for row in result['grading']],
                         [('指導教授評分', '70%'), ('開課老師評分', '30%')])

    def test_only_fixed_moodle_document_paths_and_presentation_positions(self):
        local = BASE + '/pluginfile.php/42/mod_resource/content/1/file.pdf'
        self.assertEqual(_file_url(local, BASE), local)
        self.assertIsNone(_file_url(local.replace(BASE, 'https://other.example'), BASE))
        self.assertIsNone(_file_url(local.replace('mod_resource', 'assignfeedback_file'), BASE))
        self.assertIsNone(_file_url(local.replace('/1/', '/1/%2e%2e/'), BASE))
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as archive:
            for index in (10, 2):
                archive.writestr(f'ppt/slides/slide{index}.xml',
                    f'<a:root xmlns:a="urn:test"><a:p><a:t>Slide {index}</a:t></a:p></a:root>')
        self.assertEqual(document_sections(buffer.getvalue(), '.pptx'), [('第 2 張投影片', 'Slide 2'), ('第 10 張投影片', 'Slide 10')])

    @patch('e3_tracker.assignments.services.course_insights.requests.post')
    def test_gemini_key_uses_header_and_does_not_follow_redirects(self, post):
        post.return_value.status_code = 429
        with self.assertRaisesRegex(CourseAIError, 'quota'):
            gemini('example-test-key', 'rules', {'sources': []})
        self.assertEqual(post.call_args.kwargs['headers'], {'x-goog-api-key': 'example-test-key'})
        self.assertFalse(post.call_args.kwargs['allow_redirects'])

    def test_settings_are_encrypted_user_scoped_and_fence_revoked_workers(self):
        with tempfile.TemporaryDirectory() as folder:
            owner = PersistentStorage('sqlite:///' + (Path(folder) / 'insights.sqlite3').as_posix())
            try:
                store = CourseInsightStorage(owner)
                with owner._engine.begin() as conn:
                    owner._ensure_user(conn, 'user-a')
                    owner._ensure_user(conn, 'user-b')
                key = 'sample-personal-key'
                self.assertTrue(store.configure('user-a', key))
                with owner._engine.connect() as conn:
                    self.assertNotIn(key, conn.execute(select(insight_settings.c.secret)).scalar())
                self.assertIsNone(store.settings('user-b'))
                claim = store.claim('user-a')
                self.assertIsNone(store.claim('user-a'))
                self.assertTrue(store.save('user-a', TERM, 29, claim, {'summary': 'private'}))
                self.assertEqual(store.snapshots('user-b', TERM, [29]), {})
                self.assertEqual(store.snapshots('user-a', '114-1', [29]), {})
                store.configure('user-a', None)
                self.assertFalse(store.save('user-a', TERM, 29, claim, {'summary': 'restored'}))
                self.assertEqual(store.snapshots('user-a', TERM, [29]), {})
            finally:
                owner._engine.dispose()

    def test_workbench_routes_render_and_serve_own_course_schedule(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {
            'E3_CACHE_DIR': folder, 'E3_DATABASE_URL': '', 'DATABASE_URL': '', 'E3_CANONICAL_HOST': '',
            'E3_ENV': 'development', 'E3_NOTIFICATIONS_WORKER': '0', 'E3_SESSION_PROFILE_WORKER': '0',
            'E3_COURSE_AI_WORKER': '0', 'E3_YOUTUBE_AUTO_SYNC_ENABLED': '0', 'E3_SESSION_COOKIE_SECURE': '0',
        }):
            app = create_app()
            owner = app.extensions['e3_storage']
            try:
                client = csrf_client(app)
                name = '987654321'
                owner.save_web_session('course-test', name, moodle_session='synthetic')
                owner.save_user_cache(name, {'ts': time.time(), 'result': {'courses': [
                    {'id': 42, 'title': '【115上】資料結構', 'semester_key': TERM, 'assignments': []},
                ], 'all_assignments': [], 'selected_semesters': [TERM]}, 'excel_data': ''})
                with client.session_transaction() as session:
                    session['session_token'] = 'course-test'
                setup = client.get('/settings/course-ai?semester=115-1&course=42', follow_redirects=True)
                self.assertIn('申請免費 API 金鑰', setup.get_data(as_text=True))
                self.assertIn('data-course="42"', setup.get_data(as_text=True))
                key = 'AQ.this-is-a-fake-unit-test-value'
                invalid = client.post('/settings/course-ai', data={'action': 'connect', 'api_key': key, 'semester': TERM, 'course': '42'})
                self.assertEqual(invalid.status_code, 400)
                self.assertIn('data-course="42"', invalid.get_data(as_text=True))
                self.assertNotIn(key, invalid.get_data(as_text=True))
                enabled = client.post('/settings/course-ai', data={'action': 'connect', 'api_key': key,
                    'consent': 'yes', 'semester': TERM, 'course': '42'})
                self.assertEqual(enabled.status_code, 302)
                self.assertIn('course=42', enabled.location)
                store = CourseInsightStorage(owner)
                claim = store.claim(name)
                plan = source('source', '期中考：12 月 10 日')
                snapshot = {'analyzed': time.time(), 'document': True, 'summary': '<script>fake</script>',
                            'grading': [], 'events': apply([event(plan, '12 月 10 日')], [plan])}
                self.assertTrue(store.save(name, TERM, 42, claim, snapshot))
                page = client.get(f'/courses/overview?semester={TERM}&course=42').get_data(as_text=True)
                self.assertIn('課程中心', page)
                self.assertIn('data-course="42"', page)
                self.assertIn('2026-12-10', page)
                self.assertIn('&lt;script&gt;fake&lt;/script&gt;', page)
                data = client.get(f'/api/course-overview?semester={TERM}&include=overview').get_json()
                self.assertEqual(data['events'][0]['date'], '2026-12-10')
                self.assertIn('考試與期限', data['overview'])
                self.assertNotIn(key, str(data))
                unchanged = client.get(f'/api/course-overview?semester={TERM}&include=overview'
                    f"&overview_version={data['overview_version']}").get_json()
                self.assertIsNone(unchanged['overview'])
                self.assertEqual(unchanged['courses_ready'], ['42'])
                self.assertEqual(client.post('/api/course-overview/refresh', json=[]).status_code, 400)
                self.assertEqual(client.post('/settings/course-ai', data={'action': 'disconnect'}).status_code, 302)
                self.assertEqual(client.get('/api/course-overview').get_json(), {'configured': False})
            finally:
                owner._engine.dispose()
