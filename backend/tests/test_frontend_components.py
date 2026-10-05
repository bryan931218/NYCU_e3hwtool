import json
from pathlib import Path
import unittest

from bs4 import BeautifulSoup
from flask import Flask, render_template
from e3_tracker.platform.assets import configure_frontend


class FrontendComponentTests(unittest.TestCase):
    def render(self, *, grade_text=None, extra_courses=(), submitted_count=5, participant_count=10, surname='', guest_mode=False, overdue=True, completed=False, readonly=False, google_ready=False, google_linked=False, excel_data='', admin=False):
        app = Flask(__name__)
        configure_frontend(app)
        app.secret_key = 'component-test'
        app.jinja_env.globals['url_for'] = lambda endpoint, **kwargs: '/' + endpoint
        item = {
            'course_id': 1, 'course_title': 'Course', 'semester_key': '115-1',
            'title': '<img src=x onerror=alert(1)>', 'url': 'https://example.test/task',
            'due_at': '2026-09-25 12:00', 'due_ts': 100, 'overdue': overdue,
            'completed': completed, 'raw_status_text': '', 'grade_text': grade_text,
            'submitted_count': submitted_count, 'participant_count': participant_count,
        }
        uid = '1|' + item['title'] + '|' + item['url']
        preferences = {'view_mode': 'due', 'status_filter': ['overdue'],
                       'semester_filter': ['115-1'], 'ignored_overdue_uids': [uid]}
        result = {'courses': [{'id': 1, 'title': 'Course', 'semester_key': '115-1', 'assignments': [item]}, *extra_courses],
                  'all_assignments': [item], 'errors': [], 'available_semesters': [], 'selected_semesters': ['115-1']}
        with app.test_request_context('/'):
            html = render_template('assignments/web.html', result=result, preferences=preferences,
                                   user={'username': 'qa', 'surname': surname, 'is_admin': admin}, viewed_username='qa',
                                   guest_mode=guest_mode, is_admin_view=readonly, google_ready=google_ready,
                                   google_linked=google_linked, excel_data=excel_data, now_ts=200,
                                   last_updated_label='2026-09-26 13:31', stats={'online': 3, 'total': 3286})
        return BeautifulSoup(html, 'html.parser'), uid

    def test_more_operations_groups_download_calendar_and_extension_with_explicit_disconnect(self):
        page, _uid = self.render(google_ready=True, google_linked=True, excel_data='cHJldmlldw==', admin=True)
        panel = page.select_one('#moreOperationsPanel')
        self.assertEqual([h.get_text() for h in panel.select('h3')], ['作業資料', 'Google 日曆', '瀏覽器套件'])
        self.assertIsNone(panel.select_one('.btn'))
        self.assertEqual(panel.select_one('[data-log-action="download_excel"]')['download'], '待繳作業.xlsx')
        self.assertEqual(panel.select_one('#googleSyncBtn').get_text(strip=True), '同步作業到日曆')
        self.assertEqual(panel.select_one('.more-connection').get_text(), '已連結')
        form = panel.select_one('form[action="/google_unlink"]')
        self.assertEqual(form['method'], 'post')
        self.assertIsNotNone(form.select_one('input[name="csrf_token"]'))
        button = form.select_one('button')
        self.assertEqual(button.get_text(strip=True), '解除連結')
        self.assertIn('確定解除', button['data-confirm'])
        self.assertNotIn('管理 Google 連結', panel.get_text())
        self.assertEqual(panel.select_one('a[href="/e3_navigation_extension"]')['target'], '_blank')

    def test_more_operations_preserves_unlinked_unavailable_and_readonly_states(self):
        for options, expected in [({'google_ready': True}, '連結 Google 日曆'),
                                  ({}, '尚未開放日曆同步'),
                                  ({'google_ready': True, 'google_linked': True, 'readonly': True}, '唯讀模式無法同步')]:
            with self.subTest(options=options):
                page, _uid = self.render(**options)
                panel = page.select_one('#moreOperationsPanel')
                self.assertIn(expected, panel.get_text())
                self.assertIsNone(panel.select_one('#googleSyncBtn'))
                self.assertIsNone(panel.select_one('form'))
                self.assertIsNone(panel.select_one('#moreExportHeading'))
                self.assertIsNone(panel.select_one('.more-connection'))

    def test_e3_return_entry_keeps_native_links_and_escapes_assignment_metadata(self):
        document, _uid = self.render(admin=True)
        for link in document.select('.assignment-link a'):
            self.assertTrue(link.has_attr('data-e3-assignment'))
            self.assertEqual(link['href'], 'https://example.test/task')
            self.assertEqual(link['target'], '_blank')
            self.assertIn('noopener', link['rel'])
            self.assertEqual(link.find_parent('tr')['data-title'], '<img src=x onerror=alert(1)>')
        self.assertIsNone(document.select_one('#e3NavigationRecovery'))
        self.assertIsNotNone(document.select_one('a[href="/e3_navigation_extension"]'))

    def test_extension_entry_only_shows_for_authenticated_admins(self):
        for options, visible in [({}, False), ({'guest_mode': True}, False),
                                 ({'admin': True}, True), ({'admin': True, 'readonly': True}, True),
                                 ({'admin': True, 'guest_mode': True}, False)]:
            with self.subTest(options=options):
                page, _uid = self.render(**options)
                self.assertEqual(page.select_one('#moreExtensionHeading') is not None, visible)
                self.assertEqual(page.select_one('a[href="/e3_navigation_extension"]') is not None, visible)

    def test_both_views_share_escaped_content_and_ignored_state(self):
        document, uid = self.render()
        rows = document.select('tr[data-uid]')
        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertEqual(row['data-uid'], uid)
            self.assertEqual(row['data-course-id'], '1')
            self.assertEqual(row['data-ignored'], '1')
            self.assertEqual(row['data-primary-status'], 'overdue')
            self.assertEqual(row.select_one('.assignment-title').get_text(), '<img src=x onerror=alert(1)>')
            self.assertIsNone(row.select_one('img'))
            self.assertIn('已逾期', row.select_one('.remaining-text').get_text())
        config = json.loads(document.select_one('#workbench-config').string)
        self.assertEqual(config['preferences']['ignored_overdue_uids'], [uid])

    def test_grade_and_status_are_consistent_in_both_views(self):
        document, _uid = self.render(grade_text='95')
        for row in document.select('tr[data-uid]'):
            self.assertEqual(row['data-primary-status'], 'graded')
            self.assertEqual(row.select_one('.badge.graded').get_text(), '已評分')
            self.assertIsNone(row.select_one('.badge.overdue'))

    def test_ignore_available_for_pending_and_overdue_but_not_completed_or_readonly(self):
        for extra, expected in [({'overdue': False}, 2), ({}, 2),
                                ({'completed': True}, 0), ({'grade_text': '95'}, 0),
                                ({'overdue': False, 'readonly': True}, 0)]:
            with self.subTest(extra=extra):
                document, _uid = self.render(**extra)
                self.assertEqual(len(document.select('[data-ignore-assignment]')), expected)
                self.assertIsNone(document.select_one('[data-ignore-overdue]'))
                self.assertIn('已忽略作業', document.get_text())
                self.assertNotIn('已忽略逾期作業', document.get_text())

    def test_course_options_only_contain_selected_semester_even_without_assignments(self):
        document, _uid = self.render(extra_courses=[
            {'id': 2, 'title': 'Old course', 'semester_key': '114-2', 'assignments': []},
            {'id': 3, 'title': 'Empty current course', 'semester_key': '115-1', 'assignments': []},
        ])
        self.assertEqual([option['value'] for option in document.select('#courseFilter option')],
                         ['', 'Course', 'Empty current course', 'custom'])
        self.assertEqual(len(document.select('#viewCourse .course-card')), 3)
        self.assertEqual([card['data-course-id'] for card in document.select('#viewCourse .course-card')], ['1', '2', '3'])
        link = document.select_one('#flatTable .assignment-link a')
        self.assertEqual(link.select_one('span[aria-hidden=true]').get_text(), '↗')

    def test_submission_ratio_uses_theme_aware_categories_and_handles_zero_participants(self):
        for submitted, participants, category in [(0, 0, 'ratio-low'), (2, 10, 'ratio-low'),
                                                   (5, 10, 'ratio-mid'), (9, 10, 'ratio-high')]:
            with self.subTest(submitted=submitted, participants=participants):
                document, _uid = self.render(submitted_count=submitted, participant_count=participants)
                for pill in document.select('.submission-pill'):
                    self.assertIn(category, pill['class'])
                    self.assertNotIn('--submission-hue', pill['style'])

    def test_site_name_and_statistics_are_in_header(self):
        document, _uid = self.render()
        self.assertEqual(document.title.get_text(), 'E3作業追蹤系統')
        self.assertEqual(document.select_one('meta[property="og:title"]')['content'], 'E3作業追蹤系統')
        self.assertEqual(document.select_one('.app-header h1').get_text(), 'E3作業追蹤系統')
        self.assertEqual(document.select_one('.app-header #siteOnlineCount').get_text(), '3')
        self.assertEqual(document.select_one('.app-header #siteTotalCount').get_text(), '3286')
        self.assertEqual(document.select_one('.app-header #siteLastUpdated').get_text(), '2026-09-26 13:31')
        self.assertIsNone(document.select_one('.site-footer .site-metrics'))

    def test_avatar_uses_surname_with_safe_guest_and_missing_profile_fallbacks(self):
        for surname in ('王', '歐陽', '<script>'):
            document, _uid = self.render(surname=surname)
            avatar = document.select_one('#userAvatar')
            self.assertEqual(avatar.get_text(strip=True), surname)
            self.assertIsNone(avatar.select_one('script'))
        document, _uid = self.render()
        self.assertIsNotNone(document.select_one('#userAvatar svg'))
        document, _uid = self.render(guest_mode=True)
        avatar = document.select_one('#userAvatar')
        self.assertEqual(avatar.get_text(strip=True), '訪')
        self.assertNotIn('data-profile-url', avatar.attrs)

    def test_assignment_columns_have_labels_and_submission_legend(self):
        document, _uid = self.render()
        self.assertEqual([label.get_text() for label in document.select('.assignment-column-head > span')],
                         ['課程／作業名稱', '截止時間', '剩餘時間', '繳交人數', '狀態', '操作'])
        for row in document.select('tr[data-uid]'):
            self.assertIn('截止時間', [label.get_text() for label in row.select('.assignment-field-label')])
            self.assertEqual(row.select_one('.submission-label').get_text(), '已繳交／總人數')
            self.assertEqual(row.select_one('.submission-pill-count').get_text(), '5')
            self.assertEqual(row.select_one('.submission-pill-total').get_text(), '10')

    def test_search_has_its_own_non_login_form_and_password_manager_opt_out(self):
        document, _uid = self.render()
        field = document.select_one('#assignmentSearch')
        form = field.find_parent('form')
        self.assertEqual(form['id'], 'assignmentSearchForm')
        self.assertEqual(form['role'], 'search')
        self.assertEqual(form['method'], 'post')
        self.assertEqual(form['autocomplete'], 'off')
        self.assertEqual(field['type'], 'search')
        self.assertEqual(field['name'], 'assignment_query')
        self.assertEqual(field['aria-label'], '搜尋作業或課程')
        self.assertEqual(field['autocomplete'], 'off')
        self.assertTrue(field.has_attr('data-1p-ignore'))
        self.assertEqual(field['data-lpignore'], 'true')
        self.assertFalse(field.has_attr('readonly'))
        self.assertIsNone(form.select_one('input[type="password"]'))


if __name__ == '__main__':
    unittest.main()
