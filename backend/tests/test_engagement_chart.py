import unittest
from datetime import datetime

from bs4 import BeautifulSoup
from jinja2 import Environment, FileSystemLoader, select_autoescape
from pathlib import Path

from e3_tracker.assignments.services.admin_analytics import build_account_engagement
from e3_tracker.platform.constants import TAIPEI_TZ


class EngagementChartTests(unittest.TestCase):
    def build(self, daily, start='2026-10-01', end='2026-10-04'):
        snapshot = {
            'accounts': [{'id': 1, 'username': '112550103'},
                         {'id': 2, 'username': 'Session-one', 'student_number': '112550103'},
                         {'id': 3, 'username': '113550092'}],
            'daily': [{'user_id': user, 'day': day} for user, day in daily],
            'usage': [], 'settings': [], 'linked': {'line': set(), 'browser': set(), 'google': set()},
            'state': {'started_at': 0, 'legacy_samples': 0},
        }
        return build_account_engagement(snapshot, {}, {'start': start, 'end': end},
                                        now=TAIPEI_TZ.localize(datetime(2026, 10, 4)))

    def test_chart_unions_users_instead_of_summing_visits_and_fills_idle_days(self):
        result = self.build([(1, '2026-09-30'), (1, '2026-10-01'), (2, '2026-10-02'),
                             (3, '2026-10-04'), (3, '2026-10-05')])
        self.assertEqual([row['count'] for row in result['chart']['rows']], [1, 1, 1, 2])
        self.assertEqual(result['chart']['rows'][-1]['count'], result['active'])
        self.assertEqual(result['active'], 2)
        narrowed = self.build([(1, '2026-10-01'), (3, '2026-10-04')], start='2026-10-03')
        self.assertEqual([row['count'] for row in narrowed['chart']['rows']], [0, 1])

    def test_empty_and_single_day_series_have_valid_coordinates(self):
        for daily in ([], [(1, '2026-10-04')]):
            with self.subTest(daily=daily):
                result = self.build(daily, start='2026-10-04')
                self.assertEqual(len(result['chart']['rows']), 1)
                self.assertGreater(result['chart']['ceiling'], 0)
                self.assertEqual(result['chart']['rows'][0]['x'], 8)
                self.assertEqual(result['chart']['rows'][-1]['count'], result['active'])

    def test_rolling_window_includes_history_before_plot_and_expires_old_users(self):
        result = self.build([(1, '2026-09-24'), (1, '2026-09-28'), (3, '2026-09-30')])
        chart = result['chart']
        self.assertEqual([row['count'] for row in chart['rows']], [2, 1, 1, 0])
        self.assertEqual([row['start'] for row in chart['rows']],
                         ['2026-09-28', '2026-09-29', '2026-09-30', '2026-10-01'])
        self.assertTrue(chart['has_data'])  # Still show history when today's rolling count is zero.
        self.assertEqual(result['active'], 0)
        self.assertEqual(chart['title'], '近 4 天使用人數')

    def test_rolling_counts_keep_returning_aliases_until_their_last_visit_expires(self):
        result = self.build([(1, '2026-09-28'), (2, '2026-10-01'), (3, '2026-10-03')])
        self.assertEqual([row['count'] for row in result['chart']['rows']], [1, 1, 2, 2])
        self.assertEqual(result['chart']['rows'][-1]['count'], result['active'])

    def test_template_has_accessible_chart_and_matching_table_or_empty_state(self):
        root = Path(__file__).resolve().parents[2] / 'frontend/shared/templates'
        env = Environment(loader=FileSystemLoader(root), autoescape=select_autoescape())
        template = env.get_template('components/admin_account_engagement.html')
        for daily in ([], [(1, '2026-10-01'), (3, '2026-10-04')]):
            result = self.build(daily)
            page = BeautifulSoup(template.render(account_engagement=result, ga4_primary=False,
                assignment_analytics={'start': '2026-10-01', 'end': '2026-10-04'}), 'html.parser')
            if daily:
                self.assertIsNotNone(page.select_one('svg[role="img"] desc'))
                self.assertEqual(len(page.select('.engagement-chart-values tbody tr')), 4)
                self.assertIn('2 人', page.select('.engagement-chart-values tbody tr')[-1].get_text())
            else:
                self.assertIsNone(page.select_one('svg'))
                self.assertIn('這個區間尚無使用紀錄', page.get_text())
