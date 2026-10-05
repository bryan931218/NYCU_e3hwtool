import unittest
from datetime import datetime
from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.assignments.domain.assignment_actions import deadline_dates


class DeadlineParserTests(unittest.TestCase):
    now = int(TAIPEI_TZ.localize(datetime(2026, 10, 5, 12)).timestamp())
    baseline = int(TAIPEI_TZ.localize(datetime(2026, 10, 9, 17)).timestamp())

    def dates(self, text, **kwargs):
        return deadline_dates({'title': 'HW1 deadline update', 'content': text, 'updated_ts': self.now}, now=self.now, **kwargs)

    def date(self, text, expected, **kwargs):
        entries = self.dates(text, **kwargs)
        self.assertEqual(len(entries), 1, (text, entries))
        self.assertEqual(datetime.fromtimestamp(entries[0]['due_ts'], TAIPEI_TZ).strftime('%Y-%m-%d %H:%M'), expected)
        return entries[0]

    def test_chinese_english_and_fullwidth_formats(self):
        cases = {
            '截止：２０２６／１０／０９ ２３：５９': '2026-10-09 23:59',
            '截止：115年10月9日下午11點59分': '2026-10-09 23:59',
            '截止：二〇二六年十月九日晚上十一點五十九分': '2026-10-09 23:59',
            'Due October 9, 2026 at 11:59 PM': '2026-10-09 23:59',
            'Due 9th October 2026 at 5 p.m.': '2026-10-09 17:00',
            'Due Oct 9th at 12 AM': '2026-10-09 00:00',
            'Due 2026-10-09T15:59:00Z': '2026-10-09 23:59',
            'Due 2026/10/09 17:00 UTC+08:00': '2026-10-09 17:00',
            'Due 2026/10/09 17:00 +0530': '2026-10-09 19:30',
            '截止 2026.10.09 24:00': '2026-10-10 00:00',
            'Due 15/10/2026 12:00': '2026-10-15 12:00',
            'Due at 5 PM on October 9, 2026': '2026-10-09 17:00',
            '截止 2026/10/09 中午': '2026-10-09 12:00',
            'Due October 9, 2026 at noon': '2026-10-09 12:00',
            '截止时间改为2026年10月9日下午5点': '2026-10-09 17:00',
        }
        for text, expected in cases.items():
            with self.subTest(text=text): self.date(text, expected)

    def test_ambiguous_dates_timezone_and_missing_time_are_visible(self):
        self.assertTrue(self.date('Due 10/11/2026 23:59', '2026-10-11 23:59')['warnings'])
        self.assertTrue(self.date('Due 2026/10/09 17:00 CST', '2026-10-09 17:00')['warnings'])
        self.assertTrue(self.date('截止 2026/10/09（週四）17:00', '2026-10-09 17:00')['warnings'])
        self.assertTrue(self.date('Due 2026/10/09 (Thursday) 17:00', '2026-10-09 17:00')['warnings'])
        self.assertFalse(self.date('Due October 9, 2026', '2026-10-09 23:59')['time_explicit'])

    def test_relative_dates_use_message_time_not_reading_time(self):
        self.date('截止明天下午5點', '2026-10-06 17:00')
        self.date('Due tomorrow at 5 PM', '2026-10-06 17:00')
        self.date('Due next Friday at 17:00', '2026-10-16 17:00')
        self.date('截止下週五 17:00', '2026-10-16 17:00')
        self.date('截止本週五 17:00', '2026-10-09 17:00')
        old = {'content': 'Due tomorrow 17:00', 'updated_ts': self.now-10*86400}
        self.assertEqual(deadline_dates(old, now=self.now), [])

    def test_duration_and_time_only_changes_require_unique_assignment_baseline(self):
        self.date('Deadline extended by 3 days', '2026-10-12 17:00', baseline_due=self.baseline)
        self.date('期限延後三天', '2026-10-12 17:00', baseline_due=self.baseline)
        self.date('期限改為晚上11點半', '2026-10-09 23:30', baseline_due=self.baseline)
        self.date('The deadline changed from 5 PM to 11:59 PM', '2026-10-09 23:59', baseline_due=self.baseline)
        self.assertEqual(self.dates('Deadline extended by 3 days'), [])
        self.assertEqual(self.dates('期限改為晚上11點半'), [])

    def test_original_quote_negative_and_unrelated_dates_do_not_become_changes(self):
        self.date('Original deadline: October 8, 2026 at 17:00. Extended to October 9, 2026 at 17:00.', '2026-10-09 17:00')
        self.date('From 2026/10/08 17:00 to 2026/10/09 17:00', '2026-10-09 17:00')
        self.assertEqual(self.dates('Deadline unchanged: 2026/10/09 17:00'), [])
        self.assertEqual(self.dates('期限不變 2026/10/09 17:00'), [])
        self.assertEqual(self.dates('Exam 2026/10/09 17:00'), [])
        self.assertEqual(self.dates('HW1-2 deadline extended; more information later'), [])
        self.assertEqual(self.dates('HW 1-2 deadline extended; more information later'), [])
        self.assertEqual(self.dates('Deadline unchanged. Score 3/5 points'), [])
        self.assertEqual(self.dates('> Due 2026/10/09 17:00'), [])
        self.assertEqual(self.dates('<blockquote>Due 2026/10/09 17:00</blockquote><p>See original message.</p>'), [])

    def test_invalid_times_and_dates_never_silently_fall_back(self):
        for text in ['Due 2026/10/32 17:00', 'Due 2026/10/09 23:99', 'Due 2026/10/09 24:01',
                     'Due 2026/10/09 17 PM', 'Due 2026/10/09 17:00 +2500', 'Due February 31, 2027']:
            with self.subTest(text=text): self.assertEqual(self.dates(text), [])

    def test_year_rollover_and_bounded_input(self):
        self.date('Due January 5 at 17:00', '2027-01-05 17:00')
        self.date('截止 1月5日 17:00', '2027-01-05 17:00')
        self.assertEqual(self.dates('x'*21000+' Due 2026/10/09 17:00'), [])


if __name__ == '__main__': unittest.main()
