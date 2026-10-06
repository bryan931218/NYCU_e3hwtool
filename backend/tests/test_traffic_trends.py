import unittest
from datetime import datetime, timezone

from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.platform.services.traffic_trends import build_traffic_trend


class TrafficTrendTests(unittest.TestCase):
    now = TAIPEI_TZ.localize(datetime(2026, 1, 2, 10, 30))

    def timestamp(self, day, hour):
        return int(TAIPEI_TZ.localize(datetime(2026, 1, day, hour)).timestamp())

    def build(self, buckets=None, series=None, events=None, memberships=(), daily_counts=None, **params):
        return build_traffic_trend(series or [], buckets or {}, events or [], params, memberships=memberships, daily_counts=daily_counts, now=self.now)

    def test_daily_chart_uses_homepage_source_without_adding_new_users(self):
        buckets = {self.timestamp(2, 8): {str(index) for index in range(13)}}
        members = [self.member(str(index), 2, 8) for index in range(3)]
        for count in (0, 13, 16):
            trend = self.build(buckets=buckets, memberships=members, daily_counts={'2026-01-02': count}, range='today', trend='day')
            self.assertEqual(trend['values'], [count])
            self.assertEqual(trend['new_values'], [3])
            self.assertEqual(trend['active_label'], '活躍人數')

    def test_persisted_daily_counts_keep_older_history_and_do_not_change_hourly_series(self):
        buckets = {self.timestamp(1, 8): {'legacy'}, self.timestamp(2, 8): {'alice'}}
        trend = self.build(buckets=buckets, daily_counts={'2026-01-02': 3, 'bad': 5, '2026-01-03': 99})
        self.assertEqual(trend['values'][-2:], [1, 3])
        hourly = self.build(buckets=buckets, daily_counts={'2026-01-02': 3}, range='today', trend='hour')
        self.assertEqual(hourly['values'][8], 1)
        self.assertEqual(hourly['active_label'], '活躍帳號數')

    def member(self, key, day, hour, **changes):
        return {'identity_key': key, 'joined_at': self.timestamp(day, hour), 'is_new': True, **changes}

    def test_daily_counts_deduplicate_accounts_across_hours_and_fill_quiet_days(self):
        buckets = {self.timestamp(1, 8): {"alice"}, self.timestamp(1, 9): {"alice", "bob"}}
        trend = self.build(buckets=buckets)
        self.assertEqual(trend["resolution"], "day")
        self.assertEqual(trend["labels"][0], "2025-12-27")
        self.assertEqual(trend["labels"][-1], "2026-01-02")
        self.assertEqual(trend["values"], [0, 0, 0, 0, 0, 2, 0])
        self.assertEqual(trend["peak"], 2)

    def test_today_ends_at_current_taipei_hour_instead_of_last_event(self):
        trend = self.build(series=[{"ts": self.timestamp(2, 1), "count": 3}], range="today")
        self.assertEqual(trend["resolution"], "hour")
        self.assertEqual(trend["labels"][-1], "2026-01-02 10:00")
        self.assertEqual(trend["values"], [0, 3] + [0] * 9)
        self.assertIsNone(trend["next"])

    def test_taipei_date_boundary_for_utc_clock(self):
        trend = build_traffic_trend([], {}, [], {"range": "today"},
                                    now=datetime(2026, 1, 1, 17, tzinfo=timezone.utc))
        self.assertEqual(trend["start"], "2026-01-02")
        self.assertEqual(trend["labels"], ["2026-01-02 00:00", "2026-01-02 01:00"])

    def test_fallback_ignores_guests_and_passive_events_and_deduplicates(self):
        def event(name, hour=1, **extra):
            return {"ts": self.timestamp(2, hour), "action": "login_success",
                    "meta": {"username": name}, **extra}
        events = [event("alice"), event("alice", 2), event("bob"),
                  event("guest", meta={"username": "guest", "is_guest": True}),
                  event("passive", action="heartbeat"), {"ts": "invalid", "meta": {"username": "x"}}]
        trend = self.build(events=events, range="today", trend="day")
        self.assertEqual(trend["values"], [2])

    def test_invalid_custom_ranges_recover_and_large_hourly_ranges_use_days(self):
        for start, end in [("invalid", "2026-01-02"), ("2026-01-02", "2026-01-01"),
                           ("2026-01-01", "2026-01-03"), ("0001-01-01", "2026-01-01")]:
            trend = self.build(range="custom", start=start, end=end)
            self.assertEqual(trend["range"], "7d")
            self.assertTrue(trend["notice"])
        trend = self.build(range="custom", start="2025-11-01", end="2026-01-02", trend="hour")
        self.assertEqual(trend["resolution"], "day")
        self.assertTrue(trend["notice"])

    def test_navigation_keeps_span_and_does_not_advance_into_future(self):
        trend = self.build(buckets={self.timestamp(1, 1): {"alice"}},
                           range="custom", start="2026-01-01", end="2026-01-01")
        self.assertIsNone(trend["previous"])
        self.assertEqual(trend["next"], {"start": "2026-01-02", "end": "2026-01-02"})
        self.assertEqual(trend["query"], {"range": "custom", "start": "2026-01-01", "end": "2026-01-01"})

    def test_new_users_are_deduplicated_and_baselined_accounts_are_excluded(self):
        members = [self.member('student:alice',1,8), self.member('student:alice',2,9),
                   self.member('student:bob',2,1), self.member('student:old',1,1,is_new=False),
                   self.member('student:old',2,2), self.member('student:future',2,11),
                   self.member('invalid',2,1,joined_at='invalid'), {'joined_at':self.timestamp(2,1)}]
        trend = self.build(memberships=members)
        self.assertEqual(trend['new_values'],[0,0,0,0,0,1,1])
        self.assertEqual(trend['new_total'],2)
        self.assertEqual([row['new_users'] for row in trend['rows']],trend['new_values'])
        self.assertTrue(trend['has_data'])
        self.assertEqual(trend['values'],[0]*7)

    def test_new_users_follow_taipei_hours_and_selected_range(self):
        members=[self.member('alice',1,23), self.member('bob',2,0), self.member('charlie',2,10)]
        trend=self.build(memberships=members,range='today')
        self.assertEqual(trend['new_values'],[1]+[0]*9+[1])
        self.assertEqual(trend['new_total'],2)
        trend=self.build(memberships=members,range='custom',start='2026-01-01',end='2026-01-01',trend='day')
        self.assertEqual(trend['new_values'],[1])
        self.assertEqual(trend['new_total'],1)

    def test_all_range_uses_first_join_history_even_without_retained_traffic_events(self):
        trend=self.build(memberships=[self.member('alice',1,1)],range='all')
        self.assertEqual(trend['labels'],['2026-01-01','2026-01-02'])
        self.assertEqual(trend['new_values'],[1,0])
        self.assertTrue(trend['has_history'])
        baseline=self.build(memberships=[self.member('legacy',1,1,is_new=False)],range='all')
        self.assertFalse(baseline['has_data'])
        self.assertEqual(baseline['new_total'],0)

    def test_event_claims_are_not_used_as_first_join_statistics(self):
        trend=self.build(events=[{'ts':self.timestamp(2,1),'action':'login_success',
                                 'meta':{'username':'alice','is_new_user':True}}],range='today')
        self.assertEqual(trend['new_total'],0)


if __name__ == "__main__":
    unittest.main()
