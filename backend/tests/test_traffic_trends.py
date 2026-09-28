import unittest
from datetime import datetime, timezone

from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.platform.services.traffic_trends import build_traffic_trend


class TrafficTrendTests(unittest.TestCase):
    now = TAIPEI_TZ.localize(datetime(2026, 1, 2, 10, 30))

    def timestamp(self, day, hour):
        return int(TAIPEI_TZ.localize(datetime(2026, 1, day, hour)).timestamp())

    def build(self, buckets=None, series=None, events=None, **params):
        return build_traffic_trend(series or [], buckets or {}, events or [], params, now=self.now)

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


if __name__ == "__main__":
    unittest.main()
