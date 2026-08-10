import unittest
from datetime import date, timedelta

from e3_tracker.shared.study_combined_daily_progress_runtime import (
    _combined_daily_progress_rows,
)


class CombinedDailyProgressTests(unittest.TestCase):
    def _definitions(self):
        start = date(2026, 8, 10)
        daily_targets = []
        for offset in range(7):
            current = start + timedelta(days=offset)
            allocations = (
                {"線性代數": 1.45 * 3600, "資料結構": 2 * 3600}
                if offset == 1
                else {"線性代數": 1.7 * 3600, "資料結構": 1.7 * 3600}
            )
            daily_targets.append(
                {
                    "date": current,
                    "allocations": allocations,
                }
            )
        return [
            {
                "start": start,
                "end": start + timedelta(days=6),
                "daily_targets": daily_targets,
            }
        ]

    def test_two_subjects_share_one_daily_pool(self):
        events = [
            {
                "day": "2026-08-11",
                "subject": "資料結構",
                "delta_seconds": 3 * 3600,
            }
        ]
        payload = _combined_daily_progress_rows(
            self._definitions(),
            events,
            today=date(2026, 8, 11),
        )
        day = next(row for row in payload["days"] if row["date"] == "2026-08-11")

        # DS is scheduled for 2h, but its 3h progress is allowed to count toward the
        # combined Linear Algebra + DS target of 3.45h.
        self.assertEqual(day["watched_seconds"], 10800.0)
        self.assertAlmostEqual(day["target_seconds"], 12420.0)
        self.assertAlmostEqual(day["completion"], 87.0, places=1)
        self.assertEqual(day["state"], "partial")

    def test_subject_changes_on_same_day_are_added_together(self):
        events = [
            {
                "day": "2026-08-11",
                "subject": "線性代數",
                "delta_seconds": 0.5 * 3600,
            },
            {
                "day": "2026-08-11",
                "subject": "資料結構",
                "delta_seconds": 1.25 * 3600,
            },
        ]
        payload = _combined_daily_progress_rows(
            self._definitions(),
            events,
            today=date(2026, 8, 11),
        )
        day = next(row for row in payload["days"] if row["date"] == "2026-08-11")
        self.assertEqual(day["watched_seconds"], 6300.0)
        self.assertAlmostEqual(day["completion"], 50.7, places=1)

    def test_progress_stays_on_the_day_it_happened(self):
        events = [
            {
                "day": "2026-08-11",
                "subject": "資料結構",
                "delta_seconds": 1200,
            }
        ]
        payload = _combined_daily_progress_rows(
            self._definitions(),
            events,
            today=date(2026, 8, 11),
        )
        by_date = {row["date"]: row for row in payload["days"]}
        self.assertEqual(by_date["2026-08-11"]["watched_seconds"], 1200.0)
        self.assertEqual(by_date["2026-08-12"]["watched_seconds"], 0.0)
        self.assertEqual(by_date["2026-08-16"]["watched_seconds"], 0.0)

    def test_unplanned_subject_does_not_fill_the_day(self):
        events = [
            {
                "day": "2026-08-11",
                "subject": "作業系統",
                "delta_seconds": 3600,
            }
        ]
        payload = _combined_daily_progress_rows(
            self._definitions(),
            events,
            today=date(2026, 8, 11),
        )
        day = next(row for row in payload["days"] if row["date"] == "2026-08-11")
        self.assertEqual(day["watched_seconds"], 0.0)


if __name__ == "__main__":
    unittest.main()
