import unittest
from datetime import date

from e3_tracker.shared.study_gap_fill_runtime import _redistribute_progress_to_earliest_gap


class StudyGapFillRuntimeTests(unittest.TestCase):
    def test_progress_fills_earliest_open_day_before_later_subject_day(self):
        rows = [
            {
                "start": "2026-08-03",
                "end": "2026-08-09",
                "target_seconds": 7200,
                "watched_seconds": 0,
                "daily_recommendations": [
                    {"date": "2026-08-03", "target_seconds": 3600, "allocations": {"線性代數": 3600}},
                    {"date": "2026-08-04", "target_seconds": 3600, "allocations": {"離散數學": 3600}},
                ],
                "subject_mix": [],
            },
            {
                "start": "2026-08-10",
                "end": "2026-08-16",
                "target_seconds": 7200,
                "watched_seconds": 1800,
                "daily_recommendations": [
                    {"date": "2026-08-10", "target_seconds": 3600, "allocations": {"資料結構": 3600}},
                    {"date": "2026-08-11", "target_seconds": 3600, "allocations": {"資料結構": 3600}},
                ],
                "subject_mix": [],
            },
        ]

        _redistribute_progress_to_earliest_gap(rows, today=date(2026, 8, 11))

        self.assertEqual(rows[0]["daily_recommendations"][0]["watched_seconds"], 1800)
        self.assertEqual(rows[0]["daily_recommendations"][1]["watched_seconds"], 0)
        self.assertEqual(rows[1]["watched_seconds"], 0)

    def test_credit_only_reaches_future_after_all_earlier_slots_are_full(self):
        rows = [
            {
                "start": "2026-08-10",
                "end": "2026-08-16",
                "target_seconds": 3600,
                "watched_seconds": 3600,
                "daily_recommendations": [
                    {"date": "2026-08-10", "target_seconds": 3600, "allocations": {"離散數學": 3600}},
                ],
                "subject_mix": [],
            },
            {
                "start": "2026-08-17",
                "end": "2026-08-23",
                "target_seconds": 3600,
                "watched_seconds": 900,
                "daily_recommendations": [
                    {"date": "2026-08-17", "target_seconds": 3600, "allocations": {"資料結構": 3600}},
                ],
                "subject_mix": [],
            },
        ]

        _redistribute_progress_to_earliest_gap(rows, today=date(2026, 8, 11))

        self.assertEqual(rows[0]["watched_seconds"], 3600)
        self.assertEqual(rows[1]["watched_seconds"], 900)
        self.assertEqual(rows[1]["daily_recommendations"][0]["completion"], 25.0)
        self.assertEqual(rows[1]["state"], "active")


if __name__ == "__main__":
    unittest.main()
