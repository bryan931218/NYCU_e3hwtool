import unittest

from e3_tracker.shared.study_activity_progress_runtime import credit_only_new_video_progress


class StudyActivityProgressRuntimeTests(unittest.TestCase):
    def test_completed_video_is_not_counted_again_on_later_day(self):
        events = [
            {
                "day": "2026-08-24",
                "video_id": 35,
                "subject": "資料結構",
                "sequence": 35,
                "previous_watched_seconds": 0,
                "watched_seconds": 2 * 3600 + 9 * 60,
                "delta_seconds": 2 * 3600 + 9 * 60,
                "updated_at": "2026-08-24T10:00:00",
            },
            {
                "day": "2026-08-29",
                "video_id": 35,
                "subject": "資料結構",
                "sequence": 35,
                "previous_watched_seconds": 0,
                "watched_seconds": 2 * 3600 + 9 * 60,
                "delta_seconds": 2 * 3600 + 9 * 60,
                "updated_at": "2026-08-29T12:00:00",
            },
        ]

        credited = credit_only_new_video_progress(events)

        self.assertEqual(credited[0]["delta_seconds"], 2 * 3600 + 9 * 60)
        self.assertEqual(credited[1]["raw_delta_seconds"], 2 * 3600 + 9 * 60)
        self.assertEqual(credited[1]["delta_seconds"], 0)

    def test_only_progress_beyond_previous_high_water_is_new(self):
        events = [
            {
                "day": "2026-08-27",
                "video_id": 35,
                "previous_watched_seconds": 0,
                "watched_seconds": 7200,
                "delta_seconds": 7200,
                "updated_at": "2026-08-27T10:00:00",
            },
            {
                "day": "2026-08-29",
                "video_id": 35,
                "previous_watched_seconds": 3600,
                "watched_seconds": 7500,
                "delta_seconds": 3900,
                "updated_at": "2026-08-29T10:00:00",
            },
        ]

        credited = credit_only_new_video_progress(events)

        self.assertEqual(credited[1]["delta_seconds"], 300)

    def test_rewind_never_becomes_positive_daily_progress(self):
        events = [
            {
                "day": "2026-08-28",
                "video_id": 35,
                "previous_watched_seconds": 0,
                "watched_seconds": 7740,
                "delta_seconds": 7740,
                "updated_at": "2026-08-28T10:00:00",
            },
            {
                "day": "2026-08-29",
                "video_id": 35,
                "previous_watched_seconds": 7740,
                "watched_seconds": 120,
                "delta_seconds": -7620,
                "updated_at": "2026-08-29T10:00:00",
            },
        ]

        credited = credit_only_new_video_progress(events)

        self.assertEqual(credited[1]["raw_delta_seconds"], -7620)
        self.assertEqual(credited[1]["delta_seconds"], 0)

    def test_existing_progress_before_activity_logging_is_baseline(self):
        events = [
            {
                "day": "2026-08-29",
                "video_id": 35,
                "previous_watched_seconds": 7740,
                "watched_seconds": 7800,
                "delta_seconds": 60,
                "updated_at": "2026-08-29T10:00:00",
            }
        ]

        credited = credit_only_new_video_progress(events)

        self.assertEqual(credited[0]["delta_seconds"], 60)

    def test_progress_is_tracked_independently_per_video(self):
        events = [
            {
                "day": "2026-08-29",
                "video_id": 35,
                "previous_watched_seconds": 0,
                "watched_seconds": 600,
                "delta_seconds": 600,
                "updated_at": "2026-08-29T10:00:00",
            },
            {
                "day": "2026-08-29",
                "video_id": 36,
                "previous_watched_seconds": 0,
                "watched_seconds": 900,
                "delta_seconds": 900,
                "updated_at": "2026-08-29T10:05:00",
            },
        ]

        credited = credit_only_new_video_progress(events)

        self.assertEqual([item["delta_seconds"] for item in credited], [600, 900])


if __name__ == "__main__":
    unittest.main()
