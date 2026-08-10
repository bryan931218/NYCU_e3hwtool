import unittest
from datetime import datetime

from e3_tracker.shared.constants import TAIPEI_TZ
from e3_tracker.shared.study_progress_runtime import (
    _learning_day_fraction,
    _time_aware_progress_race,
)


class StudyProgressRuntimeTests(unittest.TestCase):
    def test_learning_day_fraction(self):
        self.assertAlmostEqual(
            _learning_day_fraction(datetime(2026, 8, 10, 8, 0, tzinfo=TAIPEI_TZ)),
            0.0,
            places=4,
        )
        self.assertAlmostEqual(
            _learning_day_fraction(datetime(2026, 8, 10, 20, 0, tzinfo=TAIPEI_TZ)),
            0.5,
            places=4,
        )

    def test_day_does_not_start_behind(self):
        race = _time_aware_progress_race(
            0, 210, 1000, 210,
            now=datetime(2026, 8, 10, 8, 0, tzinfo=TAIPEI_TZ),
        )
        self.assertEqual(race["state"], "active")
        self.assertEqual(race["target_hours"], 0.0)

    def test_elapsed_target_can_be_behind(self):
        race = _time_aware_progress_race(
            60, 210, 1000, 210,
            now=datetime(2026, 8, 10, 20, 0, tzinfo=TAIPEI_TZ),
        )
        self.assertEqual(race["state"], "behind")
        self.assertEqual(race["target_hours"], 1.8)
        self.assertEqual(race["delta_minutes"], -45.0)

    def test_ten_minute_tolerance(self):
        race = _time_aware_progress_race(
            110, 210, 1000, 210,
            now=datetime(2026, 8, 10, 20, 0, tzinfo=TAIPEI_TZ),
        )
        self.assertEqual(race["state"], "active")

        race = _time_aware_progress_race(
            130, 210, 1000, 210,
            now=datetime(2026, 8, 10, 20, 0, tzinfo=TAIPEI_TZ),
        )
        self.assertEqual(race["state"], "early")


if __name__ == "__main__":
    unittest.main()
