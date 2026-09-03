import unittest
from datetime import date, timedelta
from types import SimpleNamespace

from e3_tracker.shared.study_rest_day_runtime import (
    install_study_rest_day_runtime,
    redistribute_rest_day_allocations,
)


class StudyRestDayRuntimeTests(unittest.TestCase):
    def _week(self, number, start_day, daily_allocations, *, replanned=True):
        daily_targets = []
        for index, allocations in enumerate(daily_allocations):
            scheduled_day = start_day + timedelta(days=index)
            daily_targets.append(
                {
                    "date": scheduled_day,
                    "allocations": dict(allocations),
                    "focus": "",
                    "is_rest_day": False,
                    "replanned": replanned,
                }
            )
        subject_targets = {}
        subjects = []
        for day in daily_targets:
            for subject, seconds in day["allocations"].items():
                if subject not in subjects:
                    subjects.append(subject)
                subject_targets[subject] = subject_targets.get(subject, 0.0) + seconds
        return {
            "number": number,
            "start": start_day,
            "end": start_day + timedelta(days=6),
            "subjects": subjects,
            "subject_targets": subject_targets,
            "daily_targets": daily_targets,
            "credit_baselines": {},
            "is_replanned": replanned,
        }

    def test_three_rest_days_spread_evenly_across_all_seven_remaining_days(self):
        rest_hours = 3.44
        receiver_hours = 3.63
        rest_seconds = rest_hours * 3600
        receiver_seconds = receiver_hours * 3600
        week9 = self._week(
            9,
            date(2026, 8, 24),
            [
                {"離散數學": receiver_seconds},
                {"離散數學": receiver_seconds},
                {"資料結構": receiver_seconds},
                {"資料結構": receiver_seconds},
                {"離散數學": rest_seconds},
                {"資料結構": rest_seconds},
                {"離散數學": rest_seconds},
            ],
        )
        week10 = self._week(
            10,
            date(2026, 8, 31),
            [
                {"線性代數": receiver_seconds},
                {"離散數學": receiver_seconds},
                {"資料結構": receiver_seconds},
                {"線性代數": receiver_seconds},
                {"離散數學": receiver_seconds},
                {"資料結構": receiver_seconds},
                {"線性代數": receiver_seconds},
            ],
        )

        result = redistribute_rest_day_allocations(
            [week9, week10],
            ["2026-08-28", "2026-08-29", "2026-08-30"],
        )

        moved_total = rest_seconds * 3
        expected_addition = moved_total / 7
        receiver_days = result[1]["daily_targets"]
        for day in receiver_days:
            self.assertAlmostEqual(
                sum(day["allocations"].values()),
                receiver_seconds + expected_addition,
                places=5,
            )

        for day in result[0]["daily_targets"][4:]:
            self.assertTrue(day["is_rest_day"])
            self.assertEqual(day["allocations"], {})
            self.assertEqual(day["redistributed_day_count"], 7)

    def test_subject_does_not_limit_receiver_days(self):
        week = self._week(
            1,
            date(2026, 9, 1),
            [
                {"資料結構": 3600},
                {"資料結構": 3600},
                {"線性代數": 3600},
                {"離散數學": 3600},
                {"演算法": 3600},
                {"作業系統": 3600},
                {"計算機組織": 3600},
            ],
        )

        result = redistribute_rest_day_allocations([week], ["2026-09-01"])
        days = result[0]["daily_targets"]

        self.assertEqual(days[0]["allocations"], {})
        self.assertEqual(days[0]["redistributed_day_count"], 6)
        for index, day in enumerate(days[1:], start=1):
            # The first receiver already had one hour of 資料結構; the
            # redistributed ten minutes must be added, not replace it.
            expected_subject_seconds = 4200 if index == 1 else 600
            self.assertAlmostEqual(
                day["allocations"].get("資料結構", 0),
                expected_subject_seconds,
            )
            self.assertAlmostEqual(sum(day["allocations"].values()), 4200)

    def test_rest_day_reduces_focused_week_and_moves_work_to_following_week(self):
        first_week = self._week(
            1,
            date(2026, 9, 1),
            [{"資料結構": 3600}] * 7,
        )
        second_week = self._week(
            2,
            date(2026, 9, 8),
            [{"資料結構": 3600}] * 7,
        )

        result = redistribute_rest_day_allocations(
            [first_week, second_week],
            ["2026-09-03"],
        )

        first_days = result[0]["daily_targets"]
        second_days = result[1]["daily_targets"]
        self.assertTrue(first_days[2]["is_rest_day"])
        self.assertEqual(first_days[2]["allocations"], {})
        self.assertEqual(sum(result[0]["subject_targets"].values()), 6 * 3600)
        self.assertEqual(first_days[3]["allocations"], {"資料結構": 3600})
        self.assertAlmostEqual(sum(result[1]["subject_targets"].values()), 8 * 3600)
        for day in second_days:
            self.assertAlmostEqual(day["allocations"]["資料結構"], 3600 + 3600 / 7)

    def test_other_rest_days_and_unplanned_days_are_not_receivers(self):
        week = self._week(
            1,
            date(2026, 9, 1),
            [
                {"資料結構": 3600},
                {"離散數學": 3600},
                {},
                {"線性代數": 3600},
                {"演算法": 3600},
                {},
                {"作業系統": 3600},
            ],
        )

        result = redistribute_rest_day_allocations(
            [week],
            ["2026-09-01", "2026-09-02"],
        )
        days = result[0]["daily_targets"]

        self.assertTrue(days[0]["is_rest_day"])
        self.assertTrue(days[1]["is_rest_day"])
        self.assertEqual(days[2]["allocations"], {})
        self.assertEqual(days[5]["allocations"], {})
        self.assertEqual(days[0]["redistributed_day_count"], 3)
        self.assertEqual(days[1]["redistributed_day_count"], 3)

    def test_cancelling_rest_day_restores_original_plan(self):
        week = self._week(
            1,
            date(2026, 9, 1),
            [{"資料結構": 3600}] * 7,
        )
        with_rest = redistribute_rest_day_allocations([week], ["2026-09-03"])
        restored = redistribute_rest_day_allocations([week], [])

        self.assertTrue(with_rest[0]["daily_targets"][2]["is_rest_day"])
        self.assertFalse(restored[0]["daily_targets"][2]["is_rest_day"])
        self.assertEqual(restored[0]["daily_targets"][2]["allocations"], {"資料結構": 3600})
        self.assertEqual(
            [sum(day["allocations"].values()) for day in restored[0]["daily_targets"]],
            [3600] * 7,
        )

    def test_installer_turns_rest_button_into_same_button_toggle(self):
        template = (
            '<button class="day-rest-button restore" type="submit" '
            'aria-label="恢復 {{ day.date }} 的原定安排" title="恢復原定安排">復</button>'
            "將 {{ day.date }} 設為休息日，並把原定 {{ day.hours }} 小時平均分攤到後續日期？"
        )

        def schedule(videos, replan_settings=None, rest_days=None):
            return []

        web = SimpleNamespace(
            _study_plan_schedule_definitions=schedule,
            STUDY_PLAN_TEMPLATE=template,
        )
        install_study_rest_day_runtime(web)

        self.assertIn('title="取消休息日" aria-pressed="true">休</button>', web.STUDY_PLAN_TEMPLATE)
        self.assertIn("平均分攤到下一週起的剩餘計畫日", web.STUDY_PLAN_TEMPLATE)
        self.assertNotIn(">復</button>", web.STUDY_PLAN_TEMPLATE)


if __name__ == "__main__":
    unittest.main()
