import unittest
from datetime import date
from types import SimpleNamespace

from e3_tracker.shared.study_week_progress_runtime import (
    _clear_future_week_progress,
    install_study_week_progress_runtime,
)


class StudyWeekProgressRuntimeTests(unittest.TestCase):
    def test_future_week_keeps_target_but_has_zero_progress(self):
        rows = [
            {
                "number": 8,
                "start": "2026-08-17",
                "end": "2026-08-23",
                "target_seconds": 26.9 * 3600,
                "watched_seconds": 360,
                "watched_hours": 0.1,
                "remaining_hours": 26.8,
                "completion": 0.4,
                "state": "active",
                "state_label": "進行中",
                "daily_recommendations": [
                    {
                        "date": "2026-08-17",
                        "target_seconds": 3.69 * 3600,
                        "has_target": True,
                        "credited_seconds": 398,
                        "completion": 3.0,
                        "state": "active",
                        "state_label": "進行中",
                    }
                ],
            }
        ]

        guarded = _clear_future_week_progress(rows, today=date(2026, 8, 11))

        self.assertEqual(guarded[0]["watched_hours"], 0.0)
        self.assertEqual(guarded[0]["completion"], 0.0)
        self.assertEqual(guarded[0]["remaining_hours"], 26.9)
        self.assertEqual(guarded[0]["state"], "upcoming")
        self.assertEqual(guarded[0]["state_label"], "未開始")
        self.assertEqual(guarded[0]["daily_recommendations"][0]["completion"], 0.0)
        self.assertEqual(guarded[0]["daily_recommendations"][0]["credited_seconds"], 0.0)
        self.assertEqual(guarded[0]["daily_recommendations"][0]["state"], "upcoming")

        # The source context must not be mutated because it may be cached and reused.
        self.assertEqual(rows[0]["completion"], 0.4)
        self.assertEqual(rows[0]["daily_recommendations"][0]["completion"], 3.0)

    def test_current_week_progress_is_untouched(self):
        rows = [
            {
                "start": "2026-08-10",
                "target_seconds": 20 * 3600,
                "watched_hours": 5.0,
                "completion": 25.0,
                "state": "active",
            }
        ]
        guarded = _clear_future_week_progress(rows, today=date(2026, 8, 11))
        self.assertEqual(guarded, rows)

    def test_installation_wraps_template_context_only_once(self):
        calls = []

        def render(template, *args, **context):
            calls.append(context)
            return context

        module = SimpleNamespace(
            render_template_string=render,
            _study_plan_business_date=lambda: date(2026, 8, 11),
        )
        install_study_week_progress_runtime(module)
        wrapped = module.render_template_string
        install_study_week_progress_runtime(module)
        self.assertIs(module.render_template_string, wrapped)

        result = module.render_template_string(
            "template",
            week_rows=[
                {
                    "start": "2026-08-17",
                    "target_seconds": 3600,
                    "watched_hours": 0.5,
                    "completion": 50,
                }
            ],
        )
        self.assertEqual(result["week_rows"][0]["watched_hours"], 0.0)
        self.assertEqual(result["week_rows"][0]["completion"], 0.0)
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
