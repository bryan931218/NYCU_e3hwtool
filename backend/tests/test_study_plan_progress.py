import json
import math
import os
import re
import tempfile
import unittest
from datetime import date, timedelta
from unittest.mock import patch

from e3_tracker.api.web import (
    create_app,
    _study_plan_today_progress_days,
    _study_plan_progress_race,
    _study_plan_progress_summary,
    _study_plan_progress_week,
    _study_plan_schedule_definitions,
    _study_plan_subject_status,
    _study_plan_video_completion,
)
from e3_tracker.shared.study_plan_data import STUDY_PLAN_VIDEO_INVENTORY


class StudyPlanProgressTests(unittest.TestCase):
    def test_progress_week_follows_actual_completion_instead_of_calendar(self):
        week_rows = [
            {"number": 1, "target_seconds": 100, "watched_seconds": 100},
            {"number": 2, "target_seconds": 100, "watched_seconds": 35},
            {"number": 3, "target_seconds": 100, "watched_seconds": 0},
        ]

        self.assertEqual(_study_plan_progress_week(week_rows)["number"], 2)

    def test_progress_week_skips_flexible_weeks_and_stays_on_last_when_complete(self):
        week_rows = [
            {"number": 1, "target_seconds": 100, "watched_seconds": 100},
            {"number": 2, "target_seconds": 0, "watched_seconds": 0},
            {"number": 3, "target_seconds": 100, "watched_seconds": 100},
        ]

        self.assertEqual(_study_plan_progress_week(week_rows)["number"], 3)

    def test_today_progress_maps_mixed_subject_progress_by_daily_allocations(self):
        week_rows = [
            {
                "number": 5,
                "subject": "離散數學＋資料結構",
                "target_seconds": 400,
                "daily_recommendations": [
                    {
                        "label": "週一",
                        "date": "2026-07-27",
                        "allocations": {"離散數學": 100, "資料結構": 100},
                    },
                    {
                        "label": "週二",
                        "date": "2026-07-28",
                        "allocations": {"離散數學": 100, "資料結構": 100},
                    },
                ],
            }
        ]
        videos = [
            {
                "subject": "離散數學",
                "duration_seconds": 200,
                "watched_seconds": 150,
            },
            {
                "subject": "資料結構",
                "duration_seconds": 200,
                "watched_seconds": 50,
            },
        ]
        activity_events = [
            {"subject": "離散數學", "delta_seconds": 50},
            {"subject": "資料結構", "delta_seconds": 50},
        ]

        result = _study_plan_today_progress_days(week_rows, videos, activity_events)

        self.assertEqual(
            [(item["date"], item["minutes"]) for item in result],
            [("2026-07-27", 0.8), ("2026-07-28", 0.8)],
        )
        self.assertEqual(result[0]["before_completion"], 50)
        self.assertEqual(result[0]["after_completion"], 75)
        self.assertEqual(result[1]["before_completion"], 0)
        self.assertEqual(result[1]["after_completion"], 25)
        self.assertEqual(result[0]["week_before_completion"], 25)
        self.assertEqual(result[0]["week_after_completion"], 50)

    def test_today_progress_does_not_count_cross_subject_rewind_as_progress(self):
        week_rows = [
            {
                "number": 5,
                "subject": "離散數學＋資料結構",
                "target_seconds": 200,
                "daily_recommendations": [
                    {
                        "label": "週一",
                        "date": "2026-07-27",
                        "allocations": {"離散數學": 100, "資料結構": 100},
                    }
                ],
            }
        ]
        videos = [
            {
                "subject": "離散數學",
                "duration_seconds": 100,
                "watched_seconds": 0,
            },
            {
                "subject": "資料結構",
                "duration_seconds": 100,
                "watched_seconds": 100,
            },
        ]
        activity_events = [
            {"subject": "離散數學", "delta_seconds": -100},
            {"subject": "資料結構", "delta_seconds": 100},
        ]

        self.assertEqual(
            _study_plan_today_progress_days(week_rows, videos, activity_events),
            [],
        )

    def test_today_progress_is_shown_when_overall_progress_is_ahead(self):
        week_rows = [
            {
                "number": 1,
                "subject": "線性代數",
                "target_seconds": 100,
                "daily_recommendations": [
                    {
                        "label": "週一",
                        "date": "2026-07-27",
                        "allocations": {"線性代數": 100},
                    }
                ],
            }
        ]
        videos = [
            {
                "subject": "線性代數",
                "duration_seconds": 100,
                "watched_seconds": 100,
            }
        ]
        activity_events = [{"subject": "線性代數", "delta_seconds": 100}]

        result = _study_plan_today_progress_days(week_rows, videos, activity_events)

        self.assertEqual([(item["date"], item["minutes"]) for item in result], [("2026-07-27", 1.7)])

    def test_today_progress_can_include_today_schedule(self):
        week_rows = [
            {
                "number": 1,
                "subject": "線性代數",
                "target_seconds": 200,
                "daily_recommendations": [
                    {
                        "label": "週四",
                        "date": "2026-07-30",
                        "allocations": {"線性代數": 100},
                    },
                    {
                        "label": "週五",
                        "date": "2026-07-31",
                        "allocations": {"線性代數": 100},
                    },
                ],
            }
        ]
        videos = [
            {
                "subject": "線性代數",
                "duration_seconds": 200,
                "watched_seconds": 150,
            }
        ]
        activity_events = [{"subject": "線性代數", "delta_seconds": 100}]

        result = _study_plan_today_progress_days(week_rows, videos, activity_events)

        self.assertEqual(
            [(item["date"], item["minutes"]) for item in result],
            [("2026-07-30", 0.8), ("2026-07-31", 0.8)],
        )

    def test_interleaved_schedule_keeps_subject_targets_separate(self):
        weeks = _study_plan_schedule_definitions(STUDY_PLAN_VIDEO_INVENTORY)
        future_days = [
            day
            for week in weeks
            for day in week["daily_targets"]
            if day["date"].isoformat() >= "2026-07-27"
            and day["allocations"]
        ]

        self.assertEqual(future_days[0]["date"].isoformat(), "2026-07-27")
        self.assertLessEqual(
            sum(future_days[0]["allocations"].values()),
            3.5 * 60 * 60,
        )
        self.assertAlmostEqual(
            future_days[0]["allocations"]["離散數學"],
            future_days[1]["allocations"]["離散數學"],
            places=3,
        )
        self.assertAlmostEqual(
            future_days[0]["allocations"]["資料結構"],
            future_days[1]["allocations"]["資料結構"],
            places=3,
        )
        phase_one_subjects = set(future_days[0]["allocations"])
        phase_one_days = []
        for day in future_days:
            if set(day["allocations"]) != phase_one_subjects:
                break
            phase_one_days.append(day)
        self.assertTrue(phase_one_days)
        for day in phase_one_days:
            self.assertAlmostEqual(
                sum(day["allocations"].values()),
                sum(phase_one_days[0]["allocations"].values()),
                places=3,
            )
            self.assertAlmostEqual(
                day["allocations"]["離散數學"],
                phase_one_days[0]["allocations"]["離散數學"],
                places=3,
            )
            self.assertAlmostEqual(
                day["allocations"]["資料結構"],
                phase_one_days[0]["allocations"]["資料結構"],
                places=3,
            )
        phase_two_days = future_days[len(phase_one_days):]
        self.assertTrue(phase_two_days)
        self.assertNotEqual(
            set(phase_two_days[0]["allocations"]),
            phase_one_subjects,
        )
        for day in phase_two_days:
            self.assertEqual(
                set(day["allocations"]),
                set(phase_two_days[0]["allocations"]),
            )
            self.assertLessEqual(
                sum(day["allocations"].values()),
                3.5 * 60 * 60,
            )
            for subject, seconds in phase_two_days[0]["allocations"].items():
                self.assertAlmostEqual(
                    day["allocations"][subject],
                    seconds,
                    places=3,
                )
        self.assertEqual(future_days[-1]["date"].isoformat(), "2026-12-03")

        scheduled_by_subject = {}
        for day in future_days:
            for subject, seconds in day["allocations"].items():
                scheduled_by_subject[subject] = scheduled_by_subject.get(subject, 0.0) + seconds
        inventory_by_subject = {}
        for video in STUDY_PLAN_VIDEO_INVENTORY:
            subject = video["subject"]
            if subject == "線性代數":
                continue
            inventory_by_subject[subject] = inventory_by_subject.get(subject, 0.0) + float(
                video["duration_seconds"]
            )
        self.assertEqual(set(scheduled_by_subject), set(inventory_by_subject))
        for subject, target_seconds in inventory_by_subject.items():
            self.assertAlmostEqual(scheduled_by_subject[subject], target_seconds, places=3)

    def test_smart_replan_replaces_future_schedule_and_preserves_phase_interleaving(self):
        settings = {
            "start_date": "2026-08-03",
            "end_date": "2026-08-09",
            "weekday_minutes": 180,
            "weekend_minutes": 120,
            "baseline_by_subject": {"離散數學": 1200, "資料結構": 600},
            "subject_targets": {"離散數學": 7200, "資料結構": 7200},
        }

        weeks = _study_plan_schedule_definitions(STUDY_PLAN_VIDEO_INVENTORY, settings)
        replanned = next(week for week in weeks if week["start"].isoformat() == "2026-08-03")
        allocated = {}
        for day in replanned["daily_targets"]:
            for subject, seconds in day["allocations"].items():
                allocated[subject] = allocated.get(subject, 0.0) + seconds

        self.assertTrue(replanned["is_replanned"])
        self.assertEqual(replanned["credit_baselines"], settings["baseline_by_subject"])
        self.assertEqual(set(replanned["daily_targets"][0]["allocations"]), {"離散數學", "資料結構"})
        self.assertAlmostEqual(allocated["離散數學"], 7200, places=3)
        self.assertAlmostEqual(allocated["資料結構"], 7200, places=3)
        self.assertEqual(weeks[-1]["end"].isoformat(), "2026-08-09")

    def test_progress_race_compares_watched_time_with_target_time(self):
        race = _study_plan_progress_race(65 * 60, 71.5 * 60, 520 * 60)

        self.assertEqual(race["actual_percent"], 12.5)
        self.assertEqual(race["target_percent"], 13.8)
        self.assertEqual(race["watched_hours"], 65)
        self.assertEqual(race["target_hours"], 71.5)
        self.assertEqual(race["delta_hours"], -6.5)
        self.assertEqual(race["state"], "behind")
        self.assertEqual(race["status_label"], "落後 6 小時 30 分鐘")
        self.assertEqual(race["state_label"], "落後計畫")
        self.assertEqual(race["gap_start"], race["runner_position"])
        self.assertAlmostEqual(race["gap_width"], 1.3, places=1)

    def test_progress_race_handles_ahead_progress(self):
        race = _study_plan_progress_race(91.75 * 60, 71.5 * 60, 520 * 60)

        self.assertEqual(race["state"], "early")
        self.assertEqual(race["state_label"], "超前計畫")
        self.assertEqual(race["status_label"], "領先 20 小時 15 分鐘")
        self.assertEqual(race["headline_value"], "20.2")
        self.assertEqual(race["headline_unit"], "小時領先")
        self.assertEqual(race["gap_start"], race["plan_position"])
        self.assertGreater(race["gap_width"], 0)

    def test_progress_race_treats_a_gap_smaller_than_today_target_as_on_track(self):
        within_today = _study_plan_progress_race(
            83.3 * 60,
            85.7 * 60,
            520 * 60,
            3.44 * 60,
        )
        beyond_today = _study_plan_progress_race(
            82.2 * 60,
            85.7 * 60,
            520 * 60,
            3.44 * 60,
        )

        self.assertEqual(within_today["state"], "active")
        self.assertEqual(within_today["state_label"], "進度正常")
        self.assertTrue(within_today["within_daily_allowance"])
        self.assertEqual(within_today["status_label"], "差距 2 小時 24 分鐘")
        self.assertEqual(within_today["today_target_label"], "3 小時 26 分鐘")
        self.assertEqual(beyond_today["state"], "behind")
        self.assertFalse(beyond_today["within_daily_allowance"])

    def test_progress_race_keeps_edge_markers_visible(self):
        start = _study_plan_progress_race(0, 0, 100)
        finish = _study_plan_progress_race(100, 100, 100)

        self.assertEqual(start["runner_position"], 2.5)
        self.assertEqual(start["plan_position"], 2.5)
        self.assertEqual(finish["runner_position"], 97.5)
        self.assertEqual(finish["plan_position"], 97.5)
        self.assertEqual(finish["state"], "active")
        self.assertEqual(finish["status_label"], "與計畫同步")
        self.assertEqual(finish["gap_width"], 0)

    def test_incomplete_subject_with_an_overdue_week_is_behind(self):
        state, label = _study_plan_subject_status(
            [
                {
                    "start": "2026-06-29",
                    "end": "2026-07-05",
                    "completion": 100,
                    "state": "complete",
                    "state_label": "已達標",
                },
                {
                    "start": "2026-07-20",
                    "end": "2026-07-26",
                    "completion": 94.9,
                    "state": "behind",
                    "state_label": "待補",
                },
            ],
            date(2026, 7, 27),
            subject_is_complete=False,
        )

        self.assertEqual((state, label), ("behind", "待補"))

    def test_only_a_complete_subject_can_show_achieved(self):
        state, label = _study_plan_subject_status(
            [],
            date(2026, 7, 27),
            subject_is_complete=True,
        )

        self.assertEqual((state, label), ("complete", "已達標"))

    def test_summary_is_weighted_by_video_duration(self):
        summary = _study_plan_progress_summary(
            [
                {"duration_seconds": 100, "watched_seconds": 50},
                {"duration_seconds": 300, "watched_seconds": 150},
            ]
        )

        self.assertEqual(summary["total_target_seconds"], 400)
        self.assertEqual(summary["total_watched_seconds"], 200)
        self.assertEqual(summary["completion"], 50)
        self.assertEqual(summary["completed_videos"], 0)
        self.assertEqual(summary["video_completion"], 0)

    def test_summary_clamps_positions_to_video_duration(self):
        summary = _study_plan_progress_summary(
            [{"duration_seconds": 120, "watched_seconds": 999}]
        )

        self.assertEqual(summary["total_watched_seconds"], 120)
        self.assertEqual(summary["completion"], 100)
        self.assertEqual(summary["completed_videos"], 1)

    def test_near_end_completion_uses_same_tolerance_as_summary(self):
        watched = 99.6
        self.assertEqual(_study_plan_video_completion(100, watched), 100)

        summary = _study_plan_progress_summary(
            [{"duration_seconds": 100, "watched_seconds": watched}]
        )
        self.assertEqual(summary["completion"], 100)
        self.assertEqual(summary["completed_videos"], 1)
        self.assertEqual(summary["video_completion"], 100)

    def test_invalid_numeric_values_do_not_poison_totals(self):
        summary = _study_plan_progress_summary(
            [
                {"duration_seconds": math.inf, "watched_seconds": 20},
                {"duration_seconds": 100, "watched_seconds": math.nan},
                {"duration_seconds": -50, "watched_seconds": -10},
            ]
        )

        self.assertEqual(summary["total_target_seconds"], 100)
        self.assertEqual(summary["total_watched_seconds"], 0)
        self.assertEqual(summary["completion"], 0)
        self.assertTrue(math.isfinite(summary["completion"]))

    def test_progress_api_and_rendered_summary_use_the_same_completion_rule(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(
                os.environ,
                {
                    "E3_CACHE_DIR": temp_dir,
                    "E3_DATABASE_URL": "",
                    "E3_SESSION_COOKIE_SECURE": "0",
                },
            ):
                app = create_app()

            storage = app.extensions["e3_storage"]
            first_video = storage.list_study_plan_videos_with_records()[0]
            token = "study-plan-test-session"
            storage.save_web_session(token, "test-admin")
            client = app.test_client()
            with client.session_transaction() as browser_session:
                browser_session["username"] = "test-admin"
                browser_session["session_token"] = token
                browser_session["is_admin"] = True

            response = client.post(
                "/admin/study-plan/video-progress",
                json={
                    "video_id": first_video["id"],
                    "watched_seconds": first_video["duration_seconds"] - 2,
                    "expected_version": first_video["progress_version"],
                },
            )
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["completion"], 100)
            self.assertEqual(payload["summary"]["completed_videos"], 1)
            self.assertEqual(payload["progress_version"], 1)
            current_day_subjects = payload["current_week"]["daily_recommendations"][0]["subject_progress"]
            self.assertEqual(
                [item["name"] for item in current_day_subjects],
                ["線性代數"],
            )
            self.assertIn("remaining_hours", payload["current_week"])

            stale_response = client.post(
                "/admin/study-plan/video-progress",
                json={
                    "video_id": first_video["id"],
                    "watched_seconds": 10,
                    "expected_version": 0,
                },
            )
            self.assertEqual(stale_response.status_code, 200)
            stale_payload = stale_response.get_json()
            self.assertTrue(stale_payload["stale"])
            self.assertEqual(stale_payload["progress_version"], 1)
            self.assertEqual(
                stale_payload["watched_seconds"],
                first_video["duration_seconds"] - 2,
            )

            rewind_response = client.post(
                "/admin/study-plan/video-progress",
                json={
                    "video_id": first_video["id"],
                    "watched_seconds": 10,
                    "expected_version": stale_payload["progress_version"],
                },
            )
            self.assertEqual(rewind_response.status_code, 200)
            rewind_payload = rewind_response.get_json()
            self.assertFalse(rewind_payload["stale"])
            self.assertEqual(rewind_payload["progress_version"], 2)
            self.assertEqual(rewind_payload["watched_seconds"], 10)

            page = client.get("/admin/study-plan")
            self.assertEqual(page.status_code, 200)
            plan_html = page.get_data(as_text=True)
            self.assertIn("片長加權完成率", plan_html)
            self.assertIn("離散數學＋資料結構", plan_html)
            self.assertIn("每日總時數與進度", plan_html)
            self.assertIn('class="week-daily"', plan_html)
            self.assertIn('data-timeline-day-date="2026-07-27"', plan_html)
            self.assertIn("本週目標", plan_html)
            self.assertIn("尚需完成", plan_html)
            self.assertIn("週完成率", plan_html)
            self.assertNotIn("日均", plan_html)
            self.assertIn("2026-06-29 至 2026-07-05", plan_html)
            self.assertIn("本週剩餘", plan_html)
            self.assertNotIn("每日影片上限", plan_html)
            self.assertIn('data-day-subject="線性代數"', plan_html)
            self.assertNotIn("彈性日", plan_html)
            self.assertIn("2026-12-03", plan_html)

            home_page = client.get("/admin/study-home")
            self.assertEqual(home_page.status_code, 200)
            home_html = home_page.get_data(as_text=True)
            self.assertIn("進度追趕賽", home_html)
            self.assertIn("今日任務", home_html)
            self.assertIn("進度分析", home_html)
            self.assertIn("data-study-calendar", home_html)
            self.assertIn("data-calendar-modal", home_html)
            self.assertIn("DAILY ACTIVITY", home_html)
            self.assertIn(first_video["title"], home_html)
            self.assertIn(f"繼續 {first_video['subject']}", home_html)
            self.assertNotIn("calendar-legend-gradient", home_html)
            self.assertIn("applyHeatColor", home_html)
            self.assertNotIn("學習旅程地圖", home_html)
            home_calendar_match = re.search(
                r'<script type="application/json" data-calendar-data>(.*?)</script>',
                home_html,
                re.DOTALL,
            )
            self.assertIsNotNone(home_calendar_match)
            home_calendar = json.loads(home_calendar_match.group(1))
            today_entry = next(
                item for item in home_calendar["days"] if item["date"] == date.today().isoformat()
            )
            self.assertEqual(today_entry["activities"][0]["title"], first_video["title"])

            public_page = client.get("/study-progress")
            self.assertEqual(public_page.status_code, 200)
            public_html = public_page.get_data(as_text=True)
            self.assertIn("截至今天應看", public_html)
            self.assertIn("觀看時數", public_html)
            self.assertIn("data-public-study-calendar", public_html)
            self.assertIn("data-public-calendar-modal", public_html)
            self.assertIn("data-public-calendar-average", public_html)
            self.assertIn(first_video["title"], public_html)
            self.assertNotIn("public-calendar-legend-gradient", public_html)
            self.assertIn("applyHeatColor", public_html)
            self.assertNotIn("目前已看的時間", public_html)
            public_calendar_match = re.search(
                r'<script type="application/json" data-public-calendar-data>(.*?)</script>',
                public_html,
                re.DOTALL,
            )
            self.assertIsNotNone(public_calendar_match)
            public_calendar = json.loads(public_calendar_match.group(1))
            public_today_entry = next(
                item for item in public_calendar["days"] if item["date"] == date.today().isoformat()
            )
            self.assertEqual(public_today_entry["activities"][0]["title"], first_video["title"])
            storage._engine.dispose()

    def test_video_markers_and_smart_replan_endpoints(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(
                os.environ,
                {
                    "E3_CACHE_DIR": temp_dir,
                    "E3_DATABASE_URL": "",
                    "E3_SESSION_COOKIE_SECURE": "0",
                },
            ):
                app = create_app()

            storage = app.extensions["e3_storage"]
            first_video = storage.list_study_plan_videos_with_records()[0]
            token = "study-plan-feature-session"
            storage.save_web_session(token, "test-admin")
            client = app.test_client()
            with client.session_transaction() as browser_session:
                browser_session["username"] = "test-admin"
                browser_session["session_token"] = token
                browser_session["is_admin"] = True

            marker_response = client.post(
                "/admin/study-plan/video-markers",
                json={
                    "video_id": first_video["id"],
                    "playback_seconds": 123.4,
                    "note": "特徵值推導",
                },
            )
            self.assertEqual(marker_response.status_code, 200)
            marker = marker_response.get_json()["marker"]
            self.assertEqual(marker["note"], "特徵值推導")
            self.assertAlmostEqual(marker["playback_seconds"], 123.4)

            edit_response = client.patch(
                f"/admin/study-plan/video-markers/{marker['id']}",
                json={"note": "特徵向量與基底轉換"},
            )
            self.assertEqual(edit_response.status_code, 200)
            marker = edit_response.get_json()["marker"]
            self.assertEqual(marker["note"], "特徵向量與基底轉換")

            missing_edit = client.patch(
                "/admin/study-plan/video-markers/999999",
                json={"note": "不存在"},
            )
            self.assertEqual(missing_edit.status_code, 404)

            marker_page = client.get(
                f"/admin/study-plan?subject={first_video['subject']}&video_id={first_video['id']}"
            )
            marker_html = marker_page.get_data(as_text=True)
            self.assertIn("影片關鍵點", marker_html)
            self.assertIn("video-markers-data", marker_html)
            self.assertIn('id="focus-mode-toggle"', marker_html)
            self.assertIn('id="focus-marker-save"', marker_html)
            self.assertIn("startMarkerEdit", marker_html)
            self.assertIn("flashMarkerSuccess", marker_html)
            self.assertIn("requestFullscreen", marker_html)
            self.assertIn("fs: 0", marker_html)
            self.assertIn("playsinline: 1", marker_html)
            marker_match = re.search(
                r'<script type="application/json" id="video-markers-data">(.*?)</script>',
                marker_html,
                re.DOTALL,
            )
            self.assertIsNotNone(marker_match)
            rendered_markers = json.loads(marker_match.group(1))
            self.assertEqual(rendered_markers[0]["note"], "特徵向量與基底轉換")

            delete_response = client.delete(f"/admin/study-plan/video-markers/{marker['id']}")
            self.assertEqual(delete_response.status_code, 200)
            self.assertEqual(storage.list_study_plan_video_markers(video_ids=[first_video["id"]]), [])

            today = date.today()
            next_monday = today - timedelta(days=today.weekday()) + timedelta(days=7)
            target_day = next_monday + timedelta(days=27)
            replan_response = client.post(
                "/admin/study-plan/replan",
                data={
                    "subject": first_video["subject"],
                    "end_date": target_day.isoformat(),
                    "weekday_hours": "3",
                    "weekend_hours": "2",
                },
                follow_redirects=True,
            )
            self.assertEqual(replan_response.status_code, 200)
            replan_html = replan_response.get_data(as_text=True)
            self.assertIn("智慧重排已套用", replan_html)
            self.assertIn(target_day.isoformat(), replan_html)
            self.assertIn("恢復原始計畫", replan_html)
            saved_settings = storage.get_study_plan_replan_settings()
            self.assertIsNotNone(saved_settings)
            self.assertEqual(saved_settings["start_date"], next_monday.isoformat())
            self.assertEqual(saved_settings["end_date"], target_day.isoformat())
            self.assertTrue(saved_settings["subject_targets"])
            storage._engine.dispose()


if __name__ == "__main__":
    unittest.main()
