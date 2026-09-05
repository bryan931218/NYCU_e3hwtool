import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

from e3_tracker.api.web import create_app
from e3_tracker.services.collector import (
    CollectOptions,
    annotate_result_semesters,
    collect_assignments,
    current_semester_key,
    normalize_semester_keys,
    parse_course_semester,
)
from e3_tracker.shared.constants import TAIPEI_TZ
from e3_tracker.shared.storage import PersistentStorage


class AssignmentSemesterTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 9, 6, tzinfo=TAIPEI_TZ)

    def test_current_semester_follows_nycu_academic_year(self):
        self.assertEqual(current_semester_key(self.now), "115-1")
        self.assertEqual(current_semester_key(datetime(2027, 1, 5, tzinfo=TAIPEI_TZ)), "115-1")
        self.assertEqual(current_semester_key(datetime(2027, 2, 1, tzinfo=TAIPEI_TZ)), "115-2")

    def test_course_semester_parser_accepts_common_e3_formats(self):
        cases = {
            "【115上】資料結構": "115-1",
            "【114 下】離散數學": "114-2",
            "113-1 作業系統": "113-1",
            "2025 Fall Algorithms": "114-1",
            "2026 Spring OS": "114-2",
            "沒有學期標記": "other",
        }
        for title, expected in cases.items():
            with self.subTest(title=title):
                self.assertEqual(parse_course_semester(title, self.now)["key"], expected)

    def test_semester_keys_are_deduplicated_and_reject_invalid_values(self):
        self.assertEqual(
            normalize_semester_keys(["115-1", "115-1", "other", "bad", "115-3"]),
            ["115-1", "other"],
        )

    def test_semester_catalog_is_ordered_from_newest_to_oldest(self):
        result = {
            "courses": [
                {"id": 1, "title": "【114上】資料結構", "assignments": []},
                {"id": 2, "title": "【114下】離散數學", "assignments": []},
            ],
            "all_assignments": [],
        }
        annotate_result_semesters(result)
        self.assertEqual([item["key"] for item in result["available_semesters"]], ["114-2", "114-1"])

    def test_old_cache_result_is_annotated_without_losing_assignments(self):
        result = {
            "courses": [
                {"id": 1, "title": "【115上】資料結構", "assignments": []},
                {"id": 2, "title": "【114下】離散數學", "assignments": []},
            ],
            "all_assignments": [
                {"course_id": 1, "course_title": "【115上】資料結構", "title": "作業一"},
            ],
        }
        annotate_result_semesters(result)
        self.assertEqual([item["key"] for item in result["available_semesters"]], ["115-1", "114-2"])
        self.assertEqual(result["all_assignments"][0]["semester_key"], "115-1")
        self.assertEqual(result["selected_semesters"], ["115-1", "114-2"])

    def test_collection_only_opens_courses_from_selected_semesters(self):
        response = Mock()
        response.text = "<html></html>"
        courses = [
            {"id": 101, "title": "【115上】資料結構", "url": "https://e3/course/view.php?id=101"},
            {"id": 202, "title": "【114下】離散數學", "url": "https://e3/course/view.php?id=202"},
        ]
        with patch("e3_tracker.services.collector.gather_my_courses", return_value=courses), patch(
            "e3_tracker.services.collector.safe_request", return_value=response
        ) as safe_request:
            result = collect_assignments(
                CollectOptions(
                    base_url="https://e3.nycu.edu.tw",
                    moodle_session="cookie",
                    include_completed=True,
                    all_courses_all_terms=True,
                    semester_keys=["114-2"],
                )
            )
        requested_urls = [str(call.args[2]) for call in safe_request.call_args_list]
        self.assertEqual([course["id"] for course in result["courses"]], [202])
        self.assertTrue(all("101" not in url for url in requested_urls))
        self.assertEqual(result["selected_semesters"], ["114-2"])
        self.assertEqual([item["key"] for item in result["available_semesters"]], ["115-1", "114-2"])

    def test_semester_preferences_and_catalog_survive_database_round_trip(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = PersistentStorage(f"sqlite:///{Path(temp_dir) / 'semester.db'}")
            try:
                storage.save_user_preferences("student", {"semester_filter": ["115-1", "114-2"]})
                storage.save_user_cache(
                    "student",
                    {
                        "ts": 123,
                        "result": {
                            "courses": [],
                            "all_assignments": [],
                            "errors": [],
                            "available_semesters": [
                                {"key": "115-1", "label": "115 上學期", "course_count": 6, "is_current": True},
                                {"key": "114-2", "label": "114 下學期", "course_count": 5, "is_current": False},
                            ],
                            "selected_semesters": ["115-1", "114-2"],
                        },
                    },
                )
                self.assertEqual(storage.load_user_preferences("student")["semester_filter"], ["115-1", "114-2"])
                cached = storage.load_user_cache("student")
                self.assertEqual(cached["result"]["selected_semesters"], ["115-1", "114-2"])
                self.assertEqual(len(cached["result"]["available_semesters"]), 2)
            finally:
                storage._engine.dispose()

    def test_assignment_page_renders_semester_checkboxes_and_saves_selection(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "E3_CACHE_DIR": temp_dir,
                "E3_DATABASE_URL": "",
                "E3_SESSION_COOKIE_SECURE": "0",
            },
        ):
            app = create_app()
            storage = app.extensions["e3_storage"]
            try:
                token = "semester-test-session"
                storage.save_web_session(token, "student")
                storage.save_user_cache(
                    "student",
                    {
                        "ts": 123,
                        "result": {
                            "courses": [
                                {"id": 1, "title": "【115上】資料結構", "url": "", "assignments": []},
                            ],
                            "all_assignments": [],
                            "errors": [],
                            "available_semesters": [
                                {"key": "115-1", "label": "115 上學期", "course_count": 6, "is_current": True},
                                {"key": "114-2", "label": "114 下學期", "course_count": 5, "is_current": False},
                            ],
                            "selected_semesters": ["115-1"],
                        },
                    },
                )
                client = app.test_client()
                with client.session_transaction() as browser_session:
                    browser_session["username"] = "student"
                    browser_session["session_token"] = token
                    browser_session["is_admin"] = False
                response = client.get("/")
                html = response.get_data(as_text=True)
                self.assertEqual(response.status_code, 200)
                self.assertIn('id="semesterFilterGroup"', html)
                self.assertIn('value="115-1" data-semester-filter checked', html)
                self.assertIn('value="114-2" data-semester-filter', html)
                self.assertNotIn("data-e3-global-ai", html)

                saved = client.post("/preferences", json={"semesterFilters": ["115-1", "114-2"]})
                self.assertEqual(saved.status_code, 200)
                self.assertEqual(
                    storage.load_user_preferences("student")["semester_filter"],
                    ["115-1", "114-2"],
                )
            finally:
                storage._engine.dispose()


if __name__ == "__main__":
    unittest.main()
