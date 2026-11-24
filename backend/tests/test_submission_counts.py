"""E3's Chinese and English course lists expose the same submission totals."""

import os
import tempfile
import unittest
from unittest.mock import Mock, patch

from bs4 import BeautifulSoup

from e3_tracker.assignments.domain.parsing import gather_assign_links_from_list_page
from e3_tracker.assignments.services.collector import CollectOptions, collect_assignments
from e3_tracker.platform.storage import PersistentStorage


BASE_URL = "https://e3p.nycu.edu.tw"


def course_list(header, counts):
    return f'''<table class="generaltable e3table"><thead><tr>
        <th>Name</th><th>Start Date</th><th>End Date</th><th>{header}</th><th>Function</th>
        </tr></thead><tbody><tr><td>Homework</td><td>-</td><td>2026/12/07 23:55</td>
        <td>{counts}</td><td>[ <a href="{BASE_URL}/mod/assign/view.php?id=256698&amp;scope=assignment">view</a> ]</td>
        </tr></tbody></table>'''


class SubmissionCountTests(unittest.TestCase):
    def test_actual_e3_english_and_chinese_labels_produce_the_same_counts(self):
        for header, counts in (
            ("繳交狀態", "3 個已繳<br>90 個未繳"),
            ("status", "3 Submitted<br>90 Not Submitted"),
            ("Submission status", "3 submitted<br>90 not submitted"),
        ):
            with self.subTest(header=header):
                links = gather_assign_links_from_list_page(course_list(header, counts), BASE_URL)
                self.assertEqual(len(links), 1)
                title, url, due, submitted, participants = links[0]
                self.assertEqual((title, submitted, participants), ("Homework", 3, 93))
                self.assertEqual(url, BASE_URL + "/mod/assign/view.php?id=256698&scope=assignment")
                self.assertEqual(due, "2026/12/07 23:55")

    def test_zero_fullwidth_thousands_and_nested_spans_are_preserved(self):
        for text, expected in (
            ("0 Submitted<br>93 Not Submitted", (0, 93)),
            ("93 Submitted<br>0 Not Submitted", (93, 93)),
            ("0 個已繳<br>0 個未繳", (0, 0)),
            ("１，２００ 個已繳<br>３００ 個未繳", (1200, 1500)),
            ("<b>1,200</b><span>Submitted</span><br><b>34</b><span>Not</span> <span>Submitted</span>", (1200, 1234)),
            ("3\u00a0SUBMITTED<br>90\u00a0NOT\u00a0SUBMITTED", (3, 93)),
        ):
            with self.subTest(text=text):
                parsed = gather_assign_links_from_list_page(course_list("status", text), BASE_URL)
                self.assertEqual(parsed[0][3:], expected)

    def test_missing_or_personal_status_does_not_invent_a_total(self):
        for text in ("Submitted for grading", "Not Submitted", "3 Submitted", "90 Not Submitted", "-"):
            with self.subTest(text=text):
                parsed = gather_assign_links_from_list_page(course_list("status", text), BASE_URL)
                self.assertEqual(parsed[0][3:], (None, None))

    def test_counts_flow_through_collection_storage_and_rendering(self):
        html = course_list("status", "3 Submitted<br>90 Not Submitted")
        detail = '<table><tr><th>Submission status</th><td>No submissions have been made yet</td></tr></table>'
        with patch("e3_tracker.assignments.services.collector.gather_my_courses", return_value=[
            {"id": 25889, "title": "1151.535650.Video Compression", "semester_key": "115-1"},
        ]), patch("e3_tracker.assignments.services.collector.safe_request", side_effect=[Mock(text=html), Mock(text=detail)]):
            result = collect_assignments(CollectOptions(base_url=BASE_URL, moodle_session="synthetic", include_completed=True))
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["all_assignments"][0]["submitted_count"], 3)
        self.assertEqual(result["all_assignments"][0]["participant_count"], 93)
        with tempfile.TemporaryDirectory() as directory:
            storage = PersistentStorage("sqlite:///" + os.path.join(directory, "counts.sqlite3"))
            try:
                storage.save_user_cache("student", {"result": result})
                saved = storage.load_user_cache("student")["result"]["all_assignments"][0]
                self.assertEqual((saved["submitted_count"], saved["participant_count"]), (3, 93))
                from flask import Flask, render_template
                from e3_tracker.platform.assets import configure_frontend
                app = Flask(__name__)
                configure_frontend(app)
                with app.app_context():
                    html = render_template(app.jinja_env.from_string(
                        "{% from 'assignments/components/assignments.html' import render_submission_pill %}{{ render_submission_pill(item) }}"
                    ), item=saved)
                pill = BeautifulSoup(html, "html.parser")
                self.assertEqual(pill.select_one(".submission-pill-count").get_text(), "3")
                self.assertEqual(pill.select_one(".submission-pill-total").get_text(), "93")
            finally:
                storage._engine.dispose()


if __name__ == "__main__":
    unittest.main()
