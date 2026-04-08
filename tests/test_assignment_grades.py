import unittest

from backend.e3_tracker.shared.parsing import find_due_and_status_from_assign_page


class AssignmentGradeParsingTests(unittest.TestCase):
    def test_parses_grade_from_assignment_table(self):
        html = """
        <table>
            <tr><th>Submission status</th><td>Submitted for grading</td></tr>
            <tr><th>Due date</th><td>2026-04-10 23:59</td></tr>
            <tr><th>Grade</th><td>92 / 100</td></tr>
        </table>
        """

        completed, incomplete, due_dt, raw_status, grade_text = find_due_and_status_from_assign_page(html)

        self.assertTrue(completed)
        self.assertFalse(incomplete)
        self.assertIsNotNone(due_dt)
        self.assertEqual(raw_status, "Submitted for grading")
        self.assertEqual(grade_text, "92 / 100")

    def test_parses_grade_from_definition_list(self):
        html = """
        <dl>
            <dt>繳交狀態</dt><dd>已評分</dd>
            <dt>截止時間</dt><dd>2026-04-10 23:59</dd>
            <dt>成績</dt><dd>通過</dd>
        </dl>
        """

        completed, _, due_dt, _, grade_text = find_due_and_status_from_assign_page(html)

        self.assertTrue(completed)
        self.assertIsNotNone(due_dt)
        self.assertEqual(grade_text, "通過")


if __name__ == "__main__":
    unittest.main()
