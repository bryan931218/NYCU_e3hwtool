import unittest
from types import SimpleNamespace

from e3_tracker.shared.study_timeline_rest_cancel_runtime import (
    decorate_timeline_rest_cancel,
    install_timeline_rest_cancel,
)


class StudyTimelineRestCancelRuntimeTests(unittest.TestCase):
    def test_timeline_rest_cancel_button_is_forced_visible(self):
        template = "<html><head></head><body></body></html>"
        rendered = decorate_timeline_rest_cancel(template)

        self.assertIn(".week-daily .day-chip.rest .day-status { display:none; }", rendered)
        self.assertIn(".week-daily .day-chip.rest .day-rest-button.restore", rendered)
        self.assertIn("position:absolute", rendered)
        self.assertIn("right:5px", rendered)
        self.assertIn("z-index:3", rendered)

    def test_installer_updates_study_plan_template_once(self):
        web = SimpleNamespace(STUDY_PLAN_TEMPLATE="<html><head></head><body></body></html>")
        install_timeline_rest_cancel(web)
        first = web.STUDY_PLAN_TEMPLATE
        install_timeline_rest_cancel(web)

        self.assertEqual(first, web.STUDY_PLAN_TEMPLATE)
        self.assertEqual(web.STUDY_PLAN_TEMPLATE.count("e3-timeline-rest-cancel-style"), 1)


if __name__ == "__main__":
    unittest.main()
