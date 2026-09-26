import os
import tempfile
import unittest
from unittest.mock import patch

from bs4 import BeautifulSoup

from e3_tracker.platform.application import create_app
from e3_tracker.platform.services.traffic import is_assignment_event, traffic_event_site
from tests.security_helpers import csrf_client


class TrafficEventSiteTests(unittest.TestCase):
    def test_legacy_study_actions_are_excluded(self):
        for action in (
            "study_plan_video_record_saved",
            "public_study_recall_search",
            "study_recall_note_analyzed",
            " STUDY_ASSISTANT_ACTION_APPLIED ",
        ):
            with self.subTest(action=action):
                self.assertFalse(is_assignment_event({"action": action}))

    def test_assignment_events_and_old_records_remain_visible(self):
        for action in (
            "login_success",
            "logout",
            "google_sync",
            "export_calendar",
            "guest_import",
        ):
            with self.subTest(action=action):
                self.assertTrue(is_assignment_event({"action": action, "meta": {}}))
                self.assertTrue(
                    is_assignment_event(
                        {"action": action, "meta": {"site": "assignments"}}
                    )
                )

    def test_study_site_tag_excludes_generic_actions(self):
        self.assertFalse(
            is_assignment_event({"action": "video_played", "meta": {"site": "study"}})
        )
        self.assertFalse(
            is_assignment_event(
                {"action": "study_plan_saved", "meta": {"site": "assignments"}}
            )
        )
        self.assertEqual(
            traffic_event_site("video_played", "e3_tracker.study.routes.video"), "study"
        )
        self.assertEqual(
            traffic_event_site(
                "login_success", "e3_tracker.assignments.routes.assignments"
            ),
            "assignments",
        )


class TrafficActivityTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(
            os.environ,
            {
                "E3_CACHE_DIR": self.directory.name,
                "E3_DATABASE_URL": "",
                "E3_CANONICAL_HOST": "",
                "E3_SESSION_COOKIE_SECURE": "0",
            },
        )
        self.environment.start()
        self.app = create_app()
        self.storage = self.app.extensions["e3_storage"]
        self.client = csrf_client(self.app)
        self.storage.save_web_session("activity-test", "test-admin", is_admin=True)
        with self.client.session_transaction() as session:
            session.update(username="test-admin", session_token="activity-test")

    def tearDown(self):
        self.storage._engine.dispose()
        self.environment.stop()
        self.directory.cleanup()

    def record(self, action, meta=None):
        return self.client.post(
            "/ui-event", json={"action": action, "meta": meta or {}}
        )

    def activity_text(self):
        response = self.client.get("/admin/traffic")
        self.assertEqual(response.status_code, 200)
        return (
            BeautifulSoup(response.get_data(as_text=True), "html.parser")
            .select_one(".events")
            .get_text()
        )

    def test_recent_activity_only_displays_assignments_and_keeps_study_records(self):
        self.assertEqual(self.record("login_success").status_code, 200)
        self.record("study_plan_video_record_saved", {"info": "study video details"})
        self.record("public_study_recall_search", {"info": "study search details"})
        self.record("google_sync", {"info": "assignment calendar details"})
        text = self.activity_text()
        self.assertIn("登入成功", text)
        self.assertIn("assignment calendar details", text)
        self.assertNotIn("study video details", text)
        self.assertNotIn("study search details", text)
        stored = self.storage.recent_traffic_events(500)
        study = [
            event
            for event in stored
            if event["action"] == "study_plan_video_record_saved"
        ]
        self.assertEqual(len(study), 1)
        self.assertEqual(study[0]["meta"]["site"], "study")

    def test_client_cannot_override_event_site(self):
        self.record("study_recall_cards_rated", {"site": "assignments"})
        self.record("google_sync", {"site": "study"})
        stored = {
            event["action"]: event for event in self.storage.recent_traffic_events(500)
        }
        self.assertEqual(stored["study_recall_cards_rated"]["meta"]["site"], "study")
        self.assertEqual(stored["google_sync"]["meta"]["site"], "assignments")

    def test_existing_events_are_filtered_after_reload_without_rewriting_them(self):
        for index, action in enumerate(
            (
                "export_calendar",
                "study_recall_note_analyzed",
                "public_study_recall_search",
            )
        ):
            self.storage.append_traffic_event(
                {
                    "ts": index + 1,
                    "action": action,
                    "status": "success",
                    "meta": {"username": "test-admin"},
                },
                max_events=500,
            )
        reloaded_app = create_app()
        try:
            client = csrf_client(reloaded_app)
            with client.session_transaction() as session:
                session.update(username="test-admin", session_token="activity-test")
            response = client.get("/admin/traffic")
            text = (
                BeautifulSoup(response.get_data(as_text=True), "html.parser")
                .select_one(".events")
                .get_text()
            )
            self.assertIn("export calendar", text)
            self.assertNotIn("study recall", text)
            self.assertEqual(len(self.storage.recent_traffic_events(500)), 3)
        finally:
            reloaded_app.extensions["e3_storage"]._engine.dispose()

    def test_study_only_history_has_an_empty_activity_list(self):
        self.record("study_plan_replanned")
        self.assertIn("尚無操作紀錄", self.activity_text())


if __name__ == "__main__":
    unittest.main()
