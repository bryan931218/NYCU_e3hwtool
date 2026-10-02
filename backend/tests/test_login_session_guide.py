"""Session login instructions stay available without exposing a real cookie."""

import os
import tempfile
import unittest
from unittest.mock import patch

from bs4 import BeautifulSoup

from e3_tracker.platform.application import create_app
from tests.security_helpers import csrf_client


class LoginSessionGuideTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(os.environ, {
            "E3_ENV": "development",
            "RAILWAY_ENVIRONMENT_ID": "",
            "RAILWAY_ENVIRONMENT_NAME": "",
            "E3_CACHE_DIR": self.directory.name,
            "E3_DATABASE_URL": "",
            "DATABASE_URL": "",
            "E3_CANONICAL_HOST": "",
            "E3_SESSION_COOKIE_SECURE": "0",
            "E3_DEV_RELOAD": "0",
            "E3_YOUTUBE_AUTO_SYNC_ENABLED": "0",
            "OPENAI_API_KEY": "",
        })
        self.environment.start()
        self.app = create_app()
        self.client = self.app.test_client()

    def tearDown(self):
        self.app.extensions["e3_storage"]._engine.dispose()
        self.environment.stop()
        self.directory.cleanup()

    def test_guide_renders_three_safe_steps_and_original_login_fields(self):
        response = self.client.get("/login")
        self.assertEqual(response.status_code, 200)
        page = BeautifulSoup(response.get_data(as_text=True), "html.parser")
        guide = page.select_one("dialog#sessionHelpModal")
        self.assertEqual(len(guide.select("[data-guide-step]")), 3)
        self.assertEqual(len(guide.select("[data-guide-scene]")), 3)
        self.assertEqual(len(guide.select("[data-guide-step][hidden]")), 2)
        self.assertEqual(len(guide.select("[data-guide-scene][hidden]")), 2)
        self.assertIn("Application", guide.get_text())
        self.assertIn("MoodleSession", guide.get_text())
        self.assertIn("請勿傳給他人", guide.get_text())
        self.assertEqual(
            guide.select_one("a.guide-link")["href"],
            "https://e3p.nycu.edu.tw/my/",
        )
        self.assertIsNotNone(page.select_one("#moodle_session[name='moodle_session']"))
        self.assertIsNotNone(page.select_one("#username[name='username']"))
        self.assertIsNotNone(page.select_one("#password[name='password']"))
        self.assertEqual(page.select_one("#loginTypeInput")["value"], "password")
        response.close()

    def test_password_authentication_failure_offers_session_login(self):
        with patch("e3_tracker.assignments.routes.assignments.login_with_password", side_effect=RuntimeError("authentication failed")):
            response = csrf_client(self.app).post("/login", data={
                "login_type": "password", "username": "test-user", "password": "test-password",
            })
        self.assertEqual(response.status_code, 200)
        page = BeautifulSoup(response.get_data(as_text=True), "html.parser")
        prompt = page.select_one("dialog#passwordLoginFailure")
        self.assertIsNotNone(prompt)
        self.assertIn("2FA", prompt.get_text())
        self.assertIsNotNone(prompt.select_one("#passwordFailureSession"))
        self.assertNotIn("test-password", response.get_data(as_text=True))
        response.close()

    def test_initial_page_and_missing_fields_do_not_suggest_authentication_failure(self):
        client = csrf_client(self.app)
        responses = [
            client.get("/login"),
            client.post("/login", data={"login_type": "password"}),
            client.post("/login", data={"login_type": "session"}),
        ]
        for response in responses:
            with self.subTest(response=response):
                self.assertEqual(response.status_code, 200)
                page = BeautifulSoup(response.get_data(as_text=True), "html.parser")
                self.assertIsNone(page.select_one("#passwordLoginFailure"))
                response.close()

    def test_guide_assets_are_served_from_assignment_frontend(self):
        page = BeautifulSoup(self.client.get("/login").get_data(as_text=True), "html.parser")
        script = page.select_one("script[src='/assets/assignments/js/login-session-guide.js']")
        self.assertIsNotNone(script)
        self.assertTrue(script.get("nonce"))
        self.assertTrue(script.has_attr("defer"))
        css = page.select_one("link[href='/assets/assignments/css/login-session-guide.css']")
        self.assertIsNotNone(css)
        for asset in (script["src"], css["href"]):
            with self.subTest(asset=asset):
                response = self.client.get(asset)
                self.assertEqual(response.status_code, 200)
                response.close()


if __name__ == "__main__":
    unittest.main()
