"""The public entry page stays compact and uses the live traffic summary."""

import os
import tempfile
import unittest
from unittest.mock import patch

from bs4 import BeautifulSoup

from e3_tracker.platform.application import create_app
from e3_tracker.platform.services.traffic import TrafficTracker


class AssignmentHomeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(
            os.environ,
            {
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
            },
        )
        self.environment.start()
        self.app = create_app()
        self.client = self.app.test_client()

    def tearDown(self):
        self.app.extensions["e3_storage"]._engine.dispose()
        self.environment.stop()
        self.directory.cleanup()

    def home(self, stats):
        with patch.object(TrafficTracker, "snapshot", return_value=stats):
            response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        return BeautifulSoup(response.get_data(as_text=True), "html.parser")

    def test_usage_summary_replaces_feature_copy_without_duplicate_section(self):
        page = self.home(
            {"daily_users": 12, "online_users": 3, "total_users": 24, "total": 3286}
        )
        hero = page.select_one("main > .home-hero")
        self.assertNotIn("\u7db2\u9801\u4f7f\u7528\u6982\u6cc1", page.get_text())
        self.assertEqual(len(page.select(".home-stats")), 1)
        self.assertEqual(
            [value.get_text(strip=True) for value in hero.select("dd")],
            ["12\u4eba", "3\u4eba", "24\u4eba", "3286\u6b21"],
        )
        self.assertIsNone(page.select_one("#features"))
        self.assertIsNone(page.select_one(".hero-description"))
        self.assertIsNone(page.select_one("a[href='#features']"))
        self.assertIsNotNone(hero.select_one("#workspace-preview"))

    def test_both_login_links_only_say_login_and_keep_the_login_destination(self):
        page = self.home({})
        links = page.select("a[href='/login']")
        self.assertEqual(len(links), 2)
        self.assertTrue(
            all(link.get_text(strip=True) == "\u767b\u5165" for link in links)
        )

    def test_missing_stats_render_as_zero(self):
        page = self.home(None)
        self.assertEqual(
            [value.get_text(strip=True) for value in page.select(".home-stats dd")],
            ["0\u4eba", "0\u4eba", "0\u4eba", "0\u6b21"],
        )

    def test_theme_script_is_loaded_with_the_page_csp_nonce(self):
        page = self.home({})
        script = page.select_one("script[src='/assets/assignments/js/home.js']")
        self.assertIsNotNone(script)
        self.assertTrue(script.get("nonce"))
        self.assertTrue(script.has_attr("defer"))
        response = self.client.get(script["src"])
        self.assertEqual(response.status_code, 200)
        response.close()
