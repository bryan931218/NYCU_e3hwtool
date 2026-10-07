import os
import tempfile
import time
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from bs4 import BeautifulSoup
from flask import redirect
from sqlalchemy import select

from e3_tracker.platform.application import create_app
from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.platform.page_analytics import (
    build_page_sources, page_analytics_table, register_page_analytics,
)
from e3_tracker.platform.services.traffic_sources import arrival_source, stored_source_label
from tests.security_helpers import csrf_client


class SourceClassificationTests(unittest.TestCase):
    def test_referrers_tags_and_legacy_records(self):
        for domain, label in (("www.dcard.tw", "Dcard"), ("www.google.com.tw", "Google"),
                              ("l.facebook.com", "Facebook"), ("lin.ee", "LINE"),
                              ("google.com.evil.example", "其他來源"), ("evildcard.tw", "其他來源")):
            with self.subTest(domain=domain):
                self.assertEqual(arrival_source("https://" + domain + "/?secret=private"), label)
                self.assertEqual(stored_source_label(domain), label)
        for tag, label in (("Dcard", "Dcard"), ("google", "Google"), ("fb", "Facebook"),
                           ("bookmark", "直接／來源不明"), ("newsletter", "其他來源")):
            self.assertEqual(arrival_source("https://google.com", tag), label)
        self.assertEqual(arrival_source("https://[invalid"), "直接／來源不明")
        self.assertEqual(arrival_source(""), "直接／來源不明")
        self.assertEqual(stored_source_label("直接開啟 / 書籤"), "直接／來源不明")
        self.assertEqual(arrival_source("https://example.com/a", site_hosts={"example.com"}), "站內導覽")


class TrafficSourceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(os.environ, {
            "E3_CACHE_DIR": self.directory.name, "E3_DATABASE_URL": "", "E3_CANONICAL_HOST": "",
            "E3_SESSION_COOKIE_SECURE": "0", "E3_APP_HOME_URL": "http://localhost/",
        })
        self.environment.start()
        self.app = create_app()
        self.app.add_url_rule("/source-entry", "source_entry", lambda: redirect("/login"))
        self.storage = self.app.extensions["e3_storage"]
        self.client = csrf_client(self.app)

    def tearDown(self):
        self.storage._engine.dispose()
        self.environment.stop()
        self.directory.cleanup()

    def admin(self):
        self.storage.save_web_session("source-admin-token", "source-admin", is_admin=True)
        with self.client.session_transaction() as sess:
            sess.update(username="source-admin", session_token="source-admin-token")

    def rows(self):
        with self.storage._engine.connect() as conn:
            return [dict(row._mapping) for row in conn.execute(select(page_analytics_table)).fetchall()]

    def seed(self, **changes):
        data = dict(ts=time.time(), path="/login", source="www.dcard.tw", device="Desktop",
                    browser="Chrome", visitor_key="anon:demo")
        data.update(changes)
        with self.storage._engine.begin() as conn:
            conn.execute(page_analytics_table.insert().values(**data))

    def test_redirect_preserves_tags_and_does_not_double_count(self):
        register_page_analytics(self.app)  # Factory/WSGI compatibility must not double-register.
        self.client.get("/source-entry?utm_source=dcard", follow_redirects=True, headers={"Sec-Fetch-Dest": "document"})
        self.assertEqual(len(self.rows()), 1)
        self.assertEqual(self.rows()[0]["source"], "Dcard")
        self.assertEqual(self.rows()[0]["path"], "/login")

    def test_background_admin_head_and_error_requests_are_excluded(self):
        for url, headers in (("/login", {"Sec-Fetch-Dest": "empty"}), ("/login", {"Accept": "*/*"}),
                             ("/privacy", {"Purpose": "prefetch"}),
                             ("/privacy", {"X-Requested-With": "XMLHttpRequest"}),
                             ("/admin/traffic", {}), ("/traffic/stats", {}), ("/missing", {})):
            self.client.get(url, headers=headers)
        self.client.head("/privacy")
        self.assertEqual(self.rows(), [])
        self.client.get("/privacy")
        self.assertEqual(len(self.rows()), 1)

    def test_real_pages_classify_and_strip_queries(self):
        self.client.get("/privacy?token=private", headers={"Referer": "https://google.com.tw/search?q=private"})
        self.client.get("/terms", headers={"Referer": "http://localhost/privacy?token=private"})
        self.assertEqual([row["source"] for row in self.rows()], ["Google", "站內導覽"])
        self.assertNotIn("private", str(self.rows()))

    def test_new_arrival_does_not_inherit_abandoned_redirect(self):
        self.client.get("/source-entry?utm_source=dcard")
        self.client.get("/login", headers={"Referer": "https://google.com/"})
        self.assertEqual(self.rows()[0]["source"], "Google")

    def test_latest_page_and_access_control(self):
        self.client.get("/login?utm_source=dcard")
        self.assertEqual(self.client.get("/admin/traffic").status_code, 302)
        self.storage.save_web_session("user-token", "student")
        with self.client.session_transaction() as sess:
            sess.update(username="student", session_token="user-token")
        self.assertEqual(self.client.get("/admin/traffic").status_code, 302)
        self.admin()
        response = self.client.get("/admin/traffic")
        self.assertEqual(response.status_code, 200)
        soup = BeautifulSoup(response.get_data(as_text=True), "html.parser")
        self.assertIsNotNone(soup.select_one(".admin-brand"))
        self.assertIsNotNone(soup.select_one("#assignment-analytics"))
        self.assertEqual(soup.select_one("#pageVisitTotal").text, "1")
        self.assertIn("Dcard", soup.select_one("#trafficSources").text)
        self.assertEqual(len(self.rows()), 1)

    def test_time_windows_use_taipei_midnight_and_keep_other_filters(self):
        now = TAIPEI_TZ.localize(datetime(2026, 10, 7, 12))
        self.seed(ts=(now - timedelta(days=1)).timestamp())
        self.seed(ts=now.replace(hour=0, minute=0).timestamp(), source="Google")
        self.seed(ts=(now + timedelta(days=1)).replace(hour=0, minute=0).timestamp())
        self.admin()
        with patch("e3_tracker.platform.routes.administration.time.time", return_value=now.timestamp()):
            response = self.client.get("/admin/traffic?source_days=1&usage_range=7d&range=30d")
        soup = BeautifulSoup(response.get_data(as_text=True), "html.parser")
        self.assertEqual(soup.select_one("#pageVisitTotal").text, "1")
        link = soup.select_one('#trafficSources a[href*="source_days=7"]')["href"]
        self.assertIn("usage_range=7d", link)
        self.assertIn("range=30d", link)

    def test_legacy_counts_retention_and_recent_limit(self):
        now = time.time()
        for _ in range(205):
            self.seed()
        self.seed(device="Bot")
        self.seed(path="/study/recall")
        self.seed(ts=now - 91 * 86400)
        summary = build_page_sources(self.storage, since=now - 90 * 86400, until=now + 60)
        self.assertEqual(summary["total"], 205)
        self.assertEqual(len(summary["visits"]), 200)
        self.assertEqual(summary["rows"], [{"label": "Dcard", "count": 205, "percent": 100.0}])

    def test_reset_includes_sources_and_keeps_study_history(self):
        self.seed(visitor_key="user:student")
        self.seed(path="/study/recall")
        self.admin()
        self.client.post("/admin/traffic/reset-user", data={"username": "student"})
        self.assertEqual(len(self.rows()), 1)
        self.seed()
        self.client.post("/admin/traffic/reset")
        self.assertEqual([row["path"] for row in self.rows()], ["/study/recall"])


if __name__ == "__main__":
    unittest.main()
