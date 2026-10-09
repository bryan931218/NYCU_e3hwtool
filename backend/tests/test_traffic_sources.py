import os
import tempfile
import time
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from bs4 import BeautifulSoup
from flask import redirect
from itsdangerous import URLSafeTimedSerializer
from sqlalchemy import select

from e3_tracker.platform.application import create_app
from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.platform.page_analytics import (
    build_page_sources, page_analytics_table, register_page_analytics,
)
from e3_tracker.platform.services.traffic_sources import arrival_source, detect_arrival, source_info, stored_source_label
from tests.security_helpers import csrf_client


class SourceClassificationTests(unittest.TestCase):
    def test_referrers_tags_and_legacy_records(self):
        for domain, label in (("www.dcard.tw", "Dcard"), ("www.google.com.tw", "Google"),
                              ("l.facebook.com", "Facebook"), ("lin.ee", "LINE"),
                              ("google.com.evil.example", "google.com.evil.example"), ("evildcard.tw", "evildcard.tw")):
            with self.subTest(domain=domain):
                self.assertEqual(arrival_source("https://" + domain + "/?secret=private"), label)
                self.assertEqual(stored_source_label(domain), label)
        for tag, label in (("Dcard", "Dcard"), ("google", "Google"), ("fb", "Facebook"),
                           ("bookmark", "書籤（來源標記）"), ("newsletter", "來源標記：newsletter")):
            self.assertEqual(arrival_source("https://google.com", tag), label)
        self.assertEqual(arrival_source("https://[invalid"), "直接／來源不明")
        self.assertEqual(arrival_source(""), "直接／來源不明")
        self.assertEqual(stored_source_label("直接開啟 / 書籤"), "直接／來源不明")
        self.assertEqual(arrival_source("https://example.com/a", site_hosts={"example.com"}), "站內導覽")

    def test_app_hints_are_inferred_and_stronger_evidence_wins(self):
        options = dict(query={}, site_hosts={"localhost"})
        for ua, source in (("Mozilla/5.0 Line/15.0", "LINE（推測）"), ("Mozilla/5.0 Instagram 300", "Instagram（推測）"),
                           ("Mozilla/5.0 [FBAN/FB4A;FBAV/12]", "Facebook（推測）"), ("Dcard/2.0", "Dcard（推測）")):
            value = detect_arrival("", user_agent=ua, **options)
            self.assertEqual(stored_source_label(value), source)
            self.assertTrue(source_info(value)["inferred"])
        self.assertEqual(detect_arrival("", user_agent="Chrome/100 Safari/600", **options), "直接／來源不明")
        self.assertEqual(stored_source_label(detect_arrival("https://google.com/", user_agent="Line/15", **options)), "Google")
        self.assertEqual(stored_source_label(detect_arrival("android-app://jp.naver.line.android", **options)), "LINE")
        self.assertEqual(detect_arrival("", fetch_site="same-origin", **options), "站內導覽")
        self.assertEqual(detect_arrival("http://localhost/privacy", query={"utm_source":"dcard"}, site_hosts={"localhost"}), "站內導覽")
        self.assertEqual(detect_arrival("", query={"utm_source":"dcard"}, site_hosts={"localhost"}, fetch_site="same-origin"), "站內導覽")

    def test_click_tags_custom_domains_and_invalid_encodings(self):
        self.assertEqual(detect_arrival("", query={"gclid": "private"}, site_hosts=set()), "click:Google")
        self.assertEqual(detect_arrival("", query={"gclid": "private"}, site_hosts=set(), user_agent="Instagram 300"), "click:Google")
        self.assertEqual(detect_arrival("", query={"fbclid": "private"}, site_hosts=set()), "click:Meta")
        self.assertEqual(detect_arrival("", query={"from": "line"}, site_hosts=set()), "tag:line")
        self.assertEqual(detect_arrival("", query={"ref": "private-token"}, site_hosts=set()), "直接／來源不明")
        self.assertEqual(stored_source_label("ref:news.example"), "news.example")
        self.assertEqual(stored_source_label("app:invalid"), "其他來源")
        self.assertEqual(stored_source_label("https://bad"), "bad")


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
        self.assertEqual(stored_source_label(self.rows()[0]["source"]), "Dcard")
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
        self.assertEqual([stored_source_label(row["source"]) for row in self.rows()], ["Google", "站內導覽"])
        self.assertNotIn("private", str(self.rows()))

    def test_page_dashboard_excludes_study_pages_and_has_e3_data_navigation(self):
        self.seed(path='/login')
        self.seed(path='/study-progress', visitor_key='anon:study')
        self.seed(path='/public/study-progress', visitor_key='anon:study2')
        self.admin()
        html = self.client.get('/admin/analytics').get_data(as_text=True)
        self.assertIn('E3 數據後台', html)
        self.assertIn('不含考研站', html)
        self.assertNotIn('/study-progress', html)

    def test_today_page_dashboard_uses_taipei_midnight(self):
        midnight = datetime.now(TAIPEI_TZ).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        self.seed(path='/yesterday', ts=midnight - 1)
        self.seed(path='/today', ts=midnight)
        self.admin()
        html = self.client.get('/admin/analytics?days=1').get_data(as_text=True)
        self.assertIn('/today', html)
        self.assertNotIn('/yesterday', html)

    def test_new_arrival_does_not_inherit_abandoned_redirect(self):
        self.client.get("/source-entry?utm_source=dcard")
        self.client.get("/login", headers={"Referer": "https://google.com/"})
        self.assertEqual(stored_source_label(self.rows()[0]["source"]), "Google")

    def report(self, row, **fields):
        token = URLSafeTimedSerializer(self.app.secret_key, salt="e3-page-arrival").dumps({"id": row["id"]})
        return self.client.post("/traffic/arrival", json={"token": token, "navigation": "navigate", "referrer": "", **fields})

    def test_client_referrer_recovers_unknown_and_navigation_does_not_duplicate(self):
        self.client.get("/login")
        row = self.rows()[0]
        self.assertEqual(self.report(row, referrer="https://www.dcard.tw").status_code, 200)
        self.assertEqual(len(self.rows()), 1)
        self.assertEqual(self.rows()[0]["source"], "ref:www.dcard.tw")
        self.assertEqual(self.report(row, navigation="reload").status_code, 200)
        self.assertEqual(build_page_sources(self.storage, since=0, until=time.time()+60)["total"], 0)
        self.report(row, referrer="https://google.com")
        self.assertEqual(self.rows()[0]["source"], "nav:reload")

    def test_reports_require_csrf_signature_and_same_visitor(self):
        response = self.client.get("/login")
        self.assertIn("data-arrival-token=", response.get_data(as_text=True))
        self.assertEqual(self.client.post("/traffic/arrival", json={"token":"fake"}).status_code, 400)
        row = self.rows()[0]
        self.assertEqual(self.report(row, navigation=[]).status_code, 400)
        token = URLSafeTimedSerializer(self.app.secret_key, salt="e3-page-arrival").dumps({"id":row["id"]})
        anonymous = self.app.test_client()
        self.assertEqual(anonymous.post("/traffic/arrival", json={"token":token}).status_code, 400)
        self.assertEqual(self.report(row, referrer="https://google.com").status_code, 200)

    def test_reports_cannot_overwrite_known_sources_or_another_visitor(self):
        self.client.get("/login?utm_source=dcard")
        row = self.rows()[0]
        self.report(row, referrer="https://google.com")
        self.assertEqual(self.rows()[0]["source"], "tag:dcard")
        self.admin()
        self.assertEqual(self.report(row, referrer="https://google.com").status_code, 404)

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
        # Freeze the dashboard clock without making freshly signed session
        # cookies appear to come from the future later in the day.
        with patch("e3_tracker.platform.routes.administration.time", wraps=time) as clock:
            clock.time.return_value = now.timestamp()
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
        # Exclude historical internal navigation before calculating totals or
        # taking the last 200 arrivals, even when internal views dominate.
        for _ in range(205):
            self.seed(source="站內導覽")
        summary = build_page_sources(self.storage, since=now - 90 * 86400, until=now + 60)
        self.assertEqual(summary["total"], 205)
        self.assertEqual(len(summary["visits"]), 200)
        self.assertEqual(summary["rows"], [{"label": "Dcard", "count": 205, "percent": 100.0}])
        self.assertTrue(all(visit["source_label"] == "Dcard" for visit in summary["visits"]))

    def test_evidence_quality_and_legacy_domains_are_visible(self):
        sources = ["news.example", "ref:another.example", "app:LINE", "tag:dcard", "direct", "其他來源"]
        for source in sources:
            self.seed(source=source)
        summary = build_page_sources(self.storage, since=0, until=time.time()+60)
        self.assertEqual((summary["identified"], summary["inferred"], summary["unknown"]), (3, 1, 2))
        self.assertEqual({row["label"] for row in summary["rows"]},
                         {"news.example", "another.example", "LINE（推測）", "Dcard", "直接／來源不明", "其他來源"})
        visits = {visit["source_label"]: visit for visit in summary["visits"]}
        self.assertEqual(visits["another.example"]["evidence"], "another.example")
        self.assertEqual(visits["LINE（推測）"]["method"], "App 瀏覽器線索")
        self.assertEqual([row["source"] for row in self.rows()], sources)

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
