"""Opt-in collection, administrator isolation and non-blocking report failures."""

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from e3_tracker.platform.application import create_app
from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.platform.analytics_integration import register_google_analytics
from e3_tracker.platform.services.google_analytics import GoogleAnalyticsReports, REPORTS, analytics_config
from e3_tracker.assignments.services.admin_analytics import build_account_engagement
from tests.security_helpers import csrf_client


class GoogleAnalyticsIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {
            "E3_ENV": "development", "RAILWAY_ENVIRONMENT_ID": "", "RAILWAY_ENVIRONMENT_NAME": "",
            "RAILWAY_ENVIRONMENT": "", "E3_CACHE_DIR": self.folder.name, "E3_DATABASE_URL": "", "DATABASE_URL": "",
            "E3_SESSION_COOKIE_SECURE": "0", "E3_CANONICAL_HOST": "", "E3_NOTIFICATIONS_WORKER": "0",
            "E3_SESSION_PROFILE_WORKER": "0", "E3_YOUTUBE_AUTO_SYNC_ENABLED": "0", "OPENAI_API_KEY": "",
            "E3_GA4_MEASUREMENT_ID": "G-TEST1234", "E3_GA4_PROPERTY_ID": "", "E3_GA4_SERVICE_ACCOUNT_JSON": "",
        })
        self.env.start()
        self.app = create_app()
        self.storage = self.app.extensions["e3_storage"]
        self.client = csrf_client(self.app)

    def tearDown(self):
        self.storage._engine.dispose()
        self.env.stop()
        self.folder.cleanup()

    def login(self, *, admin=False):
        self.storage.save_web_session("analytics-web", "112550103", is_admin=admin)
        with self.client.session_transaction() as state:
            state["session_token"] = "analytics-web"

    def test_default_denied_and_granted_are_explicit_without_private_urls(self):
        html = self.client.get("/login?moodle_session=secret&username=112550103").get_data(as_text=True)
        self.assertIn('id="googleAnalyticsConfig"', html)
        snippet = html.split('id="googleAnalyticsConfig">')[1].split('</script>')[0]
        config = json.loads(snippet)
        self.assertEqual(config["consent"], "")
        self.assertEqual(config["path"], "/login")
        self.assertNotIn("secret", snippet)
        self.assertNotIn("112550103", snippet)
        self.assertNotIn('src="https://www.googletagmanager.com', html)
        for removed in ('id="analyticsConsent"', 'id="analyticsPrivacy"', '公開學習進度', '分析隱私設定'):
            self.assertNotIn(removed, html)
        self.assertNotIn("e3_analytics_consent", self.client.get("/login").headers.get("Set-Cookie", ""))
        self.assertEqual(self.client.post("/analytics/consent", json={"choice": "granted"}).status_code, 200)
        self.assertIn('"consent": "granted"', self.client.get("/login").get_data(as_text=True))
        self.client.post("/analytics/consent", json={"choice": "denied"})
        self.assertIn('"consent": "denied"', self.client.get("/login").get_data(as_text=True))
        self.assertEqual(self.client.post("/analytics/consent", json=["granted"]).status_code, 400)
        self.assertEqual(self.app.test_client().post("/analytics/consent", json={"choice": "granted"}).status_code, 400)

    def test_reports_are_admin_only_and_never_expose_credentials(self):
        self.assertEqual(self.client.get("/admin/analytics/ga4").status_code, 403)
        self.assertEqual(self.client.get("/admin/ga4").location, "/login")
        self.login()
        self.assertEqual(self.client.get("/admin/analytics/ga4").status_code, 403)
        self.assertEqual(self.client.get("/admin/ga4").location, "/")
        self.login(admin=True)
        response = self.client.get("/admin/analytics/ga4?days=999")
        self.assertEqual(response.json, {"status": "not_configured"})
        self.assertIn("no-store", response.headers["Cache-Control"])
        html = self.client.get("/admin/traffic").get_data(as_text=True)
        self.assertIn("帳號回訪", html)
        self.assertIn('aria-label="E3 數據後台"', html)
        self.assertIn('href="/admin/analytics"', html)
        page_html = self.client.get("/admin/analytics").get_data(as_text=True)
        self.assertIn('href="/admin/traffic"', page_html)
        self.assertIn('href="https://analytics.google.com/analytics/web/"', page_html)
        self.assertNotIn('href="/admin/study-home"', page_html)
        self.assertNotIn('id="google-analytics"', html)
        self.assertNotIn('shared/js/admin-ga4.js', html)
        ga4_page = self.client.get("/admin/ga4")
        self.assertEqual(ga4_page.status_code, 200)
        self.assertIn("no-store", ga4_page.headers["Cache-Control"])
        self.assertIn("尚未連接報表", ga4_page.get_data(as_text=True))
        self.assertNotIn('id="googleAnalyticsConfig"', ga4_page.get_data(as_text=True))
        self.assertNotIn('id="googleAnalyticsConfig"', html)
        self.assertNotIn('id="googleAnalyticsConfig"', self.client.get("/").get_data(as_text=True))

    def test_connected_reports_replace_duplicate_dashboards_without_losing_account_controls(self):
        self.login(admin=True)
        self.app.extensions["e3_google_analytics"].config["property_id"] = "466307937"
        with patch.dict(os.environ, {"E3_GA4_SERVICE_ACCOUNT_JSON": "configured"}), \
                patch("e3_tracker.platform.routes.administration.build_page_sources", side_effect=AssertionError("Legacy sources must not be queried")):
            html = self.client.get("/admin/traffic").get_data(as_text=True)
            self.assertIn('href="/admin/ga4"', html)
            self.assertNotIn('id="google-analytics"', html)
            self.assertNotIn('shared/js/admin-ga4.js', html)
            for obsolete in ['id="system-summary"', 'id="trafficSources"', 'class="analytics-feature-table"', 'href="/admin/analytics"', 'Top 5 使用者', '熱門操作', 'LINE 啟用進度']:
                self.assertNotIn(obsolete, html)
            for retained in ['id="account-usage"', 'id="recent-activity"', 'id="visit-trend"', 'id="usageRange"', 'LINE 綁定成功', '科系碼分布', 'data-traffic-stat="online"']:
                self.assertIn(retained, html)
            ga4_html = self.client.get("/admin/ga4").get_data(as_text=True)
            self.assertIn('id="ga4-features"', ga4_html)
            self.assertIn('href="/admin/ga4" aria-current="page"', ga4_html)
            self.assertNotIn('id="account-usage"', ga4_html)
            self.assertNotIn('shared/js/traffic-stats.js', ga4_html)
            self.assertNotIn('chart.umd', ga4_html)
            response = self.client.get("/admin/analytics")
            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.location, "/admin/ga4")

    def test_guest_or_expired_admin_cannot_open_ga4_page(self):
        self.storage.save_web_session("analytics-web", "guest-preview", is_admin=True, is_guest=True)
        with self.client.session_transaction() as state:
            state["session_token"] = "analytics-web"
        self.assertEqual(self.client.get("/admin/ga4").location, "/")
        self.assertEqual(self.client.get("/admin/analytics/ga4").status_code, 403)
        self.storage.save_web_session("analytics-web", "112550103", is_admin=True, lifetime=-1)
        self.assertEqual(self.client.get("/admin/ga4").location, "/login")

    def test_retention_observes_following_week_without_expanding_usage_window(self):
        self.login()
        joined = datetime.now(TAIPEI_TZ).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=10)
        self.storage.record_assignment_usage("112550103", "login", now=joined.timestamp())
        self.storage.record_assignment_usage("112550103", "login", now=(joined + timedelta(days=3)).timestamp())
        day = joined.date().isoformat()
        snapshot = self.storage.assignment_usage_snapshot(day, day)
        self.assertEqual({row["day"] for row in snapshot["daily"]}, {day, (joined + timedelta(days=3)).date().isoformat()})
        self.assertTrue(all(row["count"] == 1 for row in snapshot["usage"]))

    def test_unknown_study_and_readonly_pages_do_not_track(self):
        privacy = self.client.get("/privacy").get_data(as_text=True)
        config = json.loads(privacy.split('id="googleAnalyticsConfig">')[1].split('</script>')[0])
        self.assertTrue(config["settings"])
        self.assertEqual(config["consent"], "")
        self.assertIn('id="analyticsPreferenceStatus"', privacy)
        self.assertNotIn('id="googleAnalyticsConfig"', self.client.get("/?view_user=113550092").get_data(as_text=True))
        self.assertNotIn('id="googleAnalyticsConfig"', self.client.get("/study").get_data(as_text=True))

    def test_privacy_preferences_keep_saved_choices_and_do_not_create_consent(self):
        for choice in ("granted", "denied"):
            self.client.post("/analytics/consent", json={"choice": choice})
            response = self.client.get("/privacy")
            html = response.get_data(as_text=True)
            config = json.loads(html.split('id="googleAnalyticsConfig">')[1].split('</script>')[0])
            self.assertEqual(config["consent"], choice)
            self.assertTrue(config["settings"])
            self.assertNotIn("e3_analytics_consent", response.headers.get("Set-Cookie", ""))

    def test_missing_configuration_hides_privacy_preferences(self):
        self.app.extensions["e3_google_analytics"].config["measurement_id"] = ""
        html = self.client.get("/privacy").get_data(as_text=True)
        self.assertNotIn('id="analyticsConsent"', html)
        self.assertNotIn('id="googleAnalyticsConfig"', html)

    def test_consent_survives_auth_session_rotation_and_login_is_consumed_once(self):
        response = self.client.post("/analytics/consent", json={"choice": "granted"})
        self.assertIn("HttpOnly", response.headers["Set-Cookie"])
        with self.client.session_transaction() as state:
            state.clear()
            state["ga4_login_events"] = [{"method": "session"}]
        first = self.client.get("/login").get_data(as_text=True)
        config = json.loads(first.split('id="googleAnalyticsConfig">')[1].split('</script>')[0])
        self.assertEqual(config["consent"], "granted")
        self.assertEqual(config["events"], [{"method": "session"}])
        second = self.client.get("/login").get_data(as_text=True)
        self.assertIn('"events": []', second)

    def test_missing_config_changes_neither_pages_nor_csp(self):
        self.app.extensions["e3_google_analytics"].config["measurement_id"] = ""
        response = self.client.get("/login")
        self.assertNotIn('id="googleAnalyticsConfig"', response.get_data(as_text=True))
        self.assertNotIn("google-analytics.com", response.headers["Content-Security-Policy"])

    def test_optional_markup_failure_does_not_break_login_page(self):
        with patch('e3_tracker.platform.analytics_integration.render_template', side_effect=RuntimeError('test only')):
            response = self.client.get('/login')
        self.assertEqual(response.status_code, 200)
        self.assertNotIn('id="googleAnalyticsConfig"', response.get_data(as_text=True))


class GoogleAnalyticsReportTests(unittest.TestCase):
    def test_validated_config_rejects_url_and_script_injection(self):
        with patch.dict(os.environ, {"E3_GA4_MEASUREMENT_ID": 'G-test\"<', "E3_GA4_PROPERTY_ID": '../secret',
                                     "E3_GA4_CAMPAIGNS": "dcard-115-1,private value,<script>"}):
            self.assertEqual(analytics_config(), {"measurement_id": "", "property_id": "", "campaigns": ["dcard-115-1"]})

    def test_cold_cache_does_not_wait_and_failure_retains_old_data_without_secret(self):
        reports = GoogleAnalyticsReports({"measurement_id": "", "property_id": "123", "campaigns": []})
        with patch.dict(os.environ, {"E3_GA4_SERVICE_ACCOUNT_JSON": "private"}), patch('threading.Thread') as thread:
            self.assertEqual(reports.get(999), {"status": "loading"})
            reports.get(30)
            thread.assert_called_once()
        with patch.object(reports, '_fetch', side_effect=ValueError('private-key')):
            reports._refresh(30)
        self.assertEqual(reports._cache[30]['status'], 'unavailable')
        self.assertNotIn('private', str(reports._cache))
        reports._cache[30] = {'status': 'ready', 'reports': {'summary': []}, 'updated_at': 'old', 'checked_at': 0}
        with patch.object(reports, '_fetch', side_effect=TimeoutError()):
            reports._refresh(30)
        self.assertEqual(reports._cache[30]['status'], 'stale')
        self.assertEqual(reports._cache[30]['updated_at'], 'old')

    def test_batch_limit_readonly_scope_funnel_isolation_and_numeric_results(self):
        reports = GoogleAnalyticsReports({'measurement_id': '', 'property_id': '123', 'campaigns': []})
        credentials = MagicMock(token='test-token')
        def response_for(url, **kwargs):
            response = MagicMock()
            if ':runFunnelReport' in url:
                raise TimeoutError('alpha offline')
            items = kwargs['json']['requests']
            self.assertLessEqual(len(items), 5)
            response.json.return_value = {'reports': [{'rows': [{
                'dimensionValues': [{'value': 'example'} for _ in item['dimensions']],
                'metricValues': [{'value': '12'} for _ in item['metrics']],
            }]} for item in items]}
            return response
        with patch.dict(os.environ, {'E3_GA4_SERVICE_ACCOUNT_JSON': json.dumps({'type': 'service_account', 'token_uri': 'https://oauth2.googleapis.com/token'})}), \
                patch('google.oauth2.service_account.Credentials.from_service_account_info', return_value=credentials) as factory, \
                patch('requests.Session') as client:
            client.return_value.__enter__.return_value.post.side_effect = response_for
            result = reports._fetch(7)
        self.assertEqual(result['summary'][0]['activeUsers'], 12)
        self.assertEqual(result['login_funnel'], None)
        self.assertEqual(len(result), len(REPORTS) + 2)
        self.assertEqual(factory.call_args.kwargs['scopes'], ['https://www.googleapis.com/auth/analytics.readonly'])

    def test_retention_deduplicates_sessions_excludes_incomplete_and_unknown_history(self):
        today = TAIPEI_TZ.localize(datetime(2026, 10, 10))
        timestamp = lambda day: TAIPEI_TZ.localize(datetime(2026, 10, day)).timestamp()
        snapshot = {'accounts': [{'id': 1, 'username': '112550103'}, {'id': 2, 'username': 'Session-one', 'student_number': '112550103'},
                                  {'id': 3, 'username': '113550092'}, {'id': 4, 'username': '114550001'}],
                    'daily': [{'user_id': 1, 'day': '2026-10-01'}, {'user_id': 2, 'day': '2026-10-03'}],
                    'usage': [], 'state': {'started_at': timestamp(1) - 10, 'legacy_samples': 0},
                    'linked': {'line': {1, 2}, 'browser': set(), 'google': set()},
                    'settings': [{'user_id': 1, 'preferences': '{"line_enabled":true}'}]}
        members = {'112550103': {'joined_at': timestamp(1), 'is_new': 1}, 'Session-one': {'joined_at': timestamp(1), 'is_new': 1},
                   '113550092': {'joined_at': timestamp(8), 'is_new': 1}, '114550001': {'joined_at': timestamp(1), 'is_new': 0}}
        result = build_account_engagement(snapshot, members, {'start': '2026-10-01', 'end': '2026-10-10'}, now=today)
        self.assertEqual((result['eligible'], result['retained'], result['pending'], result['retention']), (1, 1, 1, 100))
        self.assertEqual((result['active'], result['repeat']), (1, 1))
        self.assertEqual([row['count'] for row in result['line_steps']], [3, 1, 1])
        historical = build_account_engagement(snapshot, members, {'start': '2026-10-01', 'end': '2026-10-01'}, now=today)
        self.assertEqual((historical['active'], historical['repeat']), (1, 0))
        self.assertEqual((historical['eligible'], historical['retained'], historical['retention']), (1, 1, 100))
