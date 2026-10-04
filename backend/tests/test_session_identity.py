"""Session enrichment uses only E3's own locked student-number field."""

import hashlib
import os
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

import requests
from bs4 import BeautifulSoup
from sqlalchemy import text

from e3_tracker.assignments.services.profile import fetch_session_identity, parse_student_number
from e3_tracker.platform.application import create_app
from e3_tracker.platform.persistence import migrations
from e3_tracker.platform.services.account_labels import account_label
from tests.security_helpers import csrf_client


BASE = "https://e3.example"
MENU = '<div class="usermenu"><a href="/user/profile.php?id=42">Profile</a></div>'
PROFILE = '<header id="page-header"><h1>王小明</h1></header><a href="/user/edit.php?id=42&amp;returnto=profile">Edit</a>'
LOCKED = '<input id="id_idnumber" name="idnumber" value="112550101" disabled readonly>'
IDENTITY = {"name": "王小明", "student_number": "112550101"}


def response(html, status=200):
    return Mock(text=html, status_code=status)


class IdentityParsingTests(unittest.TestCase):
    def test_session_name_does_not_use_the_trailing_department(self):
        with patch('e3_tracker.assignments.services.profile.safe_request', side_effect=[
            response(MENU), response(PROFILE.replace('王小明', '王小明 / 藥學系')), response(LOCKED),
        ]):
            self.assertEqual(fetch_session_identity(Mock(), BASE), IDENTITY)

    def test_display_label_is_not_an_authorization_identity(self):
        self.assertEqual(account_label('Session-demo', '112550101'), '112550101（session登入）')
        self.assertEqual(account_label('112550103', '112550101'), '112550103')
        for number in ('', '１２３４５６７８９', '<script>', '123'):
            self.assertEqual(account_label('Session-demo', number), 'Session-demo')

    def test_only_locked_nine_ascii_digit_field_is_accepted(self):
        self.assertEqual(parse_student_number(LOCKED), '112550101')
        for invalid in (
            LOCKED.replace(' disabled', ''), LOCKED.replace(' readonly', ''),
            LOCKED.replace('112550101', '11598'), LOCKED.replace('112550101', '１２３４５６７８９'),
            LOCKED.replace('id_idnumber', 'id_username'),
            '<h1>112550101</h1>', '<input name="username" value="112550101">',
        ):
            with self.subTest(html=invalid):
                self.assertEqual(parse_student_number(invalid), '')

    def test_own_edit_page_is_read_without_redirects_or_submitting_data(self):
        with patch('e3_tracker.assignments.services.profile.safe_request', side_effect=[
            response(MENU), response(PROFILE), response(LOCKED),
        ]) as request:
            self.assertEqual(fetch_session_identity(Mock(), BASE), IDENTITY)
        self.assertEqual([call.args[2] for call in request.call_args_list], [
            BASE + '/my/', BASE + '/user/profile.php?id=42',
            BASE + '/user/edit.php?id=42&returnto=profile',
        ])
        for call in request.call_args_list:
            self.assertEqual(call.args[1], 'GET')
            self.assertFalse(call.kwargs['allow_redirects'])
            self.assertNotIn('data', call.kwargs)

    def test_external_or_other_users_edit_links_are_not_followed(self):
        for href in (
            'https://evil.example/user/edit.php?id=42', '/user/edit.php?id=99',
            '/user/edit.php?id=42&id=99', '/user/edit.php?id=not-an-id',
            'https://attacker@e3.example/user/edit.php?id=42',
        ):
            with self.subTest(href=href), patch('e3_tracker.assignments.services.profile.safe_request', side_effect=[
                response(MENU), response(f'<a href="{href}">Edit</a>'),
            ]) as request:
                self.assertEqual(fetch_session_identity(Mock(), BASE)['student_number'], '')
                self.assertEqual(request.call_count, 2)

    def test_untrusted_menu_and_login_redirect_are_not_followed(self):
        for menu, status in ((MENU, 302), (MENU.replace('/user/profile.php', 'https://evil.example/user/profile.php'), 200),
                             ('<form action="/login/index.php">Login</form>', 200)):
            with self.subTest(menu=menu), patch('e3_tracker.assignments.services.profile.safe_request', return_value=response(menu, status)) as request:
                self.assertEqual(fetch_session_identity(Mock(), BASE)['student_number'], '')
                self.assertEqual(request.call_count, 1)

    def test_failed_edit_response_does_not_supply_identity(self):
        with patch('e3_tracker.assignments.services.profile.safe_request', side_effect=[
            response(MENU), response(PROFILE), response(LOCKED, 302),
        ]):
            self.assertEqual(fetch_session_identity(Mock(), BASE)['student_number'], '')


class SessionIdentityTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {
            'E3_ENV': 'development', 'RAILWAY_ENVIRONMENT_ID': '', 'RAILWAY_ENVIRONMENT_NAME': '',
            'E3_CACHE_DIR': self.directory.name, 'E3_DATABASE_URL': '', 'DATABASE_URL': '',
            'E3_CANONICAL_HOST': '', 'E3_SESSION_COOKIE_SECURE': '0',
            'E3_NOTIFICATIONS_WORKER': '0', 'E3_SESSION_PROFILE_WORKER': '0',
            'E3_YOUTUBE_AUTO_SYNC_ENABLED': '0', 'OPENAI_API_KEY': '',
        })
        self.env.start()
        self.app = create_app(default_base_url=BASE)
        self.storage = self.app.extensions['e3_storage']
        self.sync = self.app.extensions['e3_session_identity']
        self.client = csrf_client(self.app)
        self.username = 'Session-existing'
        self.storage.save_user_profile(self.username, '王小明', '王')
        self.storage.save_web_session('identity-test', self.username, moodle_session='opaque-cookie')
        with self.client.session_transaction() as session:
            session['session_token'] = 'identity-test'

    def tearDown(self):
        self.sync.stop.set()
        self.storage._engine.dispose()
        self.env.stop()
        self.directory.cleanup()

    def test_existing_named_session_is_enriched_once_on_profile_request(self):
        self.storage.save_user_preferences(self.username, {'view_mode': 'course'})
        self.storage.save_user_cache(self.username, {'result': {'courses': []}, 'ts': 5})
        with patch('e3_tracker.assignments.services.session_identity.fetch_session_identity', return_value=IDENTITY) as fetch:
            self.assertEqual(self.client.get('/api/profile').json, {
                'ok': True, 'surname': '王', 'account_label': '112550101（session登入）',
            })
            self.client.get('/api/profile')
            fetch.assert_called_once()
        self.assertEqual(self.storage.load_student_number(self.username), '112550101')
        self.assertEqual(self.storage.load_user_preferences(self.username)['view_mode'], 'course')
        self.assertEqual(self.storage.load_user_cache(self.username)['ts'], 5)
        self.assertFalse(self.storage.load_web_session('identity-test')['is_admin'])

    def test_new_login_automatically_enriches_without_trusting_submitted_number(self):
        with self.client.session_transaction() as session:
            session.clear()
        cookie = 'new-opaque-cookie'
        username = 'Session-' + hashlib.sha1(cookie.encode()).hexdigest()[:10]
        self.storage.save_user_cache(username, {'result': {'courses': []}, 'ts': 5})
        with patch('e3_tracker.assignments.services.session_identity.fetch_session_identity', return_value=IDENTITY):
            result = self.client.post('/login', data={
                'login_type': 'session', 'moodle_session': cookie, 'student_number': '112550103',
            })
        self.assertEqual(result.status_code, 302)
        self.assertEqual(self.storage.load_student_number(username), '112550101')
        self.assertEqual(self.storage.load_user_profile(username)['name'], '王小明')
        with self.client.session_transaction() as session:
            token = session['session_token']
        self.assertFalse(self.storage.load_web_session(token)['is_admin'])

    def test_login_survives_identity_timeout_and_can_retry_later(self):
        with self.client.session_transaction() as session:
            session.clear()
        cookie = 'new-opaque-cookie'
        username = 'Session-' + hashlib.sha1(cookie.encode()).hexdigest()[:10]
        self.storage.save_user_cache(username, {'result': {'courses': []}, 'ts': 5})
        with patch('e3_tracker.assignments.services.session_identity.fetch_session_identity', side_effect=requests.Timeout('secret-cookie')):
            with self.assertLogs('e3_tracker.assignments.services.session_identity', level='WARNING') as logs:
                result = self.client.post('/login', data={'login_type': 'session', 'moodle_session': cookie})
        self.assertEqual(result.status_code, 302)
        self.assertNotIn('secret-cookie', ''.join(logs.output))
        self.assertEqual(self.storage.load_student_number(username), '')

    def test_background_backfills_all_valid_sessions_even_without_notifications(self):
        self.storage.save_web_session('second-token', 'Session-second', moodle_session='another-cookie')
        self.storage.save_web_session('expired', 'Session-expired', moodle_session='old', lifetime=-1)
        self.storage.save_web_session('no-cookie', 'Session-no-cookie')
        self.storage.save_web_session('guest', 'Session-guest', is_guest=True, moodle_session='guest')
        self.storage.save_web_session('password', '112550102', moodle_session='password-cookie')
        with patch('e3_tracker.assignments.services.session_identity.fetch_session_identity', return_value=IDENTITY) as fetch:
            self.sync.backfill_once()
            self.sync.backfill_once()
        self.assertEqual(fetch.call_count, 2)
        for name in (self.username, 'Session-second'):
            self.assertEqual(self.storage.load_student_number(name), '112550101')
        for name in ('Session-expired', 'Session-no-cookie', 'Session-guest', '112550102'):
            self.assertEqual(self.storage.load_student_number(name), '')

    def test_failure_is_throttled_and_later_retried(self):
        with patch('e3_tracker.assignments.services.session_identity.fetch_session_identity', side_effect=requests.Timeout()) as fetch:
            self.sync.backfill_once()
            self.sync.backfill_once()
            self.client.get('/api/profile')
            fetch.assert_called_once()
        with self.storage._engine.begin() as conn:
            conn.execute(text('UPDATE users SET student_number_sync_after=0'))
        with patch('e3_tracker.assignments.services.session_identity.fetch_session_identity', return_value=IDENTITY):
            self.sync.backfill_once()
        self.assertEqual(self.storage.load_student_number(self.username), '112550101')

    def test_admin_number_does_not_grant_roles_or_merge_accounts(self):
        self.storage.save_user_profile('112550103', '李小明', '李')
        with patch('e3_tracker.assignments.services.session_identity.fetch_session_identity', return_value={**IDENTITY, 'student_number': '112550103'}):
            self.sync.backfill_once()
        self.assertFalse(self.storage.load_web_session('identity-test')['is_admin'])
        self.assertEqual(self.storage.load_user_profile('112550103')['name'], '李小明')
        self.assertEqual(self.client.get('/admin/traffic').status_code, 302)

    def test_malformed_or_conflicting_number_never_replaces_mapping(self):
        for value in ('123', '<script>', '１２３４５６７８９'):
            self.assertFalse(self.storage.save_student_number(self.username, value))
        self.assertTrue(self.storage.save_student_number(self.username, '112550101'))
        self.assertFalse(self.storage.save_student_number(self.username, '112550102'))
        self.assertEqual(self.storage.load_student_number(self.username), '112550101')

    def test_mapping_shown_to_admin_only_and_cleared_traffic_stays_hidden(self):
        self.storage.save_student_number(self.username, '112550101')
        self.storage.save_user_cache(self.username, {'result': {'courses': [], 'all_assignments': []}, 'ts': 5})
        self.client.post('/ui-event', json={'action': 'login_success'})
        self.assertEqual(self.client.get('/api/profile').json['account_label'], '112550101（session登入）')
        self.storage.save_web_session('admin', 'admin', is_admin=True)
        admin = csrf_client(self.app)
        with admin.session_transaction() as session:
            session['session_token'] = 'admin'
        page = BeautifulSoup(admin.get('/admin/traffic').get_data(as_text=True), 'html.parser')
        table = page.find('th', string='學號／帳號').find_parent('table')
        row = table.find('td', string='112550101（session登入）').find_parent('tr')
        self.assertEqual(row.select_one('input[name="username"]')['value'], self.username)
        option = page.select_one(f'#trafficViewUser option[value="{self.username}"]')
        self.assertIn('112550101（session登入）', option.get_text())
        self.assertNotIn(self.username, option.get_text())
        self.assertIn('112550101（session登入）', page.select_one('.events').get_text())
        admin.post('/admin/traffic/reset-user', data={'username': self.username})
        page = BeautifulSoup(admin.get('/admin/traffic').get_data(as_text=True), 'html.parser')
        table = page.find('th', string='學號／帳號').find_parent('table')
        self.assertNotIn('112550101（session登入）', table.get_text())

    def test_own_header_and_admin_readonly_banner_use_label_not_internal_key(self):
        self.storage.save_student_number(self.username, '112550101')
        self.storage.save_user_cache(self.username, {'result': {'courses': [], 'all_assignments': []}, 'ts': 5})
        page = BeautifulSoup(self.client.get('/').get_data(as_text=True), 'html.parser')
        self.assertEqual(page.select_one('#userAccountLabel').get_text(), '112550101（session登入）')
        self.storage.save_web_session('admin', 'admin', is_admin=True)
        admin = csrf_client(self.app)
        with admin.session_transaction() as session:
            session['session_token'] = 'admin'
        page = BeautifulSoup(admin.get('/', query_string={'view_user': self.username}).get_data(as_text=True), 'html.parser')
        self.assertIn('112550101（session登入）', page.select_one('.readonly-banner').get_text())
        self.assertEqual(page.select_one('body')['data-view-user'], self.username)

    def test_same_student_number_never_changes_delete_target(self):
        other = 'Session-other'
        self.storage.save_web_session('other', other)
        self.storage.save_student_number(self.username, '112550101')
        self.storage.save_student_number(other, '112550101')
        self.client.post('/ui-event', json={'action': 'login_success'})
        client = csrf_client(self.app)
        with client.session_transaction() as session:
            session['session_token'] = 'other'
        client.post('/ui-event', json={'action': 'login_success'})
        self.storage.save_web_session('admin', 'admin', is_admin=True)
        with client.session_transaction() as session:
            session['session_token'] = 'admin'
        client.post('/admin/traffic/reset-user', data={'username': self.username})
        page = BeautifulSoup(client.get('/admin/traffic').get_data(as_text=True), 'html.parser')
        table = page.find('th', string='學號／帳號').find_parent('table')
        remaining_targets = [item['value'] for item in table.select('input[name="username"]')]
        self.assertNotIn(self.username, remaining_targets)
        self.assertIn(other, remaining_targets)

    def test_additive_migration_preserves_accounts_profiles_sessions_and_cache(self):
        self.storage.save_user_cache(self.username, {'result': {'courses': []}, 'ts': 5})
        with self.storage._engine.begin() as conn:
            conn.execute(text('ALTER TABLE users DROP COLUMN student_number'))
            conn.execute(text('ALTER TABLE users DROP COLUMN student_number_sync_after'))
            conn.execute(text("DELETE FROM e3_schema_migrations WHERE version='0008_session_student_number'"))
        self.assertEqual(migrations.run_migrations(self.storage._engine), ['0008_session_student_number'])
        self.assertEqual(migrations.run_migrations(self.storage._engine), [])
        self.assertEqual(self.storage.load_student_number(self.username), '')
        self.assertEqual(self.storage.load_user_profile(self.username)['name'], '王小明')
        self.assertEqual(self.storage.load_user_cache(self.username)['ts'], 5)
        self.assertTrue(self.storage.is_valid_web_session('identity-test', self.username))

    def test_retry_claim_is_shared_across_workers(self):
        now = time.time()
        self.assertTrue(self.storage.claim_student_number_sync(self.username, now))
        self.assertFalse(self.storage.claim_student_number_sync(self.username, now))
        self.assertTrue(self.storage.claim_student_number_sync(self.username, now + 3601))

    def test_worker_runs_backfill_in_app_context_and_can_stop(self):
        with patch.dict(os.environ, {'E3_SESSION_PROFILE_WORKER': '1'}), patch(
            'e3_tracker.assignments.services.session_identity.threading.Thread'
        ) as thread:
            self.sync.start(self.app)
            thread.return_value.start.assert_called_once()
            self.assertTrue(thread.call_args.kwargs['daemon'])
            run = thread.call_args.kwargs['target']
        with patch.object(self.sync.stop, 'wait', side_effect=[False, True]), patch.object(self.sync, 'backfill_once') as backfill:
            run()
            backfill.assert_called_once()

    def test_disabled_worker_does_not_start(self):
        with patch('e3_tracker.assignments.services.session_identity.threading.Thread') as thread:
            self.sync.start(self.app)
            thread.assert_not_called()


if __name__ == '__main__':
    unittest.main()
