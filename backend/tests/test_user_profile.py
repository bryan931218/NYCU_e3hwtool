import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import requests
from bs4 import BeautifulSoup
from sqlalchemy import inspect, text

from e3_tracker.platform.application import create_app
from e3_tracker.assignments.services.profile import fetch_profile_surname, parse_profile_surname, parse_profile_name
from e3_tracker.platform.persistence import migrations


class ProfileParsingTests(unittest.TestCase):
    def test_department_prefix_and_compound_surnames(self):
        for name, expected in [("資工系 / DCP 王小明", "王"), ("歐陽小明", "歐陽"),
                               ("司徒小明", "司徒"), ("王小明", "王")]:
            with self.subTest(name=name):
                self.assertEqual(parse_profile_surname(f'<header id="page-header"><h1>{name}</h1></header>'), expected)

    def test_missing_or_unrecognized_name_never_uses_account_digits(self):
        for name in ("112550103", "登入", "DCP 112550103"):
            self.assertEqual(parse_profile_surname(f'<h1>{name}</h1>'), "")
        self.assertEqual(parse_profile_surname('<header id="page-header"><h1>112550103</h1></header>'), "")

    def test_only_follows_own_profile_link_and_does_not_capture_other_fields(self):
        menu = '<div class="usermenu"><a href="/user/profile.php?id=42">關於我</a></div>'
        profile = '<header id="page-header"><h1>資工系 / DCP 王小明</h1></header><dd>private@example.test</dd>'
        with patch('e3_tracker.assignments.services.profile.safe_request', side_effect=[Mock(text=menu), Mock(text=profile)]) as request:
            self.assertEqual(fetch_profile_surname(Mock(), 'https://e3.example'), '王')
        self.assertEqual(request.call_args_list[1].args[2], 'https://e3.example/user/profile.php?id=42')

    def test_external_profile_link_is_not_followed(self):
        menu = '<div class="usermenu"><a href="https://other.example/user/profile.php">關於我</a></div>'
        with patch('e3_tracker.assignments.services.profile.safe_request', return_value=Mock(text=menu)) as request:
            self.assertEqual(fetch_profile_surname(Mock(), 'https://e3.example'), '')
            self.assertEqual(request.call_count, 1)

    def test_full_name_excludes_department_and_other_private_fields(self):
        html = '<header id="page-header"><h1>資工系 / DCP 歐陽小明</h1></header><dd>private@example.test</dd>'
        self.assertEqual(parse_profile_name(html), '歐陽小明')

    def test_login_and_error_pages_do_not_become_profile_names(self):
        for title in ('登入', '關於我', '個人資料', '焦點綜覽', '錯誤訊息', '112550103'):
            with self.subTest(title=title):
                self.assertEqual(parse_profile_name(f'<header id="page-header"><h1>{title}</h1></header>'), '')


class UserProfileTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(os.environ, {
            'E3_CACHE_DIR': self.directory.name, 'E3_DATABASE_URL': '',
            'E3_CANONICAL_HOST': '', 'E3_SESSION_COOKIE_SECURE': '0',
        })
        self.environment.start()
        self.app = create_app()
        self.storage = self.app.extensions['e3_storage']
        from tests.security_helpers import csrf_client
        self.client = csrf_client(self.app)
        self.storage.save_web_session('profile-test', 'student', moodle_session='test-cookie')
        with self.client.session_transaction() as session:
            session.update(username='student', session_token='profile-test', moodle_session='test-cookie')

    def tearDown(self):
        self.storage._engine.dispose()
        self.environment.stop()
        self.directory.cleanup()

    def test_profile_is_fetched_once_and_persisted_across_requests(self):
        with patch('e3_tracker.assignments.routes.assignments.fetch_profile_name', return_value='王小明') as fetch:
            self.assertEqual(self.client.get('/api/profile').json, {'ok': True, 'surname': '王'})
            self.assertEqual(self.client.get('/api/profile').json['surname'], '王')
            self.assertEqual(fetch.call_count, 1)
        self.assertEqual(self.storage.load_user_surname('student'), '王')
        self.assertEqual(self.storage.load_user_profile('student')['name'], '王小明')
        self.assertEqual(self.storage.load_user_surname('someone-else'), '')
        self.assertIn('王', self.client.get('/').get_data(as_text=True))

    def test_refresh_updates_name_without_affecting_other_accounts(self):
        self.storage.save_user_surname('student', '王')
        self.storage.save_user_surname('someone-else', '李')
        with patch('e3_tracker.assignments.routes.assignments.fetch_profile_name', return_value='歐陽小明'):
            self.assertEqual(self.client.get('/api/profile?refresh=1&username=someone-else').json['surname'], '歐陽')
        self.assertEqual(self.storage.load_user_surname('someone-else'), '李')
        self.assertEqual(self.storage.load_user_profile('student')['name'], '歐陽小明')
        self.assertEqual(self.storage.load_user_profile('someone-else')['name'], '')

    def test_profile_failure_preserves_saved_surname(self):
        self.storage.save_user_profile('student', '王小明', '王')
        for result in ('', requests.Timeout('unavailable')):
            with self.subTest(result=result), patch('e3_tracker.assignments.routes.assignments.fetch_profile_name') as fetch:
                if isinstance(result, Exception):
                    fetch.side_effect = result
                else:
                    fetch.return_value = result
                self.assertEqual(self.client.get('/api/profile?refresh=1').json['surname'], '王')
                self.assertEqual(self.storage.load_user_profile('student')['name'], '王小明')

    def test_guests_and_unauthenticated_requests_do_not_fetch_e3(self):
        with patch('e3_tracker.assignments.routes.assignments.fetch_profile_name') as fetch:
            self.storage.save_web_session('profile-test', 'student', is_guest=True)
            self.assertEqual(self.client.get('/api/profile').json['surname'], '')
            with self.client.session_transaction() as session:
                session.clear()
            self.assertEqual(self.client.get('/api/profile').status_code, 302)
            fetch.assert_not_called()

    def test_existing_surname_is_backfilled_with_full_name(self):
        self.storage.save_user_surname('student', '王')
        with patch('e3_tracker.assignments.routes.assignments.fetch_profile_name', return_value='王小明') as fetch:
            response = self.client.get('/api/profile')
            fetch.assert_called_once()
        self.assertEqual(response.json, {'ok': True, 'surname': '王'})
        self.assertEqual(self.storage.load_user_profile('student')['name'], '王小明')

    def test_admin_sees_stored_name_mappings_even_without_traffic(self):
        self.storage.save_user_profile('112550101', '王小明', '王')
        self.storage.save_user_cache('112550101', {'result': {'courses': []}, 'ts': 1})
        self.storage.save_user_profile('112550102', '<img src=x onerror=alert(1)>', '李')
        self.storage.save_user_surname('112550104', '陳')
        self.storage.save_web_session('profile-test', 'student', is_admin=True)
        response = self.client.get('/admin/traffic')
        self.assertEqual(response.status_code, 200)
        soup = BeautifulSoup(response.get_data(as_text=True), 'html.parser')
        table = soup.find('th', string='學號／帳號').find_parent('table')
        self.assertIn('姓名', table.get_text())
        rows = {row.select_one('td').get_text(): row for row in table.select('tbody tr')}
        self.assertEqual(rows['112550101'].select('td')[1].get_text(), '王小明')
        self.assertEqual(rows['112550104'].select('td')[1].get_text(), '尚未取得')
        self.assertFalse(rows['112550102'].select('img'))
        self.assertIn('<img src=x onerror=alert(1)>', rows['112550102'].get_text())
        option = soup.select_one('#trafficViewUser option[value="112550101"]')
        self.assertIn('王小明', option.get_text())

    def test_other_users_names_are_not_available_to_regular_users_or_guests(self):
        self.storage.save_user_profile('112550101', '王小明', '王')
        self.assertEqual(self.client.get('/admin/traffic').status_code, 302)
        self.assertNotIn('王小明', self.client.get('/').get_data(as_text=True))
        self.assertNotIn('王小明', self.client.get('/api/cache').get_data(as_text=True))
        self.storage.save_web_session('profile-test', 'student', is_guest=True)
        self.assertEqual(self.client.get('/admin/traffic').status_code, 302)
        with self.client.session_transaction() as session:
            session.clear()
        self.assertEqual(self.client.get('/admin/traffic').status_code, 302)

    def test_deleted_session_stats_are_not_restored_from_saved_profiles(self):
        username = 'Session-673ffeaeac'
        self.storage.save_user_profile(username, '王小明', '王')
        self.storage.save_user_cache(username, {'result': {'courses': []}, 'ts': 1})
        self.storage.save_user_profile('112550101', '李小明', '李')
        self.storage.save_user_surname('Session-95ae68897b', '游')
        self.storage.save_web_session('profile-test', 'student', is_admin=True)
        self.storage.save_web_session('session-test', username)
        from tests.security_helpers import csrf_client
        session_client = csrf_client(self.app)
        with session_client.session_transaction() as session:
            session['session_token'] = 'session-test'
        response = session_client.post('/ui-event', json={'action': 'login_success'})
        self.assertEqual(response.status_code, 200)

        def account_names(client):
            response = client.get('/admin/traffic')
            self.assertEqual(response.status_code, 200)
            page = BeautifulSoup(response.get_data(as_text=True), 'html.parser')
            table = page.find('th', string='學號／帳號').find_parent('table')
            return {row.select_one('td').get_text() for row in table.select('tbody tr')}

        self.assertIn(username, account_names(self.client))
        self.assertNotIn('Session-95ae68897b', account_names(self.client))
        response = self.client.post('/admin/traffic/reset-user', data={'username': username})
        self.assertEqual(response.status_code, 302)
        self.assertNotIn(username, account_names(self.client))
        self.assertIn('112550101', account_names(self.client))
        self.assertEqual(self.storage.load_user_profile(username)['name'], '王小明')
        self.assertIsNotNone(self.storage.load_user_cache(username))
        self.assertTrue(self.storage.is_valid_web_session('session-test', username))

        reloaded = create_app()
        try:
            client = csrf_client(reloaded)
            with client.session_transaction() as session:
                session['session_token'] = 'profile-test'
            self.assertNotIn(username, account_names(client))
        finally:
            reloaded.extensions['e3_storage']._engine.dispose()

        session_client.post('/ui-event', json={'action': 'login_success'})
        self.assertIn(username, account_names(self.client))

    def test_admin_can_select_and_read_session_account_cache(self):
        username = 'Session-673ffeaeac'
        self.storage.save_user_profile(username, '王小明', '王')
        self.storage.save_user_cache(username, {
            'ts': 1, 'result': {'courses': [
                {'id': 101, 'title': '【115上】Session Course', 'assignments': []},
            ], 'all_assignments': []},
        })
        self.storage.save_web_session('profile-test', 'student', is_admin=True)
        response = self.client.get('/admin/traffic')
        page = BeautifulSoup(response.get_data(as_text=True), 'html.parser')
        option = page.select_one(f'#trafficViewUser option[value="{username}"]')
        self.assertIsNotNone(option)
        self.assertIn('王小明', option.get_text())

        response = self.client.get('/', query_string={'view_user': username})
        self.assertEqual(response.status_code, 200)
        page = BeautifulSoup(response.get_data(as_text=True), 'html.parser')
        self.assertIn(username, page.select_one('.readonly-banner').get_text())
        response = self.client.get('/api/cache', query_string={'view_user': username, 'include_cache': '1'})
        self.assertTrue(response.json['readonly_view'])
        self.assertEqual(response.json['cache']['result']['courses'][0]['title'], '【115上】Session Course')
        self.assertEqual(self.client.post('/preferences', query_string={'view_user': username}, json={'view_mode': 'course'}).status_code, 403)
        self.assertEqual(self.client.post('/api/assignments', query_string={'view_user': username}, json={}).status_code, 403)

    def test_regular_user_cannot_read_other_session_account_cache(self):
        username = 'Session-673ffeaeac'
        self.storage.save_user_cache(username, {
            'ts': 1, 'result': {'courses': [
                {'id': 101, 'title': 'Private Session Course', 'assignments': []},
            ], 'all_assignments': []},
        })
        response = self.client.get('/', query_string={'view_user': username})
        self.assertEqual(response.status_code, 200)
        self.assertNotIn('Private Session Course', response.get_data(as_text=True))
        response = self.client.get('/api/cache', query_string={'view_user': username, 'include_cache': '1'})
        self.assertFalse(response.json['readonly_view'])
        self.assertNotIn('Private Session Course', response.get_data(as_text=True))

    def test_full_name_upgrade_preserves_surname_and_sessions(self):
        self.storage.save_user_surname('student', '王')
        with self.storage._engine.begin() as conn:
            conn.execute(text('ALTER TABLE users DROP COLUMN profile_name'))
            conn.execute(text("DELETE FROM e3_schema_migrations WHERE version='0005_user_profile_name'"))
        self.assertEqual(migrations.run_migrations(self.storage._engine), ['0005_user_profile_name'])
        self.assertEqual(migrations.run_migrations(self.storage._engine), [])
        self.assertEqual(self.storage.load_user_profile('student'), {'name': '', 'surname': '王'})
        self.assertTrue(self.storage.is_valid_web_session('profile-test', 'student'))

    def test_profile_write_is_atomic_and_invalid_values_do_not_erase_name(self):
        self.storage.save_user_profile('student', '王小明', '王')
        for name, surname in [('', '王'), ('x' * 129, '王'), ('李小明', ''), ('李小明', 'x' * 17)]:
            self.storage.save_user_profile('student', name, surname)
        self.assertEqual(self.storage.load_user_profile('student'), {'name': '王小明', 'surname': '王'})

    def test_additive_upgrade_preserves_existing_users_and_is_idempotent(self):
        with self.storage._engine.begin() as conn:
            conn.execute(text('ALTER TABLE users DROP COLUMN profile_surname'))
            conn.execute(text("DELETE FROM e3_schema_migrations WHERE version='0003_user_profile'"))
        self.assertEqual(migrations.run_migrations(self.storage._engine), ['0003_user_profile'])
        self.assertEqual(migrations.run_migrations(self.storage._engine), [])
        self.assertIn('profile_surname', {column['name'] for column in inspect(self.storage._engine).get_columns('users')})
        self.assertTrue(self.storage.is_valid_web_session('profile-test', 'student'))
        self.storage.save_user_surname('student', '王')
        self.assertEqual(self.storage.load_user_surname('student'), '王')


if __name__ == '__main__':
    unittest.main()
