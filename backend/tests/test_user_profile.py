import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import requests
from sqlalchemy import inspect, text

from e3_tracker.platform.application import create_app
from e3_tracker.assignments.services.profile import fetch_profile_surname, parse_profile_surname
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
        with patch('e3_tracker.assignments.routes.assignments.fetch_profile_surname', return_value='王') as fetch:
            self.assertEqual(self.client.get('/api/profile').json, {'ok': True, 'surname': '王'})
            self.assertEqual(self.client.get('/api/profile').json['surname'], '王')
            self.assertEqual(fetch.call_count, 1)
        self.assertEqual(self.storage.load_user_surname('student'), '王')
        self.assertEqual(self.storage.load_user_surname('someone-else'), '')
        self.assertIn('王', self.client.get('/').get_data(as_text=True))

    def test_refresh_updates_name_without_affecting_other_accounts(self):
        self.storage.save_user_surname('student', '王')
        self.storage.save_user_surname('someone-else', '李')
        with patch('e3_tracker.assignments.routes.assignments.fetch_profile_surname', return_value='歐陽'):
            self.assertEqual(self.client.get('/api/profile?refresh=1&username=someone-else').json['surname'], '歐陽')
        self.assertEqual(self.storage.load_user_surname('someone-else'), '李')

    def test_profile_failure_preserves_saved_surname(self):
        self.storage.save_user_surname('student', '王')
        for result in ('', requests.Timeout('unavailable')):
            with self.subTest(result=result), patch('e3_tracker.assignments.routes.assignments.fetch_profile_surname') as fetch:
                if isinstance(result, Exception):
                    fetch.side_effect = result
                else:
                    fetch.return_value = result
                self.assertEqual(self.client.get('/api/profile?refresh=1').json['surname'], '王')

    def test_guests_and_unauthenticated_requests_do_not_fetch_e3(self):
        with patch('e3_tracker.assignments.routes.assignments.fetch_profile_surname') as fetch:
            self.storage.save_web_session('profile-test', 'student', is_guest=True)
            self.assertEqual(self.client.get('/api/profile').json['surname'], '')
            with self.client.session_transaction() as session:
                session.clear()
            self.assertEqual(self.client.get('/api/profile').status_code, 302)
            fetch.assert_not_called()

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
