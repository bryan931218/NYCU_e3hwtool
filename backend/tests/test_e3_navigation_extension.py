"""Public extension download contains the current reviewed sources only."""
import io
import json
import unittest
from zipfile import ZipFile

from flask import Flask
from bs4 import BeautifulSoup
from e3_tracker.platform.assets import configure_frontend, register_frontend_assets
from e3_tracker.platform.paths import FRONTEND_ROOT
from e3_tracker.assignments.routes.dashboard import register_dashboard_routes, EXTENSION_FILES


class E3NavigationExtensionTests(unittest.TestCase):
    def setUp(self):
        app = Flask(__name__)
        configure_frontend(app)
        register_frontend_assets(app)
        register_dashboard_routes(app=app, current_user=lambda: None,
            HOME_TEMPLATE='home', WEB_TEMPLATE='dashboard', _build_dashboard_context=lambda _: {},
            usage_stats=lambda: {}, current_stats_version=lambda: 0,
            app_home_url='', support_email='')
        self.client = app.test_client()

    def test_public_install_page_links_to_a_working_download(self):
        response = self.client.get('/e3-auto-navigation')
        self.assertEqual(response.status_code, 200)
        page = BeautifulSoup(response.data, 'html.parser')
        self.assertIsNotNone(page.select_one('a[href="/e3-auto-navigation/download"]'))
        self.assertIn('載入未封裝項目', page.get_text())
        self.assertIn('手機 Chrome', page.get_text())
        self.assertIsNone(page.select_one('#e3NavigationRecovery'))

    def test_zip_includes_exact_current_sources_and_minimal_site_permissions(self):
        response = self.client.get('/e3-auto-navigation/download')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, 'application/zip')
        self.assertIn('attachment', response.headers['Content-Disposition'])
        with ZipFile(io.BytesIO(response.data)) as package:
            self.assertEqual(set(package.namelist()), {'e3-auto-navigation/' + name for name in EXTENSION_FILES})
            source = FRONTEND_ROOT / 'assignments/static/e3-navigation-extension'
            for name in EXTENSION_FILES:
                self.assertEqual(package.read('e3-auto-navigation/' + name), (source / name).read_bytes())
            manifest = json.loads(package.read('e3-auto-navigation/manifest.json'))
        self.assertEqual(manifest['manifest_version'], 3)
        self.assertEqual(manifest['permissions'], ['storage'])
        self.assertEqual(manifest['host_permissions'], ['https://e3p.nycu.edu.tw/*'])
        self.assertEqual({match for content in manifest['content_scripts'] for match in content['matches']},
            {'https://e3p.nycu.edu.tw/*', 'https://www.e3hwtool.space/*', 'https://e3hwtool.space/*'})
        response.close()
