"""Public extension download contains the current reviewed sources only."""
import io
import json
import unittest
from zipfile import ZipFile
from PIL import Image

from flask import Flask
from bs4 import BeautifulSoup
from e3_tracker.platform.assets import configure_frontend, register_frontend_assets
from e3_tracker.platform.paths import FRONTEND_ROOT
from e3_tracker.assignments.routes.dashboard import register_dashboard_routes, EXTENSION_FILES
from e3_tracker.assignments.services.e3_navigation_extension import build_extension_archive, official_store_url


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
        self.app = app

    def test_privacy_page_is_available_and_linked(self):
        page = BeautifulSoup(self.client.get('/e3-auto-navigation').data, 'html.parser')
        self.assertIsNotNone(page.select_one('a[href="/e3-auto-navigation/privacy"]'))
        response = self.client.get('/e3-auto-navigation/privacy')
        self.assertEqual(response.status_code, 200)
        self.assertIn('Cookie', response.get_data(as_text=True))

    def test_only_valid_official_store_listings_are_displayed(self):
        chrome = 'https://chromewebstore.google.com/detail/e3-tool/' + 'a' * 32
        edge = 'https://microsoftedge.microsoft.com/addons/detail/e3-tool/' + 'b' * 32
        self.app.config.update(E3_CHROME_EXTENSION_URL=chrome, E3_EDGE_EXTENSION_URL=edge)
        page = BeautifulSoup(self.client.get('/e3-auto-navigation').data, 'html.parser')
        self.assertIsNotNone(page.find('a', href=chrome))
        self.assertIsNotNone(page.find('a', href=edge))
        for store in ('chrome', 'edge'):
            for value in ('https://evil.example/listing', 'javascript:alert(1)', chrome + '?redirect=evil', ''):
                self.assertEqual(official_store_url(value, store), '')

    def test_store_package_uses_root_manifest_and_complete_icon_sizes(self):
        with ZipFile(build_extension_archive(for_store=True)) as package:
            self.assertEqual(set(package.namelist()), set(EXTENSION_FILES))
            manifest = json.loads(package.read('manifest.json'))
            for size, filename in manifest['icons'].items():
                with Image.open(io.BytesIO(package.read(filename))) as icon:
                    self.assertEqual(icon.size, (int(size), int(size)))

    def test_public_install_page_links_to_a_working_download(self):
        response = self.client.get('/e3-auto-navigation')
        self.assertEqual(response.status_code, 200)
        page = BeautifulSoup(response.data, 'html.parser')
        self.assertIsNotNone(page.select_one('a[href="/e3-auto-navigation/download"]'))
        self.assertIn('載入未封裝項目', page.get_text())
        self.assertIn('手機 Chrome', page.get_text())
        self.assertIsNone(page.select_one('#e3NavigationRecovery'))

    def test_guide_has_complete_steps_browser_controls_and_local_assets(self):
        page = BeautifulSoup(self.client.get('/e3-auto-navigation').data, 'html.parser')
        self.assertEqual(len(page.select('.install-steps > li')), 4)
        self.assertEqual({field['value'] for field in page.select('[name="install_browser"]')}, {'chrome', 'edge'})
        self.assertEqual(page.select_one('#extensionManagerUrl')['value'], 'chrome://extensions')
        self.assertIsNotNone(page.select_one('#copyManagerUrl'))
        self.assertIsNotNone(page.select_one('#guideTheme'))
        self.assertEqual(len(page.select('.guide-faq details')), 5)
        source = FRONTEND_ROOT / 'assignments/static/e3-navigation-extension'
        manifest = json.loads((source / 'manifest.json').read_text(encoding='utf-8'))
        self.assertIn('v' + manifest['version'], page.get_text())
        for node in page.select('script[src], link[rel="stylesheet"], img[src]'):
            url = node.get('src') or node['href']
            response = self.client.get(url)
            self.assertEqual(response.status_code, 200, url)
            response.close()
        self.assertIsNone(page.select_one('a[href^="chrome://"], a[href^="edge://"]'))

    def test_offline_guide_is_complete_and_checked_in_copy_matches_the_download(self):
        source = FRONTEND_ROOT / 'assignments/static/e3-navigation-extension'
        checked_in = FRONTEND_ROOT.parent / 'e3-auto-navigation'
        page = BeautifulSoup((source / 'README.html').read_text(encoding='utf-8'), 'html.parser')
        self.assertEqual(len(page.select('.install-steps > li')), 4)
        self.assertEqual(page.select_one('link[rel="stylesheet"]')['href'], 'guide.css')
        self.assertIsNotNone(page.select_one('a[href="https://www.e3hwtool.space/e3-auto-navigation"]'))
        for name in EXTENSION_FILES:
            self.assertEqual((checked_in / name).read_bytes(), (source / name).read_bytes(), name)

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
