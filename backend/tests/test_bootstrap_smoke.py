"""Exercise application assembly separately from legacy compatibility tests."""

import os
from pathlib import Path
import subprocess
import sys
import unittest


class BootstrapSmokeTests(unittest.TestCase):
    def test_complete_application_renders_admin_pages(self):
        code = """
import os
import tempfile
from bs4 import BeautifulSoup

with tempfile.TemporaryDirectory() as directory:
    os.environ.update(E3_CACHE_DIR=directory, E3_DATABASE_URL='',
                      E3_SESSION_COOKIE_SECURE='0', E3_CANONICAL_HOST='',
                      OPENAI_API_KEY='')
    from e3_tracker.api import web
    original_factory = web.create_app
    original_schedule = web._study_plan_schedule_definitions
    original_storage = web.PersistentStorage
    from e3_tracker.bootstrap import create_app
    from e3_tracker.api.features import register_application_features
    app = create_app()
    assert web.create_app is original_factory
    assert web._study_plan_schedule_definitions is original_schedule
    assert web.PersistentStorage is original_storage
    routes_before = len(app.url_map._rules)
    register_application_features(app)
    assert len(app.url_map._rules) == routes_before
    another_app = create_app()
    assert another_app.extensions['e3_storage'] is not app.extensions['e3_storage']
    another_app.extensions['e3_storage']._engine.dispose()
    storage = app.extensions['e3_storage']
    try:
        client = app.test_client()
        assert client.get('/admin/study-home').status_code in (302, 303)
        assert client.get('/admin/study-recall/favorites.json').status_code == 401
        assert client.get('/admin/study-calendar/time-summary.json?start=2026-09-01&end=2026-09-30').status_code == 401
        storage.save_web_session('bootstrap-smoke', 'test-admin')
        with client.session_transaction() as session:
            session.update(username='test-admin',
                           session_token='bootstrap-smoke', is_admin=True)
        for route in ('/healthz', '/admin/study-home', '/admin/study-plan',
                      '/admin/study-recall', '/admin/study-plan/markers',
                      '/admin/study-player-settings'):
            response = client.get(route)
            assert response.status_code == 200, (route, response.status_code)
            if route.startswith('/admin/study-') and route != '/admin/study-player-settings':
                html = response.get_data(as_text=True)
                assert len(BeautifulSoup(html, 'html.parser').select('[data-e3-global-ai]')) <= 1, route
        for route in ('/assets/css/tokens.css', '/assets/css/components.css',
                      '/assets/css/study-center.css', '/assets/js/workbench/index.js'):
            assert client.get(route).status_code == 200, route
        assert client.get('/assets/../templates/web.html').status_code == 404
        assert client.get('/admin/study-recall/favorites.json').status_code == 200
        assert client.get('/admin/study-calendar/time-summary.json?start=2026-09-01&end=2026-09-30').status_code == 200
        assert client.get('/admin/study-calendar/time-summary.json?start=2026-09-30&end=2026-09-01').status_code == 400
        with client.session_transaction() as session:
            session['is_admin'] = False
        assert client.get('/admin/study-player-settings').status_code == 403
        assert client.get('/admin/study-recall/favorites.json').status_code == 401
        assert client.get('/admin/study-calendar/time-summary.json?start=2026-09-01&end=2026-09-30').status_code == 403
    finally:
        storage._engine.dispose()
"""
        result = subprocess.run(
            [sys.executable, '-c', code],
            cwd=Path(__file__).resolve().parents[1],
            env={**os.environ, 'PYTHONIOENCODING': 'utf-8'},
            capture_output=True, text=True, encoding='utf-8', timeout=90,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
