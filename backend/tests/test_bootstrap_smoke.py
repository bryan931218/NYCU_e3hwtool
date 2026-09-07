"""Exercise runtime installation in a separate process to isolate wrappers."""

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

with tempfile.TemporaryDirectory() as directory:
    os.environ.update(E3_CACHE_DIR=directory, E3_DATABASE_URL='',
                      E3_SESSION_COOKIE_SECURE='0', E3_CANONICAL_HOST='',
                      OPENAI_API_KEY='')
    from e3_tracker.bootstrap import create_app
    app = create_app()
    storage = app.extensions['e3_storage']
    try:
        client = app.test_client()
        assert client.get('/admin/study-home').status_code in (302, 303)
        storage.save_web_session('bootstrap-smoke', 'test-admin')
        with client.session_transaction() as session:
            session.update(username='test-admin',
                           session_token='bootstrap-smoke', is_admin=True)
        for route in ('/healthz', '/admin/study-home', '/admin/study-plan',
                      '/admin/study-recall', '/admin/study-plan/markers',
                      '/admin/study-player-settings'):
            response = client.get(route)
            assert response.status_code == 200, (route, response.status_code)
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
