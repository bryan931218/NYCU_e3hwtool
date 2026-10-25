"""Render the current monitoring page against temporary data for browser checks."""
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from e3_tracker.platform.application import create_app
from e3_tracker.platform.constants import TAIPEI_TZ

with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {
    "E3_CACHE_DIR": folder, "E3_DATABASE_URL": "", "E3_SESSION_COOKIE_SECURE": "0", "E3_CANONICAL_HOST": "",
    "E3_GA4_MEASUREMENT_ID": "G-TEST1234" if "--ga4" in sys.argv else "",
    "E3_GA4_PROPERTY_ID": "123" if "--ga4" in sys.argv else "",
    "E3_GA4_SERVICE_ACCOUNT_JSON": "fixture-only" if "--ga4" in sys.argv else "",
}):
    app = create_app()
    storage = app.extensions["e3_storage"]
    try:
        client = app.test_client()
        if "--landing" in sys.argv or "--privacy" in sys.argv:
            if "--denied" in sys.argv:
                from itsdangerous import URLSafeTimedSerializer
                from e3_tracker.platform.analytics_integration import CONSENT_COOKIE
                client.set_cookie(CONSENT_COOKIE, URLSafeTimedSerializer(app.secret_key, salt="e3-analytics-consent").dumps("denied"))
            print(client.get("/privacy" if "--privacy" in sys.argv else "/login").get_data(as_text=True))
            sys.exit(0)
        if "--empty" not in sys.argv:
            for source in ("dcard", "dcard", "dcard", "google", "google", "line"):
                client.get("/login?utm_source=" + source)
            client.get("/privacy")
            client.get("/terms", headers={"Referer": "http://localhost/privacy"})
            client.get("/privacy?token=never-store", headers={"Referer": "https://unknown.example/?query=never-store"})
        storage.save_web_session("traffic-preview-token", "traffic-preview", is_admin=True)
        with client.session_transaction() as session:
            session.update(username="traffic-preview", session_token="traffic-preview-token")
        if '--active-users' in sys.argv:
            from sqlalchemy import update
            from e3_tracker.platform.persistence.migrations import history
            for name in ('112550101', '113550102', '114550103', 'Session-alice'):
                storage.save_user_profile(name, 'Synthetic Student', 'S')
            storage.save_student_number('Session-alice', '112550101')
            now = time.time()
            with storage._engine.begin() as conn:
                conn.execute(update(history).where(history.c.version == '0017_traffic_activity_retention').values(
                    applied_at=(datetime.now(TAIPEI_TZ)-timedelta(days=30)).isoformat()))
            for name, age in [('112550101',1),('Session-alice',0),('113550102',1),('114550103',0)]:
                storage.record_assignment_usage(name, 'heartbeat', now=now-age*86400)
            storage.claim_assignment_membership('114550103', now=now)
        target = '/admin/ga4' if '--ga4-page' in sys.argv else '/admin/traffic'
        query = {'range':'30d' if '--30d' in sys.argv else '7d'}
        for key in ('range', 'trend', 'start', 'end'):
            argument = next((arg for arg in sys.argv if arg.startswith('--' + key + '=')), None)
            if argument:
                query[key] = argument.split('=', 1)[1]
        print(client.get(target, query_string=query).get_data(as_text=True))
    finally:
        storage._engine.dispose()
