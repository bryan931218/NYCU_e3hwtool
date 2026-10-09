"""Render the current monitoring page against temporary data for browser checks."""
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from e3_tracker.platform.application import create_app

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
        if "--landing" in sys.argv:
            print(client.get("/login").get_data(as_text=True))
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
        print(client.get("/admin/traffic").get_data(as_text=True))
    finally:
        storage._engine.dispose()
