"""Best-effort student-number enrichment; never merges accounts or grants roles."""

import logging
import os
import threading
import time

import requests

from e3_tracker.assignments.services.http import apply_cookie
from e3_tracker.assignments.services.profile import fetch_session_identity, profile_surname
from e3_tracker.platform.security import production_mode

logger = logging.getLogger(__name__)


class SessionIdentitySync:
    def __init__(self, storage, base_url, timeout=8):
        self.storage = storage
        self.base_url = base_url
        self.timeout = min(timeout, 8)
        self.stop = threading.Event()

    def refresh_user(self, user, *, force=False):
        username = str(user.get("username") or "")
        if user.get("is_guest") or not username.startswith("Session-") or not user.get("moodle_session"):
            return
        try:
            if not self.storage.claim_student_number_sync(username, time.time(), force=force):
                return
            with requests.Session() as sess:
                apply_cookie(sess, self.base_url, user["moodle_session"])
                identity = fetch_session_identity(sess, self.base_url, timeout=self.timeout)
            number = identity["student_number"]
            if number and self.storage.save_student_number(username, number):
                name = identity["name"]
                if name:
                    self.storage.save_user_profile(username, name, profile_surname(name))
        except Exception:
            # Upstream exception text can contain cookies or personal data.
            logger.warning("E3 Session identity unavailable; will retry later")

    def backfill_once(self):
        now = time.time()
        for username in self.storage.list_session_identity_users(now):
            try:
                user = self.storage.load_session_identity_user(username, now)
                if user:
                    self.refresh_user(user)
            except Exception:
                logger.warning("E3 Session identity backfill will retry later")

    def start(self, app):
        enabled = os.getenv("E3_SESSION_PROFILE_WORKER", "1" if production_mode() else "0")
        if enabled.lower() not in {"1", "true", "on"}:
            return

        def run():
            while not self.stop.wait(60):
                try:
                    with app.app_context():
                        self.backfill_once()
                except Exception:
                    logger.warning("E3 Session identity worker will retry later")

        threading.Thread(target=run, name="assignment-session-identity", daemon=True).start()
