"""Best-effort student-number enrichment; never merges accounts or grants roles."""

import logging
import os
import threading
import time
from collections import deque

import requests

from e3_tracker.assignments.services.http import apply_cookie
from e3_tracker.assignments.services.profile import fetch_session_identity, profile_surname
from e3_tracker.platform.security import production_mode
from e3_tracker.platform.guest_privacy import is_guest_identity

logger = logging.getLogger(__name__)


class SessionIdentitySync:
    def __init__(self, storage, base_url, timeout=8):
        self.storage = storage
        self.base_url = base_url
        self.timeout = min(timeout, 8)
        self.stop = threading.Event()
        self._wake = threading.Event()
        self._pending = deque(maxlen=100)
        self._pending_lock = threading.Lock()

    def schedule_user(self, username):
        """Wake the bounded worker after login; keep credentials out of the queue."""
        if not username or is_guest_identity(username) or self.stop.is_set():
            return
        with self._pending_lock:
            if username not in self._pending:
                self._pending.append(username)
        self._wake.set()

    def refresh_user(self, user, *, force=False):
        username = str(user.get("username") or "")
        if user.get("is_guest") or is_guest_identity(username) or not username or not user.get("moodle_session"):
            return
        claimed = False
        saved = False
        diagnostics, details = {}, {}
        try:
            if not self.storage.claim_student_number_sync(username, time.time(), force=force):
                return
            claimed = True
            needs_number = username.startswith('Session-') and not self.storage.load_student_number(username)
            with requests.Session() as sess:
                apply_cookie(sess, self.base_url, user["moodle_session"])
                identity = fetch_session_identity(sess, self.base_url, timeout=self.timeout, diagnostics=diagnostics,
                                                  include_number=needs_number, profile_metadata=details)
            number = identity["student_number"]
            name = identity["name"]
            saved = self.storage.save_identity_enrichment(username, name, profile_surname(name), number)
            if number and not saved:
                details.clear()
                diagnostics['reason'] = 'identity_conflict'
            elif not name and diagnostics.get('reason') == 'success':
                diagnostics['reason'] = 'name_missing'
            if saved and (number or not needs_number):
                return True
            reason = "identity_conflict" if number and not saved else diagnostics.get("reason", "identity_missing")
            logger.warning("E3 Session identity unavailable (reason=%s); will retry later", reason)
            return bool(saved)
        except requests.Timeout:
            diagnostics['reason'] = 'upstream_timeout'
            logger.warning("E3 Session identity unavailable (reason=upstream_timeout); will retry later")
        except requests.RequestException:
            diagnostics['reason'] = 'upstream_request'
            logger.warning("E3 Session identity unavailable (reason=upstream_request); will retry later")
        except Exception:
            diagnostics['reason'] = 'internal_error'
            # Upstream exception text can contain cookies or personal data.
            logger.warning("E3 Session identity unavailable (reason=internal_error); will retry later")
        finally:
            if claimed:
                try:
                    self.storage.save_profile_metadata(username, {**details, 'checked_at':time.time(),
                        'reason':diagnostics.get('reason', 'success' if saved else 'identity_missing')})
                except Exception:
                    logger.warning('E3 profile diagnostics could not be saved')
        return False

    def backfill_once(self):
        now = time.time()
        checked = updated = 0
        with self._pending_lock:
            queued = [self._pending.popleft() for _ in range(min(10, len(self._pending)))]
        names = queued + [name for name in self.storage.list_session_identity_users(now) if name not in queued]
        for username in names[:10]:
            try:
                user = self.storage.load_session_identity_user(username, now)
                if user:
                    checked += 1
                    updated += bool(self.refresh_user(user, force=username in queued))
                else:
                    # Unusable encrypted sessions must not monopolize every batch.
                    self.storage.claim_student_number_sync(username, now)
            except Exception:
                logger.warning("E3 Session identity backfill will retry later")
        if checked:
            logger.info("E3 Session identity backfill checked=%d updated=%d", checked, updated)

    def start(self, app):
        enabled = os.getenv("E3_SESSION_PROFILE_WORKER", "1" if production_mode() else "0")
        if enabled.lower() not in {"1", "true", "on"}:
            return

        def run():
            while not self.stop.is_set():
                self._wake.wait(60)
                self._wake.clear()
                if self.stop.is_set():
                    break
                try:
                    with app.app_context():
                        self.backfill_once()
                except Exception:
                    logger.warning("E3 Session identity worker will retry later")
                with self._pending_lock:
                    if self._pending:
                        self._wake.set()

        threading.Thread(target=run, name="assignment-session-identity", daemon=True).start()
