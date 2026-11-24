"""Isolated server-browser login through NYCU's ordinary SSO page.

No personal browser profile, copied session, CAPTCHA workaround, trace, or
password persistence is used. NYCU's own page performs its normal checks.
"""
import hashlib
import logging
import os
import queue
import re
import shutil
import threading
import time
from contextlib import ExitStack
from urllib.parse import urlsplit

import requests

from e3_tracker.assignments.services.http import _authenticated_principal, apply_cookie, safe_request
from e3_tracker.platform.constants import HEADERS

logger = logging.getLogger(__name__)
_NETWORK_HOSTS = frozenset({"portal.nycu.edu.tw", "e3p.nycu.edu.tw", "www.google.com",
    "www.recaptcha.net", "www.gstatic.com", "fonts.googleapis.com", "fonts.gstatic.com"})


def allowed_browser_url(url):
    try:
        target = urlsplit(url)
        return target.scheme == "https" and target.hostname in _NETWORK_HOSTS and not target.username and not target.password and target.port in {None, 443}
    except ValueError:
        return False


def verified_cookie(cookie, user_id, timeout=8):
    if not isinstance(cookie, str) or not re.fullmatch(r"[A-Za-z0-9_-]{16,256}", cookie):
        return False
    base = "https://e3p.nycu.edu.tw"
    with requests.Session() as sess:
        apply_cookie(sess, base, cookie)
        response = safe_request(sess, "GET", base + "/my/", headers=HEADERS, timeout=timeout, allow_redirects=False)
        for _ in range(4):
            if response.status_code not in {301, 302, 303}:
                break
            from urllib.parse import urljoin
            location = urljoin(response.url, response.headers.get("Location", ""))
            target = urlsplit(location)
            if target.scheme != "https" or target.netloc != "e3p.nycu.edu.tw" or target.username or target.password:
                return False
            response = safe_request(sess, "GET", location, headers=HEADERS, timeout=timeout, allow_redirects=False)
        return _authenticated_principal(response, base) == str(user_id)


class ServerSso:
    def __init__(self, storage, app, on_connected, admin_user_id=None):
        self.storage, self.app, self.on_connected = storage, app, on_connected
        self.admin_user_id = admin_user_id
        self._lock = threading.Lock()
        self._capacity = threading.BoundedSemaphore(1)
        self._slots = threading.BoundedSemaphore(3)
        self._jobs = {}

    @staticmethod
    def _key(session_token):
        return hashlib.sha256(session_token.encode()).hexdigest()

    def enabled(self, username):
        # The first live test is restricted to the configured administrator.
        enabled = os.getenv("E3_SERVER_SSO_COUNTS", "").lower()
        return enabled == "all" or enabled == "admin" and username == self.admin_user_id

    def status(self, session_token):
        with self._lock:
            job = self._jobs.get(self._key(session_token)) if session_token else None
            if not job or time.monotonic() - job["started"] > 900:
                return {"state": "idle"}
            return {"state": job["state"], "attempts": job["attempts"]}

    def verify(self, session_token, code):
        if not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}", code):
            return False
        with self._lock:
            job = self._jobs.get(self._key(session_token))
            if not job or job["state"] != "otp_required" or time.monotonic() - job["started"] > 600 or job["attempts"] >= 3:
                return False
            try:
                job["codes"].put_nowait(code)
            except queue.Full:
                return False
            job["state"] = "verifying"
            return True

    def cancel(self, session_token):
        with self._lock:
            job = self._jobs.get(self._key(session_token)) if session_token else None
            if job:
                job["state"] = "cancelled"
                try:
                    job["codes"].put_nowait(None)
                except queue.Full:
                    pass

    def start(self, session_token, username, password, user_id):
        if (not self.enabled(username) or not isinstance(session_token, str) or not session_token or not isinstance(password, str) or not password
                or type(user_id) is not int or user_id <= 0):
            return False
        key = self._key(session_token)
        admitted = self._slots.acquire(blocking=False)
        job = {"started": time.monotonic(), "state": "queued" if admitted else "busy", "attempts": 0, "codes": queue.Queue(maxsize=1)}
        with self._lock:
            self._jobs = {k: v for k, v in self._jobs.items() if time.monotonic() - v["started"] < 900}
            if len(self._jobs) >= 128:
                inactive = [k for k, v in self._jobs.items() if v["state"] in {"idle", "busy", "complete", "cancelled", "unavailable", "expired"}]
                if inactive:
                    self._jobs.pop(min(inactive, key=lambda old: self._jobs[old]["started"]), None)
            self._jobs[key] = job
        if not admitted:
            return False
        secret = {"password": password}
        try:
            threading.Thread(target=self._run, args=(job, session_token, username, secret, user_id), daemon=True).start()
        except RuntimeError:
            secret.clear()
            self._state(job, "unavailable")
            self._slots.release()
            return False
        return True

    def preflight(self):
        """Check the deployed browser on the public login page, without signing in."""
        if not self._capacity.acquire(blocking=False):
            return "busy"
        try:
            from playwright.sync_api import sync_playwright
            with ExitStack() as cleanup:
                runtime = cleanup.enter_context(sync_playwright())
                executable = os.getenv("E3_CRAWLER_CHROMIUM") or shutil.which("chromium") or shutil.which("chromium-browser")
                browser = runtime.chromium.launch(headless=True, **({"executable_path": executable} if executable else {}))
                cleanup.callback(browser.close)
                context = browser.new_context(accept_downloads=False, service_workers="block")
                context.route("**/*", lambda route: route.continue_() if allowed_browser_url(route.request.url) else route.abort())
                page = context.new_page()
                page.goto("https://portal.nycu.edu.tw/portal/newe3p", wait_until="domcontentloaded", timeout=30000)
                page.get_by_role("button", name="帳號登入", exact=True).wait_for(state="visible", timeout=15000)
                page.wait_for_function("() => Boolean(window.grecaptcha && window.grecaptcha.execute)", timeout=15000)
                return "ready"
        except Exception:
            return "unavailable"
        finally:
            self._capacity.release()

    def _state(self, job, state):
        with self._lock:
            job["state"] = state

    def _run(self, job, session_token, username, secret, user_id):
        password = secret.pop("password", None)
        browser = None
        acquired = False
        try:
            queue_deadline = time.monotonic() + 90
            while not acquired and time.monotonic() < queue_deadline and job["state"] != "cancelled":
                acquired = self._capacity.acquire(timeout=1)
            if not acquired:
                self._state(job, "cancelled" if job["state"] == "cancelled" else "busy")
                return
            if job["state"] == "cancelled" or not self.storage.is_valid_web_session(session_token, username):
                self._state(job, "cancelled")
                return
            self._state(job, "starting")
            from playwright.sync_api import sync_playwright
            with ExitStack() as cleanup:
                runtime = cleanup.enter_context(sync_playwright())
                executable = os.getenv("E3_CRAWLER_CHROMIUM") or shutil.which("chromium") or shutil.which("chromium-browser")
                browser = runtime.chromium.launch(headless=True, **({"executable_path": executable} if executable else {}))
                cleanup.callback(browser.close)
                context = browser.new_context(accept_downloads=False, service_workers="block")
                context.route("**/*", lambda route: route.continue_() if allowed_browser_url(route.request.url) else route.abort())
                page = context.new_page()
                page.set_default_timeout(10000)
                page.goto("https://portal.nycu.edu.tw/portal/newe3p", wait_until="domcontentloaded", timeout=30000)
                page.get_by_role("textbox", name="帳號", exact=True).fill(username)
                page.locator('input[type="password"]').first.fill(password)
                password = None
                self._state(job, "verifying")
                page.get_by_role("button", name="帳號登入", exact=True).click()
                deadline = time.monotonic() + 600
                checked_cookie = None
                checked_at = 0
                action_at = time.monotonic()
                while time.monotonic() < deadline:
                    if not self.storage.is_valid_web_session(session_token, username):
                        self._state(job, "cancelled")
                        return
                    cookies = context.cookies("https://e3p.nycu.edu.tw/")
                    cookie = next((item["value"] for item in cookies if item["name"] == "MoodleSession"), None)
                    can_check = cookie and (cookie != checked_cookie or time.monotonic() - checked_at >= 8) and any(
                        urlsplit(open_page.url).hostname == "e3p.nycu.edu.tw" and not urlsplit(open_page.url).path.startswith("/login/")
                        for open_page in context.pages)
                    authenticated = False
                    if can_check:
                        checked_cookie = cookie
                        checked_at = time.monotonic()
                        authenticated = verified_cookie(cookie, user_id)
                    if authenticated:
                        if not self.storage.attach_moodle_web_session(session_token, username, user_id, cookie):
                            self._state(job, "cancelled")
                            return
                        with self.app.app_context():
                            self.on_connected(username, session_token)
                        self._state(job, "complete")
                        return
                    otp = page.locator('.el-message-box__input input')
                    if otp.count() and otp.first.is_visible():
                        if job["attempts"] >= 3:
                            self._state(job, "unavailable")
                            return
                        self._state(job, "otp_required")
                        try:
                            code = job["codes"].get(timeout=min(180, max(1, deadline - time.monotonic())))
                        except queue.Empty:
                            self._state(job, "expired")
                            return
                        if code is None or job["state"] == "cancelled":
                            self._state(job, "cancelled")
                            return
                        job["attempts"] += 1
                        otp.first.fill(code)
                        code = None
                        page.locator('.el-message-box').get_by_role("button", name="確定", exact=True).click()
                        otp.first.wait_for(state="hidden", timeout=10000)
                        action_at = time.monotonic()
                        self._state(job, "verifying")
                    else:
                        page.wait_for_timeout(500)
                    # A failed ordinary login must not remain running indefinitely.
                    if time.monotonic() - action_at > 90 and job["state"] == "verifying":
                        self._state(job, "unavailable")
                        return
                self._state(job, "expired")
        except Exception:
            # Browser exception strings can contain passwords, tokens and URLs.
            logger.warning("NYCU server SSO could not complete")
            self._state(job, "unavailable")
        finally:
            password = None
            if acquired:
                self._capacity.release()
            self._slots.release()
