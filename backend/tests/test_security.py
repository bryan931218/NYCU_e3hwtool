"""Security regressions use normal clients with protections always enabled."""

import hashlib
import base64
import importlib.util
import io
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch
from itsdangerous import URLSafeTimedSerializer

from bs4 import BeautifulSoup
from cryptography.fernet import Fernet, InvalidToken
from PIL import Image
from sqlalchemy import text

from e3_tracker.bootstrap import create_app
from e3_tracker.platform.input_security import safe_link, validate_note_image
from e3_tracker.platform.security import CredentialCipher, signing_key
from e3_tracker.platform.storage import PersistentStorage


class SecurityTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(
            os.environ,
            {
                "E3_ENV": "development",
                "RAILWAY_ENVIRONMENT_ID": "",
                "RAILWAY_ENVIRONMENT_NAME": "",
                "E3_CACHE_DIR": self.directory.name,
                "E3_DATABASE_URL": "",
                "E3_WEB_SECRET": "",
                "E3_DATA_ENCRYPTION_KEY": "",
                "E3_DATA_ENCRYPTION_PREVIOUS_KEYS": "",
                "E3_SESSION_COOKIE_SECURE": "0",
                "E3_SESSION_COOKIE_SAMESITE": "Lax",
                "E3_CANONICAL_HOST": "",
                "E3_PROXY_HOPS": "0",
                "E3_TRUSTED_HOSTS": "",
                "E3_DEV_RELOAD": "0",
                "OPENAI_API_KEY": "",
                "E3_ADMIN_USER_ID": "test-admin",
            },
        )
        self.environment.start()
        self.app = create_app()
        self.storage = self.app.extensions["e3_storage"]
        self.client = self.app.test_client()

    def tearDown(self):
        self.storage._engine.dispose()
        self.environment.stop()
        self.directory.cleanup()

    def token(self, client=None, path="/login"):
        response = (client or self.client).get(path, base_url="http://localhost")
        return BeautifulSoup(response.data, "html.parser").select_one(
            'meta[name="csrf-token"]'
        )["content"]

    def post(self, path, *, client=None, token=None, headers=None, **kwargs):
        return (client or self.client).post(
            path,
            base_url="http://localhost",
            headers={
                "X-CSRFToken": token or self.token(client),
                **(headers or {}),
            },
            **kwargs,
        )

    def login(
        self,
        *,
        admin=False,
        guest=False,
        client=None,
        token="secure-test-session",
        username="student",
        moodle=None,
    ):
        self.storage.save_web_session(
            token, username, is_admin=admin, is_guest=guest, moodle_session=moodle
        )
        with (client or self.client).session_transaction() as cookie:
            cookie["session_token"] = token

    def test_missing_or_invalid_csrf_blocks_all_mutations(self):
        for path in (
            "/login",
            "/guest-login",
            "/feedback",
            "/preferences",
            "/logout",
            "/admin/study-player-settings",
            "/admin/study-recall/upload-staging",
        ):
            with self.subTest(path=path):
                response = self.client.post(path, base_url="http://localhost")
                self.assertEqual(response.status_code, 400)
                self.assertEqual(response.json["error"], "csrf_validation_failed")
        self.assertEqual(
            self.client.post(
                "/guest-login",
                headers={"X-CSRFToken": "invalid"},
                base_url="http://localhost",
            ).status_code,
            400,
        )

    def test_public_assets_revalidate_without_caching_authenticated_data(self):
        path = "/assets/assignments/vendor/fullcalendar-6.1.21.min.js"
        for authenticated in (False, True):
            with self.subTest(authenticated=authenticated):
                client = self.app.test_client()
                if authenticated:
                    self.login(client=client, token="asset-cache-test")
                asset = client.get(path)
                self.assertEqual(asset.status_code, 200)
                self.assertIn("public", asset.headers["Cache-Control"])
                self.assertIn("no-cache", asset.headers["Cache-Control"])
                self.assertIn("must-revalidate", asset.headers["Cache-Control"])
                self.assertNotIn("no-store", asset.headers["Cache-Control"])
                etag = asset.headers["ETag"]
                asset.close()
                unchanged = client.get(path, headers={"If-None-Match": etag})
                self.assertEqual(unchanged.status_code, 304)
                self.assertIn("public", unchanged.headers["Cache-Control"])
                unchanged.close()
                for private_path in ("/login", "/api/course-messages/unread", "/assets/assignments/../templates/web.html"):
                    response = client.get(private_path)
                    self.assertIn("no-store", response.headers["Cache-Control"])
                    self.assertNotIn("public", response.headers["Cache-Control"])


    def test_changed_public_asset_does_not_reuse_an_old_etag(self):
        with tempfile.TemporaryDirectory() as directory, patch("e3_tracker.platform.assets.FRONTEND_ROOT", Path(directory)):
            asset = Path(directory) / "assignments/static/js/fixture.js"
            asset.parent.mkdir(parents=True)
            asset.write_text("const version = 1;", encoding="utf-8")
            self.login()
            first = self.client.get("/assets/assignments/js/fixture.js")
            etag = first.headers["ETag"]
            first.close()
            asset.write_text("const version = 'new-release';", encoding="utf-8")
            updated = self.client.get("/assets/assignments/js/fixture.js", headers={"If-None-Match": etag})
            self.assertEqual(updated.status_code, 200)
            self.assertNotEqual(updated.headers["ETag"], etag)
            self.assertIn(b"new-release", updated.data)
            updated.close()

    def test_csrf_is_bound_to_the_browser_and_valid_guest_login_works(self):
        other = self.app.test_client()
        foreign_token = self.token(other)
        self.token()
        self.assertEqual(
            self.post("/guest-login", token=foreign_token).status_code, 400
        )
        self.assertEqual(self.post("/guest-login").status_code, 302)
        self.assertEqual(self.client.get("/session/status").status_code, 200)

    def test_cross_origin_is_rejected_even_with_valid_csrf(self):
        self.assertEqual(
            self.post(
                "/guest-login", headers={"Origin": "https://attacker.example"}
            ).status_code,
            403,
        )
        self.assertEqual(
            self.post(
                "/guest-login", headers={"Sec-Fetch-Site": "cross-site"}
            ).status_code,
            403,
        )

    def test_cookie_roles_and_username_cannot_escalate_or_switch_accounts(self):
        self.login(guest=True)
        with self.client.session_transaction() as cookie:
            cookie.update(
                is_admin=True,
                is_guest=False,
                username="112550103",
                moodle_session="injected",
            )
        for path in (
            "/admin/traffic",
            "/admin/study-home",
            "/admin/study-player-settings",
            "/admin/study-recall/favorites.json",
        ):
            self.assertNotEqual(self.client.get(path).status_code, 200, path)
        self.assertEqual(self.client.get("/session/status").json["username"], "student")
        self.assertEqual(self.client.get("/api/profile").json["surname"], "")

    def test_trusted_role_is_server_authoritative_and_revocable(self):
        self.login(admin=True)
        with self.client.session_transaction() as cookie:
            cookie["is_admin"] = False
        self.assertEqual(
            self.client.get("/admin/study-player-settings").status_code, 200
        )
        self.storage.clear_web_session("secure-test-session")
        self.assertEqual(
            self.client.get("/admin/study-player-settings").status_code, 302
        )

    def test_session_identifier_is_hashed_and_moodle_is_encrypted(self):
        self.login(moodle="PRIVATE_E3_CREDENTIAL")
        with self.client.session_transaction() as cookie:
            self.assertNotIn("moodle_session", cookie)
            self.assertNotIn("is_admin", cookie)
            self.assertNotIn("username", cookie)
        with self.storage._engine.connect() as conn:
            row = conn.execute(text("SELECT * FROM web_sessions")).mappings().one()
        self.assertEqual(
            row["session_token"], hashlib.sha256(b"secure-test-session").hexdigest()
        )
        self.assertNotIn("PRIVATE_E3_CREDENTIAL", row["moodle_credential"])
        self.assertTrue(row["moodle_credential"].startswith("enc:v1:"))
        self.assertEqual(
            self.storage.load_web_session("secure-test-session")["moodle_session"],
            "PRIVATE_E3_CREDENTIAL",
        )

    def test_expired_and_legacy_sessions_are_invalid(self):
        self.storage.save_web_session("expired", "student", lifetime=-1, is_admin=True)
        self.assertIsNone(self.storage.load_web_session("expired"))
        with self.storage._engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO web_sessions (session_token, username, created_at, updated_at, is_admin, is_guest, expires_at) VALUES ('legacy-token','student','old','old',1,0,0)"
                )
            )
        self.assertIsNone(self.storage.load_web_session("legacy-token"))

    def test_google_tokens_are_encrypted_and_round_trip(self):
        payload = {
            "access_token": "PRIVATE_ACCESS",
            "refresh_token": "PRIVATE_REFRESH",
            "expires_at": 12345,
        }
        self.storage.save_google_tokens("student", payload)
        with self.storage._engine.connect() as conn:
            row = conn.execute(
                text("SELECT access_token, refresh_token FROM google_tokens")
            ).one()
        self.assertNotIn("PRIVATE_ACCESS", row.access_token)
        self.assertNotIn("PRIVATE_REFRESH", row.refresh_token)
        self.assertEqual(
            self.storage.load_google_tokens("student")["refresh_token"],
            "PRIVATE_REFRESH",
        )

    def test_plaintext_google_credentials_migrate_without_deleting_user_data(self):
        self.storage.save_google_tokens(
            "student", {"access_token": "temporary", "refresh_token": "temporary"}
        )
        with self.storage._engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE google_tokens SET access_token='old-access',refresh_token='old-refresh'"
                )
            )
        self.storage.migrate_google_credentials()
        self.assertEqual(
            self.storage.load_google_tokens("student")["access_token"], "old-access"
        )
        self.storage.migrate_google_credentials()
        self.assertEqual(
            self.storage.load_google_tokens("student")["refresh_token"], "old-refresh"
        )

    def test_credential_context_cannot_be_swapped_between_users(self):
        cipher = self.storage._credential_cipher
        sealed = cipher.encrypt("secret", "moodle:student")
        with self.assertRaises(ValueError):
            cipher.decrypt(sealed, "moodle:other")
        self.assertEqual(
            cipher.decrypt(cipher.encrypt("secret", "moodle:測試"), "moodle:測試"),
            "secret",
        )

    def test_wrong_encryption_key_fails_without_overwriting_data(self):
        self.storage.save_google_tokens("student", {"access_token": "private"})
        with patch.dict(
            os.environ, {"E3_DATA_ENCRYPTION_KEY": Fernet.generate_key().decode()}
        ):
            with self.assertRaises(InvalidToken):
                PersistentStorage(str(Path(self.directory.name) / "e3_tracker.sqlite3"))
        self.assertEqual(
            self.storage.load_google_tokens("student")["access_token"], "private"
        )

    def test_local_signing_keys_are_persistent_and_not_a_public_default(self):
        self.assertGreaterEqual(len(self.app.secret_key), 32)
        self.assertEqual(signing_key(self.directory.name), self.app.secret_key)
        with tempfile.TemporaryDirectory() as other:
            self.assertNotEqual(signing_key(other), self.app.secret_key)

    def test_production_rejects_missing_or_weak_signing_keys(self):
        with patch.dict(os.environ, {"E3_ENV": "production", "E3_WEB_SECRET": ""}):
            with self.assertRaisesRegex(RuntimeError, "E3_WEB_SECRET"):
                signing_key(self.directory.name)
        for value in ("e3-web-secret", "x" * 48):
            with patch.dict(os.environ, {"E3_WEB_SECRET": value}):
                with self.assertRaises(RuntimeError):
                    signing_key(self.directory.name)
        with patch.dict(
            os.environ,
            {"E3_DATA_ENCRYPTION_KEY": base64.urlsafe_b64encode(b"x" * 32).decode()},
        ):
            with self.assertRaisesRegex(ValueError, "weak"):
                CredentialCipher(self.directory.name)

    def test_google_oauth_rejects_state_from_a_different_browser_attempt(self):
        with patch.dict(
            os.environ,
            {
                "E3_GOOGLE_CLIENT_ID": "test-client",
                "E3_GOOGLE_CLIENT_SECRET": "test-secret",
                "E3_GOOGLE_REDIRECT_URI": "http://localhost/google/callback",
            },
        ):
            app = create_app()
        client = app.test_client()
        self.login(client=client)
        signer = URLSafeTimedSerializer(app.secret_key, salt="google-calendar")
        first = signer.dumps({"nonce": "first-attempt"})
        other = signer.dumps({"nonce": "different-attempt"})
        try:
            with client.session_transaction() as cookie:
                cookie["google_auth_state"] = first
            with patch(
                "e3_tracker.assignments.routes.assignments.exchange_code_for_google_token"
            ) as exchange:
                self.assertEqual(
                    client.get(
                        "/google/callback",
                        query_string={"code": "unused", "state": other},
                    ).status_code,
                    302,
                )
                exchange.assert_not_called()
            with patch(
                "e3_tracker.assignments.routes.assignments.exchange_code_for_google_token",
                return_value={
                    "access_token": "test-access",
                    "refresh_token": "test-refresh",
                    "expires_in": 600,
                },
            ) as exchange:
                self.assertEqual(
                    client.get(
                        "/google/callback",
                        query_string={"code": "once", "state": first},
                    ).status_code,
                    302,
                )
                self.assertEqual(exchange.call_count, 1)
                client.get(
                    "/google/callback", query_string={"code": "replay", "state": first}
                )
                self.assertEqual(exchange.call_count, 1)
        finally:
            app.extensions["e3_storage"]._engine.dispose()

    def test_upload_staging_rejects_fake_images_and_accepts_valid_png(self):
        self.login(admin=True)
        csrf = self.token(path="/admin/study-home")
        data = {
            "image_index": "1",
            "total_images": "1",
            "note_image": (io.BytesIO(b"not-a-png"), "fake.png"),
        }
        response = self.post(
            "/admin/study-recall/upload-staging",
            token=csrf,
            data=data,
            headers={"X-E3-Study-Upload": "1"},
        )
        self.assertEqual(response.status_code, 400)
        stream = io.BytesIO()
        Image.new("RGB", (8, 8)).save(stream, format="PNG")
        stream.seek(0)
        response = self.post(
            "/admin/study-recall/upload-staging",
            token=csrf,
            data={
                "image_index": "1",
                "total_images": "1",
                "note_image": (stream, "valid.png"),
            },
            headers={"X-E3-Study-Upload": "1"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["uploaded_count"], 1)

    def test_development_proxy_cannot_target_an_external_host_or_trust_client_forwarding(
        self,
    ):
        path = Path(__file__).resolve().parents[2] / "frontend/server.py"
        spec = importlib.util.spec_from_file_location("security_proxy_fixture", path)
        proxy = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(proxy)
        for target in (
            "https://attacker.example/private",
            "//attacker.example/private",
            "http:example",
            "path?secret=1",
        ):
            self.assertTrue(
                proxy._build_target(target).startswith(proxy.BACKEND_BASE + "/")
            )
        with proxy.app.test_request_context(
            "/",
            headers={
                "X-Forwarded-For": "attacker",
                "X-Forwarded-Host": "attacker.example",
                "X-Forwarded-Proto": "https",
            },
            environ_base={"REMOTE_ADDR": "127.0.0.1"},
        ):
            headers = proxy._non_hop_headers()
            self.assertEqual(headers["X-Forwarded-For"], "127.0.0.1")
            self.assertEqual(headers["X-Forwarded-Host"], "localhost")
            self.assertEqual(headers["X-Forwarded-Proto"], "http")

    def test_all_registered_admin_routes_deny_anonymous_and_guest_requests(self):
        import re

        for guest in (False, True):
            if guest:
                self.login(guest=True)
            csrf = self.token(path="/" if guest else "/login")
            for rule in self.app.url_map.iter_rules():
                if not rule.rule.startswith("/admin/"):
                    continue
                path = re.sub(r"<[^>]+>", "1", rule.rule)
                for method in sorted(
                    rule.methods & {"GET", "POST", "PUT", "DELETE", "PATCH"}
                ):
                    response = self.client.open(
                        path,
                        method=method,
                        base_url="http://localhost",
                        headers={"X-CSRFToken": csrf},
                    )
                    with self.subTest(path=path, method=method, guest=guest):
                        self.assertIn(response.status_code, {302, 303, 401, 403, 404})

    def test_production_requires_encryption_key_secure_cookies_and_allowed_hosts(self):
        secret = "random-Test-Secret-0123456789-abcdefghijklmnopqrstuvwxyz"
        with patch.dict(os.environ, {"E3_ENV": "production", "E3_WEB_SECRET": secret}):
            with self.assertRaisesRegex(RuntimeError, "E3_DATA_ENCRYPTION_KEY"):
                CredentialCipher(self.directory.name)
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "E3_ENV": "production",
                "E3_WEB_SECRET": secret,
                "E3_DATA_ENCRYPTION_KEY": Fernet.generate_key().decode(),
                "E3_CACHE_DIR": directory,
            },
        ):
            with self.assertRaisesRegex(RuntimeError, "secure session cookies"):
                create_app()
            os.environ["E3_SESSION_COOKIE_SECURE"] = "1"
            with self.assertRaisesRegex(RuntimeError, "E3_TRUSTED_HOSTS"):
                create_app()
            os.environ["E3_TRUSTED_HOSTS"] = "localhost"
            app = create_app()
            try:
                self.assertEqual(app.config["SESSION_COOKIE_NAME"], "__Host-e3_session")
                self.assertEqual(
                    app.test_client()
                    .get("/login", base_url="https://attacker.example")
                    .status_code,
                    400,
                )
                response = app.test_client().get("/login", base_url="https://localhost")
                self.assertIn("Strict-Transport-Security", response.headers)
                self.assertIn("Secure", response.headers["Set-Cookie"])
                self.assertIn("HttpOnly", response.headers["Set-Cookie"])
                self.assertIn("SameSite=Lax", response.headers["Set-Cookie"])
            finally:
                app.extensions["e3_storage"]._engine.dispose()

    def test_logout_requires_post_and_csrf(self):
        self.login()
        self.assertEqual(self.client.get("/logout").status_code, 405)
        self.assertIsNotNone(self.storage.load_web_session("secure-test-session"))
        self.assertEqual(
            self.client.post("/logout", base_url="http://localhost").status_code, 400
        )
        csrf = self.token(path="/")
        self.assertEqual(self.post("/logout", token=csrf).status_code, 302)
        self.assertIsNone(self.storage.load_web_session("secure-test-session"))

    def test_anonymous_and_regular_users_cannot_read_notes_or_images(self):
        for logged_in in (False, True):
            if logged_in:
                self.login()
            for path in (
                "/study-progress/notes/search?q=private",
                "/study-progress/notes/1/image/private.png",
                "/admin/study-recall/1/image/private.png",
            ):
                self.assertNotEqual(self.client.get(path).status_code, 200)
        public = self.app.test_client().get("/study-progress")
        self.assertEqual(public.status_code, 200)
        self.assertIsNone(
            BeautifulSoup(public.data, "html.parser").select_one(
                "[data-public-note-search-form]"
            )
        )

    def test_login_limits_persist_and_forwarded_ip_cannot_bypass_them(self):
        self.app.config["SECURITY_LOGIN_LIMIT"] = 2
        csrf = self.token()
        for index in range(2):
            response = self.post(
                "/login",
                token=csrf,
                data={"username": "", "password": ""},
                headers={"X-Forwarded-For": f"192.0.2.{index}"},
            )
            self.assertEqual(response.status_code, 200)
        self.assertEqual(
            self.post(
                "/login", token=csrf, headers={"X-Forwarded-For": "192.0.2.99"}
            ).status_code,
            429,
        )
        self.assertEqual(self.post("/login", token=csrf).headers["Retry-After"], "300")

    def test_limits_are_atomic_across_independent_storage_connections(self):
        other = PersistentStorage(str(Path(self.directory.name) / "e3_tracker.sqlite3"))
        try:

            def consume(index):
                storage = self.storage if index % 2 else other
                return storage.consume_security_limit("shared-test-limit", 7, 60)

            with ThreadPoolExecutor(max_workers=6) as pool:
                results = list(pool.map(consume, range(30)))
            self.assertEqual(sum(results), 7)
            self.assertFalse(other.consume_security_limit("shared-test-limit", 7, 60))
        finally:
            other._engine.dispose()

    def test_shared_ip_logins_do_not_share_the_browser_limit(self):
        for _ in range(12):
            client = self.app.test_client()
            response = self.post(
                "/login", client=client, data={"username": "", "password": ""}
            )
            self.assertEqual(response.status_code, 200)
        csrf = self.token()
        for _ in range(10):
            self.assertEqual(
                self.post("/login", token=csrf, data={"username": ""}).status_code,
                200,
            )
        self.assertEqual(self.post("/login", token=csrf).status_code, 429)

    def test_account_login_limits_apply_across_browsers(self):
        self.app.config["SECURITY_LOGIN_ACCOUNT_LIMIT"] = 2
        for index in range(3):
            client = self.app.test_client()
            response = self.post(
                "/login",
                client=client,
                data={"username": "same-student", "password": ""},
            )
            self.assertEqual(response.status_code, 200 if index < 2 else 429)

    def test_healthcheck_accepts_allowed_internal_host_without_canonical_redirect(self):
        with patch.dict(os.environ, {"E3_CANONICAL_HOST": "www.example.test"}):
            app = create_app()
        try:
            app.config["TRUSTED_HOSTS"] = [
                "www.example.test",
                "healthcheck.railway.app",
            ]
            client = app.test_client()
            self.assertEqual(
                client.get(
                    "/healthz", base_url="http://healthcheck.railway.app"
                ).status_code,
                200,
            )
            self.assertEqual(
                client.get(
                    "/login", base_url="http://healthcheck.railway.app"
                ).status_code,
                301,
            )
            self.assertEqual(
                client.get("/healthz", base_url="http://attacker.example").status_code,
                400,
            )
        finally:
            app.extensions["e3_storage"]._engine.dispose()

    def test_deployment_preflight_rejects_wildcard_hosts_and_implicit_admin(self):
        path = Path(__file__).resolve().parents[1] / "tools/check_security.py"
        spec = importlib.util.spec_from_file_location(
            "security_preflight_fixture", path
        )
        preflight = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(preflight)
        with patch.dict(
            os.environ,
            {
                "E3_ENV": "production",
                "E3_WEB_SECRET": "random-Test-Secret-0123456789-abcdefghijklmnopqrstuvwxyz",
                "E3_DATA_ENCRYPTION_KEY": Fernet.generate_key().decode(),
                "E3_SESSION_COOKIE_SECURE": "1",
                "E3_TRUSTED_HOSTS": "localhost",
                "E3_ADMIN_USER_ID": "test-admin",
            },
        ):
            self.assertEqual(preflight.configuration_errors(), [])
            for host in ("*", "https://localhost", "localhost:443"):
                os.environ["E3_TRUSTED_HOSTS"] = host
                self.assertTrue(
                    any(
                        "E3_TRUSTED_HOSTS" in error
                        for error in preflight.configuration_errors()
                    )
                )
            os.environ["E3_TRUSTED_HOSTS"] = "localhost"
            os.environ["E3_ADMIN_USER_ID"] = ""
            self.assertTrue(
                any(
                    "E3_ADMIN_USER_ID" in error
                    for error in preflight.configuration_errors()
                )
            )

    def test_html_uses_unique_script_nonces_and_real_form_csrf_fields(self):
        first = self.client.get("/login")
        second = self.client.get("/login")
        self.assertNotEqual(
            first.headers["Content-Security-Policy"],
            second.headers["Content-Security-Policy"],
        )
        document = BeautifulSoup(first.data, "html.parser")
        for script in document.find_all("script"):
            self.assertTrue(script.get("nonce"))
            self.assertIn(script["nonce"], first.headers["Content-Security-Policy"])
        for form in document.select('form[method="post"]'):
            self.assertTrue(form.select_one('input[name="csrf_token"]')["value"])
        self.assertIn(
            "script-src-attr 'none'", first.headers["Content-Security-Policy"]
        )
        for header in (
            "X-Content-Type-Options",
            "X-Frame-Options",
            "Referrer-Policy",
            "Permissions-Policy",
        ):
            self.assertIn(header, first.headers)

    def test_all_site_templates_avoid_inline_script_handlers(self):
        frontend = Path(__file__).resolve().parents[2] / "frontend"
        for owner in ("assignments", "study", "shared"):
            for path in (frontend / owner / "templates").rglob("*.html"):
                document = BeautifulSoup(
                    path.read_text(encoding="utf-8"), "html.parser"
                )
                with self.subTest(template=str(path.relative_to(frontend))):
                    for element in document.find_all(True):
                        self.assertFalse(
                            any(name.lower().startswith("on") for name in element.attrs)
                        )
                    for script in document.find_all("script"):
                        self.assertTrue(script.has_attr("nonce"))

    def test_unsafe_links_and_disguised_images_are_rejected(self):
        for value in (
            "javascript:alert(1)",
            "data:text/html,bad",
            "//attacker.test",
            "https://user:password@host.test",
            "/\\attacker.test",
            "java\nscript:alert(1)",
        ):
            self.assertEqual(safe_link(value), "#")
        self.assertEqual(
            safe_link("https://e3p.nycu.edu.tw/mod/assign/view.php?id=1"),
            "https://e3p.nycu.edu.tw/mod/assign/view.php?id=1",
        )
        with self.assertRaises((OSError, ValueError)):
            validate_note_image(b"<script>alert(1)</script>", "image/png")
        stream = io.BytesIO()
        Image.new("RGB", (4, 4)).save(stream, format="PNG")
        validate_note_image(stream.getvalue(), "image/png")
        with self.assertRaises(ValueError):
            validate_note_image(stream.getvalue(), "image/jpeg")
