"""The original password form authenticates and reads assignments without a web handoff."""
import os
import tempfile
import time
import unittest
from contextlib import ExitStack
from unittest.mock import patch

import requests
from sqlalchemy import select

from e3_tracker.assignments.services.collector import CollectOptions, collect_assignments, current_semester_key
from e3_tracker.assignments.services.http import E3LoginError, login_with_password
from e3_tracker.platform.application import create_app
from e3_tracker.platform.persistence.core_schema import web_sessions_table
from e3_tracker.platform.moodle_credentials import pack_moodle_credential, unpack_moodle_credential
from tests.security_helpers import csrf_client

BASE, USER, TOKEN, PRIVATE = "https://e3p.nycu.edu.tw", "114550001", "t" * 32, "p" * 64


class ApiPasswordTests(unittest.TestCase):
    def setUp(self):
        key = current_semester_key()
        year, term = key.split("-")
        label = "上" if term == "1" else "下"
        self.info = {"userid": 42, "username": USER, "siteurl": BASE, "fullname": "資工系 / DCP 王小明"}
        self.courses = [{"id": 10, "fullname": f"【{year}{label}】課程", "shortname": "test"},
                        {"id": 7528, "fullname": "性別平等教育線上課程"}]
        self.assignment = {"id": 123, "cmid": 456, "course": 10, "name": "HW", "duedate": int(time.time()) + 86400,
                           "intro": "<p>Read the problem.</p><script>private_script()</script>"}
        self.status = {"lastattempt": {"gradingstatus": "notgraded", "submission": {"status": "new"}}}
        self.calls = []
        self.failure = None

    def respond(self, sess, method, url, **kwargs):
        data = kwargs.get("data") or {}
        self.calls.append((method, url, kwargs))
        if url == BASE + "/login/token.php":
            result = {"token": TOKEN, "privatetoken": PRIVATE}
        elif url == BASE + "/admin/tool/mobile/autologin.php":
            self.assertEqual(data["userid"], "42")
            self.assertEqual(data["key"], "k" * 32)
            sess.cookies.set("MoodleSession", "s" * 32, domain="e3p.nycu.edu.tw", path="/")
            result = {}
        elif url == BASE + "/my/":
            result = {}
        elif url == BASE + "/local/courseextension/index.php":
            self.assertEqual(kwargs["params"]["courseid"], 10)
            result = {}
        else:
            self.assertEqual(url, BASE + "/webservice/rest/server.php")
            function = data.get("wsfunction")
            if function == "core_webservice_get_site_info":
                result = self.info
            elif function == "tool_mobile_get_autologin_key":
                self.assertEqual(data["privatetoken"], PRIVATE)
                result = {"key": "k" * 32, "autologinurl": BASE + "/admin/tool/mobile/autologin.php"}
            elif function == "core_enrol_get_users_courses":
                self.assertEqual(data["userid"], 42)
                result = self.courses
            elif function == "mod_assign_get_assignments":
                self.assertEqual(data["courseids[0]"], 10)
                result = {"courses": [{"id": 10, "assignments": [self.assignment]}]}
            elif function == "mod_assign_get_submission_status":
                self.assertEqual(data["assignid"], 123)
                self.assertEqual(data["userid"], 42)
                result = self.status
            else:
                self.fail("Unexpected API function: " + str(function))
        if self.failure:
            result = self.failure
        response = requests.Response()
        response.status_code, response.url = 200, url
        response._content = b""
        if url == BASE + "/my/":
            response._content = b'<div class="usermenu"><a href="/user/profile.php?id=42">Me</a><a href="/login/logout.php?sesskey=test">Logout</a></div>'
        elif url == BASE + "/local/courseextension/index.php":
            response._content = b'<table><tr><th>Name</th><th>status</th><th>Function</th></tr><tr><td>HW</td><td>3 Submitted<br>90 Not Submitted</td><td><a href="/mod/assign/view.php?id=456">View</a></td></tr></table>'
        response.json = lambda: result
        return response

    def mocked(self):
        from e3_tracker.assignments.services.submission_counts import _entries
        _entries.clear()
        stack = ExitStack()
        stack.enter_context(patch("e3_tracker.assignments.services.mobile_auth.safe_request", side_effect=self.respond))
        stack.enter_context(patch("e3_tracker.assignments.services.submission_counts.safe_request", side_effect=self.respond))
        return stack

    def options(self, **extra):
        return CollectOptions(BASE, moodle_token=TOKEN, moodle_user_id=42, include_completed=True, **extra)

    def test_password_auth_uses_only_the_token_api_and_verifies_the_actual_account(self):
        with self.mocked(), requests.Session() as session:
            result = login_with_password(session, BASE, USER, "private-password")
            self.assertEqual(len(session.cookies), 0)
        self.assertEqual(result["moodle_token"], TOKEN)
        self.assertEqual(result["moodle_user_id"], 42)
        self.assertEqual(result["moodle_private_token"], PRIVATE)
        self.assertEqual(len(self.calls), 2)
        for method, url, kwargs in self.calls:
            self.assertEqual(method, "POST")
            self.assertFalse(kwargs["allow_redirects"])
            for secret in (TOKEN, PRIVATE, "private-password"):
                self.assertNotIn(secret, url)
            self.assertNotIn("autologin", url)

    def test_bad_credentials_fail_without_handoff_or_retries(self):
        self.failure = {"errorcode": "invalidlogin", "error": "private server message"}
        with self.mocked(), requests.Session() as session, self.assertRaises(E3LoginError) as raised:
            login_with_password(session, BASE, USER, "private-password")
        self.assertEqual(raised.exception.reason, "login_rejected")
        self.assertEqual(len(self.calls), 1)
        self.assertNotIn("private", str(vars(raised.exception)))

    def test_wrong_account_or_site_cannot_start_a_login(self):
        for changes in ({"username": "another-user"}, {"userid": True}, {"siteurl": "https://foreign.example"}):
            original = dict(self.info)
            self.info.update(changes)
            with self.subTest(changes=changes), self.mocked(), requests.Session() as session, self.assertRaises(E3LoginError) as raised:
                login_with_password(session, BASE, USER, "private-password")
            self.assertEqual(raised.exception.reason, "identity_mismatch")
            self.info = original

    def test_api_reads_real_course_assignment_and_submission_fields_without_a_cookie(self):
        with self.mocked():
            result = collect_assignments(self.options())
        self.assertEqual(result["login_method"], "api")
        self.assertEqual(len(result["courses"]), 1)
        item, = result["all_assignments"]
        self.assertEqual(item["title"], "HW")
        self.assertIn("id=456", item["url"])
        self.assertNotIn("id=123", item["url"])
        self.assertEqual(item["due_ts"], self.assignment["duedate"])
        self.assertFalse(item["completed"])
        self.assertIsNone(item["submitted_count"])
        self.assertNotIn("private_script", str(item["description"]))
        self.assertEqual(len(self.calls), 4)

    def test_extensions_grades_and_submitted_time_are_preserved(self):
        extended = self.assignment["duedate"] + 86400
        self.status = {"lastattempt": {"extensionduedate": extended, "gradingstatus": "graded",
                                      "submission": {"status": "submitted", "timemodified": 1700000000}},
                       "feedback": {"gradefordisplay": "100.00 / 100.00"},
                       "gradingsummary": {"participantcount": 20, "submissionssubmittedcount": 18}}
        with self.mocked():
            item, = collect_assignments(self.options())["all_assignments"]
        self.assertEqual(item["due_ts"], extended)
        self.assertEqual(item["submitted_ts"], 1700000000)
        self.assertEqual(item["raw_status_text"], "已評分")
        self.assertTrue(item["completed"])
        self.assertEqual(item["submitted_count"], 18)

    def test_student_api_login_supplements_counts_and_reuses_verified_web_session(self):
        with self.mocked():
            first = collect_assignments(self.options(moodle_private_token=PRIVATE))
            second = collect_assignments(self.options(moodle_private_token=PRIVATE))
        for result in (first, second):
            self.assertEqual(result["submission_counts_status"], "available")
            item, = result["all_assignments"]
            self.assertEqual((item["submitted_count"], item["participant_count"]), (3, 93))
        self.assertEqual(sum(call[2].get("data", {}).get("wsfunction") == "tool_mobile_get_autologin_key"
                             for call in self.calls), 1)
        self.assertEqual(sum(call[1].endswith("/local/courseextension/index.php") for call in self.calls), 2)
        for _, url, kwargs in self.calls:
            self.assertFalse(kwargs["allow_redirects"])
            for secret in (TOKEN, PRIVATE, "k" * 32):
                self.assertNotIn(secret, url)

    def test_unavailable_handoff_does_not_break_api_or_retry_repeatedly(self):
        with self.mocked(), patch("e3_tracker.assignments.services.submission_counts._json_post",
                                 side_effect=E3LoginError("mobile_api_unavailable", api_code="apprequired")) as handoff:
            for _ in range(2):
                result = collect_assignments(self.options(moodle_private_token=PRIVATE))
                self.assertEqual(result["submission_counts_status"], "unavailable")
                self.assertEqual(result["all_assignments"][0]["title"], "HW")
                self.assertIsNone(result["all_assignments"][0]["submitted_count"])
            self.assertEqual(handoff.call_count, 1)

    def test_wrong_web_identity_and_external_handoff_never_read_course_counts(self):
        from e3_tracker.assignments.services import submission_counts
        for target in (BASE + "/admin/tool/mobile/autologin.php", "https://foreign.example/autologin.php"):
            with self.subTest(target=target), self.mocked(), patch.object(submission_counts, "_json_post",
                    return_value={"key": "k" * 32, "autologinurl": target}), patch.object(
                    submission_counts, "_authenticated_principal", return_value="99"):
                result = collect_assignments(self.options(moodle_private_token=PRIVATE))
            self.assertEqual(result["submission_counts_status"], "unavailable")
            self.assertFalse(any(call[1].endswith("/local/courseextension/index.php") for call in self.calls))
            self.assertFalse(any(call[1].startswith("https://foreign.example") for call in self.calls))
            self.calls.clear()

    def test_missing_and_zero_web_counts_do_not_fabricate_or_lose_values(self):
        from e3_tracker.assignments.services import submission_counts
        for rows, expected in (([("HW", BASE + "/mod/assign/view.php?id=456", None, 0, 93)], (0, 93)),
                               ([("HW", BASE + "/mod/assign/view.php?id=999", None, 3, 93)], (None, None)),
                               ([("HW", "https://foreign.example/mod/assign/view.php?id=456", None, 3, 93)], (None, None))):
            with self.subTest(expected=expected), self.mocked(), patch.object(
                    submission_counts, "gather_assign_links_from_list_page", return_value=rows):
                item, = collect_assignments(self.options(moodle_private_token=PRIVATE))["all_assignments"]
            self.assertEqual((item["submitted_count"], item["participant_count"]), expected)

    def test_optional_private_credential_roundtrips_without_changing_legacy_records(self):
        value = pack_moodle_credential(token=TOKEN, user_id=42, private_token=PRIVATE)
        self.assertEqual(unpack_moodle_credential(value)["moodle_private_token"], PRIVATE)
        for private in (True, "short", "secret\nvalue"):
            with self.subTest(private=type(private).__name__), self.assertRaises(ValueError):
                pack_moodle_credential(token=TOKEN, user_id=42, private_token=private)

    def test_expired_token_cannot_return_an_empty_success_or_another_users_courses(self):
        self.failure = {"exception": "private", "errorcode": "invalidtoken", "message": "private"}
        with self.mocked(), self.assertRaises(E3LoginError) as raised:
            collect_assignments(self.options())
        self.assertEqual(raised.exception.reason, "api_token_invalid")
        self.assertEqual(len(self.calls), 1)

    def test_credentials_are_typed_and_cannot_be_mistaken_for_cookies(self):
        self.assertEqual(unpack_moodle_credential("legacy-cookie"), {"moodle_session": "legacy-cookie"})
        self.assertEqual(unpack_moodle_credential(pack_moodle_credential(token=TOKEN, user_id=42)),
                         {"moodle_session": None, "moodle_token": TOKEN, "moodle_user_id": 42})
        with self.assertRaises(ValueError):
            pack_moodle_credential("cookie", token=TOKEN, user_id=42)
        with self.assertRaises(ValueError):
            unpack_moodle_credential('api:v1:{"token":"bad","user_id":42}')

    def test_original_form_creates_the_original_admin_and_fresh_assignment_data(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
            "E3_ENV": "development", "RAILWAY_ENVIRONMENT_ID": "", "RAILWAY_ENVIRONMENT_NAME": "",
            "E3_CACHE_DIR": directory, "E3_DATABASE_URL": "", "DATABASE_URL": "",
            "E3_CANONICAL_HOST": "", "E3_SESSION_COOKIE_SECURE": "0", "E3_ADMIN_USER_ID": USER,
            "E3_BASE_URL": BASE, "E3_SESSION_PROFILE_WORKER": "0", "E3_NOTIFICATIONS_WORKER": "0",
            "E3_YOUTUBE_AUTO_SYNC_ENABLED": "0",
        }):
            app = create_app()
            storage = app.extensions["e3_storage"]
            try:
                client = csrf_client(app)
                with self.mocked():
                    result = client.post("/login", data={"username": USER, "password": "private-password"})
                self.assertEqual(result.status_code, 302)
                with client.session_transaction() as browser:
                    session_id = browser["session_token"]
                    account = storage.load_web_session(session_id)
                self.assertTrue(account["is_admin"])
                self.assertEqual(account["username"], USER)
                self.assertEqual(account["moodle_token"], TOKEN)
                self.assertIsNone(account["moodle_session"])
                self.assertEqual(account["moodle_private_token"], PRIVATE)
                self.assertEqual(storage.load_user_cache(USER)["result"]["all_assignments"][0]["title"], "HW")
                self.assertEqual(storage.load_user_cache(USER)["result"]["all_assignments"][0]["submitted_count"], 3)
                with storage._engine.connect() as connection:
                    encrypted = connection.execute(select(web_sessions_table.c.moodle_credential)).scalar_one()
                self.assertTrue(encrypted.startswith("enc:v1:"))
                self.assertNotIn(TOKEN, encrypted)
                self.assertNotIn(PRIVATE, encrypted)
                self.assertNotIn(TOKEN, str(result.headers))
                self.assertNotIn(PRIVATE, str(result.headers))
                self.assertEqual(storage.notification_content_user(USER, time.time())["moodle_token"], TOKEN)
                self.assertEqual(storage.load_session_identity_user(USER, time.time())["moodle_token"], TOKEN)
                activity = client.get("/admin/traffic/activity")
                self.assertEqual(activity.status_code, 200)
                self.assertIn("登入成功", activity.get_data(as_text=True))
                self.assertNotIn(TOKEN, activity.get_data(as_text=True))
                self.assertEqual(client.post("/logout").status_code, 302)
                self.assertIsNone(storage.load_web_session(session_id))
            finally:
                storage._engine.dispose()
