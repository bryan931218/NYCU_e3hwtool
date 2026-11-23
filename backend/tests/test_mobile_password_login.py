"""The original password form authenticates and reads assignments without a web handoff."""
import os
import tempfile
import time
import unittest
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
        else:
            self.assertEqual(url, BASE + "/webservice/rest/server.php")
            function = data.get("wsfunction")
            if function == "core_webservice_get_site_info":
                result = self.info
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
        response.json = lambda: result
        return response

    def mocked(self):
        return patch("e3_tracker.assignments.services.mobile_auth.safe_request", side_effect=self.respond)

    def options(self, **extra):
        return CollectOptions(BASE, moodle_token=TOKEN, moodle_user_id=42, include_completed=True, **extra)

    def test_password_auth_uses_only_the_token_api_and_verifies_the_actual_account(self):
        with self.mocked(), requests.Session() as session:
            result = login_with_password(session, BASE, USER, "private-password")
            self.assertEqual(len(session.cookies), 0)
        self.assertEqual(result["moodle_token"], TOKEN)
        self.assertEqual(result["moodle_user_id"], 42)
        self.assertNotIn(PRIVATE, str(result))
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
                self.assertEqual(storage.load_user_cache(USER)["result"]["all_assignments"][0]["title"], "HW")
                with storage._engine.connect() as connection:
                    encrypted = connection.execute(select(web_sessions_table.c.moodle_credential)).scalar_one()
                self.assertTrue(encrypted.startswith("enc:v1:"))
                self.assertNotIn(TOKEN, encrypted)
                self.assertNotIn(TOKEN, str(result.headers))
                self.assertEqual(storage.notification_content_user(USER, time.time())["moodle_token"], TOKEN)
                self.assertEqual(storage.load_session_identity_user(USER, time.time())["moodle_token"], TOKEN)
                for endpoint in ("/api/course-announcements/refresh", "/api/course-mail/refresh", "/api/course-files/refresh"):
                    limited = client.post(endpoint, json={"semester": current_semester_key()})
                    self.assertEqual(limited.status_code, 409, endpoint)
                    self.assertIn("API 登入仍可更新課程與作業", limited.json["error"])
                    self.assertNotIn("失效", limited.json["error"])
                activity = client.get("/admin/traffic/activity")
                self.assertEqual(activity.status_code, 200)
                self.assertIn("登入成功", activity.get_data(as_text=True))
                self.assertNotIn(TOKEN, activity.get_data(as_text=True))
                self.assertEqual(client.post("/logout").status_code, 302)
                self.assertIsNone(storage.load_web_session(session_id))
            finally:
                storage._engine.dispose()
