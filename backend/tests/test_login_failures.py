"""Login diagnostics distinguish upstream failures without disclosing secrets."""

import unittest
from unittest.mock import Mock, patch

import requests

from e3_tracker.assignments.services.http import E3LoginError, login_failure_reason, login_with_password


BASE = "https://e3.example.test"
LOGIN = '<form method="post"><input name="logintoken" type="hidden" value="synthetic-token"><input name="username"><input name="password" type="password"></form>'
MENU = '''<div class="usermenu"><a href="/user/profile.php?id=42">Profile</a>
<a href="/login/logout.php?sesskey=synthetic-key">Logout</a></div>'''


def response(path, html, status=200):
    result = requests.Response()
    result.status_code = status
    result.url = BASE + path
    result._content = html.encode("utf-8")
    result.encoding = "utf-8"
    return result


class LoginFailureTests(unittest.TestCase):
    def test_tls_including_the_safe_request_wrapper_is_not_an_authentication_failure(self):
        tls = requests.exceptions.SSLError("private upstream URL and credential")
        wrapped = RuntimeError("TLS validation failed")
        wrapped.__cause__ = tls
        for error in (tls, wrapped):
            self.assertEqual(login_failure_reason(error), "upstream_tls")

    def test_transport_and_authentication_errors_have_separate_fixed_codes(self):
        for error, reason in (
            (requests.ConnectTimeout("private"), "upstream_timeout"),
            (requests.ReadTimeout("private"), "upstream_timeout"),
            (requests.ConnectionError("private"), "upstream_connection"),
            (E3LoginError("login_rejected"), "login_rejected"),
            (E3LoginError("session_cookie_missing"), "session_cookie_missing"),
            (E3LoginError("private"), "internal_error"),
            (RuntimeError("private"), "internal_error"),
        ):
            with self.subTest(reason=reason):
                self.assertEqual(login_failure_reason(error), reason)

    def test_http_response_status_is_kept_separate_from_exception_text(self):
        for status, reason in ((403, "upstream_access_denied"), (429, "upstream_rate_limited"), (503, "upstream_http_error")):
            response = requests.Response()
            response.status_code = status
            response.url = "https://example.test/private-credential"
            response._content = b"private upstream content"
            self.assertEqual(login_failure_reason(requests.HTTPError("private", response=response)), reason)

    def test_login_link_in_authenticated_page_is_confirmed_with_dashboard(self):
        posted = MENU + '<a href="/login/index.php">Switch account</a>'
        with patch("e3_tracker.assignments.services.http.safe_request", side_effect=[
            response("/login/index.php", LOGIN), response("/", posted), response("/my/", posted),
        ]) as request:
            login_with_password(Mock(), BASE, "synthetic-user", "synthetic-password")
        self.assertEqual(request.call_count, 3)
        post = request.call_args_list[1]
        self.assertEqual(post.kwargs["headers"]["Origin"], BASE)
        self.assertEqual(post.kwargs["headers"]["Referer"], BASE + "/login/index.php")
        self.assertEqual(post.kwargs["data"]["logintoken"], "synthetic-token")
        self.assertNotIn("anchor", post.kwargs["data"])
        self.assertFalse(request.call_args.kwargs["allow_redirects"])

    def test_fields_are_taken_from_one_form_even_when_the_page_has_duplicate_ids(self):
        second = LOGIN.replace("synthetic-token", "other-form-token").replace('</form>', '<input type="hidden" name="anchor" value="other-form"></form>')
        with patch("e3_tracker.assignments.services.http.safe_request", side_effect=[
            response("/login/index.php", LOGIN + second), response("/my/", MENU),
        ]) as request:
            login_with_password(Mock(), BASE, "synthetic-user", "synthetic-password")
        self.assertEqual(request.call_args.kwargs["data"], {
            "logintoken": "synthetic-token", "username": "synthetic-user", "password": "synthetic-password",
        })

    def test_unsupported_forms_never_receive_credentials(self):
        for html in (
            LOGIN.replace('method="post"', 'method="get"'),
            LOGIN.replace('method="post"', 'method="post" action="https://other.example.test/login/index.php"'),
            LOGIN.replace('method="post"', 'method="post" action="/login/logout.php"'),
            LOGIN.replace('value="synthetic-token"', 'value=""'),
            LOGIN.replace('name="logintoken"', 'name="unsupported"'),
        ):
            with self.subTest(html=html), patch("e3_tracker.assignments.services.http.safe_request", return_value=response("/login/index.php", html)) as request, self.assertRaises(E3LoginError):
                login_with_password(Mock(), BASE, "synthetic-user", "synthetic-password")
            self.assertEqual(request.call_count, 1)

    def test_login_form_redirect_and_unverified_menus_cannot_be_treated_as_success(self):
        pages = (
            response("/my/", LOGIN + MENU),
            response("/login/index.php", MENU, 302),
            response("/my/", '<a href="/user/profile.php?id=42">Profile</a><a href="/login/logout.php?sesskey=x">Logout</a>'),
            response("/my/", MENU.replace('/user/profile.php', 'https://other.example.test/user/profile.php')),
            response("/my/", MENU.replace('/login/logout.php', 'https://other.example.test/login/logout.php')),
            response("/my/", MENU.replace('id=42', 'id=ambiguous')),
        )
        for dashboard in pages:
            with self.subTest(page=dashboard.text), patch(
                "e3_tracker.assignments.services.http.safe_request", side_effect=[
                    response("/login/index.php", LOGIN),
                    response("/login/index.php", LOGIN + '<a href="/login/index.php">Login</a>'), dashboard,
                ]
            ), self.assertRaises(E3LoginError) as raised:
                login_with_password(Mock(), BASE, "synthetic-user", "synthetic-password")
            self.assertEqual(raised.exception.reason, "login_rejected")

    def test_rejected_login_reports_only_bounded_categories(self):
        html = LOGIN + '<div id="loginerrormessage">Invalid login private-person private-password</div><a href="/login/index.php">Login</a>'
        with patch("e3_tracker.assignments.services.http.safe_request", side_effect=[
            response("/login/index.php", LOGIN), response("/login/index.php?private-token", html),
            response("/login/index.php", LOGIN, 302),
        ]), self.assertRaises(E3LoginError) as raised:
            login_with_password(Mock(), BASE, "synthetic-user", "synthetic-password")
        error = raised.exception
        self.assertEqual((error.page, error.form, error.menu, error.notice), ("login", True, False, "credentials"))
        self.assertNotIn("private", str(error))
        error = E3LoginError("login_rejected", page="private", notice="private")
        self.assertEqual((error.page, error.notice), ("unknown", "unknown"))

    def test_rejected_redirect_trace_never_retains_urls_cookies_or_posted_credentials(self):
        first = response("/?private-query", "", 303)
        first.request = requests.Request("POST", BASE + "/login/index.php", data={"password": "private-password"}).prepare()
        first.cookies.set("MoodleSession", "private-cookie")
        final = response("/login/index.php?private-token", LOGIN + '<a href="/login/index.php">Login</a>')
        final.history = [first]
        final.request = requests.Request("GET", final.url).prepare()
        with patch("e3_tracker.assignments.services.http.safe_request", side_effect=[
            response("/login/index.php", LOGIN), final, response("/login/index.php", LOGIN, 302),
        ]), self.assertRaises(E3LoginError) as raised:
            login_with_password(Mock(), BASE, "synthetic-user", "synthetic-password")
        self.assertEqual(raised.exception.trace, [
            {"method": "POST", "status": 303, "page": "home", "session_cookie": True},
            {"method": "GET", "status": 200, "page": "login", "session_cookie": False},
        ])
        self.assertNotIn("private", str(raised.exception.trace))


if __name__ == "__main__":
    unittest.main()
