"""Login diagnostics distinguish upstream failures without disclosing secrets."""

import unittest
from unittest.mock import Mock, patch

import requests

from e3_tracker.assignments.services.http import E3LoginError, login_failure_reason, login_with_password


BASE = "https://e3.example.test"
LOGIN = '<form><input name="logintoken" value="synthetic-token"><input name="password"></form>'
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
        self.assertFalse(request.call_args.kwargs["allow_redirects"])

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


if __name__ == "__main__":
    unittest.main()
