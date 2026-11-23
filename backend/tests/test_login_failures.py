"""Login diagnostics distinguish upstream failures without disclosing secrets."""

import unittest

import requests

from e3_tracker.assignments.services.http import E3LoginError, login_failure_reason


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


if __name__ == "__main__":
    unittest.main()
