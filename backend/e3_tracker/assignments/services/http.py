import re
from typing import Optional

import requests
import urllib3
from bs4 import BeautifulSoup

from e3_tracker.platform.constants import HEADERS


class E3LoginError(RuntimeError):
    """A fixed login failure code; never include upstream content or credentials."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__("E3 login could not be completed")


def login_failure_reason(exc: Exception) -> str:
    """Classify failures without logging exception text, URLs or response bodies."""
    if isinstance(exc, E3LoginError) and exc.reason in {"login_rejected", "session_cookie_missing"}:
        return exc.reason
    if isinstance(exc, requests.exceptions.SSLError) or isinstance(exc.__cause__, requests.exceptions.SSLError):
        return "upstream_tls"
    if isinstance(exc, requests.Timeout):
        return "upstream_timeout"
    if isinstance(exc, requests.HTTPError):
        status = exc.response.status_code if exc.response is not None else None
        if status == 403:
            return "upstream_access_denied"
        if status == 429:
            return "upstream_rate_limited"
        return "upstream_http_error"
    if isinstance(exc, requests.RequestException):
        return "upstream_connection"
    return "internal_error"


def safe_request(sess: requests.Session, method: str, url: str, **kwargs) -> requests.Response:
    try:
        resp = sess.request(method, url, **kwargs)
        resp.raise_for_status()
        return resp
    except requests.exceptions.SSLError as exc:
        raise RuntimeError("TLS 憑證驗證失敗") from exc


def need_login_redirect(html_text: str) -> bool:
    return ("login/index.php" in html_text) or ("您目前尚未登入" in html_text) or ("You are not logged in" in html_text)


def login_with_password(sess: requests.Session, base_url: str, username: str, password: str, *, timeout: int = 20) -> None:
    login_url = f"{base_url}/login/index.php"
    resp = safe_request(sess, "GET", login_url, headers=HEADERS, timeout=timeout)
    soup = BeautifulSoup(resp.text, "html.parser")
    token_input = soup.find("input", {"name": "logintoken"})
    token = token_input["value"] if token_input and token_input.has_attr("value") else ""
    payload = {"username": username, "password": password, "logintoken": token, "anchor": ""}
    resp = safe_request(sess, "POST", login_url, data=payload, headers=HEADERS, timeout=timeout)
    if need_login_redirect(resp.text):
        raise E3LoginError("login_rejected")


def apply_cookie(sess: requests.Session, base_url: str, moodle_session_value: str) -> None:
    domain = re.sub(r"^https?://", "", base_url).split("/")[0]
    sess.cookies.set("MoodleSession", moodle_session_value, domain=domain)


def configure_tls(sess: requests.Session, *, cafile: Optional[str], insecure: bool) -> None:
    if cafile:
        sess.verify = cafile
    elif insecure:
        sess.verify = False
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
