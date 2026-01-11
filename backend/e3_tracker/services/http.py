import re
from typing import Optional

import requests
import urllib3
from bs4 import BeautifulSoup

from ..shared.constants import HEADERS


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
        raise RuntimeError("登入失敗：請確認帳密是否正確，或是否啟用 2FA。")


def apply_cookie(sess: requests.Session, base_url: str, moodle_session_value: str) -> None:
    domain = re.sub(r"^https?://", "", base_url).split("/")[0]
    sess.cookies.set("MoodleSession", moodle_session_value, domain=domain)


def configure_tls(sess: requests.Session, *, cafile: Optional[str], insecure: bool) -> None:
    if cafile:
        sess.verify = cafile
    elif insecure:
        sess.verify = False
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
