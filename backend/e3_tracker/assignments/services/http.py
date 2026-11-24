import re
from typing import Optional
from urllib.parse import parse_qs, urljoin, urlsplit

import requests
import urllib3
from bs4 import BeautifulSoup

from e3_tracker.platform.constants import HEADERS


class E3LoginError(RuntimeError):
    """A fixed login failure code; never include upstream content or credentials."""

    def __init__(self, reason: str, *, page="unknown", form=False, menu=False, notice="unknown", trace=(), auth_step=None, api_code=None):
        self.reason = reason
        self.page = page if page in {"login", "dashboard", "verification", "policy", "other"} else "unknown"
        self.form = bool(form)
        self.menu = bool(menu)
        self.notice = notice if notice in {"credentials", "token", "cookies", "verification", "rate_limit", "challenge", "none"} else "unknown"
        self.auth_step = auth_step if auth_step in {"token", "identity", "handoff", "web_session", "courses", "assignments", "submission"} else None
        self.api_code = api_code if api_code in {
            "invalidlogin", "invalidtoken", "invalidprivatetoken", "accessexception", "servicenotavailable",
            "enablewsdescription", "apprequired", "autologinkeygenerationlockout", "autologinnotallowedtoadmins",
            "passwordisexpired", "usernotconfirmed", "usernotfullysetup", "forcepasswordchangenotice",
            "restoredaccountresetpassword", "other",
        } else None
        self.trace = []
        for step in trace[:8]:
            if not isinstance(step, dict):
                continue
            self.trace.append({
                "method": step.get("method") if step.get("method") in {"GET", "POST"} else "other",
                "status": step.get("status") if isinstance(step.get("status"), int) and 100 <= step["status"] <= 599 else 0,
                "page": step.get("page") if step.get("page") in {"home", "login", "dashboard", "verification", "policy", "other"} else "other",
                "session_cookie": step.get("session_cookie") is True,
            })
        super().__init__("E3 login could not be completed")


def login_failure_reason(exc: Exception) -> str:
    """Classify failures without logging exception text, URLs or response bodies."""
    if isinstance(exc, E3LoginError) and exc.reason in {
        "login_rejected", "session_cookie_missing", "mobile_api_unavailable", "mobile_session_unavailable",
        "session_handoff_limited", "account_action_required", "identity_mismatch", "upstream_response_invalid",
        "api_token_invalid",
    }:
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


def _authenticated_dashboard(response, base_url):
    return _authenticated_principal(response, base_url) is not None


def _authenticated_principal(response, base_url):
    """A login URL elsewhere in the page is not proof that authentication failed."""
    base, target = urlsplit(base_url), urlsplit(response.url)
    if (response.status_code != 200 or (target.scheme, target.netloc) != (base.scheme, base.netloc)
            or target.path not in {"/my/", "/my/index.php"}):
        return None
    soup = BeautifulSoup(response.text, "html.parser")
    if soup.select_one('input[name="password"]'):
        return None
    profiles, logout = set(), False
    for link in soup.select('.usermenu a[href], #usermenu a[href], #action-menu-1-menu a[href]'):
        url = urlsplit(urljoin(base_url, link["href"]))
        if (url.scheme, url.netloc) != (base.scheme, base.netloc):
            continue
        query = parse_qs(url.query)
        if url.path == "/user/profile.php" and len(query.get("id", [])) == 1:
            if re.fullmatch(r"[1-9][0-9]*", query["id"][0]):
                profiles.add(query["id"][0])
        if url.path == "/login/logout.php" and query.get("sesskey"):
            logout = True
    return next(iter(profiles)) if logout and len(profiles) == 1 else None


def _login_rejection(response):
    """Expose only bounded page/form categories, not upstream messages or URLs."""
    soup = BeautifulSoup(response.text, "html.parser")
    path = urlsplit(response.url).path
    page = "other"
    if path == "/login/index.php":
        page = "login"
    elif path in {"/my/", "/my/index.php"}:
        page = "dashboard"
    elif path.startswith("/admin/tool/mfa/"):
        page = "verification"
    elif path in {"/user/policy.php", "/admin/tool/policy/view.php"}:
        page = "policy"
    errors = " ".join(node.get_text(" ", strip=True) for node in soup.select(
        '#loginerrormessage, .loginerrors, .errormessage, .notifyproblem, [role="alert"], [data-fieldtype="error"], .alert-danger'
    )).casefold()
    notice = "unknown" if errors else "none"
    for category, markers in (
        ("token", ("token", "權杖")),
        ("cookies", ("cookie",)),
        ("rate_limit", ("too many", "locked", "鎖定", "次數過多")),
        ("verification", ("verification code", "驗證碼", "two-factor", "雙重驗證")),
        ("credentials", ("invalid login", "無效的登入", "帳號或密碼", "帳號密碼")),
    ):
        if any(marker in errors for marker in markers):
            notice = category
            break
    trace = []
    for step in [*response.history, response][:8]:
        path = urlsplit(step.url).path
        target = {"/": "home", "/login/index.php": "login", "/my/": "dashboard", "/my/index.php": "dashboard"}.get(path, "other")
        if path.startswith("/admin/tool/mfa/"):
            target = "verification"
        elif path in {"/user/policy.php", "/admin/tool/policy/view.php"}:
            target = "policy"
        trace.append({"method": getattr(step.request, "method", "other"), "status": step.status_code,
                      "page": target, "session_cookie": "MoodleSession" in step.cookies})
    return E3LoginError("login_rejected", page=page,
                        form=bool(soup.select_one('input[name="password"]')),
                        menu=bool(soup.select_one('.usermenu, #usermenu, #user-menu-toggle')),
                        notice=notice, trace=trace)


def _password_form(soup, login_url, base_url):
    """Use one real form, including only that form's hidden fields."""
    base = urlsplit(base_url)
    for form in soup.select("form"):
        if not form.select_one('input[name="username"]:not([type="hidden"])') or not form.select_one('input[name="password"][type="password"]'):
            continue
        target = urlsplit(urljoin(login_url, form.get("action") or login_url))
        if ((target.scheme, target.netloc) != (base.scheme, base.netloc)
                or target.username or target.password or target.path != "/login/index.php"
                or str(form.get("method", "get")).casefold() != "post"):
            continue
        tokens = form.select('input[name="logintoken"][type="hidden"]')
        if len(tokens) != 1 or not tokens[0].get("value"):
            continue
        payload = {field["name"]: field.get("value", "")
                   for field in form.select('input[type="hidden"][name]') if not field.has_attr("disabled")}
        return target.geturl(), payload
    raise E3LoginError("login_rejected")


def login_with_password(sess: requests.Session, base_url: str, username: str, password: str, *, timeout: int = 20) -> Optional[dict]:
    if base_url.rstrip("/") == "https://e3p.nycu.edu.tw":
        from e3_tracker.assignments.services.mobile_auth import login_with_mobile_api
        return login_with_mobile_api(sess, base_url, username, password, timeout=timeout)
    login_url = f"{base_url}/login/index.php"
    resp = safe_request(sess, "GET", login_url, headers=HEADERS, timeout=timeout)
    soup = BeautifulSoup(resp.text, "html.parser")
    post_url, payload = _password_form(soup, resp.url, base_url)
    payload.update(username=username, password=password)
    origin = urlsplit(base_url)
    headers = {**HEADERS, "Referer": login_url, "Origin": f"{origin.scheme}://{origin.netloc}"}
    resp = safe_request(sess, "POST", post_url, data=payload, headers=headers, timeout=timeout)
    if need_login_redirect(resp.text):
        # Confirm the authenticated session with one read-only request. A footer,
        # announcement or switch-account link can also mention login/index.php.
        dashboard = safe_request(sess, "GET", f"{base_url.rstrip('/')}/my/", headers=HEADERS,
                                 timeout=timeout, allow_redirects=False)
        if _authenticated_dashboard(dashboard, base_url):
            return
        raise _login_rejection(resp)


def apply_cookie(sess: requests.Session, base_url: str, moodle_session_value: str) -> None:
    domain = re.sub(r"^https?://", "", base_url).split("/")[0]
    sess.cookies.set("MoodleSession", moodle_session_value, domain=domain)


def configure_tls(sess: requests.Session, *, cafile: Optional[str], insecure: bool) -> None:
    if cafile:
        sess.verify = cafile
    elif insecure:
        sess.verify = False
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
