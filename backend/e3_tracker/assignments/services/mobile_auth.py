"""Authenticate against the enabled Moodle API without a browser handoff."""
import re
import logging
from urllib.parse import urlsplit
from e3_tracker.assignments.services.http import E3LoginError, _authenticated_principal, _password_form, safe_request
from e3_tracker.platform.constants import HEADERS
from bs4 import BeautifulSoup
import requests

API_HEADERS = {"User-Agent": "E3hwtool/1.0 (Moodle API client)", "Accept": "application/json"}
SERVICE = "moodle_mobile_app"

def _json_post(sess, url, data, *, step, timeout):
    response = safe_request(sess, "POST", url, data=data, headers=API_HEADERS, timeout=timeout, allow_redirects=False)
    if response.status_code != 200:
        raise E3LoginError("upstream_response_invalid", auth_step=step)
    try:
        result = response.json()
    except ValueError:
        raise E3LoginError("upstream_response_invalid", auth_step=step) from None
    if not isinstance(result, (dict, list)):
        raise E3LoginError("upstream_response_invalid", auth_step=step)
    if isinstance(result, dict) and (result.get("exception") or result.get("error")):
        code = result.get("errorcode")
        code = code if isinstance(code, str) else None
        reason, notice = "mobile_api_unavailable", "unknown"
        if code == "invalidlogin":
            reason, notice = "login_rejected", "credentials"
        elif code == "invalidtoken":
            reason, notice = "api_token_invalid", "token"
        elif code in {"passwordisexpired", "usernotconfirmed", "usernotfullysetup", "forcepasswordchangenotice", "restoredaccountresetpassword"}:
            reason, notice = "account_action_required", "verification"
        raise E3LoginError(reason, auth_step=step, api_code=code, notice=notice)
    return result

def api_call(sess, base_url, token, function, *, timeout=20, step="identity", **params):
    return _json_post(sess, base_url.rstrip("/") + "/webservice/rest/server.php", {
        "wstoken": token, "wsfunction": function, "moodlewsrestformat": "json", **params,
    }, step=step, timeout=timeout)

def verified_site_info(sess, base_url, token, *, timeout=20, username=None, user_id=None):
    info = api_call(sess, base_url, token, "core_webservice_get_site_info", timeout=timeout)
    if not isinstance(info, dict):
        raise E3LoginError("upstream_response_invalid", auth_step="identity")
    site, base = urlsplit(str(info.get("siteurl") or "")), urlsplit(base_url)
    if (type(info.get("userid")) is not int or info["userid"] <= 0 or not isinstance(info.get("username"), str)
            or (username is not None and info["username"].casefold() != username.strip().casefold())
            or (user_id is not None and info["userid"] != user_id)
            or (site.scheme, site.netloc, site.path.rstrip("/")) != (base.scheme, base.netloc, base.path.rstrip("/"))):
        raise E3LoginError("identity_mismatch", auth_step="identity")
    return info

def login_with_mobile_api(sess, base_url, username, password, *, timeout=20):
    result = _json_post(sess, base_url.rstrip("/") + "/login/token.php", {
        "username": username, "password": password, "service": SERVICE,
    }, step="token", timeout=timeout)
    token = result.get("token") if isinstance(result, dict) else None
    if not isinstance(token, str) or not re.fullmatch(r"[A-Za-z0-9_-]{16,256}", token):
        raise E3LoginError("upstream_response_invalid", auth_step="token")
    info = verified_site_info(sess, base_url, token, timeout=timeout, username=username)
    identity = {"moodle_token": token, "moodle_user_id": info["userid"], "profile_name": info.get("fullname", "")}
    # A student REST token does not expose class submission totals. Try the
    # ordinary login form once while the password is available, and retain only
    # a web session whose principal matches the verified REST account.
    try:
        login_url = base_url.rstrip("/") + "/login/index.php"
        page = safe_request(sess, "GET", login_url, headers=HEADERS, timeout=min(timeout, 8), allow_redirects=False)
        if page.status_code != 200:
            raise E3LoginError("mobile_session_unavailable", auth_step="web_session")
        post_url, payload = _password_form(BeautifulSoup(page.text, "html.parser"), page.url, base_url)
        payload.update(username=username, password=password)
        response = safe_request(sess, "POST", post_url, data=payload, headers={**HEADERS,
            "Referer": login_url, "Origin": base_url.rstrip("/")}, timeout=min(timeout, 8), allow_redirects=False)
        if response.status_code not in {200, 302, 303}:
            raise E3LoginError("mobile_session_unavailable", auth_step="web_session")
        dashboard = safe_request(sess, "GET", base_url.rstrip("/") + "/my/", headers=HEADERS,
                                 timeout=min(timeout, 8), allow_redirects=False)
        if _authenticated_principal(dashboard, base_url) != str(info["userid"]):
            raise E3LoginError("identity_mismatch", auth_step="web_session")
        cookies = [cookie.value for cookie in sess.cookies if cookie.name == "MoodleSession" and cookie.value]
        if len(cookies) != 1 or not re.fullmatch(r"[A-Za-z0-9_-]{16,256}", cookies[0]):
            raise E3LoginError("session_cookie_missing", auth_step="web_session")
        identity["moodle_session"] = cookies[0]
    except (RuntimeError, requests.RequestException) as exc:
        sess.cookies.clear()
        logging.getLogger(__name__).warning("E3 optional class totals login unavailable (type=%s reason=%s status=%s)",
            type(exc).__name__, exc.reason if isinstance(exc, E3LoginError) else "request_failed",
            exc.response.status_code if isinstance(exc, requests.HTTPError) and exc.response is not None else 0)
    return identity
