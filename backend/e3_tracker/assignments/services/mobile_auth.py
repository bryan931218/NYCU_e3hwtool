"""Authenticate against the enabled Moodle API without a browser handoff."""
import re
from urllib.parse import urlsplit
from e3_tracker.assignments.services.http import E3LoginError, safe_request

API_HEADERS = {"User-Agent": "E3hwtool/1.0 (Moodle API client)", "Accept": "application/json"}
SERVICE = "moodle_mobile_app"

def _json_post(sess, url, data, *, step, timeout, headers=None):
    response = safe_request(sess, "POST", url, data=data, headers=headers or API_HEADERS, timeout=timeout, allow_redirects=False)
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
    private = result.get("privatetoken")
    if isinstance(private, str) and re.fullmatch(r"[A-Za-z0-9_-]{16,256}", private):
        identity["moodle_private_token"] = private
    return identity
