"""Versioned E3 credentials inside the existing encryption envelope."""
import json
import re

PREFIX = "api:v1:"

def pack_moodle_credential(cookie=None, *, token=None, user_id=None):
    if token is None:
        return cookie
    if not isinstance(token, str) or not re.fullmatch(r"[A-Za-z0-9_-]{16,256}", token) or type(user_id) is not int or user_id <= 0:
        raise ValueError("Invalid Moodle credential")
    data = {"token": token, "user_id": user_id}
    if cookie is not None:
        if not isinstance(cookie, str) or not re.fullmatch(r"[A-Za-z0-9_-]{16,256}", cookie):
            raise ValueError("Invalid Moodle credential")
        data["web_session"] = cookie
    return PREFIX + json.dumps(data, separators=(",", ":"))

def unpack_moodle_credential(value):
    if not isinstance(value, str) or not value.startswith(PREFIX):
        return {"moodle_session": value}
    data = json.loads(value[len(PREFIX):])
    if not isinstance(data, dict):
        raise ValueError("Invalid Moodle credential")
    pack_moodle_credential(data.get("web_session"), token=data.get("token"), user_id=data.get("user_id"))
    if not data.get("token"):
        raise ValueError("Invalid Moodle credential")
    return {"moodle_session": data.get("web_session"), "moodle_token": data["token"], "moodle_user_id": data["user_id"]}
