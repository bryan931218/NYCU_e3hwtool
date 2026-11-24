"""Versioned E3 credentials inside the existing encryption envelope."""
import json
import re

PREFIX = "api:v1:"

def pack_moodle_credential(cookie=None, *, token=None, user_id=None, private_token=None):
    if token is None:
        if private_token is not None:
            raise ValueError("Invalid Moodle credential")
        return cookie
    if cookie or not isinstance(token, str) or not re.fullmatch(r"[A-Za-z0-9_-]{16,256}", token) or type(user_id) is not int or user_id <= 0:
        raise ValueError("Invalid Moodle credential")
    data = {"token": token, "user_id": user_id}
    if private_token is not None:
        if not isinstance(private_token, str) or not re.fullmatch(r"[A-Za-z0-9_-]{16,256}", private_token):
            raise ValueError("Invalid Moodle credential")
        data["private_token"] = private_token
    return PREFIX + json.dumps(data, separators=(",", ":"))

def unpack_moodle_credential(value):
    if not isinstance(value, str) or not value.startswith(PREFIX):
        return {"moodle_session": value}
    data = json.loads(value[len(PREFIX):])
    if not isinstance(data, dict):
        raise ValueError("Invalid Moodle credential")
    pack_moodle_credential(token=data.get("token"), user_id=data.get("user_id"), private_token=data.get("private_token"))
    if not data.get("token"):
        raise ValueError("Invalid Moodle credential")
    result = {"moodle_session": None, "moodle_token": data["token"], "moodle_user_id": data["user_id"]}
    if data.get("private_token"):
        result["moodle_private_token"] = data["private_token"]
    return result
