"""Display identities without changing their persistence or authorization keys."""

import re


def account_label(username, student_number=""):
    username, student_number = str(username or ""), str(student_number or "")
    if username.startswith("Session-") and re.fullmatch(r"[0-9]{9}", student_number):
        return student_number + "\uff08session\u767b\u5165\uff09"
    return username
