"""Verified student identities for statistics, never for authentication or storage."""

import re


def student_identity(account):
    for value in (account.get("username"), account.get("student_number")):
        if isinstance(value, str) and re.fullmatch(r"[0-9]{9}", value):
            return value
    return ""


class AccountIdentities:
    def __init__(self, profiles=()):
        self.profiles = {row["username"]: dict(row) for row in profiles}
        self.groups = {}
        for username, row in self.profiles.items():
            self.groups.setdefault(student_identity(row) or username, []).append(username)

    def key(self, username):
        return student_identity(self.profiles.get(username, {"username": username})) or username

    def members(self, username):
        return self.groups.get(self.key(username), [username])

    def profile(self, username):
        key = self.key(username)
        members = self.groups.get(key, [username])
        # Prefer the password account, then an alias with a known name.
        representative = self._representative(members, key)
        name = next((self.profiles.get(member, {}).get("profile_name") for member in [representative, *members]
                     if self.profiles.get(member, {}).get("profile_name")), "")
        return {"username": representative, "profile_name": name, "student_number": student_identity({"username": key})}

    def _representative(self, usernames, key):
        return min(usernames, key=lambda name: (name != key, not bool(self.profiles.get(name, {}).get("profile_name")), name))

    def traffic_rows(self, rows):
        grouped = {}
        candidates = {}
        for row in rows:
            key = self.key(row["username"])
            candidates.setdefault(key, []).append(row["username"])
            entry = grouped.setdefault(key, {**self.profile(row["username"]), "identity_key": key,
                                            "count": 0, "last_seen": 0, "last_counted": None, "online": False})
            entry["count"] += row.get("count", 0)
            entry["last_seen"] = max(entry["last_seen"], row.get("last_seen") or 0)
            entry["online"] = entry["online"] or row.get("online", False)
            if row.get("last_counted") is not None:
                entry["last_counted"] = max(entry["last_counted"] or 0, row["last_counted"])
        for key, entry in grouped.items():
            # Reset buttons must still target an account with actual traffic.
            entry["username"] = self._representative(candidates[key], key)
        return sorted(grouped.values(), key=lambda row: row["count"], reverse=True)

    def view_options(self, options, selected_username=None):
        grouped = {}
        for row in options:
            key = self.key(row["username"])
            previous = grouped.get(key)
            rank = (row["username"] == selected_username, row.get("fetched_ts") or 0)
            if previous is None or rank > (previous["username"] == selected_username, previous.get("fetched_ts") or 0):
                grouped[key] = row
        return [{**row, **{field: self.profile(row["username"])[field] for field in ("profile_name", "student_number")}}
                for row in grouped.values()]
