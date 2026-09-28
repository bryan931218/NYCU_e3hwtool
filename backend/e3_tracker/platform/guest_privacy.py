"""Guest identities stay temporary; login history is anonymous."""

from copy import deepcopy


def is_guest_identity(username):
    return str(username or "").startswith("\u8a2a\u5ba2_")


def is_guest_event(event):
    meta = event.get("meta") or {}
    if not isinstance(meta, dict):
        meta = {}
    return bool(
        meta.get("is_guest")
        or is_guest_identity(meta.get("username"))
        or str(event.get("action") or "").strip().lower().startswith("guest_")
    )


def sanitize_traffic_event(event):
    """Retain only anonymous guest logins, never their identifiers or IPs."""
    if not isinstance(event, dict):
        return None
    action = str(event.get("action") or "").strip().lower()
    if action == "guest_login":
        meta = event.get("meta") or {}
        if not isinstance(meta, dict):
            meta = {}
        status = event.get("status") or "info"
        return {
            "ts": event.get("ts"),
            "ip": None,
            "action": "guest_login",
            "status": status if status in ("success", "error", "info", "start") else "info",
            "meta": {
                "is_guest": True,
                "site": "study" if meta.get("site") == "study" else "assignments",
            },
        }
    return None if is_guest_event(event) else dict(event)


def without_guest_traffic(payload, guest_names=()):
    """Keep aggregate counters, but remove guest identities and their IP mappings."""
    if not isinstance(payload, dict):
        return payload
    cleaned = deepcopy(payload)
    guests = set(guest_names)
    fields = (
        "active_users",
        "user_totals",
        "user_last_count",
        "user_last_seen",
        "user_flags",
    )
    for field in fields:
        values = cleaned.get(field)
        if not isinstance(values, dict):
            continue
        for name, value in values.items():
            if is_guest_identity(name) or (field == "user_flags" and value):
                guests.add(name)
    ip_users = cleaned.get("ip_users") or {}
    if not isinstance(ip_users, dict):
        ip_users = {}
    guest_ips = {
        ip for ip, name in ip_users.items() if name in guests or is_guest_identity(name)
    }
    guests.update(ip_users[ip] for ip in guest_ips)
    for field in fields:
        if isinstance(cleaned.get(field), dict):
            cleaned[field] = {
                name: value
                for name, value in cleaned[field].items()
                if name not in guests
            }
    for field in ("ip_users", "active", "last_total", "ip_totals"):
        if isinstance(cleaned.get(field), dict):
            cleaned[field] = {
                ip: value for ip, value in cleaned[field].items() if ip not in guest_ips
            }
    if isinstance(cleaned.get("hourly_buckets"), dict):
        cleaned["hourly_buckets"] = {
            ts: [
                name
                for name in names
                if name not in guests and not is_guest_identity(name)
            ]
            for ts, names in cleaned["hourly_buckets"].items()
            if isinstance(names, (list, set, tuple))
        }
        for entry in cleaned.get("hourly_series") or []:
            if not isinstance(entry, dict):
                continue
            members = cleaned["hourly_buckets"].get(str(entry.get("ts")))
            if members is None:
                members = cleaned["hourly_buckets"].get(entry.get("ts"))
            if members is not None:
                entry["count"] = len(members)
    return cleaned
