"""Read-only deployment preflight; never print credentials or mutate storage."""

import argparse
import os
from pathlib import Path
import sys
from urllib.parse import urlsplit

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))
from e3_tracker.platform.security import signing_key, validate_encryption_key


def configuration_errors():
    errors = []
    if os.getenv("E3_ENV") != "production":
        errors.append("Set E3_ENV=production for public deployment")
    value = os.getenv("E3_WEB_SECRET", "")
    if not value:
        errors.append("E3_WEB_SECRET is missing")
    else:
        try:
            signing_key(ROOT / ".localdata")
        except (ValueError, RuntimeError):
            errors.append("E3_WEB_SECRET is weak")
    key = os.getenv("E3_DATA_ENCRYPTION_KEY", "")
    try:
        if not key:
            raise ValueError()
        validate_encryption_key(key)
    except (ValueError, UnicodeError):
        errors.append("E3_DATA_ENCRYPTION_KEY is missing or invalid")
    if os.getenv("E3_SESSION_COOKIE_SECURE", "1") != "1":
        errors.append("Secure cookies must be enabled")
    if os.getenv("E3_SESSION_COOKIE_SAMESITE", "Lax") != "Lax":
        errors.append("Use SameSite=Lax")
    hosts = [
        part.strip()
        for part in os.getenv("E3_TRUSTED_HOSTS", "").split(",")
        if part.strip()
    ]
    if not hosts or any("*" in host or "/" in host or ":" in host for host in hosts):
        errors.append(
            "E3_TRUSTED_HOSTS must list explicit host names without URLs or wildcards"
        )
    if not os.getenv("E3_ADMIN_USER_ID", "").strip():
        errors.append("E3_ADMIN_USER_ID must explicitly identify the administrator")
    if os.getenv("E3_DEV_RELOAD", "0").lower() in {"1", "true", "yes", "on"}:
        errors.append("Disable E3_DEV_RELOAD")
    try:
        if not 0 <= int(os.getenv("E3_PROXY_HOPS", "0")) <= 3:
            raise ValueError()
    except ValueError:
        errors.append("E3_PROXY_HOPS must match 0-3 trusted proxies")
    return errors


def header_errors(url):
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
    ):
        return ["Header checks require an HTTPS URL without embedded credentials"]
    try:
        response = requests.head(url, timeout=15, allow_redirects=False)
        response.raise_for_status()
    except requests.RequestException:
        return ["Could not verify deployment headers"]
    if response.is_redirect:
        return ["Check the final canonical HTTPS URL, not a redirect"]
    required = {
        "Content-Security-Policy": "frame-ancestors 'none'",
        "Strict-Transport-Security": "max-age=",
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "Referrer-Policy": "strict-origin-when-cross-origin",
        "Permissions-Policy": "camera=()",
    }
    return [
        f"Missing/invalid {name}"
        for name, expected in required.items()
        if expected not in response.headers.get(name, "")
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", help="Optional HTTPS URL for a passive header check")
    args = parser.parse_args()
    load_dotenv(ROOT / ".env", override=False)
    errors = configuration_errors()
    if args.url:
        errors.extend(header_errors(args.url))
    for error in errors:
        print(f"FAIL: {error}")
    if not errors:
        print(
            "PASS: Configuration/header checks passed; this is not a complete security assessment"
        )
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
