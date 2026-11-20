"""Build extension packages and accept only official store listing URLs."""

import io
import json
import re
from urllib.parse import parse_qs, urlsplit
from zipfile import ZIP_DEFLATED, ZipFile

from e3_tracker.platform.paths import FRONTEND_ROOT


EXTENSION_SOURCE = FRONTEND_ROOT / "assignments/static/e3-navigation-extension"
EXTENSION_FILES = (
    "manifest.json", "background.js", "core.js", "tracker.js", "probe.js",
    "e3-page.js", "README.html", "guide.css", "icons/icon-16.png", "icons/icon-32.png",
    "icons/icon-48.png", "icons/icon-128.png",
)


def navigation_target(value):
    """Accept only a canonical E3 course/module viewing destination."""
    if not isinstance(value, str) or len(value) > 4096 or re.search(r"[\x00-\x20\\]", value):
        return None
    try:
        url = urlsplit(value)
        if (url.scheme != "https" or url.hostname != "e3p.nycu.edu.tw"
                or url.port not in (None, 443) or url.username or url.password
                or not re.fullmatch(r"/(?:mod/[a-z0-9_]+|course)/view\.php", url.path)):
            return None
        ids = parse_qs(url.query, keep_blank_values=True).get("id", [])
        if len(ids) == 1 and re.fullmatch(r"[1-9]\d*", ids[0]):
            return url.path, ids[0]
    except ValueError:
        return None
    return None


def extension_details():
    manifest = json.loads((EXTENSION_SOURCE / "manifest.json").read_text(encoding="utf-8"))
    return {"version": manifest["version"], "minimum_chrome_version": manifest["minimum_chrome_version"]}


def build_extension_archive(*, for_store=False):
    """Stores require manifest.json at ZIP root; manual installs use a folder."""
    archive = io.BytesIO()
    prefix = "" if for_store else "e3-auto-navigation/"
    with ZipFile(archive, "w", compression=ZIP_DEFLATED) as package:
        for filename in EXTENSION_FILES:
            package.write(EXTENSION_SOURCE / filename, arcname=prefix + filename)
    archive.seek(0)
    return archive


def official_store_url(value, store):
    """Hide incomplete or mistyped links rather than sending users elsewhere."""
    patterns = {
        "chrome": r"https://chromewebstore\.google\.com/detail/(?:[a-zA-Z0-9_-]+/)?[a-p]{32}/?",
        "edge": r"https://microsoftedge\.microsoft\.com/addons/detail/(?:[a-zA-Z0-9_-]+/)?[a-p]{32}/?",
    }
    value = str(value or "").strip()
    return value if re.fullmatch(patterns[store], value) else ""
