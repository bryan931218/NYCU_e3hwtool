"""Build extension packages and accept only official store listing URLs."""

import io
import re
from zipfile import ZIP_DEFLATED, ZipFile

from e3_tracker.platform.paths import FRONTEND_ROOT


EXTENSION_SOURCE = FRONTEND_ROOT / "assignments/static/e3-navigation-extension"
EXTENSION_FILES = (
    "manifest.json", "background.js", "core.js", "tracker.js", "probe.js",
    "e3-page.js", "README.html", "icons/icon-16.png", "icons/icon-32.png",
    "icons/icon-48.png", "icons/icon-128.png",
)


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
