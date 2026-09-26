"""Validate external links and untrusted uploaded image content."""

import io
import warnings
from urllib.parse import urlsplit

from PIL import Image


def safe_link(value):
    value = str(value or "").strip()
    if any(ord(char) < 32 for char in value) or "\\" in value:
        return "#"
    try:
        parts = urlsplit(value)
    except ValueError:
        return "#"
    if (
        parts.scheme.lower() in {"http", "https"}
        and parts.hostname
        and not parts.username
        and not parts.password
    ):
        return value
    if (
        not parts.scheme
        and not parts.netloc
        and (
            value.startswith("#")
            or (value.startswith("/") and not value.startswith("//"))
        )
    ):
        return value
    return "#"


def validate_note_image(payload, mime_type):
    formats = {
        "image/jpeg": "JPEG",
        "image/png": "PNG",
        "image/webp": "WEBP",
        "image/gif": "GIF",
    }
    with warnings.catch_warnings():
        warnings.simplefilter("error", Image.DecompressionBombWarning)
        with Image.open(io.BytesIO(payload)) as image:
            if (
                image.format != formats.get(mime_type)
                or image.width * image.height > 20_000_000
            ):
                raise ValueError("Unsupported image format or dimensions")
            image.verify()
