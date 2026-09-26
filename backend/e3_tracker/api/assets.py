"""Serve frontend-owned assets with the same URLs in every environment."""
from pathlib import Path

from flask import send_from_directory


FRONTEND_ASSET_DIR = Path(__file__).resolve().parents[3] / "frontend" / "static"


def register_frontend_assets(app):
    def frontend_asset(filename):
        return send_from_directory(FRONTEND_ASSET_DIR, filename, max_age=0)

    app.add_url_rule("/assets/<path:filename>", "frontend_asset", frontend_asset)
