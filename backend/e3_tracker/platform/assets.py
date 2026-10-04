"""Serve frontend-owned assets with the same URLs in every environment."""

from flask import abort, send_from_directory
from jinja2 import FileSystemLoader, PrefixLoader
from .paths import FRONTEND_ROOT, FRONTEND_OWNERS
from .input_security import safe_link
from .services.account_labels import account_label


def configure_frontend(app):
    app.jinja_env.filters["safe_link"] = safe_link
    app.jinja_env.filters["account_label"] = account_label
    app.jinja_loader = PrefixLoader(
        {
            owner: FileSystemLoader(FRONTEND_ROOT / owner / "templates")
            for owner in FRONTEND_OWNERS
        }
    )


def register_frontend_assets(app):
    def frontend_asset(filename):
        owner, separator, asset = filename.partition("/")
        if not separator or owner not in FRONTEND_OWNERS:
            abort(404)
        response = send_from_directory(FRONTEND_ROOT / owner / "static", asset, max_age=0)
        # Keep a cached copy, but validate its ETag on each use so deployments stay fresh.
        response.headers["Cache-Control"] = "public, no-cache, max-age=0, must-revalidate"
        return response

    app.add_url_rule("/assets/<path:filename>", "frontend_asset", frontend_asset)

    def discord_cover():
        return send_from_directory(
            FRONTEND_ROOT / "study" / "static" / "discord", "e3-study-cover.png"
        )

    # Preserve URLs already published to Discord clients.
    app.add_url_rule(
        "/static/discord/e3-study-cover.png", "discord_cover", discord_cover
    )
