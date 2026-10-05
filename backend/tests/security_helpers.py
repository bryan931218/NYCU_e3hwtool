"""Functional tests send valid CSRF tokens without disabling application checks."""

import secrets
from flask.testing import FlaskClient
from itsdangerous import URLSafeTimedSerializer


class CSRFClient(FlaskClient):
    def open(self, *args, **kwargs):
        if str(kwargs.get("method", "GET")).upper() not in {"GET", "HEAD", "OPTIONS"}:
            path = kwargs.get("path") or (args[0] if args and isinstance(args[0], str) else "/")
            with self.session_transaction(path=path, base_url=kwargs.get("base_url")) as browser_session:
                browser_session.setdefault("csrf_token", secrets.token_hex(32))
                seed = browser_session["csrf_token"]
            token = URLSafeTimedSerializer(
                self.application.secret_key, salt="wtf-csrf-token"
            ).dumps(seed)
            origin = (
                kwargs.get("base_url")
                or f"{self.application.config.get('PREFERRED_URL_SCHEME', 'http')}://{self.application.config.get('SERVER_NAME') or 'localhost'}"
            )
            kwargs["headers"] = {
                "Referer": origin + "/",
                **dict(kwargs.get("headers") or {}),
                "X-CSRFToken": token,
            }
        return super().open(*args, **kwargs)


def csrf_client(app):
    return CSRFClient(app, app.response_class)
