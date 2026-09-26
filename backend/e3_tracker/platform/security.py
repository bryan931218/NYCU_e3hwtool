"""Key management and authenticated encryption, independent of either site."""

import json
import base64
import os
import secrets
from pathlib import Path

from cryptography.fernet import Fernet, MultiFernet
from filelock import FileLock


def production_mode():
    return os.getenv("E3_ENV", "").lower() == "production" or bool(
        os.getenv("RAILWAY_ENVIRONMENT_ID") or os.getenv("RAILWAY_ENVIRONMENT_NAME")
    )


def _local_secret(root, name, factory):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    path = root / name
    with FileLock(str(path) + ".lock", timeout=10):
        return _create_local_secret(path, factory)


def _create_local_secret(path, factory):
    # Exclusive creation prevents another worker from replacing an existing key.
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return path.read_text(encoding="ascii").strip()
    value = factory()
    with os.fdopen(fd, "w", encoding="ascii") as stream:
        stream.write(value)
    return value


def signing_key(root):
    value = os.getenv("E3_WEB_SECRET", "").strip()
    if not value:
        if production_mode():
            raise RuntimeError(
                "Production requires E3_WEB_SECRET (random, >= 32 characters)"
            )
        value = _local_secret(root, ".session-key", lambda: secrets.token_urlsafe(48))
    if len(value) < 32 or len(set(value)) < 12 or value == "e3-web-secret":
        raise RuntimeError("E3_WEB_SECRET is weak; generate a new random secret")
    return value


def validate_encryption_key(key):
    fernet = Fernet(key.encode("ascii"))
    if len(set(base64.urlsafe_b64decode(key))) < 12:
        raise ValueError("Encryption key is weak; use Fernet.generate_key()")
    if key == os.getenv("E3_WEB_SECRET"):
        raise ValueError("Signing and encryption keys must be independent")
    return fernet


class CredentialCipher:
    PREFIX = "enc:v1:"

    def __init__(self, root):
        key = os.getenv("E3_DATA_ENCRYPTION_KEY", "").strip()
        if not key:
            if production_mode():
                raise RuntimeError(
                    "Production requires E3_DATA_ENCRYPTION_KEY (Fernet key)"
                )
            key = _local_secret(
                root, ".credential-key", lambda: Fernet.generate_key().decode("ascii")
            )
        previous = os.getenv("E3_DATA_ENCRYPTION_PREVIOUS_KEYS", "")
        self._cipher = MultiFernet(
            [validate_encryption_key(key)]
            + [
                Fernet(part.strip().encode("ascii"))
                for part in previous.split(",")
                if part.strip()
            ]
        )

    def encrypt(self, value, context):
        if value is None:
            return None
        payload = json.dumps([context, str(value)], ensure_ascii=True).encode("utf-8")
        return self.PREFIX + self._cipher.encrypt(payload).decode("ascii")

    def decrypt(self, value, context):
        if value is None:
            return None
        if not str(value).startswith(self.PREFIX):
            raise ValueError("Unencrypted credential cannot be used")
        stored_context, result = json.loads(
            self._cipher.decrypt(value[len(self.PREFIX) :].encode("ascii"))
        )
        if not secrets.compare_digest(
            stored_context.encode("utf-8"), context.encode("utf-8")
        ):
            raise ValueError("Credential context mismatch")
        return result
