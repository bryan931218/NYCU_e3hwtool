"""Generate VAPID keys into a private, Git-ignored file without printing secrets."""

import argparse
import base64
import os
from pathlib import Path
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    PrivateFormat,
    PublicFormat,
    NoEncryption,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=Path(".localdata/notification-vapid.env")
    )
    parser.add_argument("--subject", default="https://www.e3hwtool.space")
    args = parser.parse_args()
    if not args.subject.startswith(("mailto:", "https://")):
        parser.error("subject must be a contact mailto: or https: URL")
    key = ec.generate_private_key(ec.SECP256R1())
    b64 = lambda value: base64.urlsafe_b64encode(value).decode().rstrip("=")
    private = b64(key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption()))
    public = b64(
        key.public_key().public_bytes(Encoding.X962, PublicFormat.UncompressedPoint)
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(
            f"E3_PUSH_VAPID_PRIVATE_KEY={private}\nE3_PUSH_VAPID_PUBLIC_KEY={public}\nE3_PUSH_VAPID_SUBJECT={args.subject}\n"
        )
    print(
        f"Keys written to {args.output.resolve()}. Existing keys are never overwritten."
    )


if __name__ == "__main__":
    main()
