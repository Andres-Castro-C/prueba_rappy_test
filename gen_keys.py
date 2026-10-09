"""Generate an RSA key pair for Snowflake key-pair authentication."""

import base64
import os
from pathlib import Path

from scripts.runtime_logging import configure_logging


def main() -> int:
    logger = configure_logging(__file__)
    private_key_path = Path.home() / ".snowflake" / "keys" / "rsa_key.p8"
    if private_key_path.exists():
        logger.error(
            "Refusing to overwrite existing private key: %s", private_key_path
        )
        return 1

    try:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        private_key_path.parent.mkdir(parents=True, exist_ok=True)
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        with private_key_path.open("xb") as private_key_file:
            private_key_file.write(private_key_bytes)
        if os.name != "nt":
            private_key_path.chmod(0o600)

        public_key = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    except (ImportError, OSError, ValueError):
        logger.exception("Failed to generate the Snowflake RSA key pair.")
        return 1

    logger.info("Generated Snowflake RSA key pair at %s.", private_key_path)
    print(f"Private key saved outside the repository: {private_key_path}")
    print("Register this public key value in Snowflake (without PEM header/footer):")
    print(base64.b64encode(public_key).decode("ascii"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
