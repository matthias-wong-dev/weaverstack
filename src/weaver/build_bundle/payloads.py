"""BuildBundle payload naming and hashing."""

from __future__ import annotations

import hashlib

PAYLOAD_ROOT = "payload"


def sequence_dir(number: int, slug: str) -> str:
    return f"{number:03d}-{slug}"


def payload_path(number: int, slug: str, filename: str) -> str:
    return f"{PAYLOAD_ROOT}/{sequence_dir(number, slug)}/{filename}"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
