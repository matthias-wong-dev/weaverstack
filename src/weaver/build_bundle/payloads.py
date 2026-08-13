"""Payload naming and hashing for BuildBundle definitions.

Payload directories follow assembled sequence numbers so their layout mirrors
execution order.
"""

from __future__ import annotations

import hashlib

PAYLOAD_ROOT = "payload"


def sequence_dir(number: int, slug: str) -> str:
    """The payload subdirectory for one sequence, e.g. ``003-build-objects``."""

    return f"{number:03d}-{slug}"


def payload_path(number: int, slug: str, filename: str) -> str:
    """A bundle-relative payload path under its sequence directory."""

    return f"{PAYLOAD_ROOT}/{sequence_dir(number, slug)}/{filename}"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
