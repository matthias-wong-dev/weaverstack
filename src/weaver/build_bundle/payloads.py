"""Payload naming and hashing — where a generated definition lives in a bundle.

A bundle's ``payload/`` tree groups definitions by the sequence that runs them,
so the directory order mirrors the deployment order and a reviewer can read it
top to bottom. This module owns those names and the payload hash, so the planner
does not scatter path arithmetic through its logic.

There are deliberately no reserved sequence numbers here. A stage's number comes
from its position in the assembled plan — see :mod:`weaver.build_bundle.stages`
— so nothing has to leave arithmetic headroom for a repository's dependency depth
and nothing can collide with a region another phase claimed.
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
