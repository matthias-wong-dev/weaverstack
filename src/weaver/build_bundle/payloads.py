"""Payload naming and hashing — where a generated definition lives in a bundle.

A bundle's ``payload/`` tree groups definitions by the sequence that runs them,
so the directory order mirrors the deployment order and a reviewer can read it
top to bottom. This module owns those names and the payload hash, so the planner
does not scatter path arithmetic through its logic.
"""

from __future__ import annotations

import hashlib

#: Sequence numbers for the foundational waves. Prune reconciles the target
#: first; then schemas, folders, and the object layers, one sequence per
#: dependency layer from OBJECT_SEQUENCE_START in steps.
PRUNE_SEQUENCE = 10
SCHEMA_SEQUENCE = 20
FOLDER_SEQUENCE = 30
OBJECT_SEQUENCE_START = 40
OBJECT_SEQUENCE_STEP = 10

PAYLOAD_ROOT = "payload"


def sequence_dir(number: int, slug: str) -> str:
    """The payload subdirectory for one sequence, e.g. ``030-build-delta``."""

    return f"{number:03d}-{slug}"


def payload_path(number: int, slug: str, filename: str) -> str:
    """A bundle-relative payload path under its sequence directory."""

    return f"{PAYLOAD_ROOT}/{sequence_dir(number, slug)}/{filename}"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
