"""Payload naming and hashing — where a generated definition lives in a bundle.

A bundle's ``payload/`` tree groups definitions by the sequence that runs them,
so the directory order mirrors the deployment order and a reviewer can read it
top to bottom. This module owns those names and the payload hash, so the planner
does not scatter path arithmetic through its logic.
"""

from __future__ import annotations

import hashlib

#: Sequence numbers for the foundational waves. Catalogue reconciliation and
#: prune lead; managed drops and selected build layers follow in fixed steps.
RECONCILIATION_SEQUENCE = 10
PRUNE_CATALOGUE_SEQUENCE = 20
PRUNE_SEQUENCE = 30
MANAGED_CATALOGUE_SEQUENCE = 40
MANAGED_DROP_SEQUENCE_START = 50
OBJECT_SEQUENCE_STEP = 10

#: Catalogue work concludes a build, and its numbers sit far above the object
#: layers because there is no bound on how deep a repository's dependency chain
#: is — the SQL Server system this ports from ran past thirty layers. At ten per
#: layer that leaves room for hundreds more, and :func:`check_sequence_headroom`
#: refuses a plan that ever gets close rather than letting a deep repository
#: silently reorder its own catalogue.
#:
#: The order within the tail is the invariant: dictionaries describe, Installation
#: records the binding, Registry certifies. Registry is last, so a row in it
#: cannot outrun the work it attests to.
CATALOGUE_SEQUENCE = 9000
INSTALLATION_SEQUENCE = 9010
REGISTRY_SEQUENCE = 9020

PAYLOAD_ROOT = "payload"


def check_sequence_headroom(number: int) -> None:
    """Refuse an object layer that would collide with the catalogue tail.

    Reaching this means a repository has ~896 dependency layers, which is far
    beyond anything real — so it is a signal that something is wrong, and failing
    at generation is much better than producing a bundle whose Registry runs
    before the objects it certifies.
    """

    if number >= CATALOGUE_SEQUENCE:
        from ..errors import BuildError

        raise BuildError(
            f"object layer sequence {number} has reached the catalogue sequence "
            f"range ({CATALOGUE_SEQUENCE}) — a repository with this many dependency "
            "layers is almost certainly a cycle or a generation fault"
        )


def sequence_dir(number: int, slug: str) -> str:
    """The payload subdirectory for one sequence, e.g. ``030-build-delta``."""

    return f"{number:03d}-{slug}"


def payload_path(number: int, slug: str, filename: str) -> str:
    """A bundle-relative payload path under its sequence directory."""

    return f"{PAYLOAD_ROOT}/{sequence_dir(number, slug)}/{filename}"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
