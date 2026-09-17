"""Signatures for physically installable artefacts."""

from __future__ import annotations

import hashlib


def implementation_signature(source_signature: str, implementation_version: int) -> str:
    """Bind source identity to the implementation that installs it."""

    digest = hashlib.sha256()
    digest.update(source_signature.encode("ascii"))
    digest.update(b"\0")
    digest.update(str(implementation_version).encode("ascii"))
    return digest.hexdigest()


__all__ = ["implementation_signature"]
