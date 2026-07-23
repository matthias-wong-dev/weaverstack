"""Weaver — the core framework distributed as ``weaverstack``.

The public surface grows one checkpoint at a time. Today it carries the
version and the error hierarchy; build, load and authoring APIs arrive at
their own checkpoints.

The core must remain importable without PySpark, without Fabric credentials
and without the optional CLI. It must never import :mod:`weaver_cli`.
"""

from __future__ import annotations

from .errors import CommandError, WeaverError

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "WeaverError",
    "CommandError",
]
