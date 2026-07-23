"""The optional desktop CLI — an adapter over :mod:`weaver`.

The dependency direction is one way: this package imports ``weaver``; the core
never imports this package. The CLI owns argument parsing and presentation
only. Build, load and catalogue semantics belong to the core.
"""

from __future__ import annotations

from .main import main

__all__ = ["main"]
