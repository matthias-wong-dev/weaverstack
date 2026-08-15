"""Fabric Spark identifier rendering and catalogue operations.

The package remains importable without PySpark: callers supply a way to run
statements, and nothing here holds a Spark session of its own.
"""

from __future__ import annotations

from .catalogue import SparkCatalogue
from .target import FabricSparkTarget, escaped, identifier

__all__ = [
    "FabricSparkTarget",
    "SparkCatalogue",
    "escaped",
    "identifier",
]
