"""The small notebook-facing Weaver interface.

Internal workspace, repository, target, storage, SQL, bundle, installer, and
control-plane composition seams live in their owning modules.  Importing the
product namespace remains safe without PySpark, Fabric credentials, or the
optional desktop CLI.
"""

from __future__ import annotations

from . import config as _config  # establish declaration/workspace import order
from .errors import CommandError, ConfigError, IdentityError, WeaverError
from .operations import BuildResult, WipeReport, WipeResult, build, wipe
from .load import load
from .load_report import LoadMessage, LoadNodeReport, LoadResult, LoadRunReport
from .lakehouse import Lakehouse, default_lakehouse, lakehouse_for
from .objects import Folder, SparkSqlTable, Table, View, WeaverObject


def _resolve_version() -> str:
    """Read the git-derived installed distribution version."""

    try:
        from importlib.metadata import version

        return version("weaverstack")
    except Exception:  # pragma: no cover - never worth crashing an import over
        return "0.0.0+unknown"


__version__ = _resolve_version()

__all__ = [
    "__version__",
    # ordinary operations and results
    "build",
    "BuildResult",
    "wipe",
    "WipeReport",
    "WipeResult",
    "load",
    "LoadRunReport",
    "LoadNodeReport",
    "LoadMessage",
    "LoadResult",
    # authored objects
    "WeaverObject",
    "Folder",
    "Table",
    "SparkSqlTable",
    "View",
    "Lakehouse",
    "default_lakehouse",
    "lakehouse_for",
    # common errors
    "WeaverError",
    "CommandError",
    "ConfigError",
    "IdentityError",
]
