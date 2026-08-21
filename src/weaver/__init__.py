"""The small notebook-facing Weaver interface.

Internal workspace, repository, target, storage, SQL, bundle, installer, and
control-plane composition seams live in their owning modules.  Importing the
product namespace remains safe without PySpark, Fabric credentials, or the
optional desktop CLI.
"""

from __future__ import annotations

from . import config as _config  # establish declaration/workspace import order
from .errors import (
    CommandError,
    ConfigError,
    IdentityError,
    ValidationError,
    WeaverError,
)
from .lakehouse import Lakehouse, default_lakehouse, lakehouse_for
from .load_report import LoadMessage, LoadNodeReport, LoadResult, LoadRunReport
from .objects import (
    Assumption,
    Folder,
    SparkSqlAssumption,
    SparkSqlTable,
    SparkSqlTest,
    Table,
    Test,
    View,
    WeaverObject,
)
from .operations.build import BuildResult, build
from .operations.check import CheckResult, check
from .operations.install import install
from .operations.load import load
from .operations.test import test
from .operations.wipe import WipeReport, WipeResult, wipe
from .operations.workspace import current_workspace
from .sessions.public import session
from .shortcuts import Shortcut
from .test_report import ValidationNodeReport, ValidationRunReport


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
    "check",
    "CheckResult",
    "install",
    "wipe",
    "WipeReport",
    "WipeResult",
    "load",
    "LoadRunReport",
    "LoadNodeReport",
    "LoadMessage",
    "LoadResult",
    "test",
    "ValidationRunReport",
    # the reusable context every operation accepts
    "session",
    "ValidationNodeReport",
    # authored objects
    "WeaverObject",
    "Shortcut",
    "Folder",
    "Table",
    "SparkSqlTable",
    "View",
    # authored validation
    "Test",
    "Assumption",
    "SparkSqlTest",
    "SparkSqlAssumption",
    "Lakehouse",
    "default_lakehouse",
    "lakehouse_for",
    "current_workspace",
    # common errors
    "WeaverError",
    "CommandError",
    "ConfigError",
    "IdentityError",
    "ValidationError",
]
