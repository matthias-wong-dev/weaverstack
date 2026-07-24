"""Executor dispatch for build actions — all build, none load.

Two executors do the work: ``spark_sql`` runs a create or a frozen prune ``DROP``,
and ``folder`` makes or removes a directory. There is no prune executor — a build
freezes its drops as payloads, so the installer never enumerates the target.
"""

from __future__ import annotations

from .base import ActionExecutor, InstallationContext, ResolvedTarget
from .folder import FolderExecutor
from .spark_sql import SparkSqlExecutor
from .tsql import TSqlExecutor


def default_executors() -> dict[str, ActionExecutor]:
    """The executor registry, by name — the names actions carry."""

    return {
        SparkSqlExecutor.name: SparkSqlExecutor(),
        FolderExecutor.name: FolderExecutor(),
        TSqlExecutor.name: TSqlExecutor(),
    }


__all__ = [
    "ActionExecutor",
    "InstallationContext",
    "ResolvedTarget",
    "SparkSqlExecutor",
    "FolderExecutor",
    "TSqlExecutor",
    "default_executors",
]
