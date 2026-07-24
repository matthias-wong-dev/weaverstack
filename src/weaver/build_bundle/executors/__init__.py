"""Executor dispatch for build actions — all build, none load."""

from __future__ import annotations

from .base import ActionExecutor, InstallationContext, ResolvedTarget
from .folder import FolderExecutor
from .prune import PruneExecutor
from .spark_sql import SparkSqlExecutor
from .tsql import TSqlExecutor


def default_executors() -> dict[str, ActionExecutor]:
    """The executor registry, by name — the names actions carry."""

    return {
        SparkSqlExecutor.name: SparkSqlExecutor(),
        FolderExecutor.name: FolderExecutor(),
        PruneExecutor.name: PruneExecutor(),
        TSqlExecutor.name: TSqlExecutor(),
    }


__all__ = [
    "ActionExecutor",
    "InstallationContext",
    "ResolvedTarget",
    "SparkSqlExecutor",
    "FolderExecutor",
    "PruneExecutor",
    "TSqlExecutor",
    "default_executors",
]
