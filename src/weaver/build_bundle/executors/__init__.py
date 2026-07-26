"""Executor dispatch for build actions — all build, none load.

Four executors do the work: ``spark_sql`` runs a create or a frozen prune
``DROP``, ``spark_schema`` makes one schema in the destination, ``spark_table``
completes a Spark SQL table whose shape only the session knows, and ``folder``
makes or removes a directory. ``tsql`` is the Warehouse counterpart of the first.

There is no prune executor — a build freezes its drops as payloads, so the
installer never enumerates the target.

Every Spark executor addresses the destination its batch names, and none of them
relies on what the session is attached to.
"""

from __future__ import annotations

from .base import ActionExecutor, InstallationContext, ResolvedTarget
from .folder import FolderExecutor
from .spark_schema import SparkSchemaExecutor
from .spark_sql import SparkSqlExecutor
from .spark_table import SparkTableExecutor
from .tsql import TSqlExecutor


def default_executors() -> dict[str, ActionExecutor]:
    """The executor registry, by name — the names actions carry."""

    return {
        SparkSqlExecutor.name: SparkSqlExecutor(),
        SparkSchemaExecutor.name: SparkSchemaExecutor(),
        SparkTableExecutor.name: SparkTableExecutor(),
        FolderExecutor.name: FolderExecutor(),
        TSqlExecutor.name: TSqlExecutor(),
    }


__all__ = [
    "ActionExecutor",
    "InstallationContext",
    "ResolvedTarget",
    "SparkSchemaExecutor",
    "SparkSqlExecutor",
    "SparkTableExecutor",
    "FolderExecutor",
    "TSqlExecutor",
    "default_executors",
]
