"""Dispatch installation actions to their physical executors.

Build generation freezes prune operations as ordinary folder, shortcut or SQL
payloads, so installation never enumerates a target. Every Spark executor uses
its batch's explicit destination.

Generated load procedures need no distinct executor: ``tsql`` runs their
create-or-alter scripts.
"""

from __future__ import annotations

from .base import ActionExecutor, InstallationContext, ResolvedTarget, SkippedExecution
from .folder import FolderExecutor
from .load_file import LoadFileExecutor
from .runtime_state import RuntimeStateExecutor
from .shortcut import ShortcutExecutor
from .spark_sql import SparkSqlExecutor
from .spark_sql_batch import SparkSqlBatchExecutor
from .spark_table import SparkTableExecutor
from .sql_endpoint_refresh import SqlEndpointRefreshExecutor
from .tsql import TSqlBatchExecutor, TSqlExecutor


def default_executors() -> dict[str, ActionExecutor]:
    return {
        SparkSqlExecutor.name: SparkSqlExecutor(),
        SparkSqlBatchExecutor.name: SparkSqlBatchExecutor(),
        SparkTableExecutor.name: SparkTableExecutor(),
        FolderExecutor.name: FolderExecutor(),
        LoadFileExecutor.name: LoadFileExecutor(),
        TSqlExecutor.name: TSqlExecutor(),
        TSqlBatchExecutor.name: TSqlBatchExecutor(),
        ShortcutExecutor.name: ShortcutExecutor(),
        SqlEndpointRefreshExecutor.name: SqlEndpointRefreshExecutor(),
        RuntimeStateExecutor.name: RuntimeStateExecutor(),
    }


__all__ = [
    "ActionExecutor",
    "InstallationContext",
    "ResolvedTarget",
    "RuntimeStateExecutor",
    "ShortcutExecutor",
    "SkippedExecution",
    "SparkSqlExecutor",
    "SparkSqlBatchExecutor",
    "SparkTableExecutor",
    "SqlEndpointRefreshExecutor",
    "FolderExecutor",
    "LoadFileExecutor",
    "TSqlExecutor",
    "TSqlBatchExecutor",
    "default_executors",
]
