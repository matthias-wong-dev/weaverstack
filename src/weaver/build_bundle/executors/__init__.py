"""Executor dispatch for InstallActions.

``spark_sql`` runs one create or frozen ``DROP``; ``spark_sql_batch`` runs an
ordered catalogue payload as one reported action. ``spark_schema`` makes one
schema, ``spark_table`` completes a table whose shape only the session knows,
and ``folder`` makes or removes a directory. ``tsql`` is the Warehouse SQL path.
``alias`` points one Lakehouse name at another item's object, and ``sql_endpoint``
syncs a Lakehouse's SQL analytics endpoint.

``load_file`` writes one file of the deployed runtime tree, or removes one the
source has stopped claiming. A generated load procedure needs no executor of its
own: a create-or-alter is T-SQL, which ``tsql`` runs.

There is no prune executor — a build freezes its drops as payloads, so the
installer never enumerates the target.

Every Spark executor addresses the destination its batch names, never what the
session is attached to.
"""

from __future__ import annotations

from .alias import AliasExecutor
from .base import ActionExecutor, InstallationContext, ResolvedTarget, SkippedExecution
from .folder import FolderExecutor
from .load_file import LoadFileExecutor
from .spark_schema import SparkSchemaExecutor
from .spark_sql import SparkSqlExecutor
from .spark_sql_batch import SparkSqlBatchExecutor
from .spark_table import SparkTableExecutor
from .sql_endpoint_refresh import SqlEndpointRefreshExecutor
from .tsql import TSqlBatchExecutor, TSqlExecutor


def default_executors() -> dict[str, ActionExecutor]:
    """The executor registry, by name — the names actions carry."""

    return {
        SparkSqlExecutor.name: SparkSqlExecutor(),
        SparkSqlBatchExecutor.name: SparkSqlBatchExecutor(),
        SparkSchemaExecutor.name: SparkSchemaExecutor(),
        SparkTableExecutor.name: SparkTableExecutor(),
        FolderExecutor.name: FolderExecutor(),
        LoadFileExecutor.name: LoadFileExecutor(),
        TSqlExecutor.name: TSqlExecutor(),
        TSqlBatchExecutor.name: TSqlBatchExecutor(),
        AliasExecutor.name: AliasExecutor(),
        SqlEndpointRefreshExecutor.name: SqlEndpointRefreshExecutor(),
    }


__all__ = [
    "ActionExecutor",
    "InstallationContext",
    "ResolvedTarget",
    "AliasExecutor",
    "SkippedExecution",
    "SparkSchemaExecutor",
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
