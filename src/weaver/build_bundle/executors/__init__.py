"""Dispatch installation actions to their physical executors.

Build generation freezes prune operations as ordinary folder, shortcut or SQL
payloads, so installation never enumerates a target. Every Spark executor uses
its batch's explicit destination.

Generated load procedures need no distinct executor: ``tsql`` runs their
create-or-alter scripts.
"""

from __future__ import annotations

from .base import ActionExecutor, InstallationContext, ResolvedTarget, SkippedExecution
from .copy_files import CopyFilesExecutor
from .endpoint_objects import EndpointObjectsExecutor
from .folder import FolderExecutor
from .load_file import LoadFileExecutor
from .report import ReportDefinitionExecutor, ReportReadbackExecutor
from .runtime_state import RuntimeStateExecutor
from .semantic import (
    SemanticCatalogueExecutor,
    SemanticModelExecutor,
    SemanticReadbackExecutor,
)
from .semantic_wipe import SemanticWipeExecutor
from .shortcut import ShortcutExecutor, ShortcutReadinessExecutor
from .spark_sql import SparkSqlExecutor
from .spark_sql_batch import SparkSqlBatchExecutor
from .spark_table import SparkTableExecutor
from .tsql import TSqlBatchExecutor, TSqlExecutor
from .wipe import LakehouseWipeExecutor


def default_executors() -> dict[str, ActionExecutor]:
    return {
        ReportDefinitionExecutor.name: ReportDefinitionExecutor(),
        ReportReadbackExecutor.name: ReportReadbackExecutor(),
        SemanticModelExecutor.name: SemanticModelExecutor(),
        SemanticCatalogueExecutor.name: SemanticCatalogueExecutor(),
        SemanticReadbackExecutor.name: SemanticReadbackExecutor(),
        SemanticWipeExecutor.name: SemanticWipeExecutor(),
        SparkSqlExecutor.name: SparkSqlExecutor(),
        SparkSqlBatchExecutor.name: SparkSqlBatchExecutor(),
        SparkTableExecutor.name: SparkTableExecutor(),
        FolderExecutor.name: FolderExecutor(),
        CopyFilesExecutor.name: CopyFilesExecutor(),
        LoadFileExecutor.name: LoadFileExecutor(),
        TSqlExecutor.name: TSqlExecutor(),
        TSqlBatchExecutor.name: TSqlBatchExecutor(),
        ShortcutExecutor.name: ShortcutExecutor(),
        ShortcutReadinessExecutor.name: ShortcutReadinessExecutor(),
        RuntimeStateExecutor.name: RuntimeStateExecutor(),
        LakehouseWipeExecutor.name: LakehouseWipeExecutor(),
        EndpointObjectsExecutor.name: EndpointObjectsExecutor(),
    }


__all__ = [
    "ActionExecutor",
    "CopyFilesExecutor",
    "EndpointObjectsExecutor",
    "InstallationContext",
    "ResolvedTarget",
    "RuntimeStateExecutor",
    "ShortcutExecutor",
    "SkippedExecution",
    "SparkSqlExecutor",
    "SparkSqlBatchExecutor",
    "SparkTableExecutor",
    "ShortcutReadinessExecutor",
    "FolderExecutor",
    "LakehouseWipeExecutor",
    "LoadFileExecutor",
    "TSqlExecutor",
    "TSqlBatchExecutor",
    "default_executors",
]
