"""Executor dispatch for build actions."""

from __future__ import annotations

from .base import ActionExecutor, InstallationContext, ResolvedTarget
from .python import PythonExecutor
from .spark_sql import SparkSqlExecutor
from .tsql import TSqlExecutor


def default_executors() -> dict[str, ActionExecutor]:
    """The executor registry, by name — the names actions carry."""

    return {
        PythonExecutor.name: PythonExecutor(),
        SparkSqlExecutor.name: SparkSqlExecutor(),
        TSqlExecutor.name: TSqlExecutor(),
    }


__all__ = [
    "ActionExecutor",
    "InstallationContext",
    "ResolvedTarget",
    "PythonExecutor",
    "SparkSqlExecutor",
    "TSqlExecutor",
    "default_executors",
]
