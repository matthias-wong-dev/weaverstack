"""Run one fully addressed Spark SQL payload against its batch target."""

from __future__ import annotations

from typing import Any

from ...errors import InstallError
from ..models import InstallAction
from .base import InstallationContext


class SparkSqlExecutor:
    name = "spark_sql"

    def execute(
        self,
        action: InstallAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if payload is None:
            raise InstallError(f"spark_sql action {action.id!r} has no payload")
        if context.spark_sql is None:
            raise InstallError(
                f"spark_sql action {action.id!r} has no way to run a Spark "
                "statement: this context offers no Spark SQL capability"
            )
        statement = payload.decode("utf-8").strip()
        context.spark_sql(statement, exact_case=True)
        return {
            "destination": context.destination.item,
            "statement_first_line": statement.splitlines()[0] if statement else "",
        }
