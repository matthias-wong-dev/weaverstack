"""Run one fully addressed Spark SQL payload against its batch target."""

from __future__ import annotations

from typing import Any

from ...errors import InstallError
from ..models import InstallAction
from .base import InstallationContext


class SparkSqlExecutor:
    name = "spark_sql"

    def statement(self, action: InstallAction, payload: bytes | None) -> str:
        """Validate and decode one frozen Spark SQL payload."""

        if payload is None:
            raise InstallError(f"spark_sql action {action.id!r} has no payload")
        return payload.decode("utf-8").strip()

    @staticmethod
    def details(statement: str, context: InstallationContext) -> dict[str, Any]:
        return {
            "destination": context.destination.item,
            "statement_first_line": statement.splitlines()[0] if statement else "",
        }

    def execute(
        self,
        action: InstallAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if context.spark_sql is None:
            raise InstallError(
                f"spark_sql action {action.id!r} has no way to run a Spark "
                "statement: this context offers no Spark SQL capability"
            )
        statement = self.statement(action, payload)
        context.spark_sql(statement, exact_case=True)
        return self.details(statement, context)
