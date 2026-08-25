"""Execute one ordered batch of Spark SQL statements from a JSON payload.

The statements belong to one action, so they travel as one piece of work: one
submission where they cross, in order, under one identifier-case scope.
"""

from __future__ import annotations

import json
from typing import Any

from ...errors import InstallError
from ...tokens import substitute_build_datetime
from ..models import InstallAction
from .base import InstallationContext


class SparkSqlBatchExecutor:
    name = "spark_sql_batch"

    def execute(
        self,
        action: InstallAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any]:
        if payload is None:
            raise InstallError(f"spark_sql_batch action {action.id!r} has no payload")
        if context.spark_sql_batch is None:
            raise InstallError(
                f"spark_sql_batch action {action.id!r} has no way to run Spark "
                "statements: this context offers no Spark SQL capability"
            )
        try:
            statements = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InstallError(
                f"spark_sql_batch action {action.id!r} has an invalid JSON payload"
            ) from exc
        if not isinstance(statements, list) or not all(
            isinstance(statement, str) and statement.strip() for statement in statements
        ):
            raise InstallError(
                f"spark_sql_batch action {action.id!r} must contain SQL strings"
            )
        # The build_datetime first: it is scoped to this installation rather than to a
        # destination, and ``expand`` rejects every token it does not itself
        # resolve, so one left behind here would be reported as an unresolvable
        # name instead of reaching the engine.
        resolved = [
            substitute_build_datetime(statement.strip(), context.build_datetime)
            for statement in statements
        ]
        context.spark_sql_batch(resolved, exact_case=True)
        return {
            "destination": context.destination.item,
            "statement_count": len(statements),
        }
