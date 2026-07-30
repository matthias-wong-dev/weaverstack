"""Execute one ordered batch of Spark SQL statements from a JSON payload."""

from __future__ import annotations

import json
from typing import Any

from ...errors import InstallError
from ..models import BuildAction
from .base import InstallationContext
from .spark_case import exact_identifier_case


class SparkSqlBatchExecutor:
    name = "spark_sql_batch"

    def execute(
        self,
        action: BuildAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any]:
        if payload is None:
            raise InstallError(f"spark_sql_batch action {action.id!r} has no payload")
        if context.spark is None:
            raise InstallError(
                f"spark_sql_batch action {action.id!r} needs a Spark session"
            )
        try:
            statements = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InstallError(
                f"spark_sql_batch action {action.id!r} has an invalid JSON payload"
            ) from exc
        if not isinstance(statements, list) or not all(
            isinstance(statement, str) and statement.strip()
            for statement in statements
        ):
            raise InstallError(
                f"spark_sql_batch action {action.id!r} must contain SQL strings"
            )
        with exact_identifier_case(
            context.spark,
            enabled=context.catalogue.destination.preserve_table_identifier_case,
        ):
            for statement in statements:
                context.spark.sql(context.catalogue.expand(statement.strip()))
        return {
            "destination": context.catalogue.destination.item,
            "statement_count": len(statements),
        }
