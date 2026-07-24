"""Spark SQL execution — run the generated statement through the session.

The payload is the single executable unit ``create_ddl`` produced: a schema
create, or a ``CREATE OR REPLACE VIEW``/``TABLE``. It runs through the provided
Spark session — the same session across sequences, so a view registered earlier
is in the catalog for a later one. The SQL analytics endpoint is never used;
Spark views are Spark-catalog objects and resolve there.
"""

from __future__ import annotations

from typing import Any

from ...errors import InstallError
from ..models import BuildAction
from .base import InstallationContext


class SparkSqlExecutor:
    name = "spark_sql"

    def execute(
        self,
        action: BuildAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if payload is None:
            raise InstallError(f"spark_sql action {action.id!r} has no payload")
        if context.spark is None:
            raise InstallError(
                f"spark_sql action {action.id!r} needs a Spark session but none was provided"
            )
        statement = payload.decode("utf-8").strip()
        context.spark.sql(statement)
        return {"statement_first_line": statement.splitlines()[0] if statement else ""}
