"""Spark SQL execution — run the generated statement against the batch's target.

The payload is the single executable unit ``create_ddl`` produced: a ``CREATE OR
REPLACE VIEW``/``TABLE``, or a frozen prune ``DROP``. It names its objects
logically — ``{{object:Sales.Customer}}`` — and this resolves them against the
batch's destination before running the statement.

Resolution matters here: a two-part name resolves through the session's current
catalogue, which is the Weaver Lakehouse, so an unresolved statement would create
the object in the control plane and read it back from there.
"""

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
        names = context.names
        statement = names.expand(payload.decode("utf-8").strip())
        context.spark_sql(statement, exact_case=names.exact_case)
        # The destination is reported, not just used: an install report that says
        # which Lakehouse each statement ran against is the record a reviewer needs
        # when the answer used to depend on what the session was attached to.
        return {
            "destination": names.destination.item,
            "statement_first_line": statement.splitlines()[0] if statement else "",
        }
