"""Spark SQL execution — run the generated statement against the batch's target.

The payload is the single executable unit ``create_ddl`` produced: a ``CREATE OR
REPLACE VIEW``/``TABLE``, or a frozen prune ``DROP``. It names its objects
logically — ``{{object:Sales.Customer}}`` — and this resolves those names against
the destination the batch is bound to before running the statement.

That resolution is the whole difference between a build that works and one that
looks like it does. A two-part name resolves through the session's *current*
catalogue, and the session is attached to the Weaver Lakehouse, so every
destination statement would have landed in the control plane. On Fabric the
object would have been created in the wrong Lakehouse and then read back from the
wrong Lakehouse, and the assertion would have passed.

The same session runs every sequence, so a view registered earlier is in the
catalogue for a later one — now under a name that says which Lakehouse it is in.
The SQL analytics endpoint is never used; Spark views are Spark-catalogue objects
and resolve there.
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
        catalogue = context.catalogue
        statement = catalogue.expand(payload.decode("utf-8").strip())
        context.spark.sql(statement)
        # The destination is reported, not just used: an install report that says
        # which Lakehouse each statement ran against is the record a reviewer needs
        # when the answer used to depend on what the session was attached to.
        return {
            "destination": catalogue.destination.item,
            "statement_first_line": statement.splitlines()[0] if statement else "",
        }
