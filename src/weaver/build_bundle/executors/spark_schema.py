"""Creating one schema in the destination the batch names.

The action names the schema and nothing else, because the statement cannot be
frozen into a payload: local Spark needs a ``LOCATION`` and that is a resolved
path, while a schema-enabled Fabric Lakehouse needs none. Which schema, in which
Lakehouse, is settled in the manifest; how to make one is the destination's.
"""

from __future__ import annotations

import json
from typing import Any

from ...errors import InstallError
from ..models import InstallAction
from .base import InstallationContext


class SparkSchemaExecutor:

    name = "spark_schema"

    def execute(
        self,
        action: InstallAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if payload is None:
            raise InstallError(f"spark_schema action {action.id!r} has no payload")
        if context.spark_sql is None:
            raise InstallError(
                f"spark_schema action {action.id!r} has no way to run a Spark "
                "statement: this context offers no Spark SQL capability"
            )
        schema = json.loads(payload.decode("utf-8"))["schema"]
        names = context.names
        statement = names.create_schema_statement(schema, if_not_exists=False)
        context.spark_sql(statement)
        return {
            "destination": names.destination.item,
            "schema": names.qualified_schema(schema),
            "statement": statement,
        }
