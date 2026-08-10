"""Creating one schema in the destination the batch names.

A schema create is the one piece of build DDL that cannot be a frozen SQL
payload, and the reason is instructive: on local Spark it needs a ``LOCATION``,
and a ``LOCATION`` is a *resolved path*.

Freezing it meant a bundle generated on a laptop carried

.. code-block:: sql

    CREATE SCHEMA IF NOT EXISTS `Sales` LOCATION '/var/folders/…/T/pytest-42/Sales_LH/Tables/Sales'

— a temporary directory, in the hashed plan, deciding where a managed table
lands. Two runs of the same repository produced different bundles, and a bundle
kept overnight named a path that no longer existed (how-does-build-work §15). On
Fabric it froze the opposite mistake: no clause at all, and a bare two-part name,
so the schema was created in whatever Lakehouse the session was attached to
rather than in the destination.

So the action names the schema and nothing else, and the destination decides how
to make one. That is not an installer filling in a semantic decision — which
schema, in which Lakehouse, is settled and in the manifest — it is the same
transport-level resolution every other action gets, applied to a clause that is
purely about storage.
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
        if context.spark is None:
            raise InstallError(
                f"spark_schema action {action.id!r} needs a Spark session but none "
                "was provided"
            )
        schema = json.loads(payload.decode("utf-8"))["schema"]
        catalogue = context.catalogue
        statement = catalogue.create_schema(schema, if_not_exists=False)
        return {
            "destination": catalogue.destination.item,
            "schema": catalogue.qualified_schema(schema),
            "statement": statement,
        }
