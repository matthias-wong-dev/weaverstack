"""Statement text against one Spark destination, with no session behind it.

Naming and execution are separate needs. Qualifying an object, expanding a
payload's tokens and rendering a ``CREATE SCHEMA`` are all decided by the
destination alone, and an installer running on a desktop has a destination but no
Spark session of its own. :class:`SparkNaming` is that half;
:class:`~weaver.spark.catalogue.SparkCatalogue` is the half that needs a session,
and delegates its naming here.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import tokens
from .destination import SparkDestination


@dataclass(frozen=True)
class SparkNaming:
    """What one destination calls things, and the statements that follow from it."""

    destination: SparkDestination

    @property
    def exact_case(self) -> bool:
        """Whether statements here must be analysed with Weaver's identifier case."""

        return self.destination.preserve_table_identifier_case

    def qualify(self, schema: str, name: str) -> str:
        return self.destination.qualify(schema, name)

    def qualified_schema(self, schema: str) -> str:
        return self.destination.qualified_schema(schema)

    def expand(self, statement: str) -> str:
        """One payload's object tokens, resolved to this destination."""

        return tokens.expand(statement, self.destination)

    def create_schema_statement(
        self, schema: str, *, if_not_exists: bool = True
    ) -> str:
        """The ``CREATE SCHEMA`` this destination needs, ready to run.

        The ``LOCATION`` clause is the destination's business, not the planner's:
        local Spark needs one so a managed table lands under the Lakehouse's
        ``Tables`` area, and a schema-enabled Fabric Lakehouse pins it natively and
        must not be given one. It is also a resolved path, so it could not have
        been frozen into a payload without tying the bundle to the machine that
        generated it (how-does-build-work §15).
        """

        qualifier = " IF NOT EXISTS" if if_not_exists else ""
        statement = f"CREATE SCHEMA{qualifier} {self.qualified_schema(schema)}"
        location = self.destination.schema_location(schema)
        if location is not None:
            statement += f" LOCATION '{escaped(location)}'"
        return statement


def escaped(value: str) -> str:
    """One string literal's content, safe inside single quotes."""

    return value.replace("\\", "\\\\").replace("'", "\\'")


__all__ = ["SparkNaming", "escaped"]
