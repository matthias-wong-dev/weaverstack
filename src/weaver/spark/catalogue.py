"""Spark catalogue operations against an explicit destination.

Each operation qualifies the destination rather than relying on the attached
catalogue. Schema discovery reads the destination's ``Tables/`` area because
Fabric cannot list schemas for an arbitrary Lakehouse.
"""

from __future__ import annotations

from typing import Any

from ..errors import InstallError
from . import tokens
from .destination import SparkDestination


class SparkCatalogue:
    """Catalogue operations against one logical Spark destination."""

    def __init__(self, spark: Any, destination: SparkDestination) -> None:
        if spark is None:
            raise InstallError(
                f"a Spark session is needed to reach {destination.item!r}, "
                "and none was provided"
            )
        self.spark = spark
        self.destination = destination
        if destination.case_sensitive_analysis:
            # Local's folded schema is lower-case and every declared object keeps
            # its exact Weaver spelling. Unlike Fabric's catalogue, Spark's local
            # session catalogue cannot look that object up again after reverting
            # to case-insensitive analysis, so this is the emulator's session
            # policy rather than a one-statement override.
            spark.conf.set("spark.sql.caseSensitive", "true")

    # --- naming -----------------------------------------------------------

    def qualify(self, schema: str, name: str) -> str:
        return self.destination.qualify(schema, name)

    def qualified_schema(self, schema: str) -> str:
        return self.destination.qualified_schema(schema)

    def expand(self, statement: str) -> str:
        """One payload's object tokens, resolved to this destination."""

        return tokens.expand(statement, self.destination)

    # --- execution ---------------------------------------------------------

    def sql(self, statement: str) -> Any:
        """Run one statement here, with its object tokens resolved first."""

        return self.spark.sql(self.expand(statement))

    # --- structure ---------------------------------------------------------

    def create_schema(self, schema: str, *, if_not_exists: bool = True) -> str:
        """Create a schema in this destination, and return the statement run.

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
            statement += f" LOCATION '{_escaped(location)}'"
        self.spark.sql(statement)
        return statement

    def register_external_table(self, schema: str, name: str, location: str) -> str:
        """Name a table in this destination whose storage it does not own.

        This exists for one thing: an alias in the local emulator. Fabric
        discovers a OneLake shortcut placed under a Lakehouse's ``Tables`` area by
        itself, and the table simply appears in the catalogue; local Spark
        discovers nothing, so the emulator has to say out loud what Fabric infers.

        Unregistered first rather than created strictly, because an alias is a
        pointer and re-pointing one is not a destructive transition. Dropping an
        *external* table removes the registration and never the storage, which is
        exactly the distinction that makes this safe: the data belongs to the item
        that produced it.
        """

        qualified = self.qualify(schema, name)
        self.spark.sql(f"DROP TABLE IF EXISTS {qualified}")
        statement = (
            f"CREATE TABLE {qualified} USING DELTA LOCATION '{_escaped(location)}'"
        )
        self.spark.sql(statement)
        return statement

    # --- discovery ---------------------------------------------------------

    def schema_exists(self, schema: str) -> bool:
        return bool(self.spark.catalog.databaseExists(self.qualified_schema(schema)))

    def views(self, schema: str) -> tuple[str, ...]:
        """Persistent view names in one schema of this destination.

        Views are catalogue-only — there is no directory to find them in — so this
        is the one part of an inventory that has to be asked of Spark.
        """

        rows = self._rows(f"SHOW VIEWS IN {self.qualified_schema(schema)}")
        names = []
        for row in rows:
            data = row.asDict()
            if data.get("isTemporary"):
                continue
            name = data.get("viewName") or data.get("name")
            if name:
                names.append(name)
        return tuple(names)

    def tables(self, schema: str) -> tuple[str, ...]:
        """Table names in one schema of this destination.

        ``SHOW TABLES`` returns views as well, so the views are taken back out.
        Prune does not use this — a Delta table is a directory, and reading the
        storage is what keeps reconciliation scoped to the one Lakehouse — but a
        test asserting what a build actually created needs to ask the catalogue,
        not the filesystem.
        """

        views = {name.lower() for name in self.views(schema)}
        rows = self._rows(f"SHOW TABLES IN {self.qualified_schema(schema)}")
        names = []
        for row in rows:
            data = row.asDict()
            if data.get("isTemporary"):
                continue
            name = data.get("tableName") or data.get("name")
            if name and name.lower() not in views:
                names.append(name)
        return tuple(names)

    def exists(self, schema: str, name: str) -> bool:
        """Whether one object exists in this destination, table or view."""

        return bool(self.spark.catalog.tableExists(self.qualify(schema, name)))

    def _rows(self, statement: str) -> list:
        """Run a listing, reading an absent schema as an empty one.

        A schema that is not there holds nothing, which is the answer an inventory
        wants — and both workspaces raise for it rather than returning no rows. So the
        absence is tolerated and everything else propagates, narrowly, for the same
        reason :mod:`weaver.catalogue.reader` does it that way: a real failure read
        as "nothing here" tells the next build that nothing is managed.
        """

        try:
            return self.spark.sql(statement).collect()
        except Exception as exception:
            if _is_missing_schema(exception):
                return []
            raise


#: Spark's error class for a namespace that does not exist. A missing Lakehouse
#: reports the same one, which is what we want: an inventory of somewhere that is
#: not there is empty either way.
_ABSENT = frozenset({"SCHEMA_NOT_FOUND"})


def _is_missing_schema(exception: Exception) -> bool:
    """Whether this means "not created yet" rather than "went wrong".

    Keyed on Spark's error class, not on message text, so a reworded message
    cannot quietly turn an infrastructure failure into an empty inventory. The
    class name is consulted only when no error class is available — a stub session
    in a test, or a connector that raises a plain error.
    """

    error_class = getattr(exception, "getErrorClass", None)
    if callable(error_class):
        try:
            found = error_class()
        except Exception:  # pragma: no cover - a broken accessor is not absence
            found = None
        if found:
            return found in _ABSENT
    return any(name in str(exception) for name in _ABSENT) or (
        "NoSuchNamespaceException" in type(exception).__name__
        or "NoSuchDatabaseException" in type(exception).__name__
    )


def _escaped(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def drop_local_destination_catalogue(
    spark: Any, destination: SparkDestination
) -> tuple[str, ...]:
    """Forget every namespace folded beneath one emulated Lakehouse.

    Local CLI sessions use a persistent metastore so a later process can see
    what ``initialise`` and ``build`` registered. A local wipe must therefore
    clear catalogue registrations as well as the Fabric-shaped filesystem tree;
    Fabric performs that bookkeeping itself when its Lakehouse storage is
    emptied and never calls this emulator-only primitive.
    """

    if destination.namespace or not destination.schema_prefix:
        raise InstallError("local catalogue cleanup needs a folded local destination")
    rows = spark.sql("SHOW DATABASES").collect()
    prefix = destination.schema_prefix.casefold()
    schemas = []
    for row in rows:
        data = row.asDict() if hasattr(row, "asDict") else {}
        name = data.get("namespace") or data.get("databaseName") or data.get("schemaName")
        if name and str(name).casefold().startswith(prefix):
            schemas.append(str(name))
    statements = []
    for schema in sorted(schemas, key=str.casefold):
        statement = f"DROP SCHEMA IF EXISTS {destination.qualified_schema(schema[len(destination.schema_prefix):])} CASCADE"
        spark.sql(statement)
        statements.append(statement)
    return tuple(statements)
