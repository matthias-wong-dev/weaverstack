"""Catalogue operations against one Spark destination.

Every operation is a statement returning rows, so this needs a way to run Spark
SQL rather than a session of its own. In a session that is ``spark.sql``; from a
desktop it is the Session's Spark SQL capability, and the statements cross while
the reading stays here.

Each operation qualifies the destination rather than relying on the attached
catalogue. Schema discovery reads the destination's ``Tables/`` area because
Fabric cannot list schemas for an arbitrary Lakehouse. Naming alone is
:class:`~weaver.spark.naming.SparkNaming`, which needs neither.
"""

from __future__ import annotations

from typing import Any

from ..errors import InstallError
from .destination import SparkDestination
from .naming import SparkNaming, escaped as _escaped


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
        self.names = SparkNaming(destination)
        self._run = _session_runner(spark)
        if destination.case_sensitive_analysis:
            # A session that was not built as an emulator session still needs
            # the emulator's policy.
            from .session import apply_emulator_analysis_policy

            apply_emulator_analysis_policy(spark)

    @classmethod
    def over_sql(cls, run_sql, destination: SparkDestination) -> "SparkCatalogue":
        """A catalogue reached by running statements, with no session here.

        ``run_sql(statement)`` returns the statement's rows as dictionaries —
        :meth:`weaver.session.base.Session.execute_spark_sql`. Wherever that runs
        is where the catalogue is.
        """

        catalogue = cls.__new__(cls)
        catalogue.spark = None
        catalogue.destination = destination
        catalogue.names = SparkNaming(destination)
        catalogue._run = run_sql
        return catalogue

    # --- naming -----------------------------------------------------------

    def qualify(self, schema: str, name: str) -> str:
        return self.names.qualify(schema, name)

    def qualified_schema(self, schema: str) -> str:
        return self.names.qualified_schema(schema)

    def expand(self, statement: str) -> str:
        """One payload's object tokens, resolved to this destination."""

        return self.names.expand(statement)

    # --- execution ---------------------------------------------------------

    def sql(self, statement: str) -> list[dict]:
        """Run one statement here, with its object tokens resolved first."""

        return self._run(self.expand(statement))

    def rows(self, statement: str) -> list[dict]:
        """Run one statement already addressed to this destination."""

        return self._run(statement)

    # --- structure ---------------------------------------------------------

    def create_schema(self, schema: str, *, if_not_exists: bool = True) -> str:
        """Create a schema in this destination, and return the statement run."""

        statement = self.names.create_schema_statement(
            schema, if_not_exists=if_not_exists
        )
        self._run(statement)
        return statement

    def register_external_table(self, schema: str, name: str, location: str) -> str:
        """Name a table in this destination whose storage it does not own.

        This exists for an alias in the local emulator. Fabric discovers a
        OneLake shortcut under a Lakehouse's ``Tables`` area by itself; local
        Spark discovers nothing, so the emulator says what Fabric infers.

        Dropped first because an alias is a pointer and re-pointing one is not
        destructive: dropping an *external* table removes the registration and
        never the storage.
        """

        qualified = self.qualify(schema, name)
        self._run(f"DROP TABLE IF EXISTS {qualified}")
        statement = (
            f"CREATE TABLE {qualified} USING DELTA LOCATION '{_escaped(location)}'"
        )
        self._run(statement)
        return statement

    # --- discovery ---------------------------------------------------------

    def schema_exists(self, schema: str) -> bool:
        return self._present(f"DESCRIBE SCHEMA {self.qualified_schema(schema)}")

    def exists(self, schema: str, name: str) -> bool:
        """Whether one object exists in this destination, table or view."""

        return self._present(f"DESCRIBE TABLE {self.qualify(schema, name)}")

    def columns_of(self, qualified: str) -> tuple[str, ...]:
        """One object's column names, in order.

        ``DESCRIBE TABLE`` pads its output with a blank row and a partition
        section, so anything without a column name ends the columns.
        """

        names = []
        for row in self._run(f"DESCRIBE TABLE {qualified}"):
            name = (row.get("col_name") or "").strip()
            if not name or name.startswith("#"):
                break
            names.append(name)
        return tuple(names)

    def views(self, schema: str) -> tuple[str, ...]:
        """Persistent view names in one schema of this destination.

        Views are catalogue-only — there is no directory to find them in — so
        this is the one part of an inventory that has to be asked of Spark.
        """

        return self._named(
            f"SHOW VIEWS IN {self.qualified_schema(schema)}", "viewName"
        )

    def tables(self, schema: str) -> tuple[str, ...]:
        """Table names in one schema of this destination.

        ``SHOW TABLES`` returns views as well, so the views are taken back out.
        """

        views = {name.lower() for name in self.views(schema)}
        found = self._named(f"SHOW TABLES IN {self.qualified_schema(schema)}", "tableName")
        return tuple(name for name in found if name.lower() not in views)

    def _named(self, statement: str, column: str) -> tuple[str, ...]:
        names = []
        for row in self._absent_as_empty(statement):
            if row.get("isTemporary"):
                continue
            name = row.get(column) or row.get("name")
            if name:
                names.append(name)
        return tuple(names)

    def _absent_as_empty(self, statement: str) -> list[dict]:
        """Run a listing, reading an absent schema as an empty one.

        A schema that is not there holds nothing, which is the answer an
        inventory wants, and both workspaces raise for it rather than returning
        no rows. Everything else propagates: a real failure read as "nothing
        here" tells the next build that nothing is managed.
        """

        try:
            return self._run(statement)
        except Exception as exception:
            if is_absent(exception):
                return []
            raise

    def _present(self, statement: str) -> bool:
        """Whether a ``DESCRIBE`` finds something, rather than reporting absence."""

        try:
            self._run(statement)
        except Exception as exception:
            if is_absent(exception):
                return False
            raise
        return True


def _session_runner(spark: Any):
    """Statements against a live session, answering rows as dictionaries."""

    def run(statement: str) -> list[dict]:
        result = spark.sql(statement)
        collect = getattr(result, "collect", None)
        if collect is None:
            return []
        return [row.asDict() for row in collect()]

    return run


#: Spark's error classes for something that is not there. A missing Lakehouse
#: reports the schema one, which is what we want: an inventory of somewhere that
#: is not there is empty either way.
_ABSENT = frozenset({"SCHEMA_NOT_FOUND", "TABLE_OR_VIEW_NOT_FOUND"})


def is_absent(exception: Exception) -> bool:
    """Whether this means "not created yet" rather than "went wrong".

    Keyed on Spark's error class where there is one, so a reworded message
    cannot quietly turn an infrastructure failure into an empty inventory. A
    statement that crossed a boundary arrives as a message rather than a Spark
    exception, so the class name is matched in the text as well.
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
        or "NoSuchTableException" in type(exception).__name__
    )


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
