"""Spark catalogue operations addressed to one Fabric Lakehouse in full."""

from __future__ import annotations

from typing import Any

from ..errors import InstallError
from .target import FabricSparkTarget


class SparkCatalogue:
    """Catalogue operations against one logical Spark destination."""

    def __init__(self, spark: Any, destination: FabricSparkTarget) -> None:
        if spark is None:
            raise InstallError(
                f"no Spark session was provided for Lakehouse {destination.item!r}. "
                "Provide a Spark session or use SparkCatalogue.over_sql(...)"
            )
        self.spark = spark
        self.destination = destination
        self._run = _session_runner(spark)

    @classmethod
    def over_sql(cls, run_sql, destination: FabricSparkTarget) -> "SparkCatalogue":
        """Use a callable that returns each Spark SQL statement's rows as mappings."""

        catalogue = cls.__new__(cls)
        catalogue.spark = None
        catalogue.destination = destination
        catalogue._run = run_sql
        return catalogue

    # --- naming -----------------------------------------------------------

    def qualify(self, schema: str, name: str) -> str:
        return self.destination.qualify(schema, name)

    def qualified_schema(self, schema: str) -> str:
        return self.destination.qualified_schema(schema)

    # --- execution ---------------------------------------------------------

    def sql(self, statement: str) -> list[dict]:
        return self._run(statement)

    def rows(self, statement: str) -> list[dict]:
        return self._run(statement)

    # --- structure ---------------------------------------------------------

    def create_schema(self, schema: str, *, if_not_exists: bool = True) -> str:
        """Create a schema in this destination, and return the statement run."""

        statement = self.destination.create_schema_statement(
            schema, if_not_exists=if_not_exists
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

        Views are catalogue-only, there is no directory to find them in, so
        this is the one part of an inventory that has to be asked of Spark.
        """

        return self._named(f"SHOW VIEWS IN {self.qualified_schema(schema)}", "viewName")

    def tables(self, schema: str) -> tuple[str, ...]:
        """Table names in one schema of this destination.

        ``SHOW TABLES`` returns views as well, so the views are taken back out.
        """

        views = {name.lower() for name in self.views(schema)}
        found = self._named(
            f"SHOW TABLES IN {self.qualified_schema(schema)}", "tableName"
        )
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
        """Read an absent schema as empty and propagate every other failure."""

        try:
            return self._run(statement)
        except Exception as exception:
            if is_absent(exception):
                return []
            raise

    def _present(self, statement: str) -> bool:
        try:
            self._run(statement)
        except Exception as exception:
            if is_absent(exception):
                return False
            raise
        return True


def _session_runner(spark: Any):
    def run(statement: str) -> list[dict]:
        result = spark.sql(statement)
        collect = getattr(result, "collect", None)
        if collect is None:
            return []
        return [row.asDict() for row in collect()]

    return run


#: Spark reports a missing Lakehouse as a missing schema.
_ABSENT = frozenset({"SCHEMA_NOT_FOUND", "TABLE_OR_VIEW_NOT_FOUND"})


def is_absent(exception: Exception) -> bool:
    """Distinguish absence from other Spark failures.

    Prefer the Spark error class. Cross-session failures carry only its text, so
    class names are recognised there as well.
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
