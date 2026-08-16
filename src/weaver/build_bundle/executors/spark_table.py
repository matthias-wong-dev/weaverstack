"""Spark SQL table build — shape inference and creation in one action.

A Spark SQL table's shape is settled by asking Spark about its query, so the
payload is a JSON instruction rather than finished SQL
(:func:`weaver.declaration.ddl._spark_table_ddl`). This executor completes it:

1. ``DESCRIBE QUERY``, after whatever setup the body needs — one piece of work,
   so a temporary view the query reads is registered in the session describing it;
2. validate the columns with the guards a declared schema passes at parse
   (:func:`weaver.declaration.columns.validate_build_columns`);
3. choose the physical business columns — declared types when declared, the
   query's otherwise;
4. append Weaver's audit columns;
5. create the table with strict ``CREATE TABLE``.

Only 1 and 5 reach Spark. ``DESCRIBE QUERY`` returns the output columns in order
and each type in ``simpleString`` form without reading a row.

A Delta table has no identity column, so the ``Identity`` header is a
Warehouse-only declaration the parser refuses elsewhere.
"""

from __future__ import annotations

import json
from typing import Any

from ...declaration.columns import validate_build_columns
from ...declaration.metadata import AUDIT_COLUMNS, PYTHON, audit_column_name
from ...errors import InstallError
from ..models import InstallAction
from .base import InstallationContext

#: Reserved audit names, in the Delta (underscored) spelling, for collision
#: detection against an inferred query's own output columns.
_AUDIT_NAMES = {audit_column_name(logical, PYTHON).lower() for logical in AUDIT_COLUMNS}


class SparkTableExecutor:
    name = "spark_table"

    def execute(
        self,
        action: InstallAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if payload is None:
            raise InstallError(f"spark_table action {action.id!r} has no payload")
        if context.spark_sql is None or context.spark_sql_batch is None:
            raise InstallError(
                f"spark_table action {action.id!r} has no way to run a Spark "
                "statement: this context offers no Spark SQL capability"
            )

        instruction = json.loads(payload.decode("utf-8"))
        # Both sides are resolved against the batch's destination: the table this
        # creates, and every managed object its query reads. Inferring the shape
        # from a query that resolved through the session's own catalogue would
        # read some other Lakehouse's table of that name — and then create a table
        # of that shape, silently, in the right place.
        qualified = instruction["object"]
        query = instruction["source_query"]

        # Fabric defaults case-sensitive analysis off, and Weaver identities are
        # exact, so the query and the DDL must share one scope — or a table
        # created as ``CustomerEnriched`` cannot be read by the next action.

        # The setup and the describe are one piece of work: a view registered in
        # a different session is one the query cannot see.
        setup = list(instruction.get("setup") or ())
        query_columns, query_types = self._query_shape(
            [*setup, f"DESCRIBE QUERY {query}"],
            context,
            action=action,
            qualified=qualified,
        )

        declared = instruction["declared_columns"]
        declared_names = (
            tuple(name for name, _type, _nn in declared)
            if declared is not None
            else None
        )
        references = tuple(
            (label, column) for label, column in instruction["references"]
        )
        business_columns = validate_build_columns(
            qualified,
            query_columns,
            declared_columns=declared_names,
            references=references,
        )

        business = self._physical_columns(
            qualified, business_columns, declared, query_types, references
        )
        physical = business + [tuple(entry) for entry in instruction["audit_columns"]]

        statement = _create_table_sql(
            qualified,
            physical,
            column_mapping=instruction.get("column_mapping", True),
        )
        context.spark_sql(statement, exact_case=True)
        return {
            "object": qualified,
            "schema_mode": instruction["schema_mode"],
            "columns": [name for name, _type, _nn in physical],
        }

    def _query_shape(
        self,
        statements: list[str],
        context: InstallationContext,
        *,
        action: InstallAction,
        qualified: str,
    ) -> tuple[tuple[str, ...], dict[str, str]]:
        """The query's output columns, in order, with each column's type.

        A query that does not resolve fails here rather than at the create, so
        the failure names the action and carries Spark's message.
        """

        try:
            rows = context.spark_sql_batch(statements, exact_case=True)
        except Exception as exc:
            raise InstallError(
                f"spark_table action {action.id!r} could not read the shape of "
                f"the query behind {qualified}: {exc}"
            ) from exc

        columns: list[str] = []
        types: dict[str, str] = {}
        for row in rows:
            name = row.get("col_name")
            data_type = row.get("data_type")
            if not name or not data_type:
                raise InstallError(
                    f"spark_table action {action.id!r}: DESCRIBE QUERY answered "
                    f"for {qualified} with a row naming no column and type: {row!r}"
                )
            columns.append(name)
            types[name] = data_type
        if not columns:
            raise InstallError(
                f"spark_table action {action.id!r}: the query behind {qualified} "
                "produces no columns"
            )
        return tuple(columns), types

    def _physical_columns(
        self,
        qualified: str,
        business_columns: tuple[str, ...],
        declared: list | None,
        query_types: dict[str, str],
        references: tuple[tuple[str, str], ...],
    ) -> list[tuple[str, str, bool]]:
        """The business columns as ``(name, type, not_null)``.

        Declared columns carry their declared type and not-null. Inferred columns
        take the query's type and are not null when the primary key or a
        ``Not null`` names them — the same loading contract, applied to a shape
        the query supplied rather than a declaration.
        """

        collisions = [name for name in business_columns if name.lower() in _AUDIT_NAMES]
        if collisions:
            raise InstallError(
                f"{qualified}: the query produces column(s) reserved for Weaver's "
                "audit columns: " + ", ".join(collisions)
            )

        if declared is not None:
            declared_by_name = {name: (type_, nn) for name, type_, nn in declared}
            return [(name, *declared_by_name[name]) for name in business_columns]

        not_null_names = {
            column
            for label, column in references
            if label in ("Primary key", "Not null")
        }
        return [
            (name, query_types[name], name in not_null_names)
            for name in business_columns
        ]


def _create_table_sql(
    qualified: str, columns: list[tuple[str, str, bool]], *, column_mapping: bool
) -> str:
    column_lines = ",\n".join(
        f"    {_ident(name)} {type_}{' NOT NULL' if not_null else ''}"
        for name, type_, not_null in columns
    )
    mapping = (
        "\nTBLPROPERTIES ('delta.columnMapping.mode' = 'name')"
        if column_mapping
        else ""
    )
    return f"CREATE TABLE {qualified} (\n{column_lines}\n)\nUSING delta{mapping}\n"


def _ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"
