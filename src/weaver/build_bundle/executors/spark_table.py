"""Infer and create a Spark SQL table within one action.

Setup and ``DESCRIBE QUERY`` run in one session, then declared-column constraints
are applied and Weaver audit columns are appended before strict ``CREATE TABLE``.
Only describe and create reach Spark; shape inference reads no data rows.
"""

from __future__ import annotations

import json
from typing import Any

from ...declaration.columns import validate_build_columns
from ...declaration.metadata import (
    AUDIT_COLUMNS,
    PYTHON,
    audit_column_name,
    signature_column_name,
)
from ...errors import InstallError
from ..models import InstallAction
from .base import InstallationContext

# Delta audit names reserved from inferred business columns.
_AUDIT_NAMES = {audit_column_name(logical, PYTHON).lower() for logical in AUDIT_COLUMNS}

# A keyed load writes this internal column, so inferred output cannot also own it.
_SIGNATURE_NAME = signature_column_name(PYTHON).lower()


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
        # Both the query and table are fully qualified for the batch target;
        # session catalogue defaults must not influence inferred shape.
        qualified = instruction["object"]
        query = instruction["source_query"]

        # Setup and describe share one submission so temporary views remain visible.
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
        physical = (
            business
            + [tuple(entry) for entry in instruction["audit_columns"]]
            + [tuple(entry) for entry in instruction.get("internal_columns") or ()]
        )

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
        """Read ordered output names and types, reporting shape failures here."""

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
                    f"spark_table action {action.id!r}: DESCRIBE QUERY returned a "
                    f"row without a column name and data type for {qualified}: {row!r}"
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
        """Return business columns as ``(name, type, not_null)``.

        Declared columns retain their type and nullability. Inferred columns use
        query types and apply declared primary-key and not-null references.
        """

        collisions = [name for name in business_columns if name.lower() in _AUDIT_NAMES]
        if collisions:
            raise InstallError(
                f"{qualified}: the query produces column(s) reserved for Weaver's "
                "audit columns: " + ", ".join(collisions)
            )
        signature = [
            name for name in business_columns if name.lower() == _SIGNATURE_NAME
        ]
        if signature:
            raise InstallError(
                f"{qualified}: the query produces column(s) reserved for Weaver's "
                "row signature column: " + ", ".join(signature)
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
