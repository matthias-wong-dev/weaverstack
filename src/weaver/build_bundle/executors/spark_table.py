"""Spark SQL table build — inference and creation in one self-contained action.

A Spark SQL table's shape is only settled by running its query in the session, so
its payload is not finished SQL. It is a JSON instruction (built by
:func:`weaver.ses.ddl._spark_table_ddl`) that this executor completes in a single
pass, the Spark counterpart of the old T-SQL self-contained script
(build-philosophy §7.3):

1. run the query and read the resulting ``DataFrame`` schema — Spark resolves the
   column names and types from the logical plan without running a job, so no rows
   are read;
2. validate the columns with the same guards a declared schema passes at parse
   (:func:`weaver.ses.columns.validate_build_columns`), driven entirely by the
   frozen payload — the SES source is never reopened;
3. choose the physical business columns — declared types when declared, the
   query's inferred types otherwise;
4. append Weaver's audit columns;
5. create the table with ``CREATE OR REPLACE TABLE``.

Identity is validated (it must name a produced column) but not materialised on
Delta: an identity/generated column is not portably available on the local Delta
build, and the feature is provisional. T-SQL materialises it; here it is a no-op
beyond validation.
"""

from __future__ import annotations

import json
from typing import Any

from ...errors import InstallError
from ...declaration.columns import validate_build_columns
from ...declaration.metadata import AUDIT_COLUMNS, audit_column_name, PYTHON
from ...spark import tokens
from ..models import BuildAction
from .base import InstallationContext
from .spark_case import exact_identifier_case

#: Reserved audit names, in the Delta (underscored) spelling, for collision
#: detection against an inferred query's own output columns.
_AUDIT_NAMES = {audit_column_name(logical, PYTHON).lower() for logical in AUDIT_COLUMNS}

class SparkTableExecutor:
    name = "spark_table"

    def execute(
        self,
        action: BuildAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if payload is None:
            raise InstallError(f"spark_table action {action.id!r} has no payload")
        if context.spark is None:
            raise InstallError(
                f"spark_table action {action.id!r} needs a Spark session but none "
                "was provided"
            )

        instruction = json.loads(payload.decode("utf-8"))
        catalogue = context.catalogue
        # Both sides are resolved against the batch's destination: the table this
        # creates, and every managed object its query reads. Inferring the shape
        # from a query that resolved through the session's own catalogue would
        # read some other Lakehouse's table of that name — and then create a table
        # of that shape, silently, in the right place.
        qualified = catalogue.expand(instruction["object"])
        query = catalogue.expand(instruction["source_query"])

        # Fabric defaults case-sensitive analysis off. Weaver identities are exact,
        # so the source query and the resulting DDL must share one exact-case scope:
        # otherwise a table created as ``CustomerEnriched`` cannot be consumed by
        # the next action in the same coordinated build.
        with exact_identifier_case(
            context.spark,
            enabled=catalogue.destination.preserve_table_identifier_case,
        ):
            frame = context.spark.sql(query)
            query_columns = tuple(field.name for field in frame.schema.fields)
            query_types = {
                field.name: field.dataType.simpleString() for field in frame.schema.fields
            }

            declared = instruction["declared_columns"]
            declared_names = (
                tuple(name for name, _type, _nn in declared)
                if declared is not None
                else None
            )
            references = tuple(
                (label, column) for label, column in instruction["references"]
            )
            identity = instruction.get("identity_column")
            identity_name = identity[0] if identity else None

            business_columns = validate_build_columns(
                qualified,
                query_columns,
                declared_columns=declared_names,
                references=references,
                identity=identity_name,
            )

            business = self._physical_columns(
                qualified, business_columns, declared, query_types, references
            )
            leading = [tuple(identity)] if identity else []
            physical = leading + business + [
                tuple(entry) for entry in instruction["audit_columns"]
            ]

            statement = _create_table_sql(
                qualified,
                physical,
                column_mapping=instruction.get("column_mapping", True),
            )
            _create_preserving_identifier_case(
                context.spark,
                statement,
                catalogue=catalogue,
                logical_object=instruction["object"],
            )
        return {
            "object": qualified,
            "schema_mode": instruction["schema_mode"],
            "columns": [name for name, _type, _nn in physical],
        }

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
            column for label, column in references if label in ("Primary key", "Not null")
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
        "\nTBLPROPERTIES ('delta.columnMapping.mode' = 'name')" if column_mapping else ""
    )
    return (
        f"CREATE OR REPLACE TABLE {qualified} (\n"
        f"{column_lines}\n"
        ")\n"
        "USING delta"
        f"{mapping}\n"
    )


def _create_preserving_identifier_case(
    spark,
    statement: str,
    *,
    catalogue,
    logical_object: str,
) -> None:
    """Create one exact-case table without leaking session configuration.

    ``CREATE OR REPLACE`` keeps the registered spelling of an object it resolves
    case-insensitively.  That matters when a Lakehouse first built by an older
    Weaver contains ``registry`` and the current declaration says ``Registry``:
    merely enabling case-sensitive analysis for the create does not migrate the
    existing identifier.  Drop that one case-only predecessor first.  Build owns
    and replaces table structure; load is the phase that owns rows.
    """

    if catalogue.destination.preserve_table_identifier_case:
        _drop_case_variant(catalogue, logical_object)
    spark.sql(statement)


def _drop_case_variant(catalogue, logical_object: str) -> None:
    match = tokens.OBJECT.fullmatch(logical_object)
    if match is None:  # expand() reports the useful token error on the main path
        return
    schema, declared = match.groups()
    matches = [
        existing
        for existing in catalogue.tables(schema)
        if existing.casefold() == declared.casefold()
    ]
    if not matches or matches == [declared]:
        return
    if len(matches) != 1:
        raise InstallError(
            f"{schema}.{declared}: target contains case-colliding tables: "
            + ", ".join(sorted(matches))
        )
    catalogue.spark.sql(f"DROP TABLE {catalogue.qualify(schema, matches[0])}")


def _ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"
