"""Run a semantic model Test or Assumption from its installed definition.

The DAX runs against the model through the Power BI query API. A Test's
Expected SQL runs in its Expected source: T-SQL over TDS for a Warehouse, Spark
SQL for a Lakehouse. Both sides are compared here, with no Spark involved.
"""

from __future__ import annotations

import json
import re

from .declaration.metadata import ASSUMPTION
from .errors import ValidationError
from .runtime.relation_compare import Relation, compare_rows
from .runtime.test_compare import ACTUAL, EXPECTED, SIDE_COLUMN
from .runtime.validation_result import AssumptionResult, TestResult
from .targets import LAKEHOUSE_TARGET

#: The installed definition format this Weaver reads.
DEFINITION_VERSION = 1

#: A DAX result label: ``Table[Column]``, ``'Table name'[Column]`` or
#: ``[Measure]``. The bracketed name is the column; the rest is decoration.
_DAX_LABEL = re.compile(r"(?:'(?:[^']|'')*'|[^\[']*)\[((?:[^\]]|\]\])*)\]")


def run_semantic_validation(validation, *, session, workspace, collect: bool):
    """Return the validation's result and, when collected, its diagnostic rows."""

    definition = json.loads(validation.definition)
    if definition.get("version") != DEFINITION_VERSION:
        raise ValidationError(
            f"{validation.logical} has an installed definition this Weaver cannot "
            "read. Build the model again"
        )
    model = session.semantic_model(validation.bound_item, workspace=workspace)
    actual = dax_relation(model.query_dax(definition["dax"]), what=validation.logical)
    if validation.kind == ASSUMPTION:
        diagnostics = (
            [dict(zip(actual.columns, row)) for row in actual.rows] if collect else None
        )
        return AssumptionResult(violation_count=len(actual.rows)), diagnostics

    expected = Relation.of(_expected_rows(validation, definition, session, workspace))
    discrepancies = compare_rows(
        expected,
        actual,
        primary_key=validation.primary_key,
        what=str(validation.logical),
    )
    sides = [row[SIDE_COLUMN] for row in discrepancies]
    result = TestResult(
        missing_count=sides.count(EXPECTED), unexpected_count=sides.count(ACTUAL)
    )
    return result, discrepancies if collect else None


def dax_relation(rows, *, what) -> Relation:
    """DAX rows under their column names, refusing names that collide."""

    if not rows:
        return Relation((), ())
    labels = tuple(rows[0])
    columns = tuple(dax_column(label) for label in labels)
    repeated = sorted({c for c in columns if columns.count(c) > 1})
    if repeated:
        raise ValidationError(
            f"{what}: the DAX query returns more than one column named "
            f"{', '.join(repeated)}. Give each column its own name"
        )
    return Relation(
        columns, tuple(tuple(row[label] for label in labels) for row in rows)
    )


def dax_column(label: str) -> str:
    match = _DAX_LABEL.fullmatch(label)
    return match[1].replace("]]", "]") if match else label


def _expected_rows(validation, definition, session, workspace):
    target = validation.expected_target
    if target is None:
        raise ValidationError(
            f"{validation.logical} reads {definition['expectedSource']}, which is "
            "not installed. Build it, then build the model again"
        )
    sql = definition["expectedSql"]
    if target.kind == LAKEHOUSE_TARGET:
        return _spark_rows(sql, target, session, workspace, what=validation.logical)
    from .targets import ItemRef, WarehouseTarget

    return session.query_tsql(
        sql, target=WarehouseTarget(ItemRef(target.name)), workspace=workspace
    )


def _spark_rows(sql, target, session, workspace, *, what):
    from .declaration.dependencies import address_managed_references
    from .declaration.spark_sql_program import parse_spark_sql_program
    from .lakehouse import lakehouse_for
    from .targets import ItemRef

    lakehouse = lakehouse_for(session.resolver(workspace), ItemRef(target.name))
    program = parse_spark_sql_program(sql, what=str(what), error=ValidationError)
    statements = [
        address_managed_references(statement.sql, lakehouse)
        for statement in program.statements
    ]
    return session.execute_spark_sql_batch(statements, workspace=workspace)


__all__ = ["dax_column", "dax_relation", "run_semantic_validation"]
