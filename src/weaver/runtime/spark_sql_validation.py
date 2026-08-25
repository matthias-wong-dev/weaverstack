"""Execute a Spark-SQL validation program.

The program returns expected and actual relations for a Test, or violating rows
for an Assumption. Statements run once, in order, on the object's Spark session.
"""

from __future__ import annotations

from typing import Any

from ..declaration.metadata import ASSUMPTION, TEST
from ..declaration.spark_sql_program import parse_spark_sql_program
from ..declaration.validation_program import validate_validation_contract
from ..errors import ValidationError


def read_spark_sql_test(spark: Any, *, sql: str, what: str) -> tuple[Any, Any]:
    """Run one authored Test program and return ``(expected, actual)``."""

    frames = _run(spark, sql=sql, what=what, kind=TEST)
    return frames[0], frames[1]


def read_spark_sql_assumption(spark: Any, *, sql: str, what: str) -> Any:
    """Run one authored Assumption program and return the violating rows."""

    return _run(spark, sql=sql, what=what, kind=ASSUMPTION)[0]


def _run(spark: Any, *, sql: str, what: str, kind: str) -> list[Any]:
    if not isinstance(sql, str) or not sql.strip():
        raise ValidationError(
            f"{what}: this Spark SQL validation carries no program, a generated "
            "module sets `sql` to the authored SQL it was built from"
        )

    program = parse_spark_sql_program(sql, what=what, error=ValidationError)
    validate_validation_contract(program, what=what, kind=kind, error=ValidationError)

    frames: list[Any] = []
    for statement in program.statements:
        frame = spark.sql(statement.sql)
        if statement.produces_result:
            frames.append(frame)
    return frames


__all__ = ["read_spark_sql_assumption", "read_spark_sql_test"]
