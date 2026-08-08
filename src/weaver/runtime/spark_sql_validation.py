"""Running a Spark-SQL-authored validation's program.

The whole of what a SQL-authored validation does differently, and it is
deliberately this small. A Spark SQL Test is authored as SQL, installed as an
importable Python module, and compared by the *ordinary*
:meth:`weaver.objects.Test.read` — so the only thing that has to exist here is
the step between the two: run the program, and hand back the relations.

.. code-block:: text

    Test         setup statements   run, in order, for their effect
                 first query        expected
                 second query       actual

    Assumption   setup statements   run, in order, for their effect
                 first query        the violating rows

Nothing here compares, counts, correlates or classifies. That is
:func:`weaver.runtime.test_compare.compare`, unchanged and shared, which is the
point of the arrangement: a Test authored in SQL and a Test authored in Python
are one comparison with two fronts, so what passing means cannot come to differ
between them.

**One pass, both sides.** The program is executed once and both frames come back
together, rather than each side re-running the setup — a Test whose setup
materialised a snapshot would otherwise compare two different snapshots and
report the difference between them as failure.

**One session, one order.** Every statement runs through the object's own Spark
session, so a temporary view a setup statement creates is visible to both
queries. Spark evaluates a ``SELECT`` lazily, so a program that *replaces* a
view between its two queries has changed what the first will read when it is
finally materialised — Spark's semantics rather than Weaver's, and the author's
to avoid.
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
            f"{what}: this Spark SQL validation carries no program — a generated "
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
