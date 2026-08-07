"""Executing a Spark-SQL-authored table's extraction — the mechanics behind
``SparkSqlTable.read()``.

The whole of what a SQL-authored table does differently, and it is deliberately
this small. A Spark SQL table is authored as SQL, installed as an importable
Python module and loaded by the *ordinary* :meth:`weaver.objects.Table.load` —
so the only thing that has to exist here is the step between the two: run the
program, and hand back the pair ``read()`` returns everywhere else.

.. code-block:: text

    setup statements   run, in order, for their effect
    first query        the staging rows
    second query       the keys to delete, if there is one

Nothing here validates rows, rejects keys, merges, deletes, counts or builds a
:class:`~weaver.runtime.load_result.LoadResult`. That is
:func:`weaver.runtime.table_load.load_table`'s, unchanged and shared, which is
the point of the whole arrangement: a table authored in SQL and a table authored
in Python are the same load with two extraction fronts, so fault tolerance,
stability thresholds, rejection policy and static behaviour cannot come to
differ between them.

**One session, one order.** Every statement runs through the object's own Spark
session, so a temporary view a setup statement creates is visible to both
queries. Spark evaluates a ``SELECT`` lazily, so a program that *replaces* a
view between its two queries has changed what the first one will read when it is
finally materialised — that is Spark's semantics rather than Weaver's, and the
author's to avoid.
"""

from __future__ import annotations

from typing import Any

from ..declaration.spark_sql_program import (
    parse_spark_sql_program,
    validate_query_contract,
)
from ..errors import LoadError
from .load_contract import LoadContract


def read_spark_sql(spark: Any, *, sql: str, contract: LoadContract) -> tuple[Any, Any]:
    """Run one authored program and return ``(staging, deletes)``.

    ``deletes`` is ``None`` when the program has a single query, because that is
    what a table with nothing explicit to remove returns — and
    :func:`~weaver.runtime.table_load.load_table` already knows what to do with
    it in both the incremental and the non-incremental case. Synthesising an
    empty frame instead would be inventing a claim the program never made.
    """

    if not isinstance(sql, str) or not sql.strip():
        raise LoadError(
            f"{contract.qualified}: this Spark SQL primitive carries no program — "
            "a generated module sets `sql` to the authored SQL it was built from"
        )

    program = parse_spark_sql_program(sql, what=contract.qualified, error=LoadError)
    validate_query_contract(
        program,
        what=contract.qualified,
        primary_key=contract.primary_key,
        incremental=contract.incremental,
        error=LoadError,
    )

    frames: list[Any] = []
    for statement in program.statements:
        frame = spark.sql(statement.sql)
        if statement.produces_result:
            frames.append(frame)

    if len(frames) == 1:
        return frames[0], None
    staging, deletes = frames
    _check_delete_columns(deletes, contract)
    return staging, deletes


def _check_delete_columns(deletes: Any, contract: LoadContract) -> None:
    """The delete query names keys, and only keys.

    A delete is applied by joining on the primary key, so a result carrying
    anything else was written against a different idea of what it was for — and
    a result *missing* one would delete by a partial key, which is a different
    and much worse mistake.
    """

    columns = tuple(getattr(deletes, "columns", ()) or ())
    expected = tuple(contract.primary_key)
    if set(columns) != set(expected):
        raise LoadError(
            f"{contract.qualified}: the second query names the rows to delete, so "
            f"it must return exactly the primary key {list(expected)} — it "
            f"returned {list(columns)}"
        )


__all__ = ["read_spark_sql"]
