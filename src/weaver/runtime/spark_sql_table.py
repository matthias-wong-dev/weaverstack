"""Execute a Spark-SQL table extraction for ``SparkSqlTable.read()``.

Setup statements, the staging query, and an optional delete-key query run in
order on one Spark session. Shared table loading handles the returned relations.
"""

from __future__ import annotations

from typing import Any

from ..declaration.spark_sql_program import (
    parse_spark_sql_program,
    validate_query_contract,
)
from ..errors import LoadError
from .load_contract import LoadContract


def read_spark_sql(spark: Any, *, sql: str, contract: LoadContract) -> Any:
    """Run one authored program and return what it staged.

    One query stages and claims nothing, and the frame comes back on its own:
    that is the shape every table returns, and a non-incremental one may return
    no other. A second query names the keys to delete, and the two come back
    together as an incremental table's claim.

    Synthesising an empty second frame for a program that has one query would
    invent a claim it never made, and a load would then run a Spark job to
    establish that the claim was empty.
    """

    if not isinstance(sql, str) or not sql.strip():
        raise LoadError(
            f"{contract.qualified}: this Spark SQL primitive carries no program, "
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
        return frames[0]
    staging, deletes = frames
    _check_delete_columns(deletes, contract)
    return staging, deletes


def _check_delete_columns(deletes: Any, contract: LoadContract) -> None:
    """The delete query names keys, and only keys.

    A delete is applied by joining on the primary key, so a result carrying
    anything else was written against a different idea of what it was for, and
    a result missing one would delete by a partial key, which is a different
    and much worse mistake.
    """

    columns = tuple(getattr(deletes, "columns", ()) or ())
    expected = tuple(contract.primary_key)
    if set(columns) != set(expected):
        raise LoadError(
            f"{contract.qualified}: the second query names the rows to delete, so "
            f"it must return exactly the primary key {list(expected)}. It "
            f"returned {list(columns)}"
        )


__all__ = ["read_spark_sql"]
