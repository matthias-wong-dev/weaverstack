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
    """Return staging alone, or staging and an explicit delete claim."""

    if not isinstance(sql, str) or not sql.strip():
        raise LoadError(
            f"{contract.qualified}: the deployed Spark SQL table has no program; "
            "rebuild the object"
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
    columns = tuple(getattr(deletes, "columns", ()) or ())
    expected = tuple(contract.primary_key)
    if set(columns) != set(expected):
        raise LoadError(
            f"{contract.qualified}: the delete query must return exactly primary "
            f"key columns {list(expected)}; it returned {list(columns)}"
        )


__all__ = ["read_spark_sql"]
