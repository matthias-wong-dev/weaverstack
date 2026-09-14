"""Classify statements in an authored Spark SQL program.

Setup and query are told apart lexically, by what the statement starts with.
Spark returns a DataFrame for every statement, so running one cannot answer
whether it produced rows, which is why ``CREATE VIEW … AS SELECT`` is setup
despite containing a ``SELECT``, and ``WITH … SELECT`` is a query despite not
starting with one.

This module does not validate Spark SQL grammar.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from ..sql_statements import SqlStatement, parse_statements, unterminated

#: What a top-level statement may begin with and still produce rows. ``FROM`` is
#: Spark's leading-from form (``FROM t SELECT …``); ``(`` is a parenthesised
#: query, which is how a set operation is often written. Everything else is
#: setup, including the whole of DDL, ``CACHE``, ``SET`` and ``INSERT``, and setup
#: is what a statement is by default rather than by enumeration.
QUERY_HEADS = frozenset({"(", "FROM", "SELECT", "TABLE", "VALUES", "WITH"})


@dataclass(frozen=True)
class SparkSqlStatement:
    sql: str
    produces_result: bool

    @classmethod
    def of(cls, statement: SqlStatement) -> "SparkSqlStatement":
        return cls(
            sql=statement.text,
            produces_result=statement.keyword in QUERY_HEADS,
        )


@dataclass(frozen=True)
class SparkSqlProgram:
    statements: tuple[SparkSqlStatement, ...]

    @property
    def queries(self) -> tuple[SparkSqlStatement, ...]:
        return tuple(
            statement for statement in self.statements if statement.produces_result
        )

    @property
    def setup(self) -> tuple[SparkSqlStatement, ...]:
        return tuple(
            statement for statement in self.statements if not statement.produces_result
        )


def parse_spark_sql_program(
    body: str, *, what: str, error: type[Exception]
) -> SparkSqlProgram:
    """Split and classify a body whose every statement ends with ``;``."""

    trailing = unterminated(body)
    if trailing is not None:
        raise error(
            f"{what}: the last Spark SQL statement does not end with ';': "
            f"{_excerpt(trailing.text)}. Add the missing semicolon."
        )
    return SparkSqlProgram(
        statements=tuple(SparkSqlStatement.of(one) for one in parse_statements(body))
    )


def validate_query_contract(
    program: SparkSqlProgram,
    *,
    what: str,
    primary_key: Sequence[str],
    incremental: bool,
    error: type[Exception],
) -> None:
    """Validate the staging and delete-query contract."""

    queries = program.queries
    if not queries:
        raise error(
            f"{what}: the Spark SQL table has no query that produces rows. Add a "
            "staging query."
        )
    if len(queries) > 2:
        raise error(
            f"{what}: the Spark SQL table has {len(queries)} result queries; at "
            "most two are allowed. Turn intermediate queries into temporary views."
        )
    if len(queries) == 1:
        return
    if not primary_key:
        raise error(
            f"{what}: the delete query requires a Primary key. Declare one or "
            "remove the second query."
        )
    if not incremental:
        raise error(
            f"{what}: a non-incremental table cannot have a delete query. Remove "
            "the second query or declare Incremental: true."
        )


def _excerpt(text: str, limit: int = 60) -> str:
    flattened = " ".join(text.split())
    return flattened if len(flattened) <= limit else f"{flattened[:limit]}…"


__all__ = [
    "QUERY_HEADS",
    "SparkSqlProgram",
    "SparkSqlStatement",
    "parse_spark_sql_program",
    "validate_query_contract",
]
