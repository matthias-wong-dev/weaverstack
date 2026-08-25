"""What an authored Spark SQL table's body means, as a program.

A Spark SQL table is authored as ordinary SQL that runs unchanged in a ``%%sql``
cell, and it is installed as an importable Python primitive whose ``read()``
executes that same text. Both ends need the same reading of the body, so there
is one here rather than one each:

.. code-block:: text

    repository parsing     refuses a body that cannot mean anything
    the deployed primitive executes what the body says, in order

Setup and query are told apart lexically, by what the statement starts with.
Spark returns a DataFrame for every statement, so running one cannot answer
whether it produced rows, which is why ``CREATE VIEW … AS SELECT`` is setup
despite containing a ``SELECT``, and ``WITH … SELECT`` is a query despite not
starting with one.

.. code-block:: text

    0 queries       not a table, because nothing produces rows
    1 query         the staging rows
    2 queries       the staging rows, then the keys to delete
    3 or more       ambiguous, and refused

That is what ``read()`` returns everywhere else, as ``(staging, deletes)``, so a
SQL-authored table reaches :func:`weaver.runtime.table_load.load_table` through
the same door a Python-authored one does.

Only an incremental keyed table may name deletes: a non-incremental source is
the whole truth, so absence from staging is what retires a row. The table load
enforces the same rule; it is repeated at parse time so a build meets it first.

Nothing here parses Spark SQL grammar. Statement boundaries come from
:mod:`weaver.sql_statements`, and Spark remains the authority on validity.
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
    """One statement of an authored program, and whether it yields rows."""

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
    """One authored body, split and classified, in source order."""

    statements: tuple[SparkSqlStatement, ...]

    @property
    def queries(self) -> tuple[SparkSqlStatement, ...]:
        """The statements that produce rows, in the order they appear."""

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
    """Split and classify one authored body, refusing an unterminated statement.

    Termination is required of every statement including the last, because the
    separator is what makes the body's shape a statement of intent rather than
    something inferred: a program whose final statement trails off is one whose
    author may or may not have finished writing it, and the parser cannot tell
    the two apart.
    """

    trailing = unterminated(body)
    if trailing is not None:
        raise error(
            f"{what}: every Spark SQL statement must end with ';', and the last "
            f"one does not: {_excerpt(trailing.text)}"
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
    """Refuse a program whose queries cannot mean a load.

    Called from repository parsing, where it stops a build, and again from the
    deployed primitive, where it stops a load. One rule checked at both ends,
    because a module edited by hand after deployment never met the first.
    """

    queries = program.queries
    if not queries:
        raise error(
            f"{what}: a Spark SQL table must end in a query that produces its "
            "rows, and this body has none. Setup statements alone stage nothing"
        )
    if len(queries) > 2:
        raise error(
            f"{what}: a Spark SQL table produces its rows and, at most, the keys "
            f"to delete, and {len(queries)} statements produce results. Turn the "
            "intermediate ones into temporary views."
        )
    if len(queries) == 1:
        return
    if not primary_key:
        raise error(
            f"{what}: a second query names the rows to delete, which needs a "
            "primary key to name them by. Declare one, or return one query"
        )
    if not incremental:
        raise error(
            f"{what}: a non-incremental table cannot name explicit deletes. The "
            "source is the whole truth, so a row's absence from the staging "
            "query is what retires it. Return one query, or declare "
            "Incremental: true."
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
