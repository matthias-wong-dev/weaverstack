"""What an authored T-SQL body means, as a program.

The Warehouse counterpart of :mod:`weaver.declaration.spark_sql_program`, and it
exists for the same reason: a build and a load both have to know which part of a
body produces the object's rows, and two answers to that would be a bug waiting
for the first body complicated enough to tell them apart.

.. code-block:: text

    the build      materialises the staging query in shape-only form
    the load       stages that query, and deletes by the next one

**Setup and query, and the difference is not a semicolon.** T-SQL does not
require statement terminators, so where one statement ends cannot come from
splitting on ``;`` the way it can in Spark. It comes from
:func:`weaver.declaration.sql_shaping.query_spans`, which is the recognition the
shape-only build has always used — a ``SELECT`` that begins a query told apart
from one that is the tail of an ``INSERT``, a branch of a ``UNION``, the body of
a ``WITH`` or a subquery inside a predicate. Everything a span does not cover is
setup, and setup is what a statement is by default rather than by enumeration.

**One query is staging; two are staging and explicit deletes.**

.. code-block:: text

    0 queries       not a table — nothing visibly produces rows
    1 query         the staging rows
    2 queries       the staging rows, then the keys to delete
    3 or more       ambiguous, and refused

**Dynamic SQL is setup, and is not read.** ``EXEC`` and ``sp_executesql`` may do
whatever they like — build a working table, branch, loop — and Weaver runs them
as authored. What they must not do is *be* the staging query: a result set that
exists only inside a string literal is not something this module can see, and it
will not guess. So ``exec sp_executesql N'select …';`` is a body with no query
in it, and is refused as one. The boundary is visibility, not capability.

**A delete query is a claim only an incremental keyed table may make.** A
non-incremental source is the whole truth, so absence from staging is what
retires a row, and a second statement of the same thing would be applied on top
of a reconciliation that already accounted for it. The same rule Spark SQL
states, because it is the table load's rule rather than either dialect's.

Nothing here parses T-SQL grammar. Every statement is still handed to the
Warehouse, which remains the only authority on whether it is valid.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from ..sql_statements import parse_statements
from .sql_shaping import QuerySpan, query_spans, selects_into, top_level_go


@dataclass(frozen=True)
class TsqlStatement:
    """One piece of an authored program, and whether it yields rows.

    ``sql`` is sliced out of the body rather than reassembled, so an author's
    formatting, comments and case survive into the generated artefact exactly as
    written — which is what makes a generated procedure readable by the person
    who wrote the query in it.
    """

    sql: str
    produces_result: bool


@dataclass(frozen=True)
class TsqlProgram:
    """One authored body, split and classified, in source order."""

    statements: tuple[TsqlStatement, ...]

    @property
    def queries(self) -> tuple[TsqlStatement, ...]:
        """The statements that produce rows, in the order they appear."""

        return tuple(
            statement for statement in self.statements if statement.produces_result
        )

    @property
    def setup(self) -> tuple[TsqlStatement, ...]:
        return tuple(
            statement for statement in self.statements if not statement.produces_result
        )

    @property
    def staging(self) -> TsqlStatement | None:
        """The query that produces the object's rows, if it has one."""

        queries = self.queries
        return queries[0] if queries else None

    @property
    def deletes(self) -> TsqlStatement | None:
        """The query naming the keys to delete, if the author wrote one."""

        queries = self.queries
        return queries[1] if len(queries) > 1 else None


def parse_tsql_program(body: str, *, what: str, error: type[Exception]) -> TsqlProgram:
    """Split and classify one authored T-SQL body.

    ``GO`` is refused rather than split on. It is a client-side batch separator
    with no meaning to the server, and the load installs the body *inside* a
    stored procedure, where it is a syntax error — so a body containing one
    could never load, and saying so here costs an author a clear message instead
    of an obscure failure at install time.
    """

    text = body or ""
    marker = top_level_go(text)
    if marker is not None:
        raise error(
            f"{what}: GO is a batch separator for a client tool, and the "
            "generated load runs this body inside a stored procedure, where it "
            "cannot appear. Separate the statements with ';'."
        )

    spans = query_spans(text)
    results = tuple(span for span in spans if not selects_into(text, span))
    return TsqlProgram(statements=_in_source_order(text, results))


def validate_query_contract(
    program: TsqlProgram,
    *,
    what: str,
    primary_key: Sequence[str],
    incremental: bool,
    error: type[Exception],
) -> None:
    """Refuse a program whose queries cannot mean a load.

    Called from repository parsing, where it stops a build, and again from load
    generation, where the answer has to hold for the procedure being written.
    """

    queries = program.queries
    if not queries:
        raise error(
            f"{what}: a Warehouse table must produce its rows from a visible "
            "SELECT, and this body has none. Setup statements alone stage "
            "nothing, and a result set inside EXEC or sp_executesql is not one "
            "Weaver can see — end the body with the SELECT that produces the "
            "rows."
        )
    if len(queries) > 2:
        raise error(
            f"{what}: a Warehouse table produces its rows and, at most, the keys "
            f"to delete — {len(queries)} statements produce results. Divert the "
            "intermediate ones with SELECT … INTO #temp."
        )
    if len(queries) == 1:
        return
    if not primary_key:
        raise error(
            f"{what}: a second query names the rows to delete, which needs a "
            "primary key to name them by — declare one, or return one query"
        )
    if not incremental:
        raise error(
            f"{what}: a non-incremental table cannot name explicit deletes — the "
            "source is the whole truth, so a row's absence from the staging "
            "query is what retires it. Return one query, or declare "
            "Incremental: true."
        )


def _in_source_order(
    text: str, results: tuple[QuerySpan, ...]
) -> tuple[TsqlStatement, ...]:
    """The body cut at its result queries, everything between them setup.

    Order is the whole of what a load needs: setup written between two queries
    was written there deliberately, and running it anywhere else would change
    what the second query sees.
    """

    statements: list[TsqlStatement] = []
    cursor = 0
    for span in results:
        statements.extend(_setup_between(text, cursor, span.start))
        query = _trim(text[span.start : span.end])
        if query:
            statements.append(TsqlStatement(sql=query, produces_result=True))
        cursor = span.end
    statements.extend(_setup_between(text, cursor, len(text)))
    return tuple(statements)


def _setup_between(text: str, start: int, end: int) -> list[TsqlStatement]:
    """The setup in one gap, as separate statements where it separates.

    Terminated statements come apart cleanly; an unterminated run of them stays
    in one piece, which is harmless because setup is emitted exactly as authored
    either way. Splitting is for legibility of the generated artefact, not for
    meaning.
    """

    return [
        TsqlStatement(sql=_trim(statement.text), produces_result=False)
        for statement in parse_statements(text[start:end])
        if _trim(statement.text)
    ]


def _trim(sql: str) -> str:
    stripped = (sql or "").strip()
    while stripped.endswith(";"):
        stripped = stripped[:-1].rstrip()
    return stripped


__all__ = [
    "TsqlProgram",
    "TsqlStatement",
    "parse_tsql_program",
    "validate_query_contract",
]
