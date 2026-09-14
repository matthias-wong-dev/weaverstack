"""Classify authored T-SQL statements as setup or result-producing queries.

The parsed program supplies build and load staging queries without validating
T-SQL grammar.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from ..sql_statements import parse_statements
from .sql_shaping import QuerySpan, query_spans, selects_into, top_level_go


@dataclass(frozen=True)
class TsqlStatement:
    """A source-preserving slice of an authored program.

    Source slices preserve formatting, comments and case.
    """

    sql: str
    produces_result: bool


@dataclass(frozen=True)
class TsqlProgram:
    statements: tuple[TsqlStatement, ...]

    @property
    def queries(self) -> tuple[TsqlStatement, ...]:
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
        queries = self.queries
        return queries[0] if queries else None

    @property
    def deletes(self) -> TsqlStatement | None:
        queries = self.queries
        return queries[1] if len(queries) > 1 else None


def parse_tsql_program(body: str, *, what: str, error: type[Exception]) -> TsqlProgram:
    """Split and classify a T-SQL body, rejecting ``GO``.

    Generated loads place the body inside a stored procedure, where the
    client-side batch separator is invalid.
    """

    text = body or ""
    marker = top_level_go(text)
    if marker is not None:
        raise error(
            f"{what}: GO cannot appear in a generated load procedure. Separate "
            "the statements with ';'."
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
    """Validate the staging and delete-query contract."""

    queries = program.queries
    if not queries:
        raise error(
            f"{what}: the Warehouse table has no top-level SELECT that produces "
            "rows. Add a staging query; SELECT inside EXEC or sp_executesql does "
            "not count."
        )
    if len(queries) > 2:
        raise error(
            f"{what}: the Warehouse table has {len(queries)} result queries; at "
            "most two are allowed. Divert intermediate results with SELECT … INTO."
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


def _in_source_order(
    text: str, results: tuple[QuerySpan, ...]
) -> tuple[TsqlStatement, ...]:
    """Slice result queries and intervening setup without changing source order."""

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
    """Split terminated setup statements without reassembling their text.

    An unterminated run remains one slice. Splitting affects only generated-text
    legibility, because every slice is emitted in order.
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
