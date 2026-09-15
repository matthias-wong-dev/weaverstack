"""Enforce the shared SQL validation contract for Spark SQL and T-SQL.

.. code-block:: text

    Test         setup … then expected, then actual
    Assumption   setup … then the violating rows

Statements that return no rows are setup and must come first. Dynamic SQL can
make the count indeterminate; that alone does not invalidate a validation.
"""

from __future__ import annotations

from typing import Protocol, Sequence

from .metadata import ASSUMPTION, TEST

CONTRACT = {
    TEST: (2, "expected then actual"),
    ASSUMPTION: (1, "the violating rows"),
}


class Statement(Protocol):
    @property
    def produces_result(self) -> bool: ...


class Program(Protocol):
    @property
    def queries(self) -> Sequence[object]: ...

    @property
    def statements(self) -> Sequence[Statement]: ...


def validate_validation_contract(
    program: Program, *, what: str, kind: str, error: type[Exception]
) -> None:
    """Apply the same query contract during project parsing and execution."""

    required, contract = CONTRACT[kind]
    found = len(program.queries)
    if found == required:
        _refuse_setup_after_the_contract(program, what=what, kind=kind, error=error)
        return

    if found == 0:
        raise error(
            f"{what}: {kind} requires "
            f"{required} final result-producing {'queries' if required > 1 else 'query'} "
            f"for {contract}, but this body has none. Add the required "
            f"{'queries' if required > 1 else 'query'} after any setup."
        )
    raise error(
        f"{what}: {kind} requires exactly {required} result "
        f"{'sets' if required > 1 else 'set'} for {contract}, but this body produces "
        f"{found}. Move intermediate queries into setup that returns no rows, such "
        "as a temporary view."
    )


def _refuse_setup_after_the_contract(
    program: Program, *, what: str, kind: str, error: type[Exception]
) -> None:
    """Require setup before result queries in both SQL dialects.

    Spark evaluates result frames lazily while T-SQL captures them in place, so
    trailing setup would give the same validation different meanings.
    """

    statements = list(getattr(program, "statements", ()))
    first = next(
        (
            index
            for index, statement in enumerate(statements)
            if statement.produces_result
        ),
        None,
    )
    if first is None:
        return
    trailing = [
        index
        for index, statement in enumerate(statements[first:], start=first)
        if not statement.produces_result
    ]
    if not trailing:
        return

    raise error(
        f"{what}: {kind} has {len(trailing)} setup "
        f"{'statements' if len(trailing) != 1 else 'statement'} after its first "
        "result-producing query. Move all setup before the first query."
    )


__all__ = ["CONTRACT", "validate_validation_contract"]
