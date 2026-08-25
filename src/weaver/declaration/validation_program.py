"""What an authored SQL validation's body means, as a program.

Both dialects already split a body into statements and say which of them produce
rows: :mod:`weaver.declaration.spark_sql_program` and
:mod:`weaver.declaration.tsql_program`. A validation's contract is a statement
about how many of those there are and what each one is for, and that is the
same statement in either dialect:

.. code-block:: text

    Test         setup … then expected, then actual
    Assumption   setup … then the violating rows

So the counting rule lives here, once, over whatever program object the dialect
produced, and neither dialect gets its own idea of what a Test is.

Setup is unrestricted and comes first: anything returning no rows is setup,
however much of it there is. What the contract constrains is the final one or
two result-producing queries, because those are what the comparison reads.

An undetermined count is not a refusal. Dynamic SQL puts it beyond static reach,
and a validation whose setup builds something dynamically is ordinary.
Whether the final queries are capturable is answered where the rendering
happens.
"""

from __future__ import annotations

from typing import Protocol, Sequence

from .metadata import ASSUMPTION, TEST

#: How many result-producing queries each kind's contract is, and what they are.
CONTRACT = {
    TEST: (2, "expected then actual"),
    ASSUMPTION: (1, "the violating rows"),
}


class Statement(Protocol):
    """One statement, and whether it returns rows."""

    @property
    def produces_result(self) -> bool: ...


class Program(Protocol):
    """What both dialect parsers produce, as much of it as this needs."""

    @property
    def queries(self) -> Sequence[object]: ...

    @property
    def statements(self) -> Sequence[Statement]: ...


def validate_validation_contract(
    program: Program, *, what: str, kind: str, error: type[Exception]
) -> None:
    """Refuse a program whose queries cannot be this kind of validation.

    Called from repository parsing, where it stops a build, and again from the
    deployed primitive, where it stops a run. One rule at both ends, because a
    module edited by hand after deployment never met the first.
    """

    required, contract = CONTRACT[kind]
    found = len(program.queries)
    if found == required:
        _refuse_setup_after_the_contract(program, what=what, kind=kind, error=error)
        return

    article = "an" if kind[0].upper() in "AEIOU" else "a"
    if found == 0:
        raise error(
            f"{what}: {article} {kind} ends in the "
            f"{'queries' if required > 1 else 'query'} that "
            f"produce{'' if required > 1 else 's'} {contract}, and this body has "
            "none. Setup statements alone compare nothing"
        )
    raise error(
        f"{what}: {article} {kind} must produce exactly {required} result "
        f"{'sets' if required > 1 else 'set'}, being {contract}, and this body produces "
        f"{found}. Statements that return no rows are setup and may precede them "
        "freely; turn an intermediate query into a temporary view."
    )


def _refuse_setup_after_the_contract(
    program: Program, *, what: str, kind: str, error: type[Exception]
) -> None:
    """The contract queries end the body; nothing runs after them.

    Counting them is not enough, and the reason is Spark. A ``SELECT`` there is
    lazy: the frame is built where it is written and materialised later, so a
    setup statement sitting after the first contract query, replacing a temporary
    view for instance, changes what that query will read by the time anyone
    reads it. T-SQL has the opposite behaviour, because the compiler captures
    each contract query into a temp table at its authored position.

    So the same body would mean two different things on the two engines, and
    neither would be the one the author wrote. Requiring the contract queries to
    come last removes the question rather than answering it per dialect.
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

    article = "an" if kind[0].upper() in "AEIOU" else "a"
    raise error(
        f"{what}: {article} {kind} ends with its contract "
        f"{'queries' if CONTRACT[kind][0] > 1 else 'query'}, and "
        f"{len(trailing)} statement(s) follow. Setup belongs before them: a "
        "Spark SQL query is lazy, so a statement that runs afterwards can change "
        "what it reads before anyone reads it, while T-SQL captures it where it "
        "was written, so the same body would mean two different things on the "
        "two engines. Move the setup above the first query."
    )


__all__ = ["CONTRACT", "validate_validation_contract"]
