"""What an authored SQL validation's body means, as a program.

Both dialects already split a body into statements and say which of them produce
rows — :mod:`weaver.declaration.spark_sql_program` and
:mod:`weaver.declaration.tsql_program`. A validation's contract is a statement
about *how many* of those there are and what each one is for, and that is the
same statement in either dialect:

.. code-block:: text

    Test         setup … then expected, then actual
    Assumption   setup … then the violating rows

So the counting rule lives here, once, over whatever program object the dialect
produced. Neither parser is duplicated and neither dialect gets its own idea of
what a Test is — which is the property that lets "the Sales tests pass" mean one
thing across an estate.

**Setup is unrestricted and comes first.** Anything that returns no rows is
setup, however much of it there is: temporary views, table variables, a
``CREATE TABLE #expected``, dynamic SQL. What the contract constrains is the
final one or two result-producing queries, because those are what the comparison
reads.

**An undetermined count is not a refusal.** Dynamic SQL puts the count beyond
static reach, and a validation whose *setup* builds something dynamically is
ordinary. What the compiler then requires is that the final queries are
capturable, which is a rendering question and is answered where the rendering
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


class Program(Protocol):
    """What both dialect parsers produce, as much of it as this needs."""

    @property
    def queries(self) -> Sequence[object]: ...


def validate_validation_contract(
    program: Program, *, what: str, kind: str, error: type[Exception]
) -> None:
    """Refuse a program whose queries cannot be this kind of validation.

    Called from repository parsing, where it stops a build, and again from the
    deployed primitive, where it stops a run — the same rule at both ends,
    because a module edited by hand after deployment never met the first.
    """

    required, contract = CONTRACT[kind]
    found = len(program.queries)
    if found == required:
        return

    article = "an" if kind[0].upper() in "AEIOU" else "a"
    if found == 0:
        raise error(
            f"{what}: {article} {kind} ends in the "
            f"{'queries' if required > 1 else 'query'} that "
            f"produce{'' if required > 1 else 's'} {contract}, and this body has "
            "none — setup statements alone compare nothing"
        )
    raise error(
        f"{what}: {article} {kind} must produce exactly {required} result "
        f"{'sets' if required > 1 else 'set'} — {contract} — and this body produces "
        f"{found}. Statements that return no rows are setup and may precede them "
        "freely; turn an intermediate query into a temporary view."
    )


__all__ = ["CONTRACT", "validate_validation_contract"]
