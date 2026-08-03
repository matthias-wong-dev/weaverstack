"""Running an installed Spark SQL load program.

The generated program is a list of statements, because Spark executes one
statement per call. This is the small amount of driving that implies: substitute
the run's own choice about rejects, execute in order, read the last result set.

It is deliberately thin, and the thinness is the claim. Nothing here decides
anything about the load — the SQL does. If this module had to know what a reject
was, or when to skip a write, then the installed file would not be independently
runnable and §9's promise would be false. What it knows is how to say 1 or 0 and
how to read a row back.

::

    result = run_load_program(spark, installed_sql, fault_tolerant=True)
"""

from __future__ import annotations

from ..declaration.spark_load import (
    FAULT_TOLERANT_DEFAULT,
    FAULT_TOLERANT_MARKER,
    IGNORE_THRESHOLD_DEFAULT,
    IGNORE_THRESHOLD_MARKER,
    statements_of,
)
from ..errors import LoadError
from .load_result import RESULT_COLUMNS, LoadResult


def run_load_program(
    spark,
    program: str,
    *,
    fault_tolerant: bool = False,
    ignore_stability_threshold: bool = False,
) -> LoadResult:
    """Execute an installed load program and report what it did.

    ``program`` is the installed file's text, whose object names are already
    resolved — the installer addressed them as it wrote the file, because a
    program nobody could run without a resolver would not be a primitive.
    """

    statements = statements_of(
        _answer(program, fault_tolerant, ignore_stability_threshold)
    )
    if not statements:
        raise LoadError("the load program contains no statements")

    frame = None
    for statement in statements:
        try:
            frame = spark.sql(statement)
            if _is_terminal(statement):
                frame.collect()
        except Exception as exc:
            # The program raises natively when a run failed and was not asked to
            # tolerate it, so `exec`-ing the file and calling `.load()` fail the
            # same way. Wrapped here so a caller meets one error type whichever
            # primitive it drove.
            raise LoadError(str(exc)) from exc

    rows = frame.collect()
    if not rows:
        raise LoadError(
            "the load program's final statement returned no row — it must "
            "project the load result"
        )
    row = rows[0]
    missing = [name for name in RESULT_COLUMNS if name not in row.asDict()]
    if missing:
        raise LoadError(
            "the load program's final statement is missing "
            f"{', '.join(missing)} — it must project the load result"
        )
    result = LoadResult.from_row({name: row[name] for name in RESULT_COLUMNS})
    if result.succeeded:
        _clear(spark, program)
    return result


#: The artefacts a clean run leaves behind, and does not need to. The rule is
#: the same on all three table primitives: a run that refused rows keeps its
#: evidence, a clean one keeps nothing. Done here rather than in the program
#: because the final statement reads the result table — cleanup has to follow
#: the row being taken, and only the runner knows when that has happened.
_ARTEFACT_SUFFIXES = ("_Staging", "_Upsert", "_Reject", "_Delete", "_LoadResult")


def _clear(spark, program: str) -> None:
    for name in _artefact_names(program):
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def _artefact_names(program: str) -> list[str]:
    """The artefacts this program named, read back off its own drop statements.

    The program opens by dropping whatever a previous run left, so it already
    says which relations it owns — and reading them from there means the runner
    never has to compose a name the generator might spell differently.
    """

    names = []
    for statement in statements_of(program):
        head, _, tail = statement.partition("DROP TABLE IF EXISTS ")
        if not tail:
            continue
        candidate = tail.strip().splitlines()[0].strip()
        if candidate.endswith(_ARTEFACT_SUFFIXES) or candidate.rstrip("`").endswith(
            _ARTEFACT_SUFFIXES
        ):
            names.append(candidate)
    # The result table is dropped last: everything else is read while deciding,
    # and it is read to produce the row that was just taken.
    return names


def _is_terminal(statement: str) -> bool:
    """Whether this statement must be evaluated rather than merely planned.

    Spark is lazy, so a `SELECT` that raises does nothing until something reads
    it — and the guard's entire job is to raise. DDL and DML run eagerly; the
    guard is the one projection whose evaluation matters.
    """

    return "raise_error(" in statement


def _answer(
    program: str, fault_tolerant: bool, ignore_stability_threshold: bool
) -> str:
    """Substitute the one question the file leaves open.

    A run's tolerance of rejects, and its willingness to waive the declared
    stability thresholds, are properties of the run rather than of the object or
    of where it lives — so they are the only holes the installer leaves for
    whoever runs the program. Both already read 0, so the cautious answers need
    no substitution at all, which is what makes an installed program runnable
    exactly as it stands.
    """

    if fault_tolerant:
        program = program.replace(FAULT_TOLERANT_DEFAULT, f"{FAULT_TOLERANT_MARKER}1")
    if ignore_stability_threshold:
        program = program.replace(
            IGNORE_THRESHOLD_DEFAULT, f"{IGNORE_THRESHOLD_MARKER}1"
        )
    return program


__all__ = ["run_load_program"]
