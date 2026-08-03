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
        frame = spark.sql(statement)

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
    return LoadResult.from_row({name: row[name] for name in RESULT_COLUMNS})


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
