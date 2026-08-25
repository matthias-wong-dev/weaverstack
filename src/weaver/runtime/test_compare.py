"""Compare expected and actual relations for a validation.

The comparison returns physical discrepancy rows and uses declared primary keys
only to correlate matching rows.
"""

from __future__ import annotations

from typing import Any, Sequence

from ..errors import ValidationError
from .delta_sql import blank_key_predicate

#: Which side of the comparison a diagnostic row came from.
SIDE_COLUMN = "_weaver_side"

#: The correlation key. Runtime information, not a serialized copy of the
#: declared primary key, because what matters is which rows pair, not which
#: columns paired them.
SK_COLUMN = "_weaver_sk"

EXPECTED = "expected"
ACTUAL = "actual"

#: Reserved on a Test's own result columns, because the diagnostic frame puts
#: them beside the author's. A Test that already returned one would silently
#: have it overwritten.
RESERVED_COLUMNS = (SIDE_COLUMN, SK_COLUMN)


def compare(
    expected: Any,
    actual: Any,
    *,
    primary_key: Sequence[str] = (),
    what: str = "Test",
) -> Any:
    """The rows on which ``expected`` and ``actual`` disagree.

    Empty when the Test passes. Each row carries :data:`SIDE_COLUMN` and
    :data:`SK_COLUMN` ahead of the Test's own columns.

    Raises :class:`~weaver.errors.ValidationError` for anything that makes the
    comparison meaningless rather than failing: mismatched shapes, or a declared
    key that does not identify rows. Those are execution failures, and reporting
    them as zero discrepancies would say the Test passed.
    """

    key = tuple(primary_key)
    _check_shape(expected, actual, key=key, what=what)
    for frame, side in ((expected, EXPECTED), (actual, ACTUAL)):
        _check_key(frame, side=side, key=key, what=what)

    columns = list(expected.columns)
    named = [_quoted(column) for column in columns]
    aligned = actual.toDF(*columns)

    missing = expected.subtract(aligned)
    unexpected = aligned.subtract(expected)

    combined = missing.selectExpr(f"'{EXPECTED}' AS {SIDE_COLUMN}", *named).union(
        unexpected.selectExpr(f"'{ACTUAL}' AS {SIDE_COLUMN}", *named)
    )

    if key:
        # Pair rows from the same changed entity under one diagnostic key.
        ordering = ", ".join(_quoted(column) for column in key)
        rank = f"dense_rank() OVER (ORDER BY {ordering})"
    else:
        # Nothing to pair by, so nothing is paired. Each row gets a key of its
        # own rather than a shared placeholder, so nothing can mistake two
        # rows for the two sides of one entity.
        rank = "row_number() OVER (ORDER BY monotonically_increasing_id())"

    return combined.selectExpr(SIDE_COLUMN, f"{rank} AS {SK_COLUMN}", *named)


def _quoted(column: str) -> str:
    return f"`{column}`"


def _check_shape(
    expected: Any, actual: Any, *, key: tuple[str, ...], what: str
) -> None:
    """Refuse a comparison the engine would perform and nobody could read.

    An explicit error here rather than whatever ``EXCEPT`` says about mismatched
    relations: the low-level message is about a query plan, and the author's
    mistake is about two relations they believe are the same shape.
    """

    left, right = list(expected.columns), list(actual.columns)

    reserved = [column for column in left + right if column in RESERVED_COLUMNS]
    if reserved:
        raise ValidationError(
            f"{what}: {', '.join(sorted(set(reserved)))} is reserved. Weaver adds "
            f"{SIDE_COLUMN} and {SK_COLUMN} to the diagnostic rows, so a Test's own "
            "columns may not carry those names"
        )

    if len(left) != len(right):
        raise ValidationError(
            f"{what}: expected has {len(left)} column(s) and actual has "
            f"{len(right)}. The two sides of a Test must be the same shape to be "
            "compared. "
            f"expected: {', '.join(left) or 'none'}; actual: {', '.join(right) or 'none'}"
        )

    if left != right and sorted(left) == sorted(right):
        # Same columns, different order. Comparing positionally would compare
        # unrelated columns and report every row as a discrepancy, which reads
        # as a catastrophic data failure rather than as the ordering mistake it
        # is. Reordering silently would be a second comparison semantic.
        raise ValidationError(
            f"{what}: expected and actual name the same columns in a different order, "
            "and a Test compares them by position. Select them in the same order on "
            f"both sides. expected: {', '.join(left)}; actual: {', '.join(right)}"
        )

    missing_key = [column for column in key if column not in left]
    if missing_key:
        raise ValidationError(
            f"{what}: Primary key names {', '.join(missing_key)}, which expected does "
            f"not return. Its columns are {', '.join(left) or 'none'}"
        )
    absent_from_actual = [column for column in key if column not in right]
    if absent_from_actual:
        raise ValidationError(
            f"{what}: Primary key names {', '.join(absent_from_actual)}, which actual "
            f"does not return. Its columns are {', '.join(right) or 'none'}"
        )


def _check_key(frame: Any, *, side: str, key: tuple[str, ...], what: str) -> None:
    """A declared key that does not identify rows cannot correlate them.

    Blank and null are refused together, and duplicates are refused, on the same
    terms a load refuses them. See
    :func:`weaver.runtime.delta_sql.blank_key_predicate`. A Test whose key
    repeats would pair rows arbitrarily and call the result evidence.
    """

    if not key:
        return

    blank = frame.where(blank_key_predicate(key, alias="")).take(1)
    if blank:
        raise ValidationError(
            f"{what}: the declared Primary key ({', '.join(key)}) is null or "
            f"blank on the {side} side, so it cannot identify a row."
        )

    duplicated = frame.groupBy(*[f"`{column}`" for column in key]).count()
    repeated = duplicated.where("count > 1").take(1)
    if repeated:
        values = ", ".join(
            f"{column}={repeated[0][index]!r}" for index, column in enumerate(key)
        )
        raise ValidationError(
            f"{what}: the declared Primary key ({', '.join(key)}) repeats on "
            f"the {side} side ({values}), so it cannot correlate the two sides. "
            "Declare a key that identifies a row, or declare none."
        )


__all__ = [
    "ACTUAL",
    "EXPECTED",
    "RESERVED_COLUMNS",
    "SIDE_COLUMN",
    "SK_COLUMN",
    "compare",
]
