"""Compare Test relations held as rows, with the semantics of :mod:`test_compare`.

A semantic model Test reads its expected side over TDS or Spark SQL and its
actual side as DAX, so neither side is a Spark DataFrame. Equality, shape and
key rules are the same: both sides are sets, compared by column position, and
the Primary key only pairs discrepancies.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Sequence

from ..errors import ValidationError
from .test_compare import ACTUAL, EXPECTED, SIDE_COLUMN, SK_COLUMN, _check_shape


@dataclass(frozen=True)
class Relation:
    """Named columns and positional rows. A side with no rows may name none."""

    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]

    @classmethod
    def of(cls, rows: Sequence[dict]) -> "Relation":
        columns = tuple(rows[0]) if rows else ()
        return cls(columns, tuple(tuple(row[c] for c in columns) for row in rows))


def compare_rows(
    expected: Relation,
    actual: Relation,
    *,
    primary_key: Sequence[str] = (),
    what: str = "Test",
) -> list[dict]:
    """Return discrepancy rows, refusing shapes or keys that cannot be compared.

    Each row carries :data:`SIDE_COLUMN` and :data:`SK_COLUMN` before the Test's
    columns, under the expected side's names.
    """

    key = tuple(primary_key)
    # Transport names no columns for an empty result, which then has no shape
    # to disagree with.
    if expected.rows and actual.rows:
        _check_shape(expected, actual, key=key, what=what)
    columns = expected.columns or actual.columns
    for relation, side in ((expected, EXPECTED), (actual, ACTUAL)):
        if relation.rows:
            _check_key(relation, side=side, key=key, what=what)

    wanted = _distinct(expected.rows)
    found = _distinct(actual.rows)
    discrepancies = [(EXPECTED, wanted[k]) for k in wanted if k not in found] + [
        (ACTUAL, found[k]) for k in found if k not in wanted
    ]
    positions = [columns.index(c) for c in key]
    if key:
        # Pair rows from the same changed entity under one diagnostic key.
        ordered = sorted(
            {tuple(_canonical(row[p]) for p in positions) for _, row in discrepancies},
            key=_sort_key,
        )
        rank = {value: index + 1 for index, value in enumerate(ordered)}
        surrogate = [
            rank[tuple(_canonical(row[p]) for p in positions)]
            for _, row in discrepancies
        ]
    else:
        surrogate = list(range(1, len(discrepancies) + 1))
    return [
        {SIDE_COLUMN: side, SK_COLUMN: sk, **dict(zip(columns, row))}
        for (side, row), sk in zip(discrepancies, surrogate)
    ]


def _check_key(relation: Relation, *, side: str, key: tuple[str, ...], what: str):
    if not key:
        return
    absent = [c for c in key if c not in relation.columns]
    if absent:
        raise ValidationError(
            f"{what}: Primary key names {', '.join(absent)}, which {side} does not "
            f"return. Its columns are {', '.join(relation.columns) or 'none'}"
        )
    positions = [relation.columns.index(c) for c in key]
    seen = set()
    for row in relation.rows:
        values = tuple(row[p] for p in positions)
        if any(v is None or (isinstance(v, str) and not v.strip()) for v in values):
            raise ValidationError(
                f"{what}: the declared Primary key ({', '.join(key)}) is null or "
                f"blank on the {side} side, so it cannot identify a row."
            )
        canonical = tuple(_canonical(v) for v in values)
        if canonical in seen:
            shown = ", ".join(f"{c}={v!r}" for c, v in zip(key, values))
            raise ValidationError(
                f"{what}: the declared Primary key ({', '.join(key)}) repeats on "
                f"the {side} side ({shown}), so it cannot correlate the two sides. "
                "Declare a key that identifies a row, or declare none."
            )
        seen.add(canonical)


def _distinct(rows) -> dict:
    """Rows by canonical value, keeping each value's first transported form."""

    distinct = {}
    for row in rows:
        distinct.setdefault(tuple(_canonical(v) for v in row), row)
    return distinct


#: How DAX transports a datetime, and how SQL text spells one.
_DATETIME = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?"
)


def _canonical(value: Any):
    """One tabular value whatever transported it.

    Numbers compare by value, dates as midnight datetimes, and a timezone-aware
    datetime in UTC. Text is compared exactly.
    """

    if value is None:
        return None
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, (int, Decimal)):
        return ("number", Decimal(value))
    if isinstance(value, float):
        try:
            return ("number", Decimal(repr(value)))
        except InvalidOperation:
            return ("float", repr(value))
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return ("datetime", value)
    if isinstance(value, date):
        return ("datetime", datetime(value.year, value.month, value.day))
    if isinstance(value, str) and _DATETIME.fullmatch(value):
        return _canonical(datetime.fromisoformat(value.replace("Z", "+00:00")))
    if isinstance(value, str):
        return ("text", value)
    return ("other", repr(value))


def _sort_key(values):
    return tuple(("",) if v is None else v for v in values)


__all__ = ["Relation", "compare_rows"]
