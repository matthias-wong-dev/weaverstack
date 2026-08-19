"""The SQL fragments a Delta load is written in.

Small, shared and deliberately opinion-free: a join on a key, the predicate that
decides a key is blank, the canonical text one value enters a row signature as,
the audit column names, the table property every managed table carries. They are
here rather than inline in :mod:`weaver.runtime.table_load` because getting any
one of them subtly wrong is a silent data defect — a row that is never updated, a
blank key admitted, a null indistinguishable from an empty string — so each is
written once, explained once, and tested once.

"""

from __future__ import annotations

from ..declaration.metadata import (
    AUDIT_COLUMNS,
    AUDIT_LIVE_DELETE_DATETIME,
    PYTHON,
    audit_column_name,
    signature_column_name,
)
from .load_contract import LoadContract

#: Every managed Delta table carries column mapping, for the same reason
#: :func:`weaver.declaration.ddl._create_table_sql` gives it: a declared column
#: name may contain spaces, and Delta refuses those in a physical schema unless
#: mapping is on. Staging carries the author's own columns forward, so a table
#: created without it fails on exactly the declarations Weaver permits.
COLUMN_MAPPING = "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"

#: The text a null enters a signature as. It cannot be confused with a present
#: value, because a present value is written as its length, a colon and then
#: itself — so it always begins with a digit.
NULL_MARKER = "~"

#: How each Spark type is spelled before it enters a row signature. Named where
#: the default rendering would move with something other than the value: a
#: timestamp's with the session time zone, a boolean's and a binary's with the
#: cast Spark happens to choose. Everything else — the numerics, decimal, date —
#: casts to text exactly and stably.
_CANONICAL_TEXT = {
    "boolean": "CAST(CAST({column} AS INT) AS STRING)",
    "binary": "hex({column})",
    "timestamp": "CAST(unix_micros({column}) AS STRING)",
    "timestamp_ntz": "date_format({column}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
}

_CANONICAL_FALLBACK = "CAST({column} AS STRING)"


def delta_audit_names() -> tuple[str, str, str]:
    """The insert, update and delete audit columns, spelled for Delta."""

    return tuple(audit_column_name(logical, PYTHON) for logical in AUDIT_COLUMNS)


def delta_signature_name() -> str:
    """The row-signature column, spelled for Delta."""

    return signature_column_name(PYTHON)


def live_delete_literal() -> str:
    """The delete timestamp a *live* row carries — far enough future to sort last."""

    return f"CAST('{AUDIT_LIVE_DELETE_DATETIME}' AS TIMESTAMP)"


def key_join(left: str, right: str, columns) -> str:
    """Two relations matched on every key column."""

    return " AND ".join(f"{left}.`{c}` = {right}.`{c}`" for c in columns)


def qualified(shortcut: str, columns) -> str:
    """A column list, qualified by one relation."""

    prefix = f"{shortcut}." if shortcut else ""
    return ", ".join(f"{prefix}`{c}`" for c in columns)


def row_signature(shortcut: str, columns, types) -> str:
    """The digest of one row's comparison state, as Spark spells it.

    ``sha2`` returns the hex text rather than bytes, which is what a Delta keyed
    target stores. A Warehouse stores the bytes; the two are never compared with
    each other, only with another signature from the same table.

    Each value is written as its length, a colon and its canonical text, so text
    containing the separator cannot be read as two values. The leading ``''``
    keeps the expression complete for a table with nothing to compare — every row
    then signs identically, which is what "nothing to compare" means.
    """

    prefix = f"{shortcut}." if shortcut else ""
    pieces = []
    for column in columns:
        reference = f"{prefix}`{column}`"
        template = _CANONICAL_TEXT.get(types.get(column, ""), _CANONICAL_FALLBACK)
        text = template.format(column=reference)
        pieces.append(
            f"CASE WHEN {reference} IS NULL THEN '{NULL_MARKER}'"
            f" ELSE concat(CAST(length({text}) AS STRING), ':', {text}) END"
        )
    payload = "concat('', " + ", ".join(pieces) + ")" if pieces else "''"
    return f"sha2({payload}, 256)"


def blank_key_predicate(columns, shortcut: str = "s") -> str:
    """A key column that is null, empty or only spaces is not a key.

    Blank is rejected alongside null deliberately: a key of whitespace matches
    nothing a human would call a match, and letting it through would create a
    row nobody can find again.

    ``shortcut`` is empty when the predicate is applied to a frame rather than
    inside a join, where there is no relation to qualify.
    """

    prefix = f"{shortcut}." if shortcut else ""
    predicates = [
        f"nullif(trim(CAST({prefix}`{c}` AS STRING)), '') IS NULL" for c in columns
    ]
    if len(predicates) == 1:
        return predicates[0]
    return "(" + " OR ".join(predicates) + ")"


def violation_predicate(contract: LoadContract, shortcut: str = "s") -> str:
    """A row that cannot be loaded whatever else is true of it.

    An unusable primary key, and a declared not-null column left empty. Only
    declared ones: a business column is nullable unless the object said
    otherwise.
    """

    prefix = f"{shortcut}." if shortcut else ""
    predicates = [blank_key_predicate(contract.primary_key, shortcut)]
    predicates += [f"{prefix}`{c}` IS NULL" for c in contract.not_null_columns]
    return " OR ".join(predicates)


def participates(columns, shortcut: str = "s") -> str:
    """A row takes part in a unique key only when its whole tuple is present.

    A null is not a value, so two rows carrying one are not two rows claiming the
    same thing — and ``GROUP BY`` would put them in one group.
    """

    prefix = f"{shortcut}." if shortcut else ""
    return " AND ".join(f"{prefix}`{c}` IS NOT NULL" for c in columns)


def moves_off(columns, moving: str = "moving", holder: str = "holder") -> str:
    """Whether one proposed row gives up a holder's unique value.

    Being written is not enough on its own: a row may be changing another column
    entirely and keeping the value it has.
    """

    return " OR ".join(
        f"{moving}.`{c}` <> {holder}.`{c}` OR {moving}.`{c}` IS NULL" for c in columns
    )


__all__ = [
    "COLUMN_MAPPING",
    "NULL_MARKER",
    "blank_key_predicate",
    "delta_audit_names",
    "delta_signature_name",
    "key_join",
    "live_delete_literal",
    "moves_off",
    "participates",
    "qualified",
    "row_signature",
    "violation_predicate",
]
