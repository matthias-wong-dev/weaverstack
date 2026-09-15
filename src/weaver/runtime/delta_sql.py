"""Shared SQL fragments for Delta loads."""

from __future__ import annotations

from ..declaration.metadata import (
    AUDIT_COLUMNS,
    AUDIT_LIVE_DELETE_DATETIME,
    PYTHON,
    audit_column_name,
    signature_column_name,
)
from .load_contract import LoadContract

#: Delta column mapping permits declared column names containing spaces.
COLUMN_MAPPING = "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"

#: The text a null enters a signature as. It cannot be confused with a present
#: value, because a present value is written as its length, a colon and then
#: itself, so it always begins with a digit.
NULL_MARKER = "~"

#: Canonical forms for Spark types whose default text is session- or cast-dependent.
_CANONICAL_TEXT = {
    "boolean": "CAST(CAST({column} AS INT) AS STRING)",
    "binary": "hex({column})",
    "timestamp": "CAST(unix_micros({column}) AS STRING)",
    "timestamp_ntz": "date_format({column}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
}

_CANONICAL_FALLBACK = "CAST({column} AS STRING)"


def delta_audit_names() -> tuple[str, str, str]:
    return tuple(audit_column_name(logical, PYTHON) for logical in AUDIT_COLUMNS)


def delta_signature_name() -> str:
    return signature_column_name(PYTHON)


def live_delete_literal() -> str:
    """The delete timestamp a live row carries, far enough future to sort last."""

    return f"CAST('{AUDIT_LIVE_DELETE_DATETIME}' AS TIMESTAMP)"


def key_join(left: str, right: str, columns) -> str:
    return " AND ".join(f"{left}.`{c}` = {right}.`{c}`" for c in columns)


def qualified(alias: str, columns) -> str:
    prefix = f"{alias}." if alias else ""
    return ", ".join(f"{prefix}`{c}`" for c in columns)


def row_signature(alias: str, columns, types) -> str:
    """Build a stable, unambiguous Spark row signature."""

    prefix = f"{alias}." if alias else ""
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


def blank_key_predicate(columns, alias: str = "s") -> str:
    """Match a null, empty or whitespace-only key component."""

    prefix = f"{alias}." if alias else ""
    predicates = [
        f"nullif(trim(CAST({prefix}`{c}` AS STRING)), '') IS NULL" for c in columns
    ]
    if len(predicates) == 1:
        return predicates[0]
    return "(" + " OR ".join(predicates) + ")"


def violation_predicate(contract: LoadContract, alias: str = "s") -> str:
    prefix = f"{alias}." if alias else ""
    predicates = [blank_key_predicate(contract.primary_key, alias)]
    predicates += [f"{prefix}`{c}` IS NULL" for c in contract.not_null_columns]
    return " OR ".join(predicates)


def participates(columns, alias: str = "s") -> str:
    """Exclude incomplete tuples from unique-key grouping."""

    prefix = f"{alias}." if alias else ""
    return " AND ".join(f"{prefix}`{c}` IS NOT NULL" for c in columns)


def moves_off(columns, moving: str = "moving", holder: str = "holder") -> str:
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
