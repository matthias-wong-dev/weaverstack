"""The SQL fragments a Delta load is written in.

Small, shared and deliberately opinion-free: a join on a key, a null-safe
comparison, the predicate that decides a key is blank, the audit column names,
the table property every managed table carries. They are here rather than inline
in :mod:`weaver.runtime.table_load` because getting any one of them subtly wrong
is a silent data defect — a row that is never updated, a blank key admitted, a
table that cannot hold a column name with a space in it — so each is written
once, explained once, and tested once.

They were previously kept beside the generated Spark SQL load program, which no
longer exists: a Spark-SQL-authored table now compiles into a deployed
``SparkSqlTable`` module and loads through the ordinary Delta path. What
survived that removal is what was never really about generation.
"""

from __future__ import annotations

from ..declaration.metadata import (
    AUDIT_COLUMNS,
    AUDIT_LIVE_DELETE_DATETIME,
    PYTHON,
    audit_column_name,
)
from .load_contract import LoadContract

#: Every managed Delta table carries column mapping, for the same reason
#: :func:`weaver.declaration.ddl._create_table_sql` gives it: a declared column
#: name may contain spaces, and Delta refuses those in a physical schema unless
#: mapping is on. Staging carries the author's own columns forward, so a table
#: created without it fails on exactly the declarations Weaver permits.
COLUMN_MAPPING = "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"


def delta_audit_names() -> tuple[str, str, str]:
    """The insert, update and delete audit columns, spelled for Delta."""

    return tuple(audit_column_name(logical, PYTHON) for logical in AUDIT_COLUMNS)


def live_delete_literal() -> str:
    """The delete timestamp a *live* row carries — far enough future to sort last."""

    return f"CAST('{AUDIT_LIVE_DELETE_DATETIME}' AS TIMESTAMP)"


def key_join(left: str, right: str, columns) -> str:
    """Two relations matched on every key column."""

    return " AND ".join(f"{left}.`{c}` = {right}.`{c}`" for c in columns)


def changed_predicate(left: str, right: str, contract: LoadContract) -> str:
    """Whether a matched row differs, null-safely.

    ``<=>`` rather than ``<>`` because a column going to or from null is a
    change, and ``<>`` answers null to that question — so a row that lost a
    value would silently never be updated.
    """

    comparison = [
        column
        for column in contract.comparison_columns
        if column not in contract.primary_key
    ]
    if not comparison:
        # Nothing to compare: every matched row is unchanged by definition, and
        # saying so as `false` keeps the merge's shape identical either way.
        return "false"
    return " OR ".join(f"NOT ({left}.`{c}` <=> {right}.`{c}`)" for c in comparison)


def blank_key_predicate(columns, alias: str = "s") -> str:
    """A key column that is null, empty or only spaces is not a key.

    Blank is rejected alongside null deliberately: a key of whitespace matches
    nothing a human would call a match, and letting it through would create a
    row nobody can find again.

    ``alias`` is empty when the predicate is applied to a frame rather than
    inside a join, where there is no relation to qualify.
    """

    prefix = f"{alias}." if alias else ""
    predicates = [
        f"nullif(trim(CAST({prefix}`{c}` AS STRING)), '') IS NULL" for c in columns
    ]
    if len(predicates) == 1:
        return predicates[0]
    return "(" + " OR ".join(predicates) + ")"


__all__ = [
    "COLUMN_MAPPING",
    "blank_key_predicate",
    "changed_predicate",
    "delta_audit_names",
    "key_join",
    "live_delete_literal",
]
