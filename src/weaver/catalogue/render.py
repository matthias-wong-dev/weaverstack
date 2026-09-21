"""Render catalogue projections as deterministic Warehouse T-SQL.

This module owns translation from internal keys and values to public catalogue
columns and vocabularies. Publication timestamps are supplied at installation
and excluded from projection comparison.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from ..declaration.metadata import AUDIT_LIVE_DELETE_DATETIME
from ..errors import BuildError
from ..tokens import BUILD_DATETIME_TOKEN
from .capacity import overflows
from .tables import (
    AUDIT_DELETE_COLUMN,
    AUDIT_INSERT_COLUMN,
    AUDIT_UPDATE_COLUMN,
    ITEM_SCOPE_COLUMNS,
    SCOPE_ITEM_NAME,
    SCOPE_ITEM_TYPE,
    CatalogueTable,
    RuntimeTable,
    public_column_name,
)
from .tsql import TIMESTAMP_TYPE, identifier, literal, qualified_name, typed_literal

Table = CatalogueTable | RuntimeTable

Row = Mapping[str, object]

NOW = "SYSDATETIME()"


@dataclass(frozen=True)
class InstallationScope:
    """The complete installation scope a statement may touch."""

    item_type: str
    item_name: str

    @property
    def columns(self) -> tuple[str, ...]:
        return ITEM_SCOPE_COLUMNS

    @property
    def values(self) -> Mapping[str, str]:
        return {
            SCOPE_ITEM_TYPE: self.item_type,
            SCOPE_ITEM_NAME: self.item_name,
        }

    @property
    def predicate(self) -> str:
        return self.predicate_for()

    def predicate_for(self, qualifier: str = "") -> str:
        prefix = f"{qualifier}." if qualifier else ""
        return " AND ".join(
            f"{prefix}{identifier(public_column_name(column))} = {literal(value)}"
            for column, value in self.values.items()
        )

    def owns(self, row: Row) -> bool:
        return all(row.get(column) == value for column, value in self.values.items())

    def __str__(self) -> str:
        return f"{self.item_type}/{self.item_name}"


@dataclass(frozen=True)
class InstallationScopes:
    """The exact installation scopes addressed by one statement.

    Empty scopes are rejected because they would produce an unbounded predicate.
    """

    scopes: tuple[InstallationScope, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "scopes", tuple(dict.fromkeys(self.scopes)))

    def __bool__(self) -> bool:
        return bool(self.scopes)

    def __iter__(self):
        return iter(self.scopes)

    def __len__(self) -> int:
        return len(self.scopes)

    @property
    def columns(self) -> tuple[str, ...]:
        return ITEM_SCOPE_COLUMNS

    @property
    def predicate(self) -> str:
        return self.predicate_for()

    def predicate_for(self, qualifier: str = "") -> str:
        """Render the scopes for composition with ``AND``.

        Multiple scopes require outer parentheses because ``AND`` binds more
        tightly than ``OR``; omitting them can delete or merge outside the keep
        condition.
        """

        if not self.scopes:
            raise BuildError(
                "cannot render an installation-scope predicate without scopes; "
                "it would address the whole catalogue"
            )
        if len(self.scopes) == 1:
            return self.scopes[0].predicate_for(qualifier)
        disjunction = " OR ".join(
            f"({scope.predicate_for(qualifier)})" for scope in self.scopes
        )
        return f"({disjunction})"

    def owns(self, row: Row) -> bool:
        return any(scope.owns(row) for scope in self.scopes)

    def __str__(self) -> str:
        return ", ".join(str(scope) for scope in self.scopes)


def column_set(columns: Iterable[str]) -> str | None:
    """Join columns without sorting; key order is significant.

    An empty set is null, distinguishing no key from a key with no columns.
    """

    joined = ", ".join(columns)
    return joined or None


# --- null-safe comparison -----------------------------------------------------
#
# T-SQL has no null-safe equality; ordinary equality and negation both mishandle
# nullable catalogue columns.


def _same(left: str, right: str) -> str:
    return f"({left} = {right} OR ({left} IS NULL AND {right} IS NULL))"


def _differs(left: str, right: str) -> str:
    return (
        f"({left} <> {right}"
        f" OR ({left} IS NULL AND {right} IS NOT NULL)"
        f" OR ({left} IS NOT NULL AND {right} IS NULL))"
    )


# --- statements ---------------------------------------------------------------


def sorted_rows(table: Table, rows: Iterable[Row]) -> tuple[Row, ...]:
    def sort_key(row: Row) -> tuple[str, ...]:
        return tuple(str(row.get(name) or "") for name in table.key)

    return tuple(sorted(rows, key=sort_key))


def _public(table: Table, name: str) -> str:
    return identifier(table.public_name_of(name))


# T-SQL limits a table value constructor to 1,000 rows.
VALUES_ROWS = 1000


def render_merge(
    table: Table,
    rows: Sequence[Row],
    *,
    scope: InstallationScope | InstallationScopes,
) -> str | None:
    """Render scoped inserts and changed-row updates.

    Unchanged rows do not advance ``row_update_datetime``. Publication columns
    are set only on insert, because an update may describe an object this build
    did not rebuild. Large inputs are split at :data:`VALUES_ROWS` into
    idempotent statements in one batch. Returns ``None`` for no rows.
    """

    rows = sorted_rows(table, rows)
    if not rows:
        return None
    _check_scope(table, rows, scope)
    _check_unique_keys(table, rows)

    if len(rows) > VALUES_ROWS:
        chunks = [
            rows[start : start + VALUES_ROWS]
            for start in range(0, len(rows), VALUES_ROWS)
        ]
        return "".join(_merge_statement(table, chunk, scope=scope) for chunk in chunks)
    return _merge_statement(table, rows, scope=scope)


def _merge_statement(
    table: Table,
    rows: Sequence[Row],
    *,
    scope: InstallationScope | InstallationScopes,
) -> str:
    source = _source_relation(table, rows)

    on = " AND ".join(
        _same(f"target.{_public(table, name)}", f"source.{_public(table, name)}")
        for name in table.key
    )
    # Keep the installation boundary explicit even though the key also carries it.
    scoped = scope.predicate_for("target")

    comparison = table.comparison_columns
    changed = " OR ".join(
        _differs(f"target.{_public(table, name)}", f"source.{_public(table, name)}")
        for name in comparison
    )
    updates = ", ".join(
        [
            f"target.{_public(table, name)} = source.{_public(table, name)}"
            for name in comparison
        ]
        + [f"target.{_public(table, AUDIT_UPDATE_COLUMN)} = {NOW}"]
    )

    # Name audit values so column-order changes cannot misplace the sentinel.
    supplied = {
        AUDIT_INSERT_COLUMN: NOW,
        AUDIT_UPDATE_COLUMN: NOW,
        # All three audit columns are physically not null.
        AUDIT_DELETE_COLUMN: literal(AUDIT_LIVE_DELETE_DATETIME, "timestamp"),
    }
    # Publication timestamps are insert-only: comparing them would change every row.
    supplied.update(
        {
            name: f"CAST('{BUILD_DATETIME_TOKEN}' AS {TIMESTAMP_TYPE})"
            for name in table.published_column_names
        }
    )
    insert_columns = ", ".join(_public(table, name) for name in table.physical_columns)
    insert_values = ", ".join(
        supplied[name] if name in supplied else f"source.{_public(table, name)}"
        for name in table.physical_columns
    )

    return (
        f"MERGE INTO {qualified_name(table)} AS target\n"
        f"USING (\n"
        f"        {source}\n"
        f") AS source\n"
        f"   ON {scoped}\n"
        f"  AND {on}\n"
        f"WHEN MATCHED AND ({changed}) THEN UPDATE SET {updates}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values});\n"
    )


def _value(table: Table, row: Row, name: str) -> str:
    """One projected value, refused rather than narrowed by the cast around it.

    Defence in depth behind the offline check: a build-derived value, or a
    direct caller of this renderer, reaches no other gate. Scoped to declaration
    and deployment data, because what a run records is retained under its own
    policy.
    """

    if isinstance(table, CatalogueTable):
        for overflow in overflows(table, {name: row.get(name)}):
            raise BuildError(
                f"{table.name} cannot record this build: {overflow.describe()}."
            )
    return typed_literal(row.get(name), table.column(name))


def _source_relation(table: Table, rows: Sequence[Row]) -> str:
    """Render a typed merge source.

    Casts outside ``VALUES`` settle the type of all-null columns.
    """

    tuples = ",\n                    ".join(
        "(" + ", ".join(_value(table, row, name) for name in table.column_names) + ")"
        for row in rows
    )
    # Positional raw names remain distinct from public output aliases.
    raw = [f"c{index}" for index, _name in enumerate(table.column_names)]
    projected = ", ".join(
        f"CAST({identifier(raw[index])} AS {table.column(name).warehouse_type})"
        f" AS {_public(table, name)}"
        for index, name in enumerate(table.column_names)
    )
    names = ", ".join(identifier(name) for name in raw)
    return (
        f"SELECT {projected}\n"
        f"          FROM (VALUES\n"
        f"                    {tuples}\n"
        f"               ) AS source_values({names})"
    )


def render_keyed_merge(table: Table, rows: Sequence[Row]) -> str | None:
    """Merge runtime rows by their complete keys.

    Later rows win when a batch repeats a key, avoiding an invalid T-SQL source
    that matches one target twice. Returns ``None`` for no rows.
    """

    latest: dict[tuple, Row] = {}
    for row in rows:
        latest[tuple(row.get(name) for name in table.key)] = row
    ordered = sorted_rows(table, latest.values())
    if not ordered:
        return None
    if len(ordered) > VALUES_ROWS:
        chunks = [
            ordered[start : start + VALUES_ROWS]
            for start in range(0, len(ordered), VALUES_ROWS)
        ]
        return "".join(_keyed_merge_statement(table, chunk) for chunk in chunks)
    return _keyed_merge_statement(table, ordered)


def _keyed_merge_statement(table: Table, rows: Sequence[Row]) -> str:
    on = " AND ".join(
        _same(f"target.{_public(table, name)}", f"source.{_public(table, name)}")
        for name in table.key
    )
    comparison = table.comparison_columns
    changed = " OR ".join(
        _differs(f"target.{_public(table, name)}", f"source.{_public(table, name)}")
        for name in comparison
    )
    updates = ", ".join(
        [
            f"target.{_public(table, name)} = source.{_public(table, name)}"
            for name in comparison
        ]
        + [f"target.{_public(table, AUDIT_UPDATE_COLUMN)} = {NOW}"]
    )
    supplied = {
        AUDIT_INSERT_COLUMN: NOW,
        AUDIT_UPDATE_COLUMN: NOW,
        AUDIT_DELETE_COLUMN: literal(AUDIT_LIVE_DELETE_DATETIME, "timestamp"),
    }
    insert_columns = ", ".join(_public(table, name) for name in table.physical_columns)
    insert_values = ", ".join(
        supplied[name] if name in supplied else f"source.{_public(table, name)}"
        for name in table.physical_columns
    )
    return (
        f"MERGE INTO {qualified_name(table)} AS target\n"
        f"USING (\n"
        f"        {_source_relation(table, rows)}\n"
        f") AS source\n"
        f"   ON {on}\n"
        f"WHEN MATCHED AND ({changed}) THEN UPDATE SET {updates}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values});\n"
    )


def render_delete_obsolete(
    table: Table,
    rows: Sequence[Row],
    *,
    scope: InstallationScope | InstallationScopes,
) -> str | None:
    """Delete scoped rows absent from the projection.

    An empty projection produces a scoped delete. The Installation table instead
    returns ``None`` because its key is the scope itself. Keep rows form a
    relation to avoid T-SQL's expression limit; batches use ``UNION ALL`` within
    one statement because separate deletes would remove rows another batch keeps.
    """

    rows = sorted_rows(table, rows)
    _check_scope(table, rows, scope)
    if not rows:
        return f"DELETE FROM {qualified_name(table)}\n WHERE {scope.predicate}\n"

    beyond = tuple(name for name in table.key if name not in scope.columns)
    if not beyond:
        return None

    # Across scopes, the complete key prevents one installation's matching object
    # from preserving another installation's row.
    identity = table.key if isinstance(scope, InstallationScopes) else beyond

    # A qualified target correlates without T-SQL's second FROM clause.
    matched = "\n                     AND ".join(
        _same(
            f"keep.{_public(table, name)}",
            f"{qualified_name(table)}.{_public(table, name)}",
        )
        for name in identity
    )
    return (
        f"DELETE FROM {qualified_name(table)}\n"
        f" WHERE {scope.predicate}\n"
        f"   AND NOT EXISTS (\n"
        f"           SELECT 1\n"
        f"             FROM (\n"
        f"                  {_keep_relation(table, rows, identity)}\n"
        f"                  ) AS keep\n"
        f"            WHERE {matched}\n"
        f"       )\n"
    )


def render_delete_rows(table: Table, rows: Sequence[Row]) -> str | None:
    """Delete exactly the rows named by their complete table keys."""

    rows = sorted_rows(table, rows)
    if not rows:
        return None

    matched = "\n                     AND ".join(
        _same(
            f"gone.{_public(table, name)}",
            f"{qualified_name(table)}.{_public(table, name)}",
        )
        for name in table.key
    )
    return (
        f"DELETE FROM {qualified_name(table)}\n"
        f" WHERE EXISTS (\n"
        f"           SELECT 1\n"
        f"             FROM (\n"
        f"                  {_keep_relation(table, rows, table.key)}\n"
        f"                  ) AS gone\n"
        f"            WHERE {matched}\n"
        f"       )\n"
    )


def _keep_relation(table: Table, rows: Sequence[Row], identity: Sequence[str]) -> str:
    """Render kept identities with explicit types across all branches."""

    raw = [f"c{index}" for index, _name in enumerate(identity)]
    projected = ", ".join(
        f"CAST({identifier(raw[index])} AS {table.column(name).warehouse_type})"
        f" AS {_public(table, name)}"
        for index, name in enumerate(identity)
    )
    names = ", ".join(identifier(name) for name in raw)
    branches = []
    for start in range(0, len(rows), VALUES_ROWS):
        tuples = ",\n                              ".join(
            "(" + ", ".join(_value(table, row, name) for name in identity) + ")"
            for row in rows[start : start + VALUES_ROWS]
        )
        branches.append(
            f"SELECT {projected}\n"
            f"                    FROM (VALUES\n"
            f"                              {tuples}\n"
            f"                         ) AS keep_values({names})"
        )
    return "\n                  UNION ALL\n                  ".join(branches)


def render_delete_scope(
    table: Table,
    *,
    scope: InstallationScope | InstallationScopes,
) -> str:
    """Delete whole installations during explicit target decommissioning.

    An ordinary build must not interpret an omitted target type as removal.
    """

    return f"DELETE FROM {qualified_name(table)}\n WHERE {scope.predicate}\n"


def _check_unique_keys(table: Table, rows: Sequence[Row]) -> None:
    """Reject duplicate merge-source keys during generation."""

    seen: dict[tuple, int] = {}
    for row in rows:
        key = tuple(row.get(name) for name in table.key)
        seen[key] = seen.get(key, 0) + 1
    duplicated = [key for key, count in seen.items() if count > 1]
    if duplicated:
        shown = "; ".join(
            ", ".join(str(part) for part in key) for key in duplicated[:3]
        )
        raise ValueError(
            f"{table.qualified}: projected rows contain {len(duplicated)} duplicate "
            f"key(s) ({shown}); each merge source key must be unique"
        )


def _check_scope(
    table: Table,
    rows: Iterable[Row],
    scope: InstallationScope | InstallationScopes,
) -> None:
    """Reject rows outside the statement's installation scope."""

    stray = [row for row in rows if not scope.owns(row)]
    if stray:
        found = ", ".join(
            "/".join(repr(row.get(column)) for column in scope.columns)
            for row in stray[:3]
        )
        raise ValueError(
            f"{table.qualified}: {len(stray)} row(s) do not belong to installation "
            f"{scope} ({found}). A statement may only touch one installation"
        )
