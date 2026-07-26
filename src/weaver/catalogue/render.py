"""Rendering catalogue rows as deterministic, scoped Spark SQL.

Every statement this module produces is frozen into a build bundle at generation
time and executed unchanged (build-philosophy §3, §4). Three properties follow,
and each is enforced here rather than trusted to a caller:

**Deterministic.** The same rows always render the same text, byte for byte. Rows
are sorted by their key before rendering, so a mapping's iteration order cannot
change a payload — and therefore cannot change a bundle's identity (§10).

**Scoped.** Every ``DELETE`` and every ``MERGE`` predicate names one
``repository`` and one ``target_type``. A Lakehouse build physically cannot touch
a Warehouse row, because the scope is not an argument a renderer might forget: it
is part of the row's identity and part of every statement's ``WHERE``.

**Explicit about values.** Every literal is cast to its declared column type, so
a null is a typed null and a row of all-nulls cannot silently change the source
frame's schema. Strings are escaped for Spark's default parser, where a backslash
escapes.

The one thing deliberately *not* frozen is the clock. ``current_timestamp()`` is
rendered as a call, not as a literal: a rendered time would make the same input
produce a different payload every run, which would destroy bundle identity for no
gain. The engine supplies the instant; the payload stays stable.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from ..ses.metadata import AUDIT_LIVE_DELETE_DATETIME
from .tables import (
    AUDIT_DELETE_COLUMN,
    AUDIT_INSERT_COLUMN,
    AUDIT_UPDATE_COLUMN,
    BOOLEAN,
    CATALOGUE_SCHEMA,
    SCOPE_REPOSITORY,
    SCOPE_TARGET_TYPE,
    TIMESTAMP,
    CatalogueTable,
)

#: A row as projected: column name to value. Values are ``str``, ``bool`` or
#: ``None`` — nothing needing a renderer of its own.
Row = Mapping[str, object]


@dataclass(frozen=True)
class InstallationScope:
    """The one installation a statement may touch.

    Carried as a value rather than passed as two strings, so a renderer cannot be
    called without it and a caller cannot supply half of it.
    """

    repository: str
    target_type: str

    @property
    def predicate(self) -> str:
        return (
            f"{identifier(SCOPE_REPOSITORY)} = {literal(self.repository)}"
            f" AND {identifier(SCOPE_TARGET_TYPE)} = {literal(self.target_type)}"
        )

    def owns(self, row: Row) -> bool:
        return (
            row.get(SCOPE_REPOSITORY) == self.repository
            and row.get(SCOPE_TARGET_TYPE) == self.target_type
        )

    def __str__(self) -> str:
        return f"{self.repository}/{self.target_type}"


def identifier(name: str) -> str:
    """A back-tick quoted Spark identifier, safe for spaces and keywords."""

    return "`" + name.replace("`", "``") + "`"


def qualified_name(table: CatalogueTable) -> str:
    return f"{identifier(CATALOGUE_SCHEMA)}.{identifier(table.name)}"


def literal(value: object) -> str:
    """One value as a Spark SQL literal.

    Spark's default parser treats a backslash as an escape, so both it and the
    quote are escaped. Booleans and nulls are rendered as themselves rather than
    as strings that happen to read that way.
    """

    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"
    if isinstance(value, (int, float)):
        return repr(value)
    raise TypeError(
        f"catalogue values are strings, booleans or null, not {type(value).__name__}"
    )


def typed_literal(value: object, column_type: str) -> str:
    """One value, cast to its declared type.

    The cast is not decoration. A ``MERGE`` source is a ``SELECT`` union whose
    schema comes from its first branch, so an uncast null would type a column by
    accident and a later branch could then fail to match the target.
    """

    if column_type == BOOLEAN and value is not None and not isinstance(value, bool):
        raise TypeError(f"expected a boolean for a {column_type} column, got {value!r}")
    return f"CAST({literal(value)} AS {column_type.upper()})"


def column_set(columns: Iterable[str]) -> str | None:
    """A comma-separated column set, declared order preserved.

    Order is meaning here — a key on ``(Region, Country)`` is not the same key as
    one on ``(Country, Region)`` — so this never sorts. An empty set is null
    rather than an empty string, because "no key" and "a key of no columns" are
    different claims.
    """

    joined = ", ".join(columns)
    return joined or None


# --- statements ---------------------------------------------------------------


def sorted_rows(table: CatalogueTable, rows: Iterable[Row]) -> tuple[Row, ...]:
    """Rows in key order — the canonical order every statement renders in."""

    def sort_key(row: Row) -> tuple[str, ...]:
        return tuple(str(row.get(name) or "") for name in table.key)

    return tuple(sorted(rows, key=sort_key))


def render_merge(
    table: CatalogueTable, rows: Sequence[Row], *, scope: InstallationScope
) -> str | None:
    """A scoped ``MERGE`` that inserts new rows and updates changed ones.

    Returns None when there is nothing to merge, so a caller emits no action
    rather than an empty statement.

    An unchanged row is a genuine no-op: the ``MATCHED`` branch is guarded by a
    null-safe comparison of every non-key column, so it neither writes nor
    advances ``row_update_datetime``. That is what makes a rebuild of unchanged
    SES leave the catalogue alone.
    """

    rows = sorted_rows(table, rows)
    if not rows:
        return None
    _check_scope(table, rows, scope)
    _check_unique_keys(table, rows)

    columns = table.column_names
    source = _source_relation(table, rows)

    on = " AND ".join(
        f"target.{identifier(name)} <=> source.{identifier(name)}" for name in table.key
    )
    # The target side is narrowed to this installation as well as matched on the
    # key. The key already carries the scope, so this is belt and braces — and it
    # is the belt that shows in a review.
    scoped = f"target.{identifier(SCOPE_REPOSITORY)} = {literal(scope.repository)} AND " \
             f"target.{identifier(SCOPE_TARGET_TYPE)} = {literal(scope.target_type)}"

    comparison = table.comparison_columns
    changed = " OR ".join(
        f"NOT (target.{identifier(name)} <=> source.{identifier(name)})"
        for name in comparison
    )
    updates = ", ".join(
        [f"target.{identifier(name)} = source.{identifier(name)}" for name in comparison]
        + [f"target.{identifier(AUDIT_UPDATE_COLUMN)} = current_timestamp()"]
    )

    # Named rather than positional: the audit columns are appended by the build in
    # a fixed order, and pairing values to that order by position would put the
    # sentinel in the wrong column the day the order changed.
    audit = {
        AUDIT_INSERT_COLUMN: "current_timestamp()",
        AUDIT_UPDATE_COLUMN: "current_timestamp()",
        # A live row's delete datetime is a sentinel maximum, never null — all
        # three audit columns are physically not null.
        AUDIT_DELETE_COLUMN: (
            f"CAST({literal(AUDIT_LIVE_DELETE_DATETIME)} AS {TIMESTAMP.upper()})"
        ),
    }
    insert_columns = ", ".join(
        identifier(name) for name in table.physical_columns
    )
    insert_values = ", ".join(
        audit[name] if name in audit else f"source.{identifier(name)}"
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
        f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})\n"
    )


def _source_relation(table: CatalogueTable, rows: Sequence[Row]) -> str:
    """The merge source: one ``VALUES`` relation, cast by an enclosing projection.

    The obvious construction — one ``SELECT`` of cast literals per row, chained
    with ``UNION ALL`` — does not scale, and the failure is nasty. Spark generates
    Java for the plan, a method's bytecode may not exceed 64 KB, and a union of a
    hundred projections exceeds it: the catalogue's own ``ColumnDictionary`` has a
    row per column of every catalogue table, and that was enough to break the
    bootstrap with ``Code grows beyond 64 KB``.

    One ``VALUES`` relation is a single plan node however many rows it carries, so
    the casts move outward into one projection over it. The values themselves are
    bare literals: ``VALUES`` unifies a column's type across rows — all-null
    becomes void — and the enclosing ``CAST`` settles it either way, which is what
    keeps the source's schema exactly the target's.
    """

    tuples = ",\n                    ".join(
        "(" + ", ".join(literal(row.get(name)) for name in table.column_names) + ")"
        for row in rows
    )
    # Positional names for the raw relation, so a column called `repository` in the
    # values cannot be confused with the aliased output of the same name.
    raw = [f"c{index}" for index, _name in enumerate(table.column_names)]
    projected = ", ".join(
        f"CAST({identifier(raw[index])} AS {table.column(name).type.upper()})"
        f" AS {identifier(name)}"
        for index, name in enumerate(table.column_names)
    )
    names = ", ".join(identifier(name) for name in raw)
    return (
        f"SELECT {projected}\n"
        f"          FROM VALUES\n"
        f"                    {tuples}\n"
        f"               AS source_values({names})"
    )


def render_delete_obsolete(
    table: CatalogueTable, rows: Sequence[Row], *, scope: InstallationScope
) -> str | None:
    """A scoped ``DELETE`` of everything in this installation the rows do not claim.

    An installation that now projects *no* rows for a table gets a plain scoped
    delete: rendering nothing there would leave stale rows behind forever.

    Returns None only for a table whose key is the installation scope itself —
    :data:`~weaver.catalogue.tables.INSTALLATION`. There is at most one such row
    per scope, so "the rows this projection does not claim" is empty by
    construction and the merge alone keeps it current. Rendering a predicate over
    no columns beyond the scope would delete the very row about to be merged.

    The predicate is written as a disjunction of key equalities rather than a
    tuple ``IN``: it renders identically on any engine, reads in a review, and
    keeps the scope visible at the front of the statement.
    """

    rows = sorted_rows(table, rows)
    _check_scope(table, rows, scope)
    if not rows:
        return f"DELETE FROM {qualified_name(table)}\n WHERE {scope.predicate}\n"

    # Only the key columns beyond the scope: the scope is already in the WHERE.
    identity = tuple(name for name in table.key if name not in (SCOPE_REPOSITORY, SCOPE_TARGET_TYPE))
    if not identity:
        return None

    keep = "\n           OR ".join(
        "("
        + " AND ".join(
            f"{identifier(name)} <=> {typed_literal(row.get(name), table.column(name).type)}"
            for name in identity
        )
        + ")"
        for row in rows
    )
    return (
        f"DELETE FROM {qualified_name(table)}\n"
        f" WHERE {scope.predicate}\n"
        f"   AND NOT (\n"
        f"              {keep}\n"
        f"           )\n"
    )


def render_delete_scope(table: CatalogueTable, *, scope: InstallationScope) -> str:
    """A scoped ``DELETE`` of one whole installation from one table.

    This is installation pruning: it is what decommissioning a target does, and it
    is never what an ordinary build does. A build that did not include a target
    type has no opinion about it, which is a different thing from having removed
    it.
    """

    return f"DELETE FROM {qualified_name(table)}\n WHERE {scope.predicate}\n"


def render_delete_repository(table: CatalogueTable, *, repository: str) -> str:
    """A ``DELETE`` of every installation of one repository from one table.

    Deliberately not scoped by target type — this is the repository lifecycle
    operation, and being cross-scope is the whole of what distinguishes it. It is
    never reached from a build.
    """

    return (
        f"DELETE FROM {qualified_name(table)}\n"
        f" WHERE {identifier(SCOPE_REPOSITORY)} = {literal(repository)}\n"
    )


def _check_unique_keys(table: CatalogueTable, rows: Sequence[Row]) -> None:
    """Refuse a merge whose source holds two rows with one key.

    Delta fails a ``MERGE`` when several source rows match one target row, and it
    fails at *install* time — long after the bundle was reviewed. Catching it here
    turns a late, obscure runtime error into a generation error naming the table
    and the key, and it means a duplicate can only be a projection fault rather
    than a mystery.
    """

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
            f"{table.qualified}: {len(duplicated)} duplicated key(s) in the projected "
            f"rows ({shown}) — a merge source must hold one row per key"
        )


def _check_scope(
    table: CatalogueTable, rows: Iterable[Row], scope: InstallationScope
) -> None:
    """Refuse to render a statement over rows from another installation.

    The guard exists because the consequence is invisible: a stray row would be
    merged into the wrong installation's scope and read as truth. Cheap to check,
    expensive to discover.
    """

    stray = [row for row in rows if not scope.owns(row)]
    if stray:
        found = ", ".join(
            f"{row.get(SCOPE_REPOSITORY)!r}/{row.get(SCOPE_TARGET_TYPE)!r}"
            for row in stray[:3]
        )
        raise ValueError(
            f"{table.qualified}: {len(stray)} row(s) do not belong to installation "
            f"{scope} ({found}) — a statement may only touch one installation"
        )
