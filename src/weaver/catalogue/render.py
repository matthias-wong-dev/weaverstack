"""Render catalogue projections as deterministic T-SQL statements.

The catalogue is a set of Warehouse tables under ``_``, so every statement here
is T-SQL and reaches the Warehouse over TDS. Publication timestamps are supplied
at installation time and are excluded from projection comparison.

Layers above hold plain Python values under internal snake-case keys. The
translation into the public column names and stored vocabularies the ``_`` schema
publishes happens here and nowhere else.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from ..declaration.metadata import AUDIT_LIVE_DELETE_DATETIME
from ..errors import BuildError
from ..tokens import BUILD_DATETIME_TOKEN
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

#: What these renderers work on. A projected catalogue table and a
#: runtime-maintained one are merged, deleted and compared identically; what
#: differs is which side decides the rows, not how a statement is written.
Table = CatalogueTable | RuntimeTable

#: A row as projected: column name to value. Values are ``str``, ``bool`` or
#: ``None``, so nothing needs a renderer of its own.
Row = Mapping[str, object]

#: What T-SQL spells "now".
NOW = "SYSDATETIME()"


@dataclass(frozen=True)
class InstallationScope:
    """The one installation a statement may touch.

    Carried as a value rather than passed as two strings, so a renderer cannot be
    called without it and a caller cannot supply half of it.
    """

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
    """Several installations addressed by one statement.

    A build's items share the same physical catalogue tables, so addressing them
    one at a time costs a round trip per item per table for an answer one
    predicate contains.

    Still a bounded address: the predicate names exactly the scopes it was
    given, so widening what a build touches means widening this at the call
    site. An empty collection is refused rather than rendered, because ``WHERE``
    with no predicate is every row in the catalogue.
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
        """The scopes as one predicate, safe to compose with ``AND``.

        The outer parentheses are load-bearing: removing them loses data.
        ``AND`` binds tighter than ``OR``, so a bare ``(a AND b) OR (c AND d)``
        embedded in

        .. code-block:: text

            WHERE <scopes> AND NOT (<keep>)

        reassociates to ``(a AND b) OR ((c AND d) AND NOT (<keep>))``, and the
        first scope's rows are then deleted unconditionally, keep-list and all.
        The same reassociation in a ``MERGE`` ``ON`` clause makes every source
        row match every target row in the later scopes. Both are silent until
        a second installation exists.
        """

        if not self.scopes:
            raise BuildError(
                "an installation-scope predicate over no scopes would address "
                "the whole catalogue; the caller must not reach a statement"
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
    """A comma-separated column set, declared order preserved.

    Order is meaning, and ``(Region, Country)`` is not the key
    ``(Country, Region)`` is, so this never sorts. An empty set is null rather
    than an empty string: "no key" and "a key of no columns" are different claims.
    """

    joined = ", ".join(columns)
    return joined or None


# --- null-safe comparison -----------------------------------------------------
#
# T-SQL has no null-safe equality operator, and the catalogue is full of nullable
# columns. Written out rather than approximated, because both halves are wrong in
# a way that is silent: `a = b` skips rows where either side is null, and
# `NOT (a = b)` is UNKNOWN, and not TRUE, when exactly one side is.


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
    """Rows in key order, the canonical order every statement renders in."""

    def sort_key(row: Row) -> tuple[str, ...]:
        return tuple(str(row.get(name) or "") for name in table.key)

    return tuple(sorted(rows, key=sort_key))


def _public(table: Table, name: str) -> str:
    return identifier(table.public_name_of(name))


#: How many rows one table value constructor carries. T-SQL accepts at most a
#: thousand, and a catalogue table passes that without the estate being large:
#: a thousand described columns is an ordinary repository. So the rows are
#: chunked, and the chunk is the engine's limit rather than a guess.
VALUES_ROWS = 1000


def render_merge(
    table: Table,
    rows: Sequence[Row],
    *,
    scope: InstallationScope | InstallationScopes,
) -> str | None:
    """A scoped ``MERGE`` that inserts new rows and updates changed ones.

    Returns None when there is nothing to merge, so a caller emits no action
    rather than an empty statement.

    An unchanged row is a no-op: the ``MATCHED`` branch is guarded by a
    null-safe comparison of every non-key column, so it neither writes nor
    advances ``row_update_datetime``.

    A published column is set on insert and never on update. Every object a
    build rebuilds reaches this statement as an insert, being either new or
    stripped of its Registry claim before any physical work began. So an update
    can only be a row whose projection changed while the object was left alone.
    Dating such a row to this build would say it was rebuilt when it was not.

    Above :data:`VALUES_ROWS` the result is several ``MERGE`` statements rather
    than one. They are returned together because they are one decision and one
    action; T-SQL is content with several statements in a batch, and each is
    idempotent, so the split changes nothing observable.
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
    # The target side is narrowed to this installation as well as matched on the
    # key. The key already carries the scope, so this is belt and braces, and it
    # is the belt that shows in a review.
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

    # Named rather than positional: the audit columns are appended by the build in
    # a fixed order, and pairing values to that order by position would put the
    # sentinel in the wrong column the day the order changed.
    supplied = {
        AUDIT_INSERT_COLUMN: NOW,
        AUDIT_UPDATE_COLUMN: NOW,
        # A live row's delete datetime is a sentinel maximum, never null, because
        # all three audit columns are physically not null.
        AUDIT_DELETE_COLUMN: literal(AUDIT_LIVE_DELETE_DATETIME, "timestamp"),
    }
    # A published column appears here and in no other clause. Not in the source
    # relation, because no projection carries it; not in the comparison, because a
    # value that is new every build would make every row differ; and not in the
    # UPDATE, which is the substantive decision. See the note above the
    # statement.
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


def _source_relation(table: Table, rows: Sequence[Row]) -> str:
    """The merge source: one table value constructor, cast by a projection over it.

    The casts sit outside rather than in every row. A ``VALUES`` constructor
    unifies each column's type across its rows, so an all-null column would take
    whatever type the engine inferred and could fail to match the target; one
    enclosing ``CAST`` settles it whatever the rows hold.
    """

    tuples = ",\n                    ".join(
        "("
        + ", ".join(
            typed_literal(row.get(name), table.column(name))
            for name in table.column_names
        )
        + ")"
        for row in rows
    )
    # Positional names for the raw relation, so a value column cannot be confused
    # with the aliased output of the same name.
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
    """A ``MERGE`` that updates rows by their whole key, or inserts them.

    For runtime state a run writes as it goes, such as ``_.Bookmark``, rather than
    for a build's projection, so there is no installation scope: the key carries
    item, so the ``ON`` clause identifies exactly the rows named and nothing
    wider. That is the whole difference from :func:`render_merge`, which narrows
    the target to the installation it is publishing.

    Later rows win where a batch names one key twice. An earlier value for the
    same object is superseded rather than a conflict, and T-SQL refuses a
    ``MERGE`` whose source matches one target row twice.

    Returns None when there is nothing to write.
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
    """A scoped ``DELETE`` of everything in this installation the rows do not claim.

    An installation projecting no rows for a table gets a plain scoped delete;
    rendering nothing would leave stale rows behind forever.

    Returns None only for a table keyed by the installation scope itself
    (:data:`~weaver.catalogue.tables.INSTALLATION`): there is at most one such
    row per scope, so a predicate over no columns beyond the scope would delete
    the row about to be merged.

    The rows the build keeps are a relation rather than a predicate. A
    disjunction of key equalities grows a term per row per key column, and
    ColumnDictionary holds a row per column of every object: five hundred
    objects of fifteen columns reach the engine's expression limit. A table
    value constructor grows in rows only, so the comparison is written once.

    Unlike :func:`render_merge`, this cannot be split into several statements,
    because each part deletes what the others keep, so the constructor's
    thousand-row cap is met with ``UNION ALL`` inside the one statement.
    """

    rows = sorted_rows(table, rows)
    _check_scope(table, rows, scope)
    if not rows:
        return f"DELETE FROM {qualified_name(table)}\n WHERE {scope.predicate}\n"

    beyond = tuple(name for name in table.key if name not in scope.columns)
    if not beyond:
        return None

    # Which key columns the kept rows are identified by, and the one place the
    # aggregated form differs. Within one installation the scope is
    # already in the WHERE, so the columns beyond it identify a row. Across
    # several, they do not: two installations can hold the same `DWG.Customer`,
    # and a keep-list naming only the object would spare one installation's row
    # because another installation still claims that name. So the aggregated
    # delete identifies rows by the whole key, scope columns included.
    identity = table.key if isinstance(scope, InstallationScopes) else beyond

    # The target is named in full rather than aliased: a DELETE that aliases its
    # target needs T-SQL's second FROM clause, and the qualified name correlates
    # without it.
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
    """A ``DELETE`` of exactly these rows, identified by the table's own key.

    The counterpart to :func:`render_delete_obsolete`, for a caller that has read
    the table and knows which rows are going. It names what it removes rather
    than what it keeps, so the statement grows with the rows removed instead of
    with the estate, and the bundle shows which rows those are.

    Needs no installation scope: the key of a runtime table carries the scope
    already, so the rows name themselves in full.
    """

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
    """The rows a build still claims, as one relation.

    The casts sit outside the constructor for the reason they do in
    :func:`_source_relation`: a column of all nulls would otherwise take
    whatever type the engine inferred, and here the branches must agree as well
    as match the target.
    """

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
            "("
            + ", ".join(
                typed_literal(row.get(name), table.column(name)) for name in identity
            )
            + ")"
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
    """A scoped ``DELETE`` of whole installations from one table.

    Installation pruning: what decommissioning a target does, and never what an
    ordinary build does. A build that did not include a target type has no
    opinion about it.
    """

    return f"DELETE FROM {qualified_name(table)}\n WHERE {scope.predicate}\n"


def _check_unique_keys(table: Table, rows: Sequence[Row]) -> None:
    """Refuse a merge whose source holds two rows with one key.

    T-SQL refuses a ``MERGE`` when several source rows match one target row, and
    it refuses at install time, long after the bundle was reviewed. Caught here
    it is a generation error naming the table and the key.
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
            f"rows ({shown}). A merge source must hold one row per key"
        )


def _check_scope(
    table: Table,
    rows: Iterable[Row],
    scope: InstallationScope | InstallationScopes,
) -> None:
    """Refuse to render a statement over rows from another installation.

    The consequence is invisible: a stray row would be merged into the wrong
    installation's scope and read as truth.
    """

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
