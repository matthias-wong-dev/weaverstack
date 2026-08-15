"""Render catalogue changes for a build bundle.

Only changed catalogue rows produce statements. Catalogue reads are validated
before planning. Dictionaries describe, Installation records bindings, and
Registry certification is written last. See ``design/catalogue.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from .projection import CatalogueProjection
from .render import (
    InstallationScope,
    InstallationScopes,
    Row,
    render_delete_obsolete,
    render_delete_scope,
    render_merge,
)
from .tables import (
    CATALOGUE_TABLES,
    DICTIONARY_TABLES,
    INSTALLATION,
    REGISTRY,
    SCOPE_ITEM_NAME,
    SCOPE_ITEM_TYPE,
    CatalogueTable,
)


@dataclass(frozen=True)
class TableChanges:
    """What reconciling one table would do. Reporting only — see the module note."""

    table: CatalogueTable
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    deleted: int = 0

    @property
    def touched(self) -> int:
        return self.inserted + self.updated + self.deleted

    @property
    def is_noop(self) -> bool:
        return self.touched == 0

    def __str__(self) -> str:
        return (
            f"{self.table.name}: +{self.inserted} ~{self.updated} "
            f"-{self.deleted} ={self.unchanged}"
        )


@dataclass(frozen=True)
class TableReconciliation:
    """One table's scoped statements, in the order they must run.

    The *unconditional* form — see :func:`reconcile`. Ordinary build uses
    :class:`TablePublication`, which emits nothing for a table with nothing to
    do.
    """

    table: CatalogueTable
    #: None only for Installation, whose key *is* the installation scope: there is
    #: at most one such row, so there is never an obsolete one to remove and the
    #: merge alone keeps it current.
    delete: str | None
    #: None when the projection has no rows for this table — there is nothing to
    #: merge, and an empty statement is worse than no action.
    merge: str | None

    @property
    def statements(self) -> tuple[str, ...]:
        return tuple(
            statement
            for statement in (self.delete, self.merge)
            if statement is not None
        )


@dataclass(frozen=True)
class CatalogueReconciliation:
    """One installation's catalogue statements, unconditionally.

    The grouping is the contract: dictionaries in any order among themselves,
    then Installation, then Registry. A caller turns each group into a barrier.

    Not what a build produces — see :class:`CataloguePublication`.
    """

    scope: InstallationScope
    dictionaries: tuple[TableReconciliation, ...]
    installation: TableReconciliation
    registry: TableReconciliation

    @property
    def groups(self) -> tuple[tuple[str, tuple[TableReconciliation, ...]], ...]:
        return (
            ("reconcile catalogue dictionaries", self.dictionaries),
            ("record the installation", (self.installation,)),
            ("publish the registry", (self.registry,)),
        )

    @property
    def statements(self) -> tuple[str, ...]:
        """Every statement, in execution order. Registry's are last."""

        return tuple(
            statement
            for _description, group in self.groups
            for reconciliation in group
            for statement in reconciliation.statements
        )


def reconcile(
    projection: CatalogueProjection, *, destination
) -> CatalogueReconciliation:
    """Authoritative scoped replacement of one installation, from its projection.

    Not the build path: a build publishes a difference (:func:`publish`), so an
    unchanged table produces no statement. This renders one installation from
    the desired side alone — the delete keeps exactly the keys the projection
    claims and the merge is idempotent, so the pair is correct against any prior
    state, including one nobody read.

    That is what an explicit repair mode wants and what ordinary build must not
    have, so the two are kept apart by name rather than by a flag.
    """

    scope = projection.scope
    return CatalogueReconciliation(
        scope=scope,
        dictionaries=tuple(
            _for_table(table, projection.for_table(table), scope, destination)
            for table in DICTIONARY_TABLES
        ),
        installation=_for_table(
            INSTALLATION, projection.for_table(INSTALLATION), scope, destination
        ),
        registry=_for_table(
            REGISTRY, projection.for_table(REGISTRY), scope, destination
        ),
    )


def _for_table(
    table: CatalogueTable,
    rows: Sequence[Row],
    scope: InstallationScope,
    destination,
) -> TableReconciliation:
    return TableReconciliation(
        table=table,
        delete=render_delete_obsolete(
            table, rows, scope=scope, destination=destination
        ),
        merge=render_merge(table, rows, scope=scope, destination=destination),
    )


# --- what it would change ----------------------------------------------------


@dataclass(frozen=True)
class TablePublication:
    """One physical catalogue table's statements, aggregated across scopes."""

    table: CatalogueTable
    #: Present only when a scope holds rows absent from the desired state.
    delete: str | None
    #: Present only when some row is new or changed. Unchanged rows are left
    #: alone rather than merged to the same values.
    merge: str | None

    @property
    def statements(self) -> tuple[str, ...]:
        return tuple(
            statement
            for statement in (self.delete, self.merge)
            if statement is not None
        )

    @property
    def is_noop(self) -> bool:
        return not self.statements


@dataclass(frozen=True)
class CataloguePublication:
    """Every catalogue statement one build appends, grouped by when it may run.

    The grouping is the contract: dictionaries in any order among themselves,
    then Installation, then Registry.
    """

    dictionaries: tuple[TablePublication, ...]
    installation: TablePublication
    registry: TablePublication

    @property
    def groups(self) -> tuple[tuple[str, tuple[TablePublication, ...]], ...]:
        return (
            ("reconcile catalogue dictionaries", self.dictionaries),
            ("record the installation", (self.installation,)),
            ("publish the registry", (self.registry,)),
        )

    @property
    def statements(self) -> tuple[str, ...]:
        return tuple(
            statement
            for _description, group in self.groups
            for publication in group
            for statement in publication.statements
        )

    @property
    def is_noop(self) -> bool:
        return not self.statements


def publish(current, desired, *, destination) -> CataloguePublication:
    """The statements that move ``current`` to ``desired``, table by table.

    Read it as *persisted* → *certified*. Only the items ``desired`` names are
    considered, so a scoped build cannot touch an installation it was not
    pointed at.
    """

    return CataloguePublication(
        dictionaries=tuple(
            _publish_table(
                table, current=current, desired=desired, destination=destination
            )
            for table in DICTIONARY_TABLES
        ),
        installation=_publish_table(
            INSTALLATION, current=current, desired=desired, destination=destination
        ),
        registry=_publish_table(
            REGISTRY, current=current, desired=desired, destination=destination
        ),
    )


def _publish_table(
    table: CatalogueTable, *, current, desired, destination
) -> TablePublication:
    """One table's delete and merge, across every scope that needs them.

    The two take different row sets, and conflating them loses data. The merge
    carries only new or changed rows. The delete is given every desired row for
    the scopes it covers, because it works by keeping what is claimed: handed
    only the changed rows, it would delete every unchanged one.
    """

    changed: list[Row] = []
    delete_scopes: list[InstallationScope] = []
    keep: list[Row] = []

    for item in sorted(desired.rows, key=str):
        scope = InstallationScope(item.item_type, item.item_name)
        wanted = _keyed(table, desired.rows[item].get(table.name, ()))
        found = _keyed(table, current.rows.get(item, {}).get(table.name, ()))

        for key, row in wanted.items():
            existing = found.get(key)
            if existing is None or any(
                row.get(name) != existing.get(name) for name in table.comparison_columns
            ):
                changed.append(row)

        if any(key not in wanted for key in found):
            delete_scopes.append(scope)
            keep.extend(wanted.values())

    delete = None
    if delete_scopes:
        delete = render_delete_obsolete(
            table,
            keep,
            scope=InstallationScopes(tuple(delete_scopes)),
            destination=destination,
        )

    merge = None
    if changed:
        merge = render_merge(
            table, changed, scope=_scopes_of(changed), destination=destination
        )

    return TablePublication(table=table, delete=delete, merge=merge)


def _scopes_of(rows: Iterable[Row]) -> InstallationScopes:
    return InstallationScopes(
        tuple(
            InstallationScope(
                str(row.get(SCOPE_ITEM_TYPE) or ""), str(row.get(SCOPE_ITEM_NAME) or "")
            )
            for row in rows
        )
    )


def key_of(table: CatalogueTable, row: Row) -> tuple:
    return tuple(row.get(name) for name in table.key)


def _keyed(table: CatalogueTable, rows: Iterable[Row]) -> dict[tuple, Row]:
    return {key_of(table, row): row for row in rows}


def compare(
    table: CatalogueTable, desired: Iterable[Row], existing: Iterable[Row]
) -> TableChanges:
    """How one table's rows differ from what is there — for review, not for DML.

    A row is unchanged when every non-key column matches, which is what the
    merge's ``MATCHED`` guard tests — so a reported no-op is a real one.
    """

    wanted = _keyed(table, desired)
    found = _keyed(table, existing)
    inserted = updated = unchanged = 0
    for key, row in wanted.items():
        if key not in found:
            inserted += 1
        elif any(
            row.get(name) != found[key].get(name) for name in table.comparison_columns
        ):
            updated += 1
        else:
            unchanged += 1
    return TableChanges(
        table=table,
        inserted=inserted,
        updated=updated,
        unchanged=unchanged,
        deleted=sum(1 for key in found if key not in wanted),
    )


def summarise(
    projection: CatalogueProjection, existing: Mapping[str, Sequence[Row]]
) -> tuple[TableChanges, ...]:
    """What a build's catalogue work would change, table by table.

    ``existing`` is keyed by table name, as :func:`weaver.catalogue.reader.
    read_installation` returns it.
    """

    return tuple(
        compare(table, projection.for_table(table), existing.get(table.name, ()))
        for table in CATALOGUE_TABLES
    )


# --- the explicit prune scopes -----------------------------------------------


def prune_installation(
    scope: InstallationScope | InstallationScopes, *, destination
) -> tuple[str, ...]:
    """Remove whole installations, in dependency-safe order.

    What decommissioning a target does, and never what a build does: a build
    that did not include a target type has no opinion about it. Nothing in the
    build path may reach this.

    Registry goes first, so no row is left certified while what described it is
    gone.

    Several installations go in one statement per table, because each ``DELETE``
    is a Delta transaction that rewrites files and costs seconds whether it
    removes one row or a thousand.
    """

    # Uncertify first, remove dependent dictionaries next, and remove the
    # installation root last. Delta does not enforce foreign keys, so this is
    # the explicit ordered equivalent of ON DELETE CASCADE.
    ordered = (REGISTRY, *reversed(DICTIONARY_TABLES), INSTALLATION)
    return tuple(
        render_delete_scope(table, scope=scope, destination=destination)
        for table in ordered
    )
