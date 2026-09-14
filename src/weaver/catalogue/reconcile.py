"""Render catalogue changes for a build bundle.

Only changed catalogue rows produce statements. Catalogue reads are validated
before planning. Dictionaries describe, Installation records bindings, and
Registry certification is written last. See ``design/catalogue.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

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
    DICTIONARY_TABLES,
    INSTALLATION,
    REGISTRY,
    SCOPE_ITEM_NAME,
    SCOPE_ITEM_TYPE,
    CatalogueTable,
)


@dataclass(frozen=True)
class TableChanges:
    """Counts one table's projected changes without executing them."""

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
    """One table's unconditional delete-before-merge statements."""

    table: CatalogueTable
    #: None only for Installation, whose key is the installation scope.
    delete: str | None
    #: None when the projection has no rows for this table.
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
    """One installation's unconditional catalogue statements.

    Groups are execution barriers: dictionaries, then Installation, then
    Registry certification. Builds use :class:`CataloguePublication` instead.
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
        return tuple(
            statement
            for _description, group in self.groups
            for reconciliation in group
            for statement in reconciliation.statements
        )


def reconcile(projection: CatalogueProjection) -> CatalogueReconciliation:
    """Render authoritative scoped replacement from desired state alone.

    Delete-then-merge is correct without reading prior state. This is for repair;
    ordinary builds publish only differences through :func:`publish`.
    """

    scope = projection.scope
    return CatalogueReconciliation(
        scope=scope,
        dictionaries=tuple(
            _for_table(table, projection.for_table(table), scope)
            for table in DICTIONARY_TABLES
        ),
        installation=_for_table(
            INSTALLATION, projection.for_table(INSTALLATION), scope
        ),
        registry=_for_table(REGISTRY, projection.for_table(REGISTRY), scope),
    )


def _for_table(
    table: CatalogueTable,
    rows: Sequence[Row],
    scope: InstallationScope,
) -> TableReconciliation:
    return TableReconciliation(
        table=table,
        delete=render_delete_obsolete(table, rows, scope=scope),
        merge=render_merge(table, rows, scope=scope),
    )


@dataclass(frozen=True)
class TablePublication:
    """One catalogue table's changed statements across installation scopes."""

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
    """Changed catalogue statements grouped by execution barrier.

    Dictionaries run first, then Installation, then Registry certification.
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


def publish(current, desired) -> CataloguePublication:
    """Move named installation scopes from persisted to certified state."""

    return CataloguePublication(
        dictionaries=tuple(
            _publish_table(table, current=current, desired=desired)
            for table in DICTIONARY_TABLES
        ),
        installation=_publish_table(INSTALLATION, current=current, desired=desired),
        registry=_publish_table(REGISTRY, current=current, desired=desired),
    )


def _publish_table(table: CatalogueTable, *, current, desired) -> TablePublication:
    """Render one table's deletes and merges across changed scopes.

    Merges receive changed rows; deletes receive every desired row in their
    scopes so unchanged rows remain in the keep relation.
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
        )

    merge = None
    if changed:
        merge = render_merge(table, changed, scope=_scopes_of(changed))

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
    """Compare rows using the same non-key columns as the merge guard."""

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


def prune_installation(
    scope: InstallationScope | InstallationScopes,
) -> tuple[str, ...]:
    """Remove explicitly decommissioned installations in dependency-safe order.

    Registry goes first to remove certification; Installation goes last.
    Multiple scopes share one statement per table.
    """

    # Fabric Warehouse does not enforce its declared foreign keys, so ordering
    # provides the equivalent of ON DELETE CASCADE.
    ordered = (REGISTRY, *reversed(DICTIONARY_TABLES), INSTALLATION)
    return tuple(render_delete_scope(table, scope=scope) for table in ordered)
