"""Turning a projection into the statements a build appends, and nothing wider.

Reconciliation for one installation is two statements per table: delete the rows
this installation no longer projects, then merge the rows it does. Both are scoped
to one ``(repository, target_type)``, so the reach of a whole build's catalogue
work is bounded by construction rather than by care.

**The statements do not depend on reading the catalogue first.** The delete keeps
exactly the keys the projection claims and the merge is idempotent, so the pair is
correct against any prior state — including a state the planner could not see.
That is deliberate: a build that derived its deletes from an inventory would have
its deletion scope widened by a failed read, which is the failure mode
build-philosophy §6 exists to prevent. Here a failed read cannot widen anything,
because nothing is derived from it.

Reading is still worth doing, for a different reason: a reviewer should be able to
see what a bundle will change before it runs (§3, §17). :func:`compare` produces
that summary — how many rows are new, changed, unchanged and removed — without any
statement depending on it.

**Ordering is the one strict invariant.** Dictionaries describe, Installation
records the binding, Registry certifies. Registry is written last, so a row in it
cannot outrun the work it attests to; the installer's barriers do the rest. Prune
runs the order backwards — uncertify first, so nothing is left certified while its
description is being removed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from .projection import CatalogueProjection
from .render import (
    InstallationScope,
    Row,
    render_delete_obsolete,
    render_delete_repository,
    render_delete_scope,
    render_merge,
)
from .tables import (
    CATALOGUE_TABLES,
    DICTIONARY_TABLES,
    INSTALLATION,
    REGISTRY,
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
    """One table's scoped statements, in the order they must run."""

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
            statement for statement in (self.delete, self.merge) if statement is not None
        )


@dataclass(frozen=True)
class CatalogueReconciliation:
    """Every catalogue statement one build appends, grouped by when it may run.

    The grouping is the contract: dictionaries may run in any order among
    themselves, Installation follows them, and Registry follows everything. A
    caller turns each group into its own barrier.
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


def reconcile(projection: CatalogueProjection) -> CatalogueReconciliation:
    """The statements that make one installation's catalogue match its projection."""

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
    table: CatalogueTable, rows: Sequence[Row], scope: InstallationScope
) -> TableReconciliation:
    return TableReconciliation(
        table=table,
        delete=render_delete_obsolete(table, rows, scope=scope),
        merge=render_merge(table, rows, scope=scope),
    )


# --- what it would change ----------------------------------------------------


def key_of(table: CatalogueTable, row: Row) -> tuple:
    return tuple(row.get(name) for name in table.key)


def _keyed(table: CatalogueTable, rows: Iterable[Row]) -> dict[tuple, Row]:
    return {key_of(table, row): row for row in rows}


def compare(
    table: CatalogueTable, desired: Iterable[Row], existing: Iterable[Row]
) -> TableChanges:
    """How one table's rows differ from what is there — for review, not for DML.

    A row is *unchanged* when every non-key column matches, which is exactly the
    condition the merge's ``MATCHED`` guard tests. So a reported no-op is a real
    no-op: the statement will run and write nothing.
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


def prune_installation(scope: InstallationScope) -> tuple[str, ...]:
    """Remove one installation entirely, in dependency-safe order.

    This is what decommissioning a target does. It is emphatically **not** what a
    build does: a build that did not include a target type has no opinion about
    it, which is a different thing from having removed it. Nothing in the build
    path may reach this.

    Registry goes first — uncertify before removing the descriptions, so no row is
    ever left certified while what described it is gone.
    """

    ordered = (REGISTRY, INSTALLATION, *reversed(DICTIONARY_TABLES))
    return tuple(render_delete_scope(table, scope=scope) for table in ordered)


def prune_repository(repository: str) -> tuple[str, ...]:
    """Remove every installation of one repository, across target types.

    Being cross-scope is the whole of what distinguishes this from installation
    prune, so it names no target type. A repository lifecycle operation, reached
    explicitly and never from a build.
    """

    ordered = (REGISTRY, INSTALLATION, *reversed(DICTIONARY_TABLES))
    return tuple(
        render_delete_repository(table, repository=repository) for table in ordered
    )
