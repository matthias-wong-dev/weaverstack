"""Local host resolution — names to locations.

Turns a :class:`~weaver.hosts.LocalHost` plus the level-three identities into
concrete :class:`~weaver.locations.Location` values::

    LocalResolver(LocalHost(root=".local"))

    DeltaTarget("Sales") + Budget.Expense
        -> .local/Sales/Tables/Budget/Expense

    FolderTarget("Sales/Files/Extracts") + Budget.BudgetPaper
        -> .local/Sales/Files/Extracts/Budget/BudgetPaper

    RepositoryRef("sales-etl")
        -> .local/Weaver/Files/repos/sales-etl

This is arithmetic only. Nothing here touches the filesystem — every location
can be inspected before any mutation occurs. Mutation is a
:class:`~weaver.store.Store` concern.

Together with the Fabric resolver (checkpoint 7) this is the *only* place that
knows how a name becomes a location. Everything downstream receives resolved
locations and never derives them, which is what makes "every target root is
explicit" enforceable rather than aspirational.
"""

from __future__ import annotations

from .errors import CommandError
from .hosts import REPOS_AREA, LocalHost
from .locations import Location
from .targets import (
    FILES_AREA,
    DeltaTarget,
    FolderTarget,
    ItemRef,
    RepositoryRef,
    WarehouseTarget,
    validate_name,
)

#: The Lakehouse area holding Delta tables. Never written by a user — a Delta
#: target names a Lakehouse and the area follows from the object kind.
TABLES_AREA = "Tables"


class LocalResolver:
    """Resolves level-three identities against a local host root.

    Checkpoint 7 adds a Fabric resolver with the same surface, returning URL
    locations. No shared protocol is declared yet: one implementation is a
    guess at the shape, two make it visible.
    """

    def __init__(self, host: LocalHost) -> None:
        if not isinstance(host, LocalHost):
            raise CommandError(f"LocalResolver needs a LocalHost, got {type(host).__name__}")
        self.host = host

    # --- level four ------------------------------------------------------

    @property
    def root(self) -> Location:
        return Location(str(self.host.root))

    # --- level three -----------------------------------------------------

    def lakehouse(self, item: ItemRef) -> Location:
        """A Lakehouse root — a directory holding Files/ and Tables/.

        Named ``lakehouse`` to match the Fabric resolver, where resolution is
        typed: a bare name is never asked "what are you?".
        """

        return self.root / item.name

    def lakehouse_exists(self, item: ItemRef) -> bool:
        return self.lakehouse(item).path.is_dir()

    def files_root(self, item: ItemRef) -> Location:
        return self.lakehouse(item) / FILES_AREA

    def tables_root(self, item: ItemRef) -> Location:
        return self.lakehouse(item) / TABLES_AREA

    # --- folder targets --------------------------------------------------

    def folder_root(self, target: FolderTarget) -> Location:
        """The configured folder root, including any subpath."""

        return self.files_root(target.lakehouse).join(*target.subpath)

    def folder_object(self, target: FolderTarget, schema: str, name: str) -> Location:
        """Where one Folder object materialises, beneath the configured root."""

        return self.folder_root(target).join(
            validate_name(schema, what="schema"),
            validate_name(name, what="object name"),
        )

    def folder_staging(self, target: FolderTarget, schema: str, name: str) -> Location:
        """The object-local staging sibling. There is no shared staging area."""

        destination = self.folder_object(target, schema, name)
        return Location(f"{destination.value}_Staging")

    # --- delta targets ---------------------------------------------------

    def delta_table(self, target: DeltaTarget, schema: str, name: str) -> Location:
        return self.tables_root(target.lakehouse).join(
            validate_name(schema, what="schema"),
            validate_name(name, what="object name"),
        )

    # --- warehouse targets -----------------------------------------------

    def warehouse(self, target: WarehouseTarget) -> Location:
        """Always fails: a local host has no SQL implementation.

        Explicit rather than silently skipped, so a build carrying SQL objects
        against a local host reports the reason.
        """

        raise CommandError(
            f"local host has no SQL implementation, so warehouse target "
            f"{target.warehouse.name!r} cannot be resolved — Warehouse work is Fabric-only"
        )

    # --- the weaver lakehouse --------------------------------------------

    @property
    def weaver_lakehouse(self) -> Location:
        return self.lakehouse(ItemRef(self._weaver_lakehouse_name()))

    @property
    def repos_root(self) -> Location:
        """``<weaver-lakehouse>/Files/repos`` — where installed repositories live."""

        return self.files_root(ItemRef(self._weaver_lakehouse_name())) / REPOS_AREA

    def repository(self, repository: RepositoryRef) -> Location:
        return self.repos_root / repository.name

    @property
    def control_tables_root(self) -> Location:
        """``<weaver-lakehouse>/Tables`` — the control-plane tables.

        The table names and whether they sit under a schema are a checkpoint 16
        decision; this is only their root.
        """

        return self.tables_root(ItemRef(self._weaver_lakehouse_name()))

    def _weaver_lakehouse_name(self) -> str:
        name = self.host.weaver_lakehouse
        if name is None:
            raise CommandError(
                "no Weaver Lakehouse for this host — set weaver_lakehouse on the host "
                "or supply it explicitly"
            )
        return name


# --- choosing an implementation for a host -----------------------------------


def resolver_for(host):
    """The resolver for a host: local paths, or OneLake locations.

    Fabric is imported lazily, so the core stays importable without the
    optional extra installed.
    """

    from .hosts import LocalHost

    if isinstance(host, LocalHost):
        return LocalResolver(host)

    from .fabric.resolution import FabricResolver

    return FabricResolver(host)


def store_for(host):
    """The store for a host: the filesystem, or OneLake."""

    from .hosts import LocalHost
    from .store import LocalStore

    if isinstance(host, LocalHost):
        return LocalStore()

    from .fabric.onelake import FabricStore

    return FabricStore()
