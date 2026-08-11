"""Local workspace resolution — names to locations.

Turns a :class:`~weaver.workspaces.LocalWorkspace` plus the level-three identities into
concrete :class:`~weaver.locations.Location` values::

    LocalResolver(LocalWorkspace(workspace=".local"))

    DeltaTarget("Sales") + Budget.Expense
        -> .local/Sales/Tables/Budget/Expense

    FolderTarget("Sales/Files") + Budget.BudgetPaper
        -> .local/Sales/Files/Budget/BudgetPaper

    CLI bundle handover
        -> .local/Weaver/Files/cli/<execution-id>/install.weaver.zip

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
from .workspaces import BUILD_BUNDLES_AREA, WEAVER_ITEMS_AREA, LocalWorkspace
from .locations import LakehouseSparkLocation, Location
from .spark import SparkDestination, local_destination
from .targets import (
    FILES_AREA,
    DeltaTarget,
    FolderTarget,
    ItemRef,
    WarehouseTarget,
    validate_name,
)

#: The Lakehouse area holding Delta tables. Never written by a user — a Delta
#: target names a Lakehouse and the area follows from the object kind.
TABLES_AREA = "Tables"


class LocalResolver:
    """Resolves level-three identities against a local workspace root.

    Checkpoint 7 adds a Fabric resolver with the same surface, returning URL
    locations. No shared protocol is declared yet: one implementation is a
    guess at the shape, two make it visible.
    """

    def __init__(self, workspace: LocalWorkspace) -> None:
        if not isinstance(workspace, LocalWorkspace):
            raise CommandError(f"LocalResolver needs a LocalWorkspace, got {type(workspace).__name__}")
        self.workspace = workspace

    # --- level four ------------------------------------------------------

    @property
    def root(self) -> Location:
        return Location(str(self.workspace.workspace))

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

    def spark_root(self, item: ItemRef) -> str:
        """The root Spark writes through, for a Lakehouse.

        The local counterpart of the Fabric ``abfss://`` root: same contract,
        filesystem transport, and the same reason for existing — a destination is
        addressed explicitly rather than by attaching the session to it.
        """

        return self.lakehouse(item).value

    # --- folder targets --------------------------------------------------

    def folder_root(self, target: FolderTarget) -> Location:
        """The Files area a folder target names. There is nothing below it to configure."""

        return self.files_root(target.lakehouse)

    def folder_object(self, target: FolderTarget, schema: str, name: str) -> Location:
        """Where one Folder object materialises — ``Files/<Schema>/<Object>``."""

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

    def lakehouse_spark_location(self, item: ItemRef) -> LakehouseSparkLocation:
        """One destination Lakehouse's physical roots, for Spark to address.

        The local counterpart of the Fabric ``abfss://`` roots: same contract,
        filesystem transport. Resolving a target once here is what keeps the
        session's attached Lakehouse (Weaver) separate from the destinations a
        build writes to — see :class:`~weaver.locations.LakehouseSparkLocation`.
        """

        return LakehouseSparkLocation(
            item=item.name,
            tables_root=self.tables_root(item).value,
            files_root=self.files_root(item).value,
        )

    def spark_destination(self, item: ItemRef) -> SparkDestination:
        """One Lakehouse, as this session's Spark catalogue names it.

        The local proxy for Fabric's four-part namespace. Local Spark has one
        namespace level and cannot be given another, so the Lakehouse is folded
        into the database name and the database carries an explicit ``LOCATION``
        under the Lakehouse's ``Tables`` area — same isolation, same storage
        layout, different syntax. See
        :mod:`weaver.spark.destination` for why it is folded rather than nested.
        """

        return local_destination(
            item=item.name, tables_root=self.tables_root(item).value
        )

    # --- warehouse targets -----------------------------------------------

    def warehouse(self, target: WarehouseTarget) -> Location:
        """Always fails: a local workspace has no SQL implementation.

        Explicit rather than silently skipped, so a build carrying SQL objects
        against a local workspace reports the reason.
        """

        raise CommandError(
            f"local Workspace has no SQL implementation, so Warehouse target "
            f"{target.warehouse.name!r} cannot be resolved — Warehouse work is Fabric-only"
        )

    # --- the weaver lakehouse --------------------------------------------

    @property
    def weaver_lakehouse(self) -> Location:
        return self.lakehouse(ItemRef(self._weaver_lakehouse_name()))

    @property
    def weaver_items_root(self) -> Location:
        """The workspace's one declaration, with item types directly below it."""

        return (
            self.files_root(ItemRef(self._weaver_lakehouse_name()))
            / WEAVER_ITEMS_AREA
        )

    @property
    def build_bundles_root(self) -> Location:
        """``<weaver-lakehouse>/Files/build_bundles`` — where persisted bundles live.

        A generated bundle normally lands in a throwaway directory that is passed
        straight to the installer. When one is kept — for handover, audit, or
        inspection — its single .weaver.zip archive belongs here, beside the
        repositories it was built from rather than in a remote working tree.
        """

        return self.files_root(ItemRef(self._weaver_lakehouse_name())) / BUILD_BUNDLES_AREA

    def build_bundle(self, name: str) -> Location:
        """One named bundle directory beneath ``build_bundles_root``.

        Initialisation uses this because its bootstrap bundle is idempotent and there is
        no value in a new name each run. A build that keeps its bundle for
        handover or audit uses a timestamped archive instead.
        """

    @property
    def control_tables_root(self) -> Location:
        """``<weaver-lakehouse>/Tables`` — the control-plane tables.

        The table names and whether they sit under a schema are a checkpoint 16
        decision; this is only their root.
        """

        return self.tables_root(ItemRef(self._weaver_lakehouse_name()))

    def _weaver_lakehouse_name(self) -> str:
        name = self.workspace.weaver_lakehouse
        if name is None:
            raise CommandError(
                "no Weaver Lakehouse for this Workspace — set weaver_lakehouse on the Workspace "
                "or supply it explicitly"
            )
        return name


# --- choosing an implementation for a workspace -----------------------------------


def resolver_for(workspace):
    """The resolver for a workspace in the current executor.

    A Fabric session resolves within its current workspace through
    NotebookUtils. A desktop process uses the REST-backed Fabric resolver; that
    cross-boundary caller supplies its DFS store explicitly.
    """

    from .workspaces import FabricWorkspace, LocalWorkspace

    if isinstance(workspace, LocalWorkspace):
        return LocalResolver(workspace)

    if isinstance(workspace, FabricWorkspace):
        try:
            from notebookutils import lakehouse, runtime
        except ImportError:
            pass
        else:
            from .fabric.session import FabricSessionResolver

            return FabricSessionResolver(
                workspace, lakehouse=lakehouse, runtime=runtime
            )

    from .fabric.resolution import FabricResolver

    return FabricResolver(workspace)


def store_for(workspace):
    """The **within-workspace** default store for a workspace.

    Local execution uses the filesystem. Fabric execution uses NotebookUtils,
    which is available only inside a Fabric session. A desktop caller crossing
    into Fabric still constructs ``OneLakeDfsClient`` and injects it explicitly,
    so DFS is never mistaken for the within-workspace default.
    """

    from .workspaces import FabricWorkspace, LocalWorkspace
    from .store import FilesystemStore

    if isinstance(workspace, LocalWorkspace):
        return FilesystemStore()
    if isinstance(workspace, FabricWorkspace):
        from .fabric.store import FabricStore

        return FabricStore()

    raise CommandError(
        f"{type(workspace).__name__} has no within-workspace store"
    )
