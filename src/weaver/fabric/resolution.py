"""Fabric host resolution — names to OneLake locations.

The twin of :class:`~weaver.resolution.LocalResolver`, and deliberately the same
surface, so everything above resolution is written once and neither build nor
wipe learns which host it is talking to.

The difference is that a Fabric name has to be *asked about* — a name maps to a
GUID only by consulting the workspace. It is asked about **with its type**:
identity is ``host + type + name``, so a Lakehouse and a Warehouse may share a
display name (indeed a Lakehouse grows a same-named SQL endpoint), and the
caller always knows the type from the slot. Answers are cached, because asking
costs an API call.
"""

from __future__ import annotations

from ..errors import CommandError
from ..hosts import REPOS_AREA, FabricHost
from ..locations import Location
from ..resolution import TABLES_AREA
from ..targets import (
    FILES_AREA,
    DeltaTarget,
    FolderTarget,
    ItemRef,
    RepositoryRef,
    WarehouseTarget,
    validate_name,
)
from .client import ONELAKE_DFS, FabricClient
from .onelake import abfss_root, lakehouse_artifact_segment
from .resources import LAKEHOUSE, WAREHOUSE, Item, Workspace, find_item, find_workspace

#: Where the Weaver package is shipped when it is not installed in a Fabric
#: Environment. A convention rather than configuration, like ``repos`` — the
#: common case needs no host key, and the key exists for anyone who needs it
#: somewhere else.
RUNTIME_AREA = "weaver"


class FabricResolver:
    """Resolves level-three names against one Fabric workspace."""

    def __init__(
        self,
        host: FabricHost,
        *,
        client: FabricClient | None = None,
        base_url: str = ONELAKE_DFS,
    ) -> None:
        if not isinstance(host, FabricHost):
            raise CommandError(
                f"FabricResolver needs a FabricHost, got {type(host).__name__}"
            )
        self.host = host
        self.client = client or FabricClient()
        self.base_url = base_url.rstrip("/")
        self._workspace: Workspace | None = None
        self._items: dict[str, Item] = {}

    # --- level four -------------------------------------------------------

    @property
    def workspace(self) -> Workspace:
        if self._workspace is None:
            self._workspace = find_workspace(self.host.workspace, client=self.client)
        return self._workspace

    @property
    def root(self) -> Location:
        """The workspace root. Everything resolved sits beneath it."""

        return Location(f"{self.base_url}/{self.workspace.id}")

    # --- level three ------------------------------------------------------

    def resolve(self, item: ItemRef, *, item_type: str) -> Item:
        """The workspace item of this name and type. Cached.

        A type is required: identity is ``host + type + name``, and asking the
        workspace what a bare name *is* would make a caller depend on ambiguous
        name inference. The caller knows the type from the slot — a
        ``DeltaTarget`` is a Lakehouse, a ``WarehouseTarget`` is a Warehouse.
        """

        key = f"{item.name}:{item_type}"
        if key not in self._items:
            self._items[key] = find_item(
                self.workspace, item.name, item_type=item_type, client=self.client
            )
        return self._items[key]

    def lakehouse(self, item: ItemRef) -> Location:
        return self.root / lakehouse_artifact_segment(
            self.resolve(item, item_type=LAKEHOUSE).id
        )

    def files_root(self, item: ItemRef) -> Location:
        return self.lakehouse(item) / FILES_AREA

    def tables_root(self, item: ItemRef) -> Location:
        return self.lakehouse(item) / TABLES_AREA

    def spark_root(self, item: ItemRef) -> str:
        """The ``abfss://`` root Spark writes through, for a Lakehouse.

        Explicit, so a session never needs the item attached.
        """

        return abfss_root(self.workspace.id, self.resolve(item, item_type=LAKEHOUSE).id)

    # --- targets ----------------------------------------------------------

    def folder_root(self, target: FolderTarget) -> Location:
        return self.files_root(target.lakehouse).join(*target.subpath)

    def folder_object(self, target: FolderTarget, schema: str, name: str) -> Location:
        return self.folder_root(target).join(
            validate_name(schema, what="schema"),
            validate_name(name, what="object name"),
        )

    def folder_staging(self, target: FolderTarget, schema: str, name: str) -> Location:
        destination = self.folder_object(target, schema, name)
        return Location(f"{destination.value}_Staging")

    def delta_table(self, target: DeltaTarget, schema: str, name: str) -> Location:
        return self.tables_root(target.lakehouse).join(
            validate_name(schema, what="schema"),
            validate_name(name, what="object name"),
        )

    def warehouse(self, target: WarehouseTarget) -> Item:
        """The Warehouse item. Its SQL endpoint is reached over TDS, not OneLake."""

        return self.resolve(target.warehouse, item_type=WAREHOUSE)

    # --- the weaver lakehouse ---------------------------------------------

    def _weaver_lakehouse(self) -> ItemRef:
        name = self.host.weaver_lakehouse
        if name is None:
            raise CommandError(
                "no Weaver Lakehouse for this host — set weaver_lakehouse on the host "
                "or supply it explicitly"
            )
        return ItemRef(name)

    @property
    def weaver_lakehouse(self) -> Location:
        return self.lakehouse(self._weaver_lakehouse())

    @property
    def repos_root(self) -> Location:
        return self.files_root(self._weaver_lakehouse()) / REPOS_AREA

    def repository(self, repository: RepositoryRef) -> Location:
        return self.repos_root / repository.name

    @property
    def runtime_root(self) -> Location:
        """Where the Weaver package is shipped for a session to import.

        Unused once Weaver is installed in a Fabric Environment, which is the
        intended end state — a bootstrap tries ``import weaver`` first.
        """

        return self.files_root(self._weaver_lakehouse()) / RUNTIME_AREA

    @property
    def control_tables_root(self) -> Location:
        return self.tables_root(self._weaver_lakehouse())
