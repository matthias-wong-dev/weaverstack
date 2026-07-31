"""Fabric workspace resolution — names to OneLake locations.

The twin of :class:`~weaver.resolution.LocalResolver`, and deliberately the same
surface, so everything above resolution is written once and neither build nor
wipe learns which workspace it is talking to.

The difference is that a Fabric name has to be *asked about* — a name maps to a
GUID only by consulting the workspace. It is asked about **with its type**:
identity is ``workspace + type + name``, so a Lakehouse and a Warehouse may share a
display name (indeed a Lakehouse grows a same-named SQL endpoint), and the
caller always knows the type from the slot. Answers are cached, because asking
costs an API call.
"""

from __future__ import annotations

from ..errors import CommandError
from ..workspaces import BUILD_BUNDLES_AREA, WEAVER_ITEMS_AREA, FabricWorkspace
from ..locations import LakehouseSparkLocation, Location
from ..resolution import TABLES_AREA
from ..spark import SparkDestination, fabric_destination
from ..targets import (
    FILES_AREA,
    DeltaTarget,
    FolderTarget,
    ItemRef,
    WarehouseTarget,
    validate_name,
)
from .client import ONELAKE_DFS, FabricClient
from .onelake import abfss_root, lakehouse_artifact_segment
from .resources import (
    LAKEHOUSE,
    SQL_ENDPOINT,
    WAREHOUSE,
    Item,
    Workspace,
    find_item,
    find_workspace,
    refresh_sql_endpoint_metadata,
)


class FabricResolver:
    """Resolves level-three names against one Fabric workspace."""

    def __init__(
        self,
        workspace: FabricWorkspace,
        *,
        client: FabricClient | None = None,
        base_url: str = ONELAKE_DFS,
    ) -> None:
        if not isinstance(workspace, FabricWorkspace):
            raise CommandError(
                f"FabricResolver needs a FabricWorkspace, got {type(workspace).__name__}"
            )
        self.configuration = workspace
        self.client = client or FabricClient()
        self.base_url = base_url.rstrip("/")
        self._workspace: Workspace | None = None
        self._items: dict[str, Item] = {}

    # --- level four -------------------------------------------------------

    @property
    def workspace(self) -> Workspace:
        if self._workspace is None:
            self._workspace = find_workspace(
                self.configuration.workspace, client=self.client
            )
        return self._workspace

    @property
    def root(self) -> Location:
        """The workspace root. Everything resolved sits beneath it."""

        return Location(f"{self.base_url}/{self.workspace.id}")

    # --- level three ------------------------------------------------------

    def resolve(self, item: ItemRef, *, item_type: str) -> Item:
        """The workspace item of this name and type. Cached.

        A type is required: identity is ``workspace + type + name``, and asking the
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

    def _rest_client(self) -> FabricClient:
        return self.client

    def refresh_sql_endpoint(self, item: ItemRef) -> dict:
        """Refresh the SQL analytics endpoint paired with a named Lakehouse."""

        client = self._rest_client()
        endpoint = find_item(
            self.workspace,
            item.name,
            item_type=SQL_ENDPOINT,
            client=client,
        )
        return refresh_sql_endpoint_metadata(endpoint, client=client)

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
        return self.files_root(target.lakehouse)

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

    # --- what a Fabric workspace can do that a filesystem cannot -----------
    #
    # Two operations an installed bundle needs that are neither path arithmetic
    # nor a SQL statement: pointing one Lakehouse at another's data, and asking a
    # Lakehouse's SQL analytics endpoint to catch up. Both belong here because
    # this is the adapter that already knows how to reach this workspace, and the
    # local resolver deliberately offers neither — the emulator links files, and
    # has no SQL endpoint at all.

    def create_onelake_shortcut(
        self,
        item: ItemRef,
        *,
        path: str,
        name: str,
        source: ItemRef,
        source_path: str,
    ) -> dict:
        """Point ``item``'s ``path/name`` at ``source``'s ``source_path``."""

        from .shortcuts import create_shortcut

        return create_shortcut(
            self.resolve(item, item_type=LAKEHOUSE),
            path=path,
            name=name,
            source=self.resolve(source, item_type=LAKEHOUSE),
            source_path=source_path,
            client=self._rest_client(),
        )

    def onelake_shortcuts(self, item: ItemRef) -> tuple:
        """Every shortcut a Lakehouse holds. Scoping is the caller's business."""

        from .shortcuts import list_shortcuts

        return list_shortcuts(
            self.resolve(item, item_type=LAKEHOUSE), client=self._rest_client()
        )

    def remove_onelake_shortcut(self, item: ItemRef, *, path: str, name: str) -> None:
        """Take away this Lakehouse's name for another item's data.

        Never the data: see :func:`weaver.fabric.shortcuts.delete_shortcut`.
        """

        from .shortcuts import delete_shortcut

        delete_shortcut(
            self.resolve(item, item_type=LAKEHOUSE),
            path=path,
            name=name,
            client=self._rest_client(),
        )


    def sql_endpoint(self, target: WarehouseTarget):
        """Resolve a typed Warehouse to the common SQL endpoint record."""

        from ..sql import SqlEndpoint

        warehouse = self.warehouse(target)
        payload = self.client.get_json(
            f"workspaces/{self.workspace.id}/warehouses/"
            f"{warehouse.id}/connectionString"
        )
        value = payload.get("connectionString")
        if not isinstance(value, str) or not value.strip():
            raise CommandError(
                f"Fabric returned no SQL connection string for Warehouse "
                f"{warehouse.name!r}"
            )
        return SqlEndpoint(
            server=_server_name(value),
            database=warehouse.name,
            workspace_id=self.workspace.id,
            warehouse_id=warehouse.id,
            warehouse_name=warehouse.name,
        )

    # --- the weaver lakehouse ---------------------------------------------

    def _weaver_lakehouse(self) -> ItemRef:
        name = self.configuration.weaver_lakehouse
        if name is None:
            raise CommandError(
                "no Weaver Lakehouse for this Workspace — set weaver_lakehouse on the Workspace "
                "or supply it explicitly"
            )
        return ItemRef(name)

    @property
    def weaver_lakehouse(self) -> Location:
        return self.lakehouse(self._weaver_lakehouse())

    @property
    def weaver_items_root(self) -> Location:
        """The workspace's one declaration, with item types directly below it."""

        return self.files_root(self._weaver_lakehouse()) / WEAVER_ITEMS_AREA

    @property
    def build_bundles_root(self) -> Location:
        return self.files_root(self._weaver_lakehouse()) / BUILD_BUNDLES_AREA

    def build_bundle(self, name: str) -> Location:
        from ..targets import validate_name

        return self.build_bundles_root / validate_name(name, what="bundle name")

    def lakehouse_spark_location(self, item: ItemRef) -> LakehouseSparkLocation:
        """One destination Lakehouse's ``abfss://`` roots, for Spark to address.

        Built from :meth:`spark_root`, which exists precisely so a session never
        needs the item attached. The session stays attached to the Weaver
        Lakehouse — the control plane — and destinations are reached explicitly.
        """

        root = self.spark_root(item).rstrip("/")
        return LakehouseSparkLocation(
            item=item.name,
            tables_root=f"{root}/{TABLES_AREA}",
            files_root=f"{root}/{FILES_AREA}",
        )

    def spark_destination(self, item: ItemRef) -> SparkDestination:
        """One Lakehouse, as Fabric's Spark catalogue names it.

        Fabric's namespace is the fundamental representation:
        ``workspace.lakehouse.schema.object``. One session addresses every
        Lakehouse in the workspace through it, so nothing has to be attached and
        nothing has to be switched — and a schema-enabled Lakehouse pins its own
        managed tables, which is why no path appears in the destination.

        Display names, because that is what the namespace is spelled with. The
        ids stay in resolution and in the bundle's target block.
        """

        return fabric_destination(
            workspace=self.workspace.name,
            lakehouse=self.resolve(item, item_type=LAKEHOUSE).name,
        )

    @property
    def control_tables_root(self) -> Location:
        return self.tables_root(self._weaver_lakehouse())


def _server_name(value: str) -> str:
    """Extract a server from Fabric's endpoint response.

    The current Warehouse API returns a bare FQDN.  Accepting the familiar full
    connection-string form as well makes the boundary tolerant without keeping
    a credential-bearing connection string as endpoint identity.
    """

    text = value.strip().strip(";")
    if ";" in text or "=" in text:
        fields = {}
        for part in text.split(";"):
            if "=" in part:
                key, item = part.split("=", 1)
                fields[key.strip().lower()] = item.strip()
        text = (
            fields.get("server")
            or fields.get("data source")
            or fields.get("address")
            or fields.get("addr")
            or fields.get("network address")
            or ""
        )
    text = text.removeprefix("tcp:").strip()
    if "," in text:
        text = text.rsplit(",", 1)[0].strip()
    if not text:
        raise CommandError("Fabric returned an invalid Warehouse SQL endpoint")
    return text
