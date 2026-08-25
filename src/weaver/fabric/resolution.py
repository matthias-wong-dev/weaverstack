"""Resolve typed Fabric item names to OneLake locations.

Fabric item identity is workspace, type, and name. Resolution uses the declared
item type and caches REST lookups, so a Session's resolver answers a repeated
name without asking the workspace again.
"""

from __future__ import annotations

from ..build_bundle.targets import WAREHOUSE_TARGET
from ..errors import CommandError
from ..locations import LakehouseSparkLocation, Location
from ..resolution import TABLES_AREA
from ..spark import FabricSparkTarget
from ..targets import (
    FILES_AREA,
    DeltaTarget,
    FolderTarget,
    ItemRef,
    WarehouseTarget,
    validate_name,
)
from ..workspaces import Workspace
from .client import ONELAKE_DFS, FabricClient
from .onelake import abfss_root, lakehouse_artifact_segment
from .resources import (
    LAKEHOUSE,
    SQL_ENDPOINT,
    WAREHOUSE,
    Item,
    find_item,
    find_workspace,
    refresh_sql_endpoint_metadata,
)


class FabricResolver:
    """Resolves level-three names against one Fabric workspace."""

    def __init__(
        self,
        workspace: Workspace,
        *,
        client: FabricClient | None = None,
        base_url: str = ONELAKE_DFS,
    ) -> None:
        self.configuration = workspace
        self.client = client or FabricClient()
        self.base_url = base_url.rstrip("/")
        self._workspace: Workspace | None = None
        self._items: dict[str, Item] = {}
        #: Answers this resolver gave without asking the workspace. A Session
        #: owns one resolver for its lifetime, so this is what a reused item
        #: cache is worth, and a hit is the absence of a call, which nothing
        #: above the cache can observe for itself.
        self.cache_hits = 0

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
        workspace what a bare name is would make a caller depend on ambiguous
        name inference. The caller takes the type from the slot, a
        ``DeltaTarget`` is a Lakehouse, a ``WarehouseTarget`` is a Warehouse.
        """

        key = f"{item.name}:{item_type}"
        if key in self._items:
            self.cache_hits += 1
        else:
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
    # Lakehouse's SQL analytics endpoint to catch up. Both are REST, so they
    # belong to the adapter that already reaches this workspace. A
    # resolver inside a Fabric session offers neither, and an action that needs
    # one is recorded as skipped rather than failed.

    def create_onelake_shortcut(
        self,
        item: ItemRef,
        *,
        path: str,
        name: str,
        source: ItemRef | Item,
        source_kind: str | None = None,
        source_path: str,
    ) -> dict:
        """Point ``item``'s ``path/name`` at ``source``'s ``source_path``.

        ``source`` is a name in this workspace, or an item already resolved
        elsewhere. A direct shortcut may point outside the workspace the build is
        bound to, and that address is settled when the bundle is generated, so
        there is nothing left to look up here.
        """

        from .shortcuts import create_shortcut

        resolved_source = (
            source
            if getattr(source, "id", None) and getattr(source, "workspace_id", None)
            else self.resolve(
                source,
                item_type=(WAREHOUSE if source_kind == WAREHOUSE_TARGET else LAKEHOUSE),
            )
        )
        return create_shortcut(
            self.resolve(item, item_type=LAKEHOUSE),
            path=path,
            name=name,
            source=resolved_source,
            source_path=source_path,
            client=self._rest_client(),
        )

    def external_item(self, name: str, *, item_type: str, workspace: str | None = None):
        """One item, in this workspace or a named one, resolved by its type.

        For a shortcut's source, which may sit outside the workspace the build
        is bound to. Nothing binds it and nothing builds into it.

        A Warehouse resolves here as well as a Lakehouse, because a Fabric
        Warehouse publishes its tables into OneLake.
        """

        if workspace is None or workspace == self.workspace.name:
            return self.resolve(ItemRef(name), item_type=item_type)
        # Through this host's own REST client, as every other crossing here is:
        # inside Fabric that is the session's identity, not a desktop credential.
        client = self._rest_client()
        return find_item(
            find_workspace(workspace, client=client),
            name,
            item_type=item_type,
            client=client,
        )

    def external_root(self, item) -> Location:
        """The root of an already-resolved item, as this host addresses one.

        The same spelling :meth:`lakehouse` gives for an item of this workspace,
        so what reads it is the store this host already has.
        """

        return Location(
            f"{self.base_url}/{item.workspace_id}/"
            + lakehouse_artifact_segment(item.id)
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
            f"workspaces/{self.workspace.id}/warehouses/{warehouse.id}/connectionString"
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

    # --- the weaver catalogue ---------------------------------------------

    def _catalogue(self) -> ItemRef:
        """The item the catalogue lives in, from the workspace's typed value."""

        if self.configuration.catalogue is None:
            raise CommandError(
                "A catalogue is required for this Workspace. Set "
                "catalogue='Warehouse/Weaver' on the Workspace or supply it "
                "explicitly."
            )
        return self.configuration.catalogue_item

    def lakehouse_spark_location(self, item: ItemRef) -> LakehouseSparkLocation:
        """One destination Lakehouse's ``abfss://`` roots, for Spark to address.

        Built from :meth:`spark_root`, which exists precisely so a session never
        needs the item attached. A session's own attachment carries no meaning,
        and destinations are reached explicitly.
        """

        root = self.spark_root(item).rstrip("/")
        return LakehouseSparkLocation(
            item=item.name,
            tables_root=f"{root}/{TABLES_AREA}",
            files_root=f"{root}/{FILES_AREA}",
        )

    def spark_destination(self, item: ItemRef) -> FabricSparkTarget:
        """One Lakehouse, as Fabric's Spark catalogue names it.

        Fabric's namespace is the fundamental representation:
        ``workspace.lakehouse.schema.object``. One session addresses every
        Lakehouse in the workspace through it, so nothing has to be attached and
        nothing has to be switched, and a schema-enabled Lakehouse pins its own
        managed tables, which is why no path appears in the destination.

        Display names, because that is what the namespace is spelled with. The
        ids stay in resolution and in the bundle's target block.
        """

        return FabricSparkTarget(
            workspace=self.workspace.name,
            lakehouse=self.resolve(item, item_type=LAKEHOUSE).name,
        )

    @property
    def control_tables_root(self) -> Location:
        return self.tables_root(self._catalogue())


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
