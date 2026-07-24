"""Resolution from inside a Microsoft Fabric session.

The desktop resolver crosses into Fabric through REST. This resolver stays
inside the current workspace: NotebookUtils supplies the workspace identity and
resolves Lakehouse names, and the resulting locations are native ``abfss``.
"""

from __future__ import annotations

from typing import Any, Mapping

from ..errors import CommandError
from ..hosts import FabricHost
from ..locations import Location
from ..targets import ItemRef
from .onelake import abfss_root
from .resolution import FabricResolver
from .resources import LAKEHOUSE, Item, Workspace


def _value(record: Any, name: str) -> Any:
    if isinstance(record, Mapping):
        return record.get(name)
    return getattr(record, name, None)


class FabricSessionResolver(FabricResolver):
    """Resolve names without leaving the Fabric session."""

    def __init__(
        self,
        host: FabricHost,
        *,
        runtime: Any | None = None,
        lakehouse: Any | None = None,
    ) -> None:
        if not isinstance(host, FabricHost):
            raise CommandError(
                f"FabricSessionResolver needs a FabricHost, got {type(host).__name__}"
            )
        if runtime is None or lakehouse is None:
            try:
                from notebookutils import lakehouse as notebook_lakehouse
                from notebookutils import runtime as notebook_runtime
            except ImportError as exc:
                raise CommandError(
                    "FabricSessionResolver is available only inside a Fabric session"
                ) from exc
            runtime = runtime or notebook_runtime
            lakehouse = lakehouse or notebook_lakehouse

        context = runtime.context
        if callable(context):
            context = context()
        workspace_name = _value(context, "currentWorkspaceName")
        workspace_id = _value(context, "currentWorkspaceId")
        if not workspace_name or not workspace_id:
            raise CommandError("Fabric runtime context carries no current workspace")
        if workspace_name != host.workspace:
            raise CommandError(
                f"this session runs in workspace {workspace_name!r}, "
                f"not host workspace {host.workspace!r}"
            )

        self.host = host
        self._workspace = Workspace(id=str(workspace_id), name=str(workspace_name))
        self._lakehouse_utils = lakehouse
        self._items: dict[str, Item] = {}

    @property
    def workspace(self) -> Workspace:
        return self._workspace

    @property
    def root(self) -> Location:
        return Location(
            f"abfss://{self.workspace.id}@onelake.dfs.fabric.microsoft.com"
        )

    def resolve(self, item: ItemRef, *, item_type: str) -> Item:
        if item_type != LAKEHOUSE:
            raise CommandError(
                f"session-native resolution for {item_type} is not implemented"
            )
        key = f"{item.name}:{item_type}"
        if key not in self._items:
            artifact = self._lakehouse_utils.get(
                item.name, workspaceId=self.workspace.id
            )
            item_id = _value(artifact, "id")
            display_name = _value(artifact, "displayName")
            if not item_id:
                raise CommandError(
                    f"no Lakehouse named {item.name!r} in workspace "
                    f"{self.workspace.name!r}"
                )
            self._items[key] = Item(
                id=str(item_id),
                name=str(display_name or item.name),
                type=LAKEHOUSE,
                workspace_id=self.workspace.id,
            )
        return self._items[key]

    def lakehouse(self, item: ItemRef) -> Location:
        resolved = self.resolve(item, item_type=LAKEHOUSE)
        return Location(abfss_root(self.workspace.id, resolved.id))
