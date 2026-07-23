"""Resolving Fabric workspace items by name.

Item names are unique within a workspace, which is the whole reason level three
needs no configuration. This is where that assumption meets the API.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ..errors import CommandError
from .client import FabricClient, FabricError

LAKEHOUSE = "Lakehouse"
WAREHOUSE = "Warehouse"
ENVIRONMENT = "Environment"


@dataclass(frozen=True)
class Workspace:
    id: str
    name: str

    def __str__(self) -> str:
        return f"{self.name} ({self.id})"


@dataclass(frozen=True)
class Item:
    """One workspace item — a Lakehouse, a Warehouse, an Environment."""

    id: str
    name: str
    type: str
    workspace_id: str

    def __str__(self) -> str:
        return f"{self.type} {self.name} ({self.id})"


def find_workspace(name: str, *, client: FabricClient | None = None) -> Workspace:
    """The workspace with this name."""

    client = client or FabricClient()
    matches = [
        workspace
        for workspace in client.paged("workspaces")
        if workspace.get("displayName") == name
    ]
    if not matches:
        available = ", ".join(
            sorted(w.get("displayName", "?") for w in client.paged("workspaces"))
        )
        raise CommandError(
            f"no workspace named {name!r} — found: {available or 'none'}"
        )
    if len(matches) > 1:
        raise CommandError(f"more than one workspace named {name!r}")
    return Workspace(id=matches[0]["id"], name=name)


def list_items(
    workspace: Workspace, *, item_type: str | None = None, client: FabricClient | None = None
) -> tuple[Item, ...]:
    client = client or FabricClient()
    path = f"workspaces/{workspace.id}/items"
    if item_type:
        path += f"?type={item_type}"
    return tuple(
        Item(
            id=item["id"],
            name=item.get("displayName", ""),
            type=item.get("type", ""),
            workspace_id=workspace.id,
        )
        for item in client.paged(path)
    )


def find_item(
    workspace: Workspace,
    name: str,
    *,
    item_type: str | None = None,
    client: FabricClient | None = None,
) -> Item:
    """The item with this name, which is unique within a workspace."""

    matches = [
        item
        for item in list_items(workspace, item_type=item_type, client=client)
        if item.name == name and (item_type is None or item.type == item_type)
    ]
    if not matches:
        raise CommandError(
            f"no {item_type or 'item'} named {name!r} in workspace {workspace.name!r}"
        )
    if len(matches) > 1:
        raise CommandError(
            f"more than one {item_type or 'item'} named {name!r} in {workspace.name!r} — "
            "item names are assumed unique within a workspace"
        )
    return matches[0]


def create_lakehouse(
    workspace: Workspace, name: str, *, client: FabricClient | None = None
) -> Item:
    """Create a Lakehouse. Returns the existing one if the name is taken."""

    client = client or FabricClient()
    try:
        return find_item(workspace, name, item_type=LAKEHOUSE, client=client)
    except CommandError:
        pass

    response = client.request(
        "POST",
        f"workspaces/{workspace.id}/lakehouses",
        payload={"displayName": name},
        expected=(200, 201, 202),
    )
    if response.status_code == 202:
        # Long-running create: the item exists once the operation settles.
        return _await_item(workspace, name, LAKEHOUSE, client=client)
    body = response.json()
    return Item(id=body["id"], name=name, type=LAKEHOUSE, workspace_id=workspace.id)


def delete_item(item: Item, *, client: FabricClient | None = None) -> None:
    client = client or FabricClient()
    client.request(
        "DELETE",
        f"workspaces/{item.workspace_id}/items/{item.id}",
        expected=(200, 202, 204),
    )


def _await_item(
    workspace: Workspace,
    name: str,
    item_type: str,
    *,
    client: FabricClient,
    attempts: int = 30,
    pause: float = 2.0,
) -> Item:
    import time

    for _ in range(attempts):
        try:
            return find_item(workspace, name, item_type=item_type, client=client)
        except CommandError:
            time.sleep(pause)
    raise FabricError(
        f"{item_type} {name!r} did not appear in {workspace.name!r} after "
        f"{int(attempts * pause)}s"
    )
