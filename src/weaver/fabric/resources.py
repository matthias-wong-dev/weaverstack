"""Discover and resolve Fabric workspace items by name and type.

Fabric item identity is workspace, type and name. A bare name can be ambiguous.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from ..errors import CommandError
from .client import FabricClient, FabricError

LAKEHOUSE = "Lakehouse"
WAREHOUSE = "Warehouse"
SEMANTIC_MODEL = "SemanticModel"
ENVIRONMENT = "Environment"
NOTEBOOK = "Notebook"
SQL_ENDPOINT = "SQLEndpoint"

# A Lakehouse's generated SQLEndpoint is not a separately addressed item.
FACET_TYPES = frozenset({SQL_ENDPOINT})


class ItemNotFoundError(CommandError):
    pass


@dataclass(frozen=True)
class WorkspaceItem:
    id: str
    name: str

    def __str__(self) -> str:
        return f"{self.name} ({self.id})"


@dataclass(frozen=True)
class Item:
    id: str
    name: str
    type: str
    workspace_id: str

    def __str__(self) -> str:
        return f"{self.type} {self.name} ({self.id})"


def find_workspace(
    name: str, *, client: FabricClient | None = None, workspaces=None
) -> WorkspaceItem:
    client = client or FabricClient()
    visible = list(client.paged("workspaces")) if workspaces is None else workspaces
    matches = [
        workspace for workspace in visible if workspace.get("displayName") == name
    ]
    if not matches:
        available = ", ".join(sorted(w.get("displayName", "?") for w in visible))
        raise CommandError(
            f"Workspace {name!r} was not found. Available workspaces: {available or 'none'}."
        )
    if len(matches) > 1:
        raise CommandError(f"More than one workspace is named {name!r}.")
    return WorkspaceItem(id=matches[0]["id"], name=name)


def list_items(
    workspace: WorkspaceItem,
    *,
    item_type: str | None = None,
    client: FabricClient | None = None,
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
    workspace: WorkspaceItem,
    name: str,
    *,
    item_type: str | None = None,
    client: FabricClient | None = None,
) -> Item:

    matches = [
        item
        for item in list_items(workspace, item_type=item_type, client=client)
        if item.name == name and (item_type is None or item.type == item_type)
    ]
    if item_type is None and len(matches) > 1:
        matches = [item for item in matches if item.type not in FACET_TYPES] or matches
    if not matches:
        raise ItemNotFoundError(
            f"{item_type or 'Item'} {name!r} was not found in workspace {workspace.name!r}."
        )
    if len(matches) > 1:
        found = ", ".join(sorted(item.type for item in matches))
        raise CommandError(
            f"More than one item named {name!r} was found in {workspace.name!r}: {found}. "
            "Specify the item type."
        )
    return matches[0]


def create_lakehouse(
    workspace: WorkspaceItem, name: str, *, client: FabricClient | None = None
) -> Item:
    """Create a schema-enabled Lakehouse or return the existing typed match.

    Weaver requires ``Tables/<schema>/<table>`` layout. Fabric sets schema support
    only when the Lakehouse is created.
    """

    client = client or FabricClient()
    try:
        return find_item(workspace, name, item_type=LAKEHOUSE, client=client)
    except CommandError:
        pass

    # Fabric holds a deleted item's name for some minutes and answers 409
    # ItemDisplayNameNotAvailableYet until it is free again.
    response = client.request(
        "POST",
        f"workspaces/{workspace.id}/lakehouses",
        payload={"displayName": name, "creationPayload": {"enableSchemas": True}},
        expected=(200, 201, 202, 409),
    )
    if response.status_code == 409:
        raise CommandError(
            f"Lakehouse {name!r} could not be created in {workspace.name!r}: "
            + (response.json().get("message") or response.text.strip()[:200])
        )
    if response.status_code == 202:
        return _await_item(workspace, name, LAKEHOUSE, client=client)
    body = response.json()
    return Item(id=body["id"], name=name, type=LAKEHOUSE, workspace_id=workspace.id)


def create_warehouse(
    workspace: WorkspaceItem, name: str, *, client: FabricClient | None = None
) -> Item:
    """Create a Warehouse or return the existing typed match."""

    client = client or FabricClient()
    try:
        return find_item(workspace, name, item_type=WAREHOUSE, client=client)
    except ItemNotFoundError:
        pass

    response = client.request(
        "POST",
        f"workspaces/{workspace.id}/warehouses",
        payload={"displayName": name},
        expected=(200, 201, 202, 409),
    )
    if response.status_code == 409:
        raise CommandError(
            f"Warehouse {name!r} could not be created in {workspace.name!r}: "
            + (response.json().get("message") or response.text.strip()[:200])
        )
    if response.status_code == 202:
        return _await_item(workspace, name, WAREHOUSE, client=client)
    body = response.json()
    return Item(id=body["id"], name=name, type=WAREHOUSE, workspace_id=workspace.id)


def create_semantic_model(
    workspace: WorkspaceItem, name: str, *, definition: dict, client=None
) -> Item:
    """Create a semantic item with a definition, or reuse its typed match."""
    client = client or FabricClient()
    try:
        return find_item(workspace, name, item_type="SemanticModel", client=client)
    except ItemNotFoundError:
        pass
    response = client.request(
        "POST",
        f"workspaces/{workspace.id}/semanticModels",
        payload={"displayName": name, "definition": definition},
        expected=(201, 202),
        retry_transient=False,
    )
    if response.status_code == 202:
        client.wait_for_operation(response)
        return _await_item(workspace, name, "SemanticModel", client=client)
    return Item(
        id=response.json()["id"],
        name=name,
        type="SemanticModel",
        workspace_id=workspace.id,
    )


def delete_item(item: Item, *, client: FabricClient | None = None) -> None:
    client = client or FabricClient()
    client.request(
        "DELETE",
        f"workspaces/{item.workspace_id}/items/{item.id}",
        expected=(200, 202, 204),
    )


#: The most tables Fabric syncs for one refresh request.
TABLES_PER_REFRESH = 25


def refresh_sql_endpoint_metadata(
    endpoint: Item, *, tables=None, client: FabricClient | None = None
) -> dict:
    """Refresh one SQL analytics endpoint and await completion."""

    client = client or FabricClient()
    refresh = start_sql_endpoint_refresh(endpoint, tables=tables, client=client)
    while not refresh["done"]:
        time.sleep(refresh["retry_after"])
        refresh = observe_sql_endpoint_refresh(refresh, client=client)
    return refresh_details(refresh)


def start_sql_endpoint_refresh(
    endpoint: Item,
    *,
    tables=None,
    timeout: float | None = None,
    client: FabricClient | None = None,
) -> dict:
    """Ask Fabric to refresh an endpoint and return the plain-data handle.

    ``tables`` names the ``(schema, table)`` pairs to sync, and ``None`` syncs
    every table. Fabric syncs 25 tables a request, so the handle keeps the rest
    and observation asks for each batch once the one before it completes.
    Fabric cancels a request still running after ``timeout`` seconds, 15
    minutes when it is omitted.
    """

    if endpoint.type != SQL_ENDPOINT:
        raise CommandError(
            f"SQL endpoint refresh requires a {SQL_ENDPOINT} item. Received {endpoint.type!r}."
        )
    batches = None if tables is None else _batches(tables)
    handle = {
        "lakehouse": endpoint.name,
        "workspace_id": endpoint.workspace_id,
        "sql_endpoint_id": endpoint.id,
        "timeout": timeout,
        "remaining": batches[1:] if batches else [],
    }
    if batches == []:
        return {
            **handle,
            "operation_id": None,
            "location": None,
            "retry_after": 0.0,
            "done": True,
            "status": "Succeeded",
        }
    return _request(
        handle, None if batches is None else batches[0], client or FabricClient()
    )


def _batches(tables) -> list[list[dict]]:
    """Fabric's table definitions, at most 25 tables to a request."""

    pairs = sorted(dict.fromkeys((str(schema), str(table)) for schema, table in tables))
    batches = []
    for first in range(0, len(pairs), TABLES_PER_REFRESH):
        by_schema: dict[str, list[str]] = {}
        for schema, table in pairs[first : first + TABLES_PER_REFRESH]:
            by_schema.setdefault(schema, []).append(table)
        batches.append(
            [
                {"schema": schema, "tableNames": names}
                for schema, names in by_schema.items()
            ]
        )
    return batches


def _request(handle: dict, batch, client: FabricClient) -> dict:
    from .client import accepted_operation

    payload: dict = {"recreateTables": False}
    if batch is not None:
        payload["tables"] = batch
    if handle["timeout"] is not None:
        payload["timeout"] = {"timeUnit": "Seconds", "value": int(handle["timeout"])}
    response = client.request(
        "POST",
        f"workspaces/{handle['workspace_id']}/sqlEndpoints/"
        f"{handle['sql_endpoint_id']}/refreshMetadata",
        payload=payload,
        expected=(200, 202),
    )
    started = {
        **handle,
        "operation_id": response.headers.get("x-ms-operation-id"),
        "location": None,
        "retry_after": 0.0,
        "done": response.status_code != 202,
        "status": "Running",
    }
    if started["done"]:
        return _next({**started, "status": _refresh_status(response)}, client)
    operation = accepted_operation(response)
    return {
        **started,
        "location": operation.location,
        "retry_after": operation.retry_after,
    }


def _next(refresh: dict, client: FabricClient) -> dict:
    """Ask for the next batch once the one before it has completed."""

    if not refresh["remaining"]:
        return refresh
    batch, *rest = refresh["remaining"]
    return _request({**refresh, "remaining": rest}, batch, client)


def observe_sql_endpoint_refresh(
    refresh: dict, *, client: FabricClient | None = None
) -> dict:
    """Poll a started refresh once; a failed refresh raises."""

    if refresh["done"]:
        return refresh
    from .client import Operation

    client = client or FabricClient()
    operation = client.poll_operation(
        Operation(location=refresh["location"], operation_id=refresh["operation_id"])
    )
    observed = {
        **refresh,
        "location": operation.location,
        "retry_after": operation.retry_after,
        "done": operation.done,
        "status": operation.body.get("status", "Succeeded")
        if operation.done and isinstance(operation.body, dict)
        else "Running",
    }
    return _next(observed, client) if operation.done else observed


def refresh_details(refresh: dict) -> dict:
    return {
        key: refresh[key]
        for key in ("lakehouse", "sql_endpoint_id", "operation_id", "status")
    }


def _refresh_status(response) -> str:
    body = response.json() if response.content else {}
    return body.get("status", "Succeeded") if isinstance(body, dict) else "Succeeded"


def _await_item(
    workspace: WorkspaceItem,
    name: str,
    item_type: str,
    *,
    client: FabricClient,
    attempts: int = 30,
    pause: float = 2.0,
) -> Item:
    for _ in range(attempts):
        try:
            return find_item(workspace, name, item_type=item_type, client=client)
        except CommandError:
            time.sleep(pause)
    raise FabricError(
        f"{item_type} {name!r} did not appear in {workspace.name!r} after "
        f"{int(attempts * pause)}s"
    )
