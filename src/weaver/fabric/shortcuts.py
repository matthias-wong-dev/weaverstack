"""OneLake shortcuts — Fabric's own way of pointing one item at another's data.

A shortcut is how an alias exists in Fabric. It has no bytes: the destination
Lakehouse gains a table (or a folder) under its own name, and reads pass through
to the item that owns the data. That is precisely what a Weaver alias claims —
the destination item owns the name, the source stays the canonical producer — so
the two map onto each other exactly.

Created by replacement. A shortcut carries no data of its own, so removing and
remaking one loses nothing, and a build that could not re-run over its own aliases
would not be re-runnable.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote

from .client import FabricClient
from .resources import Item


@dataclass(frozen=True)
class Shortcut:
    """One shortcut an item holds: where it appears, and what it points at."""

    path: str
    name: str
    target_workspace_id: str | None = None
    target_item_id: str | None = None
    target_path: str | None = None

    @property
    def qualified(self) -> str:
        return f"{self.path}/{self.name}"


def list_shortcuts(item: Item, *, client: FabricClient) -> tuple[Shortcut, ...]:
    """Every shortcut this item holds.

    Fabric echoes a path back rooted — ``/Tables/DWG`` for the ``Tables/DWG`` it
    was given — so the leading separator is normalised here rather than by every
    caller.
    """

    found = []
    for entry in client.paged(
        f"workspaces/{item.workspace_id}/items/{item.id}/shortcuts"
    ):
        onelake = (entry.get("target") or {}).get("oneLake") or {}
        found.append(
            Shortcut(
                path=(entry.get("path") or "").strip("/"),
                name=entry.get("name") or "",
                target_workspace_id=onelake.get("workspaceId"),
                target_item_id=onelake.get("itemId"),
                target_path=onelake.get("path"),
            )
        )
    return tuple(sorted(found, key=lambda shortcut: shortcut.qualified))


def create_shortcut(
    destination: Item,
    *,
    path: str,
    name: str,
    source: Item,
    source_path: str,
    client: FabricClient,
) -> dict:
    """Point ``destination``'s ``path/name`` at ``source``'s ``source_path``."""

    delete_shortcut(destination, path=path, name=name, client=client)
    response = client.request(
        "POST",
        f"workspaces/{destination.workspace_id}/items/{destination.id}/shortcuts",
        payload={
            "path": path,
            "name": name,
            "target": {
                "oneLake": {
                    "workspaceId": source.workspace_id,
                    "itemId": source.id,
                    "path": source_path,
                }
            },
        },
    )
    return {
        "shortcut": f"{path}/{name}",
        "in": destination.name,
        "target": f"{source.name}/{source_path}",
        # Reported because it says which contract Fabric honoured. Creating one
        # shortcut is documented as synchronous — a 201 — while bulk creation is
        # not; so a 202 here would mean the shortcut itself is still being made,
        # which is a different thing from the destination Lakehouse not yet having
        # registered it as a table. Only the second is what the readability wait
        # in `weaver.build_bundle.executors.alias` exists for.
        "status": response.status_code,
    }


def delete_shortcut(
    destination: Item, *, path: str, name: str, client: FabricClient
) -> None:
    """Remove a shortcut if it is there. A 404 is the intended state, not a fault.

    Removing the shortcut is not removing what it points at: the data belongs to
    the item that produced it, and this only takes away this item's name for it.
    That distinction is the whole reason a wipe must remove shortcuts *through the
    workspace* rather than by deleting a directory (see :mod:`weaver.wipe`).
    """

    client.request(
        "DELETE",
        f"workspaces/{destination.workspace_id}/items/{destination.id}/shortcuts/"
        f"{quote(path.strip('/'), safe='')}/{quote(name, safe='')}",
        expected=(200, 202, 204, 404),
    )
