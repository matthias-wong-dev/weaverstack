"""Fabric OneLake shortcut operations for shortcuts.

Shortcuts are recreated during installation because they contain no data.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from urllib.parse import quote

from ..errors import CommandError
from .client import FabricClient, FabricError
from .resources import Item

#: How long a removed shortcut may take to stop conflicting with a new one of
#: the same name, and how often to retry. Fabric accepts the delete immediately
#: and settles it shortly afterwards, so a create issued in between is refused.
#: Measured at under a second; the bound is for a slow tenant.
REPLACE_TIMEOUT = 30.0
REPLACE_POLL_INTERVAL = 2.0

#: How long to wait for a source Fabric has accepted but not yet published to
#: OneLake. A Warehouse creates a table in its own catalogue first and publishes
#: the Delta directory behind it a moment later, so a shortcut created in the same
#: build as its source can arrive before there is anything to point at. Bounded,
#: because a source that is genuinely absent has to fail.
SOURCE_TIMEOUT = 120.0
SOURCE_POLL_INTERVAL = 5.0

#: How Fabric distinguishes the two conflicts a create can meet, both of which
#: are a 409. The first is the shortcut being replaced, still settling. The
#: second is something else already occupying the path, which waiting will not
#: change. ``Shorcuts`` is Fabric's spelling.
_STILL_SETTLING = "ShorcutsOperationNotAllowed"
_SOURCE_MISSING = "Target path doesn't exist"
_PATH_OCCUPIED = "NameConflictError"


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
    """Point ``destination``'s ``path/name`` at ``source``'s ``source_path``.

    An existing shortcut of the same name is replaced rather than treated as a
    collision: a shortcut holds no data, and a build has to be able to run twice.
    Fabric settles a deletion a moment after accepting it, so the create is
    retried while it reports the old name as still there.

    Retried for a second reason, and on a longer deadline: Fabric validates the
    target and a source created earlier in this same build may not be published to
    OneLake yet. Waiting is what lets one build create a thing and point at it;
    the deadline is what makes a source that will never appear still fail.
    """

    delete_shortcut(destination, path=path, name=name, client=client)
    payload = {
        "path": path,
        "name": name,
        "target": {
            "oneLake": {
                "workspaceId": source.workspace_id,
                "itemId": source.id,
                "path": source_path,
            }
        },
    }
    endpoint = f"workspaces/{destination.workspace_id}/items/{destination.id}/shortcuts"
    settling_deadline = time.monotonic() + REPLACE_TIMEOUT
    source_deadline = time.monotonic() + SOURCE_TIMEOUT
    while True:
        try:
            response = client.request("POST", endpoint, payload=payload)
            break
        except FabricError as exc:
            message = str(exc)
            if _PATH_OCCUPIED in message:
                raise CommandError(
                    f"{destination.name} already holds something at {path}/{name}, "
                    "so a shortcut cannot be created there. Remove it, or point "
                    "the shortcut at another name."
                ) from exc
            if _SOURCE_MISSING in message:
                if time.monotonic() >= source_deadline:
                    raise CommandError(
                        f"could not create the shortcut {path}/{name} in "
                        f"{destination.name}: {source.name}/{source_path} did not "
                        f"appear in OneLake within {SOURCE_TIMEOUT:.0f}s. A source "
                        "created in this build is published a moment after it is "
                        "made; one that never appears is not there."
                    ) from exc
                time.sleep(SOURCE_POLL_INTERVAL)
                continue
            if _STILL_SETTLING not in message or time.monotonic() >= settling_deadline:
                raise CommandError(
                    f"could not create the shortcut {path}/{name} in "
                    f"{destination.name}: {exc}"
                ) from exc
            time.sleep(REPLACE_POLL_INTERVAL)
    return {
        "path": f"{path}/{name}",
        "in": destination.name,
        "target": f"{source.name}/{source_path}",
        # Reported because it says which contract Fabric honoured. Creating one
        # shortcut is documented as synchronous — a 201 — while bulk creation is
        # not; so a 202 here would mean the shortcut itself is still being made,
        # which is a different thing from the destination Lakehouse not yet having
        # registered it as a table. Only the second is what the readability wait
        # in `weaver.build_bundle.executors.shortcut` exists for.
        "status": response.status_code,
    }


def delete_shortcut(
    destination: Item, *, path: str, name: str, client: FabricClient
) -> None:
    """Remove a shortcut if it is there. A 404 is the intended state, not a fault.

    Removing the shortcut is not removing what it points at: the data belongs to
    the item that produced it, and this only takes away this item's name for it.
    That distinction is the whole reason a wipe must remove shortcuts *through the
    workspace* rather than by deleting a directory (see :mod:`weaver.physical_wipe`).
    """

    client.request(
        "DELETE",
        f"workspaces/{destination.workspace_id}/items/{destination.id}/shortcuts/"
        f"{quote(path.strip('/'), safe='')}/{quote(name, safe='')}",
        expected=(200, 202, 204, 404),
    )
