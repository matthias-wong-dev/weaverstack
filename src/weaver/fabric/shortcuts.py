"""Create, inspect and delete Fabric OneLake shortcuts.

Build planning decides which shortcuts change. Creation submits those shortcuts
as one long-running bulk operation and handles each member's outcome separately.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Sequence
from urllib.parse import quote

from ..errors import CommandError
from .client import FabricClient, FabricError
from .resources import Item

#: Overwrite avoids the reservation Fabric leaves briefly after a delete.
OVERWRITE_POLICY = "CreateOrOverwrite"

#: Warehouse tables can reach OneLake after their catalogue transaction settles.
SOURCE_TIMEOUT = 120.0
SOURCE_POLL_INTERVAL = 5.0

#: A missing source is retried; an occupied path fails immediately.
_SOURCE_MISSING = "Target path doesn't exist"
_PATH_OCCUPIED = "NameConflictError"

_SUCCEEDED = "Succeeded"


@dataclass(frozen=True)
class Shortcut:
    path: str
    name: str
    target_workspace_id: str | None = None
    target_item_id: str | None = None
    target_path: str | None = None

    @property
    def qualified(self) -> str:
        return f"{self.path}/{self.name}"


def list_shortcuts(item: Item, *, client: FabricClient) -> tuple[Shortcut, ...]:
    """Normalise the leading separator Fabric adds to returned paths."""

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


@dataclass(frozen=True)
class ShortcutRequest:
    path: str
    name: str
    source: Item
    source_path: str

    @property
    def qualified(self) -> str:
        return f"{self.path}/{self.name}"

    @property
    def key(self) -> tuple[str, str]:
        return (self.path.strip("/"), self.name)


@dataclass(frozen=True)
class BulkShortcutResult:
    """Results in request order and the number of bulk calls made.

    A source still being published to OneLake can require another bulk call.
    """

    created: tuple[dict, ...]
    calls: int


def create_shortcuts(
    destination: Item,
    requests: Sequence[ShortcutRequest],
    *,
    client: FabricClient,
) -> BulkShortcutResult:
    """Create or repoint shortcuts in one bulk request.

    Successful members are kept. Members waiting for their sources in OneLake are
    retried under one deadline; permanent failures stop the batch.
    """

    if not requests:
        return BulkShortcutResult(created=(), calls=0)

    endpoint = (
        f"workspaces/{destination.workspace_id}/items/{destination.id}"
        f"/shortcuts/bulkCreate?shortcutConflictPolicy={OVERWRITE_POLICY}"
    )
    made: dict[tuple[str, str], dict] = {}
    pending = list(requests)
    deadline: float | None = None
    calls = 0

    while pending:
        try:
            response = client.request(
                "POST",
                endpoint,
                payload={
                    "createShortcutRequests": [
                        _request_payload(request) for request in pending
                    ]
                },
                expected=(200, 202),
            )
        except FabricError as exc:
            # The whole batch failed before Fabric produced member outcomes.
            raise CommandError(
                f"could not create {len(pending)} shortcut(s) in "
                f"{destination.name}: {exc}"
            ) from exc
        members = _members(response, client=client)
        calls += 1
        retry: list[ShortcutRequest] = []
        for request in pending:
            member = members.get(request.key)
            if member is None:
                raise CommandError(
                    f"Fabric reported no outcome for the shortcut "
                    f"{request.qualified} in {destination.name}, so whether it "
                    "was created is unknown."
                )
            if not member.get("error") and member.get("status") == _SUCCEEDED:
                made[request.key] = _detail(destination, request)
                continue
            _refuse_permanent(destination, request, member)
            retry.append(request)

        if not retry:
            break
        if deadline is None:
            deadline = time.monotonic() + SOURCE_TIMEOUT
        if time.monotonic() >= deadline:
            raise CommandError(
                "could not create the shortcut(s) "
                + ", ".join(sorted(request.qualified for request in retry))
                + f" in {destination.name}: their sources did not appear in "
                f"OneLake within {SOURCE_TIMEOUT:.0f}s."
            )
        time.sleep(SOURCE_POLL_INTERVAL)
        pending = retry

    return BulkShortcutResult(
        created=tuple(made[request.key] for request in requests),
        calls=calls,
    )


def _request_payload(request: ShortcutRequest) -> dict:
    return {
        "path": request.path,
        "name": request.name,
        "target": {
            "oneLake": {
                "workspaceId": request.source.workspace_id,
                "itemId": request.source.id,
                "path": request.source_path,
            }
        },
    }


def _detail(destination: Item, request: ShortcutRequest) -> dict:
    return {
        "path": request.qualified,
        "in": destination.name,
        "target": f"{request.source.name}/{request.source_path}",
    }


def _members(response, *, client: FabricClient) -> dict[tuple[str, str], dict]:
    """Read long-running outcomes and match them by echoed request, not order."""

    if response.status_code == 200:
        body = response.json() if response.content else {}
    else:
        operation = response.headers.get("x-ms-operation-id")
        client.wait_for_operation(response)
        body = client.request(
            "GET", f"operations/{operation}/result", expected=(200,)
        ).json()
    outcomes = {}
    for member in body.get("value") or ():
        echoed = member.get("request") or {}
        key = (str(echoed.get("path") or "").strip("/"), echoed.get("name") or "")
        outcomes[key] = member
    return outcomes


def _refuse_permanent(destination: Item, request: ShortcutRequest, member) -> None:
    """Raise unless the source is still being published to OneLake."""

    error = member.get("error") or {}
    reported = (
        " ".join(
            part for part in (error.get("errorCode"), error.get("message")) if part
        )
        or f"Fabric reported status {member.get('status')!r}"
    )
    if _PATH_OCCUPIED in reported:
        raise CommandError(
            f"{destination.name} already holds something at {request.qualified}, "
            "so a shortcut cannot be created there. Remove it, or point the "
            "shortcut at another name."
        )
    if _SOURCE_MISSING in reported:
        return
    raise CommandError(
        f"could not create the shortcut {request.qualified} in "
        f"{destination.name}: {reported}"
    )


def delete_shortcut(
    destination: Item, *, path: str, name: str, client: FabricClient
) -> None:
    """Remove a shortcut if present, without deleting its target data.

    Wipe must therefore use the shortcut API rather than delete a directory.
    """

    client.request(
        "DELETE",
        f"workspaces/{destination.workspace_id}/items/{destination.id}/shortcuts/"
        f"{quote(path.strip('/'), safe='')}/{quote(name, safe='')}",
        expected=(200, 202, 204, 404),
    )
