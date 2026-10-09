"""Create, inspect and delete Fabric OneLake shortcuts.

Planning decides which shortcuts change. A submission sends them as one
long-running bulk operation and reports each member's outcome; the caller
resubmits members whose sources are still reaching OneLake.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Sequence
from urllib.parse import quote

from ..errors import CommandError
from .client import FabricClient, FabricError
from .resources import Item

#: Overwrite avoids the reservation Fabric leaves briefly after a delete.
OVERWRITE_POLICY = "CreateOrOverwrite"

#: Warehouse tables can reach OneLake after their catalogue transaction settles.
SOURCE_TIMEOUT = 120.0
SOURCE_POLL_INTERVAL = 5.0

#: Fabric's ``errorCode`` for a target path not yet in OneLake. It also covers
#: other invalid requests, which are retried until ``SOURCE_TIMEOUT`` and then
#: fail with Fabric's message.
_SOURCE_PENDING = "RequestBodyValidationFailed"
#: Fabric's ``errorCode`` for a path already holding something.
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
class BulkSubmission:
    """One bulk call's outcome by request position.

    ``waiting`` holds the positions whose sources may still be being published
    to OneLake; submit them again. ``reported`` holds Fabric's message for each.
    """

    created: Mapping[int, dict]
    waiting: tuple[int, ...]
    reported: Mapping[int, str] = field(default_factory=dict)


#: Fabric refuses a bulk request of more than 100 shortcuts with a 400.
BULK_LIMIT = 100
#: Bulk requests for one destination in flight at once.
BULK_REQUESTS = 4


def submit_shortcuts(
    destination: Item,
    requests: Sequence[ShortcutRequest],
    *,
    client: FabricClient,
) -> BulkSubmission:
    """Create or repoint shortcuts, at most ``BULK_LIMIT`` per bulk request.

    Successful members are kept. Permanent member failures raise.
    """

    requests = list(requests)
    if len(requests) <= BULK_LIMIT:
        return _submit(destination, requests, client=client)
    from concurrent.futures import ThreadPoolExecutor
    from contextvars import copy_context

    starts = range(0, len(requests), BULK_LIMIT)
    with ThreadPoolExecutor(max_workers=BULK_REQUESTS) as pool:
        submitted = [
            pool.submit(
                copy_context().run,
                _submit,
                destination,
                requests[start : start + BULK_LIMIT],
                client=client,
            )
            for start in starts
        ]
        outcomes = [each.result() for each in submitted]
    created: dict[int, dict] = {}
    waiting: list[int] = []
    reported: dict[int, str] = {}
    for start, outcome in zip(starts, outcomes):
        created.update({start + i: detail for i, detail in outcome.created.items()})
        waiting.extend(start + i for i in outcome.waiting)
        reported.update({start + i: text for i, text in outcome.reported.items()})
    return BulkSubmission(created=created, waiting=tuple(waiting), reported=reported)


def _submit(destination: Item, requests, *, client) -> BulkSubmission:
    if not requests:
        return BulkSubmission(created={}, waiting=())
    endpoint = (
        f"workspaces/{destination.workspace_id}/items/{destination.id}"
        f"/shortcuts/bulkCreate?shortcutConflictPolicy={OVERWRITE_POLICY}"
    )
    try:
        response = client.request(
            "POST",
            endpoint,
            payload={
                "createShortcutRequests": [
                    _request_payload(request) for request in requests
                ]
            },
            expected=(200, 202),
        )
    except FabricError as exc:
        # The whole batch failed before Fabric produced member outcomes.
        raise CommandError(
            f"could not create {len(requests)} shortcut(s) in {destination.name}: {exc}"
        ) from exc
    members = _members(response, client=client)
    created: dict[int, dict] = {}
    waiting: list[int] = []
    reported: dict[int, str] = {}
    for position, request in enumerate(requests):
        member = members.get(request.key)
        if member is None:
            raise CommandError(
                f"Fabric reported no outcome for the shortcut "
                f"{request.qualified} in {destination.name}, so whether it "
                "was created is unknown."
            )
        if not member.get("error") and member.get("status") == _SUCCEEDED:
            created[position] = _detail(destination, request)
            continue
        reported[position] = _refuse_permanent(destination, request, member)
        waiting.append(position)
    return BulkSubmission(created=created, waiting=tuple(waiting), reported=reported)


def sources_not_published(
    destination: str, reported: Mapping[str, str]
) -> CommandError:
    """``reported`` maps each shortcut to Fabric's last message for it."""

    detail = "; ".join(
        f"{shortcut}: {reported[shortcut]}" if reported[shortcut] else shortcut
        for shortcut in sorted(reported)
    )
    return CommandError(
        f"could not create the shortcut(s) in {destination}: Fabric still "
        f"refused them after {SOURCE_TIMEOUT:.0f}s. {detail}"
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


def _refuse_permanent(destination: Item, request: ShortcutRequest, member) -> str:
    """Raise unless the source may still be being published to OneLake.

    Returns Fabric's message for a member to submit again.
    """

    error = member.get("error") or {}
    code = error.get("errorCode")
    reported = (
        " ".join(part for part in (code, error.get("message")) if part)
        or f"Fabric reported status {member.get('status')!r}"
    )
    if code == _PATH_OCCUPIED:
        raise CommandError(
            f"{destination.name} already holds something at {request.qualified}, "
            "so a shortcut cannot be created there. Remove it, or point the "
            f"shortcut at another name. Fabric reported: {reported}"
        )
    if code == _SOURCE_PENDING:
        return reported
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
