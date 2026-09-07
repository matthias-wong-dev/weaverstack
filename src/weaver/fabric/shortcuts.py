"""Fabric OneLake shortcut operations for shortcuts.

A shortcut is made where one is declared and none stands, and repointed where
the pair it declares changed. What a build leaves alone it does not touch: which
shortcuts an installation acts on is settled in
:mod:`weaver.build_bundle.incremental`.

Creation is bulk. Fabric takes a whole batch in one request and reports each
member's outcome separately, so a Lakehouse mirror of seventy shortcuts costs one
crossing.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Sequence
from urllib.parse import quote

from ..errors import CommandError
from .client import FabricClient, FabricError
from .resources import Item

#: What tells Fabric to point an existing shortcut somewhere else. The default
#: policy is ``Abort``, under which a create over a live name is a 409 and the
#: name a delete released stays held for up to thirty-five seconds afterwards.
#: Measured against a Fabric tenant: an overwrite of a live name answers 200 in
#: under a second, an overwrite issued moments after a delete answers 201, and
#: neither waits.
OVERWRITE_POLICY = "CreateOrOverwrite"

#: How long to wait for a source Fabric has accepted but not yet published to
#: OneLake. A Warehouse creates a table in its own catalogue first and publishes
#: the Delta directory behind it a moment later, so a shortcut created in the same
#: build as its source can arrive before there is anything to point at. Bounded,
#: because a source that is absent has to fail.
SOURCE_TIMEOUT = 120.0
SOURCE_POLL_INTERVAL = 5.0

#: The two failures a create reports as something other than success. The source
#: is not in OneLake yet, which waiting answers; or something that is not a
#: shortcut occupies the path, which it does not.
_SOURCE_MISSING = "Target path doesn't exist"
_PATH_OCCUPIED = "NameConflictError"

#: What Fabric calls a member that was created.
_SUCCEEDED = "Succeeded"


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

    Fabric echoes a path back rooted, ``/Tables/DWG`` for the ``Tables/DWG`` it
    was given, so the leading separator is normalised here rather than by every
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


@dataclass(frozen=True)
class ShortcutRequest:
    """One member of a batch: where it appears, and what it points at."""

    path: str
    name: str
    source: Item
    source_path: str

    @property
    def qualified(self) -> str:
        return f"{self.path}/{self.name}"

    @property
    def key(self) -> tuple[str, str]:
        """What a response member is matched back by, rooting normalised."""

        return (self.path.strip("/"), self.name)


@dataclass(frozen=True)
class BulkShortcutResult:
    """What one batch of shortcut creations produced.

    ``created`` is one detail mapping per request, in request order. ``calls`` is
    how many bulk requests it took, which is more than one where a source was
    still being published when the batch reached Fabric.
    """

    created: tuple[dict, ...]
    calls: int


def create_shortcuts(
    destination: Item,
    requests: Sequence[ShortcutRequest],
    *,
    client: FabricClient,
) -> BulkShortcutResult:
    """Point each request's ``path/name`` at its source, in one bulk request.

    Under ``CreateOrOverwrite``: a shortcut holds no data, so an existing name is
    repointed, and a build has to be able to run twice.

    Fabric settles each member separately, so a batch can come back part
    succeeded. What succeeded is kept. A member whose source is not published to
    OneLake yet is sent again, and only that member: a source created earlier in
    this same build may not be readable when the batch reaches Fabric. One
    deadline covers the whole batch, so a source that will never appear still
    fails.
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
            # The batch was refused as a whole, so no member has an outcome.
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
                f"OneLake within {SOURCE_TIMEOUT:.0f}s. A source created in this "
                "build is published a moment after it is made; one that never "
                "appears is not there."
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
    """Each member's outcome, keyed by the request Fabric echoes back.

    Bulk creation is long-running, so a 202 carries the outcomes at the
    operation's result address. Members are matched by the request Fabric echoes
    back, because the order they return in is not part of the contract.
    """

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
    """Raise unless this member failed for a source still being published.

    A member that failed carries an error. One reporting a status Fabric added
    after this was written carries none, and the status is then what there is to
    report.
    """

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
