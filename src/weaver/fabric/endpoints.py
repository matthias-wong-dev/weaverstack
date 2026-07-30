"""Refreshing a Lakehouse's SQL analytics endpoint metadata.

A Fabric Lakehouse presents its Delta tables twice, and the SQL side is
synchronised behind the Spark side rather than with it. Anything that reads the
Lakehouse *as SQL* — a Warehouse view over it, a report, a downstream shortcut —
reads that metadata, so a build has to ask for the sync explicitly and wait for it.

Waiting is the point. The refresh is a long-running operation, and returning as
soon as Fabric accepted it would put the barrier in the wrong place: the next item
would start against endpoint metadata that has not caught up, which is the exact
failure the refresh exists to prevent.
"""

from __future__ import annotations

import time

from ..errors import CommandError
from .client import FabricClient, FabricError
from .resources import Item

#: The refresh is metadata only, so it is quick in normal use; the bound exists
#: so a stuck operation fails visibly instead of holding a build open.
REFRESH_TIMEOUT = 600.0
REFRESH_POLL_INTERVAL = 5.0

#: Fabric's terminal long-running-operation states.
_TERMINAL = frozenset({"succeeded", "success", "completed", "failed", "cancelled"})


def sql_endpoint_id(lakehouse: Item, *, client: FabricClient) -> str:
    """The id of the SQL analytics endpoint Fabric generated for a Lakehouse."""

    payload = client.get_json(
        f"workspaces/{lakehouse.workspace_id}/lakehouses/{lakehouse.id}"
    )
    properties = payload.get("properties") or {}
    endpoint = (properties.get("sqlEndpointProperties") or {}).get("id")
    if not endpoint:
        raise CommandError(
            f"Lakehouse {lakehouse.name!r} reports no SQL analytics endpoint yet, so "
            "its metadata cannot be refreshed"
        )
    return str(endpoint)


def refresh_sql_endpoint_metadata(
    lakehouse: Item,
    *,
    client: FabricClient,
    timeout: float = REFRESH_TIMEOUT,
    poll_interval: float = REFRESH_POLL_INTERVAL,
) -> dict:
    """Sync one Lakehouse's endpoint metadata, and wait for the sync to finish."""

    endpoint = sql_endpoint_id(lakehouse, client=client)
    response = client.request(
        "POST",
        f"workspaces/{lakehouse.workspace_id}/sqlEndpoints/{endpoint}"
        "/refreshMetadata?preview=true",
        payload={},
        expected=(200, 202),
    )
    state = "succeeded"
    if response.status_code == 202:
        state = _await_operation(response, client=client, timeout=timeout, poll_interval=poll_interval)
        if state.lower() not in {"succeeded", "success", "completed"}:
            raise FabricError(
                f"refreshing the SQL endpoint of {lakehouse.name!r} finished as {state!r}"
            )
    return {
        "lakehouse": lakehouse.name,
        "sql_endpoint_id": endpoint,
        "state": state,
    }


def _await_operation(
    response,
    *,
    client: FabricClient,
    timeout: float,
    poll_interval: float,
) -> str:
    location = response.headers.get("Operation-Location") or response.headers.get("Location")
    if not location:
        raise FabricError(
            "Fabric accepted the SQL endpoint refresh but returned no operation to poll"
        )
    deadline = time.time() + timeout
    while time.time() < deadline:
        payload = client.get_json(location)
        state = str(payload.get("status") or payload.get("state") or "")
        if state.lower() in _TERMINAL:
            return state
        time.sleep(poll_interval)
    raise FabricError(
        f"the SQL endpoint refresh did not finish within {int(timeout)}s"
    )
