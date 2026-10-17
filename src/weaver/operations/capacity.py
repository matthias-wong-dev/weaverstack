"""Azure capacity operations without a terminal process."""

from __future__ import annotations

from ..fabric.capacity import CapacityAction


def capacity(
    action: str,
    *,
    resource_group: str,
    capacity_name: str,
    subscription_id: str | None = None,
    credential=None,
    client=None,
) -> CapacityAction:
    """Control an Azure Fabric capacity and return its observed state.

    ``credential`` is an Azure TokenCredential for Resource Manager, independent
    of a notebook's Fabric identity. The library default is used otherwise.
    Subscription selection uses ``subscription_id`` or FABRIC_SUBSCRIPTION_ID.
    """
    import os
    import re
    from contextlib import ExitStack

    from azure.core.exceptions import AzureError

    from ..fabric.capacity import SUBSCRIPTION_ENV, CapacityError, capacity_command

    if subscription_id is None:
        subscription_id = os.environ.get(SUBSCRIPTION_ENV)
    capacity_command(
        action,
        resource_group=resource_group,
        capacity_name=capacity_name,
        subscription_id=subscription_id,
    )
    if (
        not isinstance(subscription_id, str)
        or re.fullmatch(
            r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", subscription_id
        )
        is None
    ):
        raise CapacityError(
            "Pass subscription_id as a UUID for the Azure subscription."
        )
    try:
        with ExitStack() as owned:
            if client is None:
                from azure.mgmt.fabric import FabricMgmtClient

                if credential is None:
                    from ..fabric.auth import credential as default_credential

                    credential = default_credential()
                client = FabricMgmtClient(
                    credential=credential, subscription_id=subscription_id
                )
                owned.callback(client.close)
            operations = client.fabric_capacities
            if action != "status":
                getattr(operations, f"begin_{action}")(
                    resource_group_name=resource_group, capacity_name=capacity_name
                ).result()
            resource = operations.get(
                resource_group_name=resource_group, capacity_name=capacity_name
            )
            body = resource.as_dict()
            return CapacityAction(
                action,
                capacity_name,
                (body.get("properties") or {}).get("state") or body.get("state"),
                (body.get("sku") or {}).get("name"),
            )
    except AzureError as exc:
        detail = getattr(getattr(exc, "error", None), "message", None) or exc.message
        raise CapacityError(f"{capacity_name}: {detail}") from exc
