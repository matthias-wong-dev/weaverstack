"""Control Fabric capacity through the Azure CLI.

Capacity lives in Azure Resource Manager rather than the Fabric REST API. The
Azure CLI supplies the subscription context.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from typing import Sequence

from ..errors import WeaverError

CAPACITY_ACTIONS = ("status", "resume", "suspend")

_AZ_VERB = {"status": "show", "resume": "resume", "suspend": "suspend"}

# Subscription fallback when the Azure CLI has more than one.
SUBSCRIPTION_ENV = "FABRIC_SUBSCRIPTION_ID"


class CapacityError(WeaverError):
    pass


@dataclass(frozen=True)
class CapacityAction:
    action: str
    capacity: str
    state: str | None
    sku: str | None = None
    returncode: int = 0

    @property
    def running(self) -> bool:
        return (self.state or "").lower() == "active"

    def __str__(self) -> str:
        detail = f"{self.state or 'unknown'}"
        if self.sku:
            detail += f", {self.sku}"
        return f"{self.capacity}: {detail}"


def capacity_command(
    action: str,
    *,
    resource_group: str,
    capacity_name: str,
    subscription_id: str | None = None,
    extra_args: Sequence[str] = (),
) -> list[str]:
    verb = _AZ_VERB.get(action)
    if verb is None:
        raise CapacityError(
            f"Unsupported capacity action {action!r}. Choose from: "
            + ", ".join(CAPACITY_ACTIONS)
        )
    if not resource_group:
        raise CapacityError("A capacity resource group is required.")
    if not capacity_name:
        raise CapacityError("A capacity name is required.")

    command = [
        "az",
        "fabric",
        "capacity",
        verb,
        "--resource-group",
        resource_group,
        "--capacity-name",
        capacity_name,
    ]
    if subscription_id:
        command.extend(["--subscription", subscription_id])
    command.extend(extra_args)
    return command


def run_capacity_action(
    action: str,
    *,
    resource_group: str,
    capacity_name: str,
    subscription_id: str | None = None,
    extra_args: Sequence[str] = (),
) -> CapacityAction:
    if shutil.which("az") is None:
        raise CapacityError(
            "Capacity commands require the Azure CLI. Install it from "
            "https://learn.microsoft.com/cli/azure/install-azure-cli"
        )

    command = capacity_command(
        action,
        resource_group=resource_group,
        capacity_name=capacity_name,
        subscription_id=subscription_id or os.environ.get(SUBSCRIPTION_ENV),
        extra_args=(*extra_args, "--output", "json"),
    )
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        raise CapacityError(
            f"Azure CLI {action} failed for Fabric capacity {capacity_name!r}: "
            + (completed.stderr.strip() or completed.stdout.strip() or "no output")
        )

    payload = _payload(completed.stdout)
    return CapacityAction(
        action=action,
        capacity=capacity_name,
        state=_state(payload),
        sku=(payload.get("sku") or {}).get("name") if payload else None,
        returncode=completed.returncode,
    )


def _payload(stdout: str) -> dict:
    text = (stdout or "").strip()
    if not text:
        return {}
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _state(payload: dict) -> str | None:
    """The running state, which az reports in more than one place."""

    if not payload:
        return None
    properties = payload.get("properties") or {}
    return properties.get("state") or payload.get("state")
