"""Turning a Fabric capacity on and off.

Capacity is billed while it runs, so this is the first and last thing a session
touches. It goes through the Azure CLI rather than a REST call because capacity
lives in ARM rather than in the Fabric API, and ``az`` already holds the
subscription context.
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

#: Environment fallback for the subscription, when az has more than one.
SUBSCRIPTION_ENV = "FABRIC_SUBSCRIPTION_ID"


class CapacityError(WeaverError):
    """Raised when a capacity action cannot be run."""


@dataclass(frozen=True)
class CapacityAction:
    """The outcome of one capacity action."""

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
    """The Azure CLI command for one capacity action, without running it."""

    verb = _AZ_VERB.get(action)
    if verb is None:
        raise CapacityError(
            f"unknown capacity action {action!r} — expected one of "
            + ", ".join(CAPACITY_ACTIONS)
        )
    if not resource_group:
        raise CapacityError("a capacity needs its resource group")
    if not capacity_name:
        raise CapacityError("a capacity needs its name")

    command = [
        "az", "fabric", "capacity", verb,
        "--resource-group", resource_group,
        "--capacity-name", capacity_name,
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
    """Run a capacity action and report the resulting state."""

    if shutil.which("az") is None:
        raise CapacityError(
            "the Azure CLI is not installed — install it with: brew install azure-cli"
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
            f"az {action} failed for {capacity_name!r}: "
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
