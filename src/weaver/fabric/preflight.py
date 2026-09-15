"""Validate a desktop build's Fabric items before starting a Livy session.

Preflight lists each workspace once and never creates Fabric items.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..build_bundle.targets import LAKEHOUSE_TARGET, WAREHOUSE_TARGET
from ..errors import BuildError
from .resources import (
    ENVIRONMENT,
    FACET_TYPES,
    LAKEHOUSE,
    WAREHOUSE,
    Item,
    WorkspaceItem,
    find_workspace,
    list_items,
)

# Keep binding kinds and Fabric item types as separate vocabularies.
_ITEM_TYPE_FOR_BINDING = {
    LAKEHOUSE_TARGET: LAKEHOUSE,
    WAREHOUSE_TARGET: WAREHOUSE,
}


class PreflightError(BuildError):
    """A required Fabric item is missing, mistyped or ambiguous."""


@dataclass(frozen=True)
class RequiredItem:
    name: str
    item_type: str
    role: str

    def __str__(self) -> str:
        return f"{self.role} {self.name!r}"


@dataclass(frozen=True)
class Preflight:
    workspace: WorkspaceItem
    resolved: dict[str, Item]

    def item(self, name: str, item_type: str) -> Item:
        return self.resolved[f"{item_type}/{name}"]


def required_items(
    bindings,
    *,
    control_item: str,
    environment=None,
) -> tuple[RequiredItem, ...]:
    """Derive requirements from bindings.

    Targets need not appear in workspace configuration.
    """

    wanted: list[RequiredItem] = [
        RequiredItem(str(control_item), WAREHOUSE, "Weaver catalogue")
    ]
    if environment:
        from ..workspaces import EnvironmentRef

        environment_ref = EnvironmentRef.parse(environment)
        wanted.append(RequiredItem(environment_ref.name, ENVIRONMENT, "Environment"))
    for binding in bindings.entries:
        item_type = _ITEM_TYPE_FOR_BINDING[binding.target.kind]
        wanted.append(
            RequiredItem(binding.target.item.name, item_type, f"{item_type} target")
        )

    seen: dict[tuple[str, str], RequiredItem] = {}
    for item in wanted:
        seen.setdefault((item.item_type, item.name), item)
    return tuple(seen.values())


def preflight_fabric_targets(
    bindings,
    *,
    workspace: str,
    control_item: str,
    environment=None,
    client=None,
) -> Preflight:
    """Resolve all requirements without modifying a workspace."""

    from ..workspaces import EnvironmentRef

    physical = find_workspace(workspace, client=client)
    inventory = list_items(physical, client=client)
    environment_ref = EnvironmentRef.parse(environment) if environment else None
    local_environment = (
        environment_ref
        if environment_ref and environment_ref.owner(workspace) == workspace
        else None
    )

    by_name_and_type: dict[tuple[str, str], list[Item]] = {}
    for item in inventory:
        by_name_and_type.setdefault((item.type, item.name), []).append(item)

    resolved: dict[str, Item] = {}
    problems: list[str] = []
    for required in required_items(
        bindings, control_item=control_item, environment=local_environment
    ):
        matches = by_name_and_type.get((required.item_type, required.name), [])
        if not matches:
            problems.append(_missing(required, inventory))
            continue
        if len(matches) > 1:
            problems.append(
                f"- {required} matches {len(matches)} items of that type, so "
                "the name is ambiguous"
            )
            continue
        resolved[f"{required.item_type}/{required.name}"] = matches[0]

    if environment_ref and local_environment is None:
        owner_name = environment_ref.owner(workspace)
        owner = find_workspace(owner_name, client=client)
        try:
            environment_item = next(
                item
                for item in list_items(owner, item_type=ENVIRONMENT, client=client)
                if item.name == environment_ref.name
            )
        except StopIteration:
            problems.append(f"- Environment {str(environment_ref)!r} was not found")
        else:
            resolved[f"{ENVIRONMENT}/{str(environment_ref)}"] = environment_item

    if problems:
        raise PreflightError(
            f"Fabric build preflight failed in workspace {workspace!r}:\n"
            + "\n".join(problems)
        )
    return Preflight(workspace=physical, resolved=resolved)


def _missing(required: RequiredItem, inventory) -> str:
    """Distinguish a type mismatch from an absent item."""

    others = sorted(
        {
            item.type
            for item in inventory
            if item.name == required.name
            and item.type != required.item_type
            and item.type not in FACET_TYPES
        }
    )
    if others:
        return (
            f"- {required} was not found; the workspace holds a "
            f"{', '.join(others)} of that name"
        )
    return f"- {required} was not found"
