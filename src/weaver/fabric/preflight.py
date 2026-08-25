"""Proving a desktop build's Fabric targets exist before a session is started.

A Livy session costs tens of seconds and a slice of a capacity, and a missing
item discovered inside one surfaces as a Spark failure about a catalogue rather
than a sentence about the item. So a desktop Fabric build asks the workspace
what it holds first, and starts nothing until every required item is found with
the type its binding implies.

Preflight reads and never creates: a missing catalogue Warehouse is a failure here,
because creating a workspace item is provisioning rather than building.

The workspace's items are listed once and every target resolved from that one
result, and every missing item is reported together, because a build stopped twice
has paid two round trips to learn one thing.
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

#: A binding's target kind, as the Fabric item type the workspace must hold it
#: as. Two vocabularies that happen to name the same two things, so the mapping
#: is written out rather than left to a spelling coincidence.
_ITEM_TYPE_FOR_BINDING = {
    LAKEHOUSE_TARGET: LAKEHOUSE,
    WAREHOUSE_TARGET: WAREHOUSE,
}


class PreflightError(BuildError):
    """Raised when a required Fabric item is missing, mistyped or ambiguous."""


@dataclass(frozen=True)
class RequiredItem:
    """One item a build needs, and what it needs it to be.

    ``role`` is what the item is to this build: the Weaver catalogue, a bound
    target, the Environment. It exists so the report says why the item was
    wanted, which is the part needed to act on it.
    """

    name: str
    item_type: str
    role: str

    def __str__(self) -> str:
        return f"{self.role} {self.name!r}"


@dataclass(frozen=True)
class Preflight:
    """What the workspace holds, and the resolved identity of each requirement."""

    workspace: WorkspaceItem
    resolved: dict[str, Item]

    def item(self, name: str, item_type: str) -> Item:
        return self.resolved[f"{item_type}/{name}"]


def required_items(
    bindings,
    *,
    control_item: str,
    environment: str | None = None,
) -> tuple[RequiredItem, ...]:
    """Everything a desktop build must find, deduplicated and ordered.

    Derived from the bindings rather than from configuration, so a target a
    binding names is checked even when nothing in the workspace file mentions
    it. The catalogue's Warehouse is included as a Warehouse like any other, being
    special only in what it holds.
    """

    wanted: list[RequiredItem] = [
        RequiredItem(str(control_item), WAREHOUSE, "Weaver catalogue")
    ]
    if environment:
        wanted.append(RequiredItem(environment, ENVIRONMENT, "Environment"))
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
    environment: str | None = None,
    client=None,
) -> Preflight:
    """Resolve every required item from one workspace listing, or fail saying why.

    Returns the resolved items so a caller need not look them up again. Reading
    only: a successful preflight leaves the workspace exactly as it found it.
    """

    physical = find_workspace(workspace, client=client)
    inventory = list_items(physical, client=client)

    by_name_and_type: dict[tuple[str, str], list[Item]] = {}
    for item in inventory:
        by_name_and_type.setdefault((item.type, item.name), []).append(item)

    resolved: dict[str, Item] = {}
    problems: list[str] = []
    for required in required_items(
        bindings, control_item=control_item, environment=environment
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

    if problems:
        raise PreflightError(
            f"Fabric build preflight failed in workspace {workspace!r}:\n"
            + "\n".join(problems)
        )
    return Preflight(workspace=physical, resolved=resolved)


def _missing(required: RequiredItem, inventory) -> str:
    """One missing item, and the type confusion behind it when there is one.

    A name that exists as the wrong type is the common mistake, such as a Lakehouse
    bound where a Warehouse was meant. Reporting it as a plain absence sends the
    search after something that is already there.
    """

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
