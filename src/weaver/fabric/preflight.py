"""Proving a desktop build's Fabric targets exist before a session is started.

A Livy session costs tens of seconds to create and a slice of a capacity to
hold. Discovering inside it that the Warehouse a binding names was never created
is the most expensive possible way to learn a fact that one REST call already
knew — and it surfaces as a Py4J or Spark failure about a missing catalogue
rather than as a sentence about a missing item.

So a desktop Fabric build asks the workspace what it holds *first*, and starts
nothing until every required item has been found with the type its binding
implies. Preflight reads; it never creates. A missing Weaver Lakehouse is a
failure here rather than something a build quietly provisions, because a Fabric
Lakehouse is a workspace item and creating one is provisioning, not building.

**One inventory, every check.** The workspace's items are listed once and every
target is resolved from that one result. Asking per target would turn a fixed
cost into one proportional to the number of bindings, for an answer that cannot
change between the calls.

**Every failure at once.** A build stopped by a missing Lakehouse, restarted,
then stopped by a missing Warehouse has cost two round trips to learn one thing:
the estate is not ready. The report names everything missing together.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..errors import BuildError
from .resources import (
    ENVIRONMENT,
    FACET_TYPES,
    LAKEHOUSE,
    WAREHOUSE,
    Item,
    Workspace,
    find_workspace,
    list_items,
)


class PreflightError(BuildError):
    """Raised when a required Fabric item is missing, mistyped or ambiguous."""


@dataclass(frozen=True)
class RequiredItem:
    """One item a build needs, and what it needs it to be.

    ``role`` is what the item is *to this build* — the Weaver Lakehouse, a bound
    target, the Environment. It exists so the report says why the item was
    wanted, which is the part a reader needs in order to act on it.
    """

    name: str
    item_type: str
    role: str

    def __str__(self) -> str:
        return f"{self.role} {self.name!r}"


@dataclass(frozen=True)
class Preflight:
    """What the workspace holds, and the resolved identity of each requirement."""

    workspace: Workspace
    resolved: dict[str, Item]

    def item(self, name: str, item_type: str) -> Item:
        return self.resolved[f"{item_type}/{name}"]


def required_items(
    bindings,
    *,
    weaver_lakehouse: str,
    environment: str | None = None,
) -> tuple[RequiredItem, ...]:
    """Everything a desktop build must find, deduplicated and ordered.

    Derived from the bindings rather than from configuration, so a target a
    binding names is checked even when nothing in the workspace file mentions
    it. The Weaver Lakehouse is included as a Lakehouse like any other — it is
    only special in what it holds.
    """

    wanted: list[RequiredItem] = [
        RequiredItem(weaver_lakehouse, LAKEHOUSE, "Weaver Lakehouse")
    ]
    if environment:
        wanted.append(RequiredItem(environment, ENVIRONMENT, "Environment"))
    for binding in bindings.entries:
        target = binding.target
        if hasattr(target, "lakehouse"):
            wanted.append(
                RequiredItem(target.lakehouse.name, LAKEHOUSE, "Lakehouse target")
            )
        else:
            wanted.append(
                RequiredItem(target.warehouse.name, WAREHOUSE, "Warehouse target")
            )

    seen: dict[tuple[str, str], RequiredItem] = {}
    for item in wanted:
        seen.setdefault((item.item_type, item.name), item)
    return tuple(seen.values())


def preflight_fabric_targets(
    bindings,
    *,
    workspace: str,
    weaver_lakehouse: str,
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
        bindings, weaver_lakehouse=weaver_lakehouse, environment=environment
    ):
        matches = by_name_and_type.get((required.item_type, required.name), [])
        if not matches:
            problems.append(_missing(required, inventory))
            continue
        if len(matches) > 1:
            problems.append(
                f"- {required} matches {len(matches)} items of that type — "
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

    A name that exists as the wrong type is the common mistake — a Lakehouse
    bound where a Warehouse was meant — and reporting it as a plain absence
    sends a reader looking for something that is in front of them.
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
