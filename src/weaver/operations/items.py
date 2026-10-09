"""Resolve load and test item syntax and installed targets for the API and CLI."""

from __future__ import annotations

from typing import Sequence

from ..declaration.model import (
    LAKEHOUSE,
    REPORT,
    SEMANTIC_MODEL,
    WAREHOUSE,
    WeaverItemId,
)
from ..declaration.selectors import is_powerbi_selector, powerbi_items
from ..errors import CommandError, IdentityError


def requested_items(
    items: str | Sequence[str] | None, *, what: str, project=None
) -> tuple[WeaverItemId, ...]:
    """Return requested items in input order, deduplicated.

    ``PowerBI`` and ``PowerBI/<project>`` expand to the semantic models in
    ``project``, a :class:`~weaver.operations.project.Project` read only for
    them. An empty result means every installed item after the catalogue is
    read.
    """

    if items is None:
        return ()
    values = (items,) if isinstance(items, str) else tuple(items)
    found: list[WeaverItemId] = []
    for value in values:
        if isinstance(value, str) and is_powerbi_selector(value.strip()):
            found.extend(_powerbi_models(value.strip(), project=project, what=what))
        else:
            found.append(parse_run_item(value, what=what))
    return tuple(dict.fromkeys(found))


def _powerbi_models(value: str, *, project, what: str) -> tuple[WeaverItemId, ...]:
    if project is None:
        raise CommandError(f"{what} does not accept {value}; name the semantic models")
    return tuple(
        item
        for item in powerbi_items(
            value, repository=project.repository, error=CommandError
        )
        if item.item_type != REPORT
    )


def parse_run_item(text: object, *, what: str) -> WeaverItemId:
    """Parse ``Lakehouse/Name``, ``Warehouse/Name`` or ``SemanticModel/Name``.

    A value carrying ``=`` is the build grammar, refused by name so the message
    says where the physical target actually comes from.
    """

    if not isinstance(text, str):
        raise CommandError(f"a {what} item must be a string, got {type(text).__name__}")
    written = text.strip()
    if "=" in written:
        item = written.partition("=")[0].strip() or f"{LAKEHOUSE}/Name"
        raise CommandError(
            f"{what} accepts installed item names only. Write {item}; Weaver reads "
            "its target from the catalogue."
        )
    try:
        return WeaverItemId.parse(written)
    except IdentityError:
        raise CommandError(
            f"a {what} item must be {LAKEHOUSE}/Name, {WAREHOUSE}/Name, "
            f"{SEMANTIC_MODEL}/Name, PowerBI or PowerBI/<project>, got {text!r}"
        ) from None


def uncatalogued_target(workspace, item: WeaverItemId):
    """Where an item no catalogue records is deployed.

    Its target in workspace configuration, or else the item's own name.
    """

    from ..targets import PhysicalTargetRef

    if item in workspace.configured_items:
        return PhysicalTargetRef.of(workspace.target_for(item))
    return PhysicalTargetRef(kind=item.item_type.lower(), name=item.item_name)


def run_scope(dag, items, *, what: str, catalogue: str | None = None):
    selected = tuple(items) or installed_items(dag, what=what, catalogue=catalogue)
    return selected, installed_targets(dag, selected, catalogue=catalogue)


def run_context_lines(workspace, items, installed) -> tuple[str, ...]:
    from ..catalogue.builtin import BUILTIN_ITEM

    lines = [
        f"Workspace  {workspace.workspace}",
        f"Catalogue  {workspace.catalogue or 'none'}",
        "Targets",
    ]
    for item in items:
        if item == BUILTIN_ITEM:
            continue
        logical = str(item)
        physical = str(installed[item])
        lines.append(
            f"  {logical}" if logical == physical else f"  {logical} → {physical}"
        )
    return tuple(lines)


def installed_items(
    dag, *, what: str, catalogue: str | None = None
) -> tuple[WeaverItemId, ...]:
    """Return items in ``_.Installation`` in identity order.

    A Report is left out: it has nothing to load or test.
    """

    items = tuple(
        sorted(
            (item for item in dag.installations if item.item_type != REPORT), key=str
        )
    )
    if not items:
        where = f" in catalogue {catalogue}" if catalogue else ""
        raise CommandError(
            f"{what} found no installed items{where}. Build an item first."
        )
    return items


def installed_targets(dag, items, *, catalogue: str | None = None):
    """Resolve each item to its physical target and report missing installations."""

    installed = {}
    missing = []
    for item in items:
        target = dag.installations.get(item)
        if target is None:
            missing.append(item)
        else:
            installed[item] = target
    if missing:
        where = f" in catalogue {catalogue}" if catalogue else ""
        known = ", ".join(sorted(str(item) for item in dag.installations)) or "none"
        raise CommandError(
            ", ".join(str(item) for item in missing)
            + (" have" if len(missing) > 1 else " has")
            + f" no installation{where}. Build it first. Installed: {known}"
        )
    return installed


__all__ = [
    "installed_items",
    "installed_targets",
    "parse_run_item",
    "requested_items",
    "run_context_lines",
    "run_scope",
    "uncatalogued_target",
]
