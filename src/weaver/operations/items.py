"""Resolve run items and their installed targets."""

from __future__ import annotations

from typing import Sequence

from ..declaration.model import LAKEHOUSE, WAREHOUSE, WeaverItemId
from ..errors import CommandError, IdentityError


def requested_items(
    items: str | Sequence[str] | None, *, what: str
) -> tuple[WeaverItemId, ...]:
    """Return requested items in input order, deduplicated.

    An empty result means every installed item after the catalogue is read.
    """

    if items is None:
        return ()
    values = (items,) if isinstance(items, str) else tuple(items)
    return tuple(dict.fromkeys(parse_run_item(value, what=what) for value in values))


def parse_run_item(text: object, *, what: str) -> WeaverItemId:
    """Parse ``Lakehouse/Name`` or ``Warehouse/Name``.

    A value carrying ``=`` is the build grammar, refused by name so the message
    says where the physical target actually comes from.
    """

    if not isinstance(text, str):
        raise CommandError(f"a {what} item must be a string, got {type(text).__name__}")
    written = text.strip()
    if "=" in written:
        item = written.partition("=")[0].strip() or f"{LAKEHOUSE}/Name"
        raise CommandError(
            f"{what} accepts installed item names without target bindings. "
            f"Write {item}; the Weaver catalogue supplies its physical target."
        )
    try:
        return WeaverItemId.parse(written)
    except IdentityError:
        raise CommandError(
            f"a {what} item must be {LAKEHOUSE}/Name or {WAREHOUSE}/Name, got {text!r}"
        ) from None


def run_scope(dag, items, *, what: str, catalogue: str | None = None):
    """Resolve the run's item scope and physical targets."""

    selected = tuple(items) or installed_items(dag, what=what, catalogue=catalogue)
    return selected, installed_targets(dag, selected, catalogue=catalogue)


def installed_items(
    dag, *, what: str, catalogue: str | None = None
) -> tuple[WeaverItemId, ...]:
    """Return items in ``_.Installation`` in identity order."""

    items = tuple(sorted(dag.installations, key=str))
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
    "run_scope",
]
