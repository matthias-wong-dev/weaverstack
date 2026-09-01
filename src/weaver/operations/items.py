"""The items a run is asked for, and where each one is installed.

Shared by load and test, and owned by core, so a notebook call and a command line
accept and refuse the same text.
"""

from __future__ import annotations

from typing import Sequence

from ..declaration.model import LAKEHOUSE, WAREHOUSE, WeaverItemId
from ..errors import CommandError, IdentityError


def requested_items(
    items: str | Sequence[str] | None, *, what: str
) -> tuple[WeaverItemId, ...]:
    """The items one run was asked for, in the order given, deduplicated.

    Naming none returns none, which a run reads as every installed item once it
    has read the catalogue. ``what`` is the operation's own noun, so a refusal
    reads in its vocabulary.
    """

    if items is None:
        return ()
    values = (items,) if isinstance(items, str) else tuple(items)
    return tuple(dict.fromkeys(parse_run_item(value, what=what) for value in values))


def parse_run_item(text: object, *, what: str) -> WeaverItemId:
    """One run item: ``Lakehouse/Name`` or ``Warehouse/Name``.

    A value carrying ``=`` is the build grammar, refused by name so the message
    says where the physical target actually comes from.
    """

    if not isinstance(text, str):
        raise CommandError(f"a {what} item must be a string, got {type(text).__name__}")
    written = text.strip()
    if "=" in written:
        item = written.partition("=")[0].strip() or f"{LAKEHOUSE}/Name"
        raise CommandError(
            f"{what} names installed items, and the physical target each one runs "
            f"in comes from the Weaver catalogue. Write {item}."
        )
    try:
        return WeaverItemId.parse(written)
    except IdentityError:
        raise CommandError(
            f"a {what} item must be {LAKEHOUSE}/Name or {WAREHOUSE}/Name, got {text!r}"
        ) from None


def run_scope(dag, items, *, what: str, catalogue: str | None = None):
    """The items this run covers, and the physical target each one runs in.

    An empty ``items`` is every installed item, resolved here because the
    catalogue that answers it has just been read. Above this the scope is a
    concrete tuple of item identities.
    """

    selected = tuple(items) or installed_items(dag, what=what, catalogue=catalogue)
    return selected, installed_targets(dag, selected, catalogue=catalogue)


def installed_items(
    dag, *, what: str, catalogue: str | None = None
) -> tuple[WeaverItemId, ...]:
    """Every item the catalogue records an installation for, in identity order.

    The scope comes from ``_.Installation``, so an item a workspace
    configuration declares and no build has installed is not one of them.
    """

    items = tuple(sorted(dag.installations, key=str))
    if not items:
        where = f" in catalogue {catalogue}" if catalogue else ""
        raise CommandError(
            f"{what} found no installed items{where}. Build an item first."
        )
    return items


def installed_targets(dag, items, *, catalogue: str | None = None):
    """The physical target each item is installed in, or a refusal naming the gaps.

    The one place a run turns its item scope into execution addresses.
    """

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
