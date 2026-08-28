"""The items a run is asked for, and where each one is installed.

Shared by load and test, and owned by core, so a notebook call and a command line
accept and refuse the same text.
"""

from __future__ import annotations

from typing import Sequence

from ..declaration.model import LAKEHOUSE, WAREHOUSE, WeaverItemId
from ..errors import CommandError, IdentityError


def requested_items(
    items: str | Sequence[str], *, what: str
) -> tuple[WeaverItemId, ...]:
    """The items one run was asked for, in the order given, deduplicated.

    ``what`` is the operation's own noun, so a refusal reads in its vocabulary.
    """

    values = (items,) if isinstance(items, str) else tuple(items or ())
    if not values:
        raise CommandError(f"{what} needs at least one item")
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


__all__ = ["installed_targets", "parse_run_item", "requested_items"]
