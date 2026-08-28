"""The logical items a run is asked for, parsed once for load and test.

A run selects installed logical Weaver items. Where each one physically lives is
recorded in the catalogue's ``_.Installation``, so a run target carries no
physical half and cannot override the installed one.

Core owns this parsing. ``weaver.load(["Lakehouse/Landing"])`` in a notebook and
``weaver load --target Lakehouse/Landing`` on a desktop accept and refuse the
same text.
"""

from __future__ import annotations

from typing import Sequence

from ..declaration.model import LAKEHOUSE, WAREHOUSE, WeaverItemId
from ..errors import CommandError, IdentityError


def requested_items(
    targets: str | Sequence[str], *, what: str
) -> tuple[WeaverItemId, ...]:
    """The logical items one run was asked for, in the order given, deduplicated.

    ``what`` is the operation's own noun, so the refusal reads in that
    operation's vocabulary.
    """

    values = (targets,) if isinstance(targets, str) else tuple(targets or ())
    if not values:
        raise CommandError(f"{what} needs at least one target")
    return tuple(dict.fromkeys(parse_run_item(value, what=what) for value in values))


def parse_run_item(text: object, *, what: str) -> WeaverItemId:
    """One run target: ``Lakehouse/Name`` or ``Warehouse/Name``, logical.

    A value carrying ``=`` is the build grammar, and it is refused by name. The
    catalogue is authoritative about where an installed item runs.
    """

    if not isinstance(text, str):
        raise CommandError(
            f"a {what} target must be a string, got {type(text).__name__}"
        )
    written = text.strip()
    if "=" in written:
        logical = written.partition("=")[0].strip() or f"{LAKEHOUSE}/Name"
        raise CommandError(
            f"{what} targets are logical installed items, and physical targets "
            f"are read from the Weaver catalogue. Write {logical}."
        )
    try:
        return WeaverItemId.parse(written)
    except IdentityError:
        raise CommandError(
            f"a {what} target must name a logical Weaver item as "
            f"{LAKEHOUSE}/Name or {WAREHOUSE}/Name, got {text!r}"
        ) from None


def installed_targets(dag, items, *, catalogue: str | None = None):
    """Where the catalogue says each requested logical item is installed.

    The one place a run turns its logical scope into physical execution
    addresses, read from ``_.Installation`` through the installed graph. A miss
    is refused here, which is after the catalogue read over TDS and before any
    Spark session starts.
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
