"""Resolve what a direct shortcut points at, before a bundle is generated.

A bound shortcut names a Weaver document, so the installer addresses it exactly
as it addresses every other target of the build. A direct one names a physical
Fabric item, possibly in another workspace, which is not a target of anything and
which the installer has no reason to be able to find.

So it is resolved here, where the estate can be read, and frozen into the bundle:
workspace id, item id, and the source path spelled as storage spells it. Fabric
validates a shortcut's target when it is created and its paths are
case-sensitive, so an address guessed later is a 400 rather than a wrong answer.
"""

from __future__ import annotations

from ..declaration.model import LAKEHOUSE, SCHEMA_SHORTCUT, TABLE_SHORTCUT
from ..errors import BuildError
from ..locations import Location
from .shortcuts import ResolvedShortcutSource

TABLES_AREA = "Tables"


def read_shortcut_sources(
    shortcuts,
    *,
    resolver,
    store,
) -> dict[str, ResolvedShortcutSource]:
    """The physical address behind every direct shortcut, by declaration."""

    resolved: dict[str, ResolvedShortcutSource] = {}
    for declaration in shortcuts:
        if declaration.bind:
            continue
        resolved[f"{declaration.owner}/{declaration.name}"] = _resolve(
            declaration, resolver=resolver, store=store
        )
    return resolved


def _resolve(declaration, *, resolver, store) -> ResolvedShortcutSource:
    target = declaration.target_item
    if target.item_type != LAKEHOUSE:
        raise BuildError(
            f"shortcut {declaration.name} in {declaration.owner} points at "
            f"{declaration.target}. A OneLake shortcut reads a Lakehouse, and "
            f"{target.item_type} items are reached over TDS."
        )
    try:
        item = resolver.external_lakehouse(
            target.item_name, workspace=declaration.workspace
        )
    except Exception as exc:
        where = declaration.workspace or "this workspace"
        raise BuildError(
            f"shortcut {declaration.name} in {declaration.owner} points at "
            f"{target.item_name} in {where}, which did not resolve: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    root = resolver.external_root(item)
    path = _source_path(declaration, root=root, store=store)
    return ResolvedShortcutSource(
        workspace_id=item.workspace_id,
        item_id=item.id,
        item_name=item.name,
        path=path,
    )


def _source_path(declaration, *, root: Location, store) -> str:
    """Where the source sits, spelled as storage spells it.

    Weaver's identities are exact-case, but a Fabric estate may materialise an
    authored ``Customer`` table directory as ``customer``. The authored spelling
    is preferred where it exists; failing that one case-insensitive match is
    taken, and anything else is refused here rather than at install time.
    """

    tail = declaration.target_tail
    if declaration.shortcut_type == SCHEMA_SHORTCUT:
        components = [TABLES_AREA, tail]
    elif declaration.shortcut_type == TABLE_SHORTCUT:
        schema, _, name = tail.partition(".")
        components = [TABLES_AREA, schema, name]
    else:
        components = tail.split("/")

    settled: list[str] = []
    for component in components:
        parent = root.join(*settled) if settled else root
        settled.append(_stored_name(declaration, parent, component, store=store))
    return "/".join(settled)


def _stored_name(declaration, parent: Location, wanted: str, *, store) -> str:
    if store.exists(parent / wanted):
        return wanted
    try:
        entries = store.list(parent)
    except Exception as exc:
        raise BuildError(
            f"shortcut {declaration.name} in {declaration.owner} points at "
            f"{declaration.target}, and {parent.value} could not be read: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    matches = sorted(
        entry.location.name
        for entry in entries
        if entry.location.name.casefold() == wanted.casefold()
    )
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise BuildError(
            f"shortcut {declaration.name} in {declaration.owner} points at "
            f"{declaration.target}, and {wanted!r} is not in {parent.value}"
        )
    raise BuildError(
        f"shortcut {declaration.name} in {declaration.owner} points at "
        f"{declaration.target}, and {wanted!r} matches more than one entry in "
        f"{parent.value}: " + ", ".join(matches)
    )
