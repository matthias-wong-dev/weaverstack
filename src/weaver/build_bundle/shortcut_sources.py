"""Resolve physical shortcut sources before bundle generation.

Physical sources are not build targets, so their workspace, item and case-exact
storage path are frozen while the source inventory is available. Warehouse
tables are published under ``Tables/<schema>/<table>``; Files sources require a
Lakehouse.
"""

from __future__ import annotations

from ..declaration.model import LAKEHOUSE, SCHEMA_SHORTCUT, TABLE_SHORTCUT
from ..errors import BuildError
from ..locations import Location
from .shortcuts import ResolvedShortcutSource

TABLES_AREA = "Tables"


def physical_shortcuts(shortcuts, *, bindings):
    """Return physical shortcuts owned by items bound for this build.

    An unreachable source fails only the build that includes its owning item.
    """

    return tuple(
        declaration
        for declaration in shortcuts
        if not declaration.is_logical
        and not declaration.is_view
        and declaration.owner in bindings.by_item
    )


def read_shortcut_sources(
    shortcuts,
    *,
    resolver,
    store,
) -> dict[str, ResolvedShortcutSource]:
    resolved: dict[str, ResolvedShortcutSource] = {}
    for declaration in shortcuts:
        if declaration.is_logical:
            continue
        resolved[f"{declaration.owner}/{declaration.name}"] = _resolve(
            declaration, resolver=resolver, store=store
        )
    return resolved


def _resolve(declaration, *, resolver, store) -> ResolvedShortcutSource:
    target = declaration.target_item
    if declaration.is_files and target.item_type != LAKEHOUSE:
        raise BuildError(
            f"shortcut {declaration.name} in {declaration.owner} points at "
            f"{declaration.target}, but Files shortcuts require a Lakehouse source; "
            f"a {target.item_type} has no Files area"
        )
    try:
        item = resolver.external_item(
            target.item_name,
            item_type=target.item_type,
            workspace=declaration.workspace,
        )
    except Exception as exc:
        where = declaration.workspace or "this workspace"
        raise BuildError(
            f"could not resolve shortcut {declaration.name} in {declaration.owner} "
            f"source {target.item_name} in {where}: "
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
    tail = declaration.target_tail
    if declaration.shortcut_type == SCHEMA_SHORTCUT:
        components = [TABLES_AREA, tail]
    elif declaration.shortcut_type == TABLE_SHORTCUT:
        schema, _, name = tail.partition(".")
        components = [TABLES_AREA, schema, name]
    else:
        components = tail.split("/")

    return stored_path(
        root,
        components,
        store=store,
        what=(
            f"shortcut {declaration.name} in {declaration.owner} points at "
            f"{declaration.target}"
        ),
    )


def stored_path(root: Location, components, *, store, what: str) -> str:
    """Resolve an item-relative path to its unambiguous storage spelling.

    Exact authored spelling wins; otherwise one case-insensitive match is
    accepted. ``what`` identifies the subject in failures.
    """

    settled: list[str] = []
    for component in components:
        parent = root.join(*settled) if settled else root
        settled.append(_stored_name(what, parent, component, store=store))
    return "/".join(settled)


def _stored_name(what: str, parent: Location, wanted: str, *, store) -> str:
    if store.exists(parent / wanted):
        return wanted
    try:
        entries = store.list(parent)
    except Exception as exc:
        raise BuildError(
            f"{what}; could not read {parent.value}: {type(exc).__name__}: {exc}"
        ) from exc
    matches = sorted(
        entry.location.name
        for entry in entries
        if entry.location.name.casefold() == wanted.casefold()
    )
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise BuildError(f"{what}; {wanted!r} is not in {parent.value}")
    raise BuildError(
        f"{what}; {wanted!r} matches more than one entry in "
        f"{parent.value}: " + ", ".join(matches)
    )
