"""Recreate shortcuts recorded by a forked catalogue.

A mirror recreates each copied ``_.Shortcut`` row as a Warehouse view or OneLake
shortcut. A shortcut owns no data, so it remains a Registry ``shortcut`` and
gets no ``_.Mirror`` row.

A logical target follows the destination's bindings, so a pointer at
``Lakehouse/Landing`` in the source stands at ``Lakehouse/DEV_Landing`` here. A
physical target names a Fabric item directly and is recreated as recorded.

The Weaver-owned ``_`` surface is excluded here. These functions rebuild it from
the shared declaration:
:func:`weaver.catalogue.borrow.surface_statements` and
:func:`weaver.catalogue.borrow.surface_shortcuts`.

See https://docs.weaverstack.dev/core-concepts/catalogue/.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from ..declaration.model import (
    FILES,
    FOLDER_SHORTCUT,
    LAKEHOUSE,
    SCHEMA_SHORTCUT,
    TABLES,
    VIEW_SHORTCUT,
    WeaverItemId,
    WeaverSchemaId,
)
from ..errors import WeaverError
from ..installed import InstalledShortcut
from .claims import stored_area
from .tables import CATALOGUE_SCHEMA
from .tsql import identifier


class UnresolvedShortcut(WeaverError):
    """A recorded shortcut has no resolvable destination target."""


@dataclass(frozen=True)
class Recreated:
    shortcut: InstalledShortcut
    #: The physical item the target resolves to in the destination.
    target_name: str
    #: The workspace that item is in, or ``None`` for the one being built.
    target_workspace: str | None = None

    @property
    def destination(self):
        return self.shortcut.destination

    @property
    def area(self) -> str:
        if isinstance(self.destination, WeaverSchemaId):
            return TABLES
        return FILES if self.destination.is_files else TABLES

    @property
    def path(self) -> str:
        # Schema shortcuts sit directly under Tables.
        if isinstance(self.destination, WeaverSchemaId):
            return TABLES
        return f"{self.area}/{self.destination.object_id.schema}"

    @property
    def name(self) -> str:
        if isinstance(self.destination, WeaverSchemaId):
            return self.destination.schema
        return self.destination.object_id.object

    @property
    def source_components(self) -> tuple[str, ...]:
        """Return the target's path within its physical item.

        A logical target's stored schema carries its area. A physical target's
        shortcut type selects ``Files`` or ``Tables``.
        """

        shortcut = self.shortcut
        if shortcut.is_logical:
            area, schema = stored_area(shortcut.target_schema)
            tail = (shortcut.target_object,) if shortcut.target_object else ()
            return (area or TABLES, *_parts(schema), *tail)
        area = FILES if shortcut.shortcut_type == FOLDER_SHORTCUT else TABLES
        tail = (shortcut.target_object,) if shortcut.target_object else ()
        return (area, *_parts(shortcut.target_schema), *tail)


def _parts(path: str) -> tuple[str, ...]:
    return tuple(part for part in path.split("/") if part)


def recreatable(
    shortcuts: Iterable[InstalledShortcut],
    *,
    item: WeaverItemId,
    bindings: Mapping[WeaverItemId, str],
) -> tuple[Recreated, ...]:
    """Resolve one item's recorded shortcuts to physical targets.

    ``bindings`` is the destination's final Installation map, allowing logical
    targets to move with the fork. Every selected shortcut must resolve.
    """

    found = []
    for shortcut in shortcuts:
        if shortcut.destination.item != item or _is_surface(shortcut):
            continue
        target_item = shortcut.target_item
        if target_item is None:
            raise UnresolvedShortcut(
                f"{shortcut.destination} is recorded as a shortcut naming no "
                "target item"
            )
        if shortcut.is_logical:
            bound = bindings.get(target_item)
            if bound is None:
                raise UnresolvedShortcut(
                    f"{shortcut.destination} points at {target_item}, which the "
                    "catalogue records no installation for"
                )
            found.append(Recreated(shortcut=shortcut, target_name=bound))
            continue
        found.append(
            Recreated(
                shortcut=shortcut,
                target_name=target_item.item_name,
                target_workspace=shortcut.target_workspace,
            )
        )
    return tuple(sorted(found, key=lambda each: str(each.destination)))


def _is_surface(shortcut: InstalledShortcut) -> bool:
    destination = shortcut.destination
    if isinstance(destination, WeaverSchemaId):
        return destination.schema == CATALOGUE_SCHEMA
    return destination.object_id.schema == CATALOGUE_SCHEMA


def view_statement(recreated: Recreated) -> str:
    """Materialise a same-workspace Warehouse shortcut using a three-part name."""

    shortcut = recreated.shortcut
    if shortcut.is_logical and shortcut.source is not None:
        schema = shortcut.source.object_id.schema
        name = shortcut.source.object_id.object
    else:
        schema = shortcut.target_schema
        name = shortcut.target_object or ""
    source = ".".join(
        identifier(part) for part in (recreated.target_name, schema, name)
    )
    destination = shortcut.destination.object_id
    return (
        f"create or alter view {identifier(destination.schema)}."
        f"{identifier(destination.object)} as select * from {source};"
    )


def shortcut_request(recreated: Recreated, *, source, source_path: str) -> dict:
    """Return a Lakehouse shortcut request using the target's physical path."""

    return {
        "shortcut": str(recreated.destination),
        "type": recreated.shortcut.shortcut_type,
        "path": recreated.path,
        "name": recreated.name,
        "source": source,
        "source_path": source_path,
    }


def schemas_of(recreated: Iterable[Recreated]) -> tuple[str, ...]:
    """Return schemas needed for views, including those holding only shortcuts."""

    found = set()
    for each in recreated:
        destination = each.destination
        if isinstance(destination, WeaverSchemaId):
            found.add(destination.schema)
        else:
            found.add(destination.object_id.schema)
    return tuple(sorted(found))


def unsupported(recreated: Recreated, *, kind: str) -> str | None:
    is_view = recreated.shortcut.shortcut_type == VIEW_SHORTCUT
    if kind == LAKEHOUSE and is_view:
        return "a view shortcut is a Warehouse view, and this item is a Lakehouse"
    if kind != LAKEHOUSE and not is_view:
        return (
            f"a {recreated.shortcut.shortcut_type} shortcut is a OneLake "
            "shortcut, and this item is a Warehouse"
        )
    if (
        recreated.shortcut.shortcut_type == SCHEMA_SHORTCUT
        and recreated.shortcut.is_logical
    ):
        return "a schema shortcut names a namespace, which Weaver does not bind"
    return None


__all__: Sequence[str] = [
    "Recreated",
    "UnresolvedShortcut",
    "recreatable",
    "schemas_of",
    "shortcut_request",
    "unsupported",
    "view_statement",
]
