"""Standing up again the shortcuts a forked catalogue records.

A fork copies ``_.Shortcut``, so the destination catalogue certifies every
pointer the source item had. A mirror leaves a complete physical estate, so it
recreates them: a view in a Warehouse, a OneLake shortcut in a Lakehouse. A
pointer holds no data of its own, so none of it is borrowed and no ``_.Mirror``
row is written for one. Its Registry role stays ``shortcut``.

A logical target follows this estate's own bindings, so a pointer at
``Lakehouse/Landing`` in the source stands at ``Lakehouse/DEV_Landing`` here. A
physical target names a Fabric item directly and is recreated as recorded.

The ``_`` surface is left out. It is Weaver's own, and
:func:`weaver.catalogue.borrow.surface_statements` and
:func:`weaver.catalogue.borrow.surface_shortcuts` stand it up from one
declaration for a mirrored item and a built one alike.

See ``design/catalogue.md``.
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
    """A recorded pointer this estate has no address to stand up again."""


@dataclass(frozen=True)
class Recreated:
    """One recorded shortcut, and the physical target it now points at."""

    shortcut: InstalledShortcut
    #: The physical item the target resolves to for this estate.
    target_name: str
    #: The workspace that item is in, or ``None`` for the one being built.
    target_workspace: str | None = None

    @property
    def destination(self):
        return self.shortcut.destination

    @property
    def area(self) -> str:
        """The Lakehouse area this shortcut is created in."""

        if isinstance(self.destination, WeaverSchemaId):
            return TABLES
        return FILES if self.destination.is_files else TABLES

    @property
    def path(self) -> str:
        """Where Fabric creates it: a schema shortcut sits directly under Tables."""

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
        """The target's path inside its item, component by component.

        A logical target is a Weaver identity, so its stored schema carries the
        area. A physical one names a path whose area the shortcut's own type
        gives: a folder reads ``Files`` and everything else ``Tables``.
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
    """One item's recorded shortcuts, each resolved to a physical target.

    ``bindings`` is the estate's final Installation map, which is what makes a
    logical pointer move with the fork.

    Every recorded pointer resolves or the run stops: a mirror reconstructs the
    estate it was asked for and says what it could not. A logical target no
    binding names has no address to point at, and a row naming no target item at
    all is a catalogue that contradicts itself.
    """

    found = []
    for shortcut in shortcuts:
        if shortcut.destination.item != item or _is_surface(shortcut):
            continue
        target_item = shortcut.target_item
        if target_item is None:
            raise UnresolvedShortcut(
                f"{shortcut.destination} is recorded as a shortcut naming no "
                "target item, so there is no address to point it at"
            )
        if shortcut.is_logical:
            bound = bindings.get(target_item)
            if bound is None:
                raise UnresolvedShortcut(
                    f"{shortcut.destination} points at {target_item}, which the "
                    "catalogue records no installation for, so there is no "
                    "target to point it at"
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
    """Whether this is one of the ``_`` surface pointers a mirror already makes."""

    destination = shortcut.destination
    if isinstance(destination, WeaverSchemaId):
        return destination.schema == CATALOGUE_SCHEMA
    return destination.object_id.schema == CATALOGUE_SCHEMA


def view_statement(recreated: Recreated) -> str:
    """One Warehouse shortcut, as the statement that materialises it.

    Three-part, which is how a Fabric Warehouse reaches another item in its own
    workspace, and the same spelling
    :func:`weaver.build_bundle.shortcuts.view_statement` renders from a
    declaration.
    """

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
    """One Lakehouse shortcut, in the shape a create submission carries.

    ``source`` is the resolved item it reads and ``source_path`` where the
    target sits inside it, spelled as storage spells it.
    """

    return {
        "shortcut": str(recreated.destination),
        "type": recreated.shortcut.shortcut_type,
        "path": recreated.path,
        "name": recreated.name,
        "source": source,
        "source_path": source_path,
    }


def schemas_of(recreated: Iterable[Recreated]) -> tuple[str, ...]:
    """The relational schemas these shortcuts are created in.

    A Warehouse view needs its schema to exist, and a schema holding only
    pointers has no borrowed relation to have made it.
    """

    found = set()
    for each in recreated:
        destination = each.destination
        if isinstance(destination, WeaverSchemaId):
            found.add(destination.schema)
        else:
            found.add(destination.object_id.schema)
    return tuple(sorted(found))


def unsupported(recreated: Recreated, *, kind: str) -> str | None:
    """Why this item cannot stand this shortcut up, or ``None`` when it can."""

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
