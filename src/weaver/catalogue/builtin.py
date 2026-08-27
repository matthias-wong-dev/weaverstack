"""The Weaver-owned ``Warehouse/_weaver`` item, and the surface it presents.

Weaver's catalogue is declared as ordinary Weaver documents, checked into
:mod:`weaver.fragments` and read through the ordinary repository readers like
any authored tree. A change to a catalogue table is a change to its document.

``tests/targeted/test_builtin_catalogue_declaration.py`` holds the documents
against :mod:`weaver.catalogue.tables`, so a table that gains a column without
its document changing is refused before it can reach a build.
"""

from __future__ import annotations

from ..declaration.model import WAREHOUSE, WeaverDocumentId, WeaverItemId
from .tables import CATALOGUE_SCHEMA

#: The reserved Item that owns the catalogue declaration.
BUILTIN_ITEM = WeaverItemId(WAREHOUSE, "_weaver")


def standard_surface_references(item: WeaverItemId):
    """The standard Weaver catalogue surface one normal item presents.

    Each surface table is an ordinary logical shortcut declaration from the
    item's ``_`` namespace to the built-in item. The destination carries its
    identity rather than decoding one from a ``Schema__Object`` name, because
    the schema is Weaver's ``_`` and not something the item declares.
    """

    from ..declaration.metadata import ObjectId
    from ..declaration.model import (
        LOGICAL_TARGET,
        TABLE_SHORTCUT,
        VIEW_SHORTCUT,
        RepositoryShortcut,
        ShortcutDeclaration,
    )
    from .tables import STANDARD_SURFACE_TABLES

    declarations = []
    pairs = []
    for table in STANDARD_SURFACE_TABLES:
        object_id = ObjectId(CATALOGUE_SCHEMA, table.name)
        destination = WeaverDocumentId(item, object_id)
        source = WeaverDocumentId(BUILTIN_ITEM, object_id)
        declarations.append(
            ShortcutDeclaration(
                owner=item,
                name=object_id.qualified,
                shortcut_type=(
                    VIEW_SHORTCUT if item.item_type == WAREHOUSE else TABLE_SHORTCUT
                ),
                target_type=LOGICAL_TARGET,
                target=str(source),
                destination_identity=destination,
            )
        )
        pairs.append(RepositoryShortcut(destination=destination, source=source))
    return tuple(declarations), tuple(pairs)
