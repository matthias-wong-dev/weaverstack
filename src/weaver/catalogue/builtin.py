"""The Weaver-owned ``Warehouse/_weaver`` item and its standard surface.

Catalogue tables are ordinary checked-in Weaver documents read through the
repository readers.
"""

from __future__ import annotations

from ..declaration.model import WAREHOUSE, WeaverDocumentId, WeaverItemId
from .tables import CATALOGUE_SCHEMA

#: The reserved Item that owns the catalogue declaration.
BUILTIN_ITEM = WeaverItemId(WAREHOUSE, "_weaver")


def standard_surface_references(item: WeaverItemId):
    """Declare logical shortcuts from an item's ``_`` schema to ``_weaver``.

    Destinations carry explicit identities because ``_`` is not an authored
    schema from which identity can be decoded.
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
