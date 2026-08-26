"""The Weaver-owned ``Warehouse/_weaver`` repository content.

Weaver's catalogue is declared as ordinary Weaver documents, checked into the
package under ``repository/`` and read through the ordinary repository readers
like any authored tree. A change to a catalogue table is a change to its
document: there is no second generator to keep in step, and what an estate is
built from is exactly what is reviewed.

``tests/targeted/test_builtin_catalogue_content.py`` holds the documents
against :mod:`weaver.catalogue.tables`, so a table that gains a column without
its document changing is refused before it can reach a build.
"""

from __future__ import annotations

from importlib.resources import files as resource_files

from ..declaration.model import WAREHOUSE, WeaverItemId

#: The reserved Item that owns the catalogue declaration.
BUILTIN_ITEM = WeaverItemId(WAREHOUSE, "_weaver")
ITEM_ROOT = str(BUILTIN_ITEM)

#: The package directory holding Weaver-owned repository content, rooted so the
#: paths inside it are repository-relative.
_REPOSITORY_ROOT = "repository"


def item_repository_files() -> dict[str, bytes]:
    """Every checked-in file of the built-in catalogue item, by relative path."""

    base = resource_files("weaver.catalogue").joinpath(_REPOSITORY_ROOT)
    found: dict[str, bytes] = {}

    def walk(traversable, prefix: str) -> None:
        for entry in traversable.iterdir():
            relative = f"{prefix}/{entry.name}" if prefix else entry.name
            if entry.is_dir():
                walk(entry, relative)
            else:
                found[relative] = entry.read_bytes()

    walk(base, "")
    return found
