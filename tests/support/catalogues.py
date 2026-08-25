"""Catalogues for a test that is not talking to a Warehouse.

The real :class:`~weaver.catalogue.state.Catalogue`, built from rows in the shape
the ``_`` schema holds them. Not a fake: an object anchored to one of these
resolves its identity and reads its bookmark exactly as it does in a run, so what
is under test is the object rather than a stand-in.

The round trip, a clean load writing a row, and a later load reading it back,
needs a Warehouse, so it is proved where there is one.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import MappingProxyType

from weaver.catalogue.state import Catalogue
from weaver.declaration.metadata import ObjectId
from weaver.declaration.model import WeaverDocumentId, WeaverItemId

#: The item and physical target a test's objects belong to, unless it says
#: otherwise. Neutral names, as every fixture here uses.
ITEM = "Lakehouse/Sales"
TARGET = "Sales_LH"

#: A plausible instant for "this was loaded". Any value above the sentinel does.
LOADED_AT = datetime(2026, 8, 20, 6, 0, tzinfo=timezone.utc)


class Recording:
    """A catalogue writer that keeps what it was given.

    Stands where a Warehouse would, so a test can assert what a load recorded
    without one, and can be made to fail, which is the other thing worth
    proving about a write.
    """

    def __init__(self, *, failing: Exception | None = None) -> None:
        self.submitted: list[tuple[str, dict]] = []
        self.updated: list[tuple[str, dict]] = []
        self.flushes = 0
        self._failing = failing

    def submit(self, table, row) -> None:
        self.submitted.append((table.name, dict(row)))

    def update(self, table, row) -> None:
        self.updated.append((table.name, dict(row)))

    def flush(self) -> None:
        self.flushes += 1
        if self._failing is not None:
            raise self._failing

    def rows(self, table_name: str) -> list[dict]:
        """What was written to one table, appended and merged together."""

        return [
            row for name, row in self.submitted + self.updated if name == table_name
        ]


def installed(
    *objects: str,
    at: datetime | None = None,
    item: str = ITEM,
    target: str = TARGET,
    files: bool = False,
    writer=None,
) -> Catalogue:
    """A catalogue recording these ``Schema.Object`` names as installed.

    ``at`` gives each one a bookmark; without it they have none, which is what an
    object no clean load has run for has.

    ``files`` says the objects are Folders, whose catalogue identity carries the
    ``Files/`` prefix, a Folder and a Table of the same name are two objects.
    """

    owner = WeaverItemId.parse(item)
    schema_prefix = "Files/" if files else ""
    registry = []
    bookmarks = []
    for name in objects:
        schema, object = name.split(".", 1)
        registry.append(
            {
                "item_type": owner.item_type,
                "item_name": owner.item_name,
                "schema_name": f"{schema_prefix}{schema}",
                "object_name": object,
                "object_type": "folder" if files else "table",
                "object_role": "data",
                "signature": "s",
                "build_datetime": None,
            }
        )
        if at is not None:
            bookmarks.append(
                {
                    "item_type": owner.item_type,
                    "item_name": owner.item_name,
                    "schema_name": f"{schema_prefix}{schema}",
                    "object_name": object,
                    "bookmark_datetime": at,
                }
            )
    tables = {
        "Installation": (
            {
                "item_type": owner.item_type,
                "item_name": owner.item_name,
                "target_name": target,
                "weaver_version": "0.1",
                "signature": "s",
            },
        ),
        "Registry": tuple(registry),
        "Bookmark": tuple(bookmarks),
    }
    return Catalogue(
        MappingProxyType({owner: MappingProxyType(tables)}),
        writer=writer if writer is not None else Recording(),
    )


def validating(
    *validations: str,
    item: str = ITEM,
    target: str = TARGET,
    kind: str = "test",
    writer=None,
) -> Catalogue:
    """A catalogue recording these ``Schema.Object`` names as declared validations.

    A validation materialises nothing, so it has no Registry row: what records it
    is ``_.TestDictionary``, and its compiled artefact is what Registry certifies.
    """

    owner = WeaverItemId.parse(item)
    tables = {
        "Installation": (
            {
                "item_type": owner.item_type,
                "item_name": owner.item_name,
                "target_name": target,
                "weaver_version": "0.1",
                "signature": "s",
            },
        ),
        "TestDictionary": tuple(
            {
                "item_type": owner.item_type,
                "item_name": owner.item_name,
                "schema_name": name.split(".", 1)[0],
                "object_name": name.split(".", 1)[1],
                "test_type": kind,
                "signature": "s",
            }
            for name in validations
        ),
    }
    return Catalogue(
        MappingProxyType({owner: MappingProxyType(tables)}),
        writer=writer if writer is not None else Recording(),
    )


def never(*objects: str, **kwargs) -> Catalogue:
    """A catalogue in which these objects are installed and never cleanly loaded."""

    return installed(*objects, at=None, **kwargs)


def loaded(*objects: str, at: datetime = LOADED_AT, **kwargs) -> Catalogue:
    """A catalogue in which these objects were cleanly loaded at ``at``."""

    return installed(*objects, at=at, **kwargs)


def identity(name: str, *, item: str = ITEM, files: bool = False) -> WeaverDocumentId:
    """One ``Schema.Object`` as the catalogue keys it."""

    return WeaverDocumentId(
        WeaverItemId.parse(item), ObjectId(*name.split(".", 1)), is_files=files
    )


__all__ = [
    "ITEM",
    "LOADED_AT",
    "TARGET",
    "Recording",
    "identity",
    "installed",
    "loaded",
    "never",
    "validating",
]
