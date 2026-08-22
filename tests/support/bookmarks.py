"""Bookmark context for a test that is not talking to a catalogue.

An orchestrated run reads every bookmark once and hands the map down, so a test
can hand down the same thing. That is the real supplied form rather than a fake:
:class:`~weaver.runtime.bookmark.BookmarkContext` is what production passes, and
what an object does with it is what is under test.

The round trip — a clean load advancing the row, and a later load reading it
back — needs a catalogue, so it is proved where there is one.
"""

from __future__ import annotations

from datetime import datetime, timezone

from weaver.declaration.metadata import ObjectId
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.runtime.bookmark import BookmarkContext

#: The item a test's objects belong to, unless it says otherwise.
ITEM = "Lakehouse/Sales"

#: A plausible instant for "this was loaded". Any value above the sentinel does.
LOADED_AT = datetime(2026, 8, 20, 6, 0, tzinfo=timezone.utc)


def never(*, item: str = ITEM) -> BookmarkContext:
    """A context in which nothing has ever had a clean load."""

    return BookmarkContext(item=WeaverItemId.parse(item), bookmarks={})


def loaded(
    *objects: str,
    at: datetime = LOADED_AT,
    item: str = ITEM,
    files: bool = False,
) -> BookmarkContext:
    """A context in which each ``Schema.Object`` was cleanly loaded at ``at``.

    ``files`` says the objects are Folders, whose catalogue identity carries the
    ``Files/`` prefix — a Folder and a Table of the same name are two objects.
    """

    owner = WeaverItemId.parse(item)
    return BookmarkContext(
        item=owner,
        bookmarks={
            WeaverDocumentId(owner, ObjectId(*name.split(".", 1)), is_files=files): at
            for name in objects
        },
    )


__all__ = ["ITEM", "LOADED_AT", "loaded", "never"]
