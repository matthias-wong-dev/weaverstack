"""How authored Python code reaches its own bookmark.

``self.bookmark`` is the UTC instant immediately before this object's most
recent clean load began, and an incremental read asks its source for changes
after it::

    def read(self):
        return Source__Thing(self).changes_since(self.bookmark)

There are two ways an object comes to have one, and they are the two positions a
load runs from.

An orchestrated run reads the whole table once and hands the map down with the
logical item its objects belong to, so ``read()`` costs no Warehouse query
however many objects the run touches.

A standalone load is given a catalogue instead — ``My__Table(spark,
catalogue="Warehouse/Weaver")`` — and reads what it needs on first use: which
installed object this is, and how far it has been loaded.

An object with neither has no way to know, and says so rather than guessing. The
sentinel is what an object that has never had a clean load carries, not what an
object whose bookmark could not be read carries; substituting it for a failed
lookup would silently turn an unreachable catalogue into a full reload.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Mapping

from ..errors import LoadError

#: What an object says when its ``read()`` used a bookmark and nothing supplied
#: one. Deliberately short: it names the one thing the caller has to add.
NO_CATALOGUE_MESSAGE = "Catalogue must be supplied if read uses a bookmark."


@dataclass(frozen=True)
class BookmarkContext:
    """What an object needs to answer ``self.bookmark``.

    ``item`` and ``bookmarks`` come from a run: the logical item whose objects
    these are, and every bookmark the catalogue holds. ``catalogue`` is the
    target a standalone load was given, and is what makes the same two
    discoverable.

    Passed down unchanged when one object constructs another, because a child is
    in the same item and reads from the same catalogue. What the child does *not*
    inherit is a bookmark value: it resolves its own, by its own identity.
    """

    catalogue: str | None = None
    item: Any = None
    bookmarks: Mapping | None = None

    @property
    def supplied(self) -> bool:
        """Whether a run handed the bookmarks down rather than a catalogue."""

        return self.bookmarks is not None and self.item is not None


def sentinel() -> datetime:
    """The bookmark of an object no clean load has run for."""

    from ..catalogue.tables import BOOKMARK_SENTINEL

    return BOOKMARK_SENTINEL


def bookmark_of(
    context: BookmarkContext | None,
    *,
    lakehouse: str,
    schema: str,
    object: str,
    is_files: bool,
) -> datetime:
    """How far this object has been loaded, from whichever context it has."""

    if context is None:
        raise LoadError(NO_CATALOGUE_MESSAGE)
    if context.supplied:
        return _from_map(context, schema=schema, object=object, is_files=is_files)
    if not context.catalogue:
        raise LoadError(NO_CATALOGUE_MESSAGE)
    with catalogue_scope(context.catalogue) as scope:
        identity = scope.identity(
            lakehouse=lakehouse, schema=schema, object=object, is_files=is_files
        )
        return scope.bookmark(identity)


def advance(
    context: BookmarkContext | None,
    *,
    lakehouse: str,
    schema: str,
    object: str,
    is_files: bool,
    at: datetime,
) -> None:
    """Record a clean standalone load, so the next one reads from here.

    Only for a load nothing is orchestrating. In a run the bookmark is advanced
    once, beside the record of the run that advanced it, so an object asked to do
    it as well would be a second decision about the same row.
    """

    if context is None or context.supplied or not context.catalogue:
        return
    with catalogue_scope(context.catalogue) as scope:
        identity = scope.identity(
            lakehouse=lakehouse, schema=schema, object=object, is_files=is_files
        )
        scope.write(identity, at)


def _from_map(
    context: BookmarkContext, *, schema: str, object: str, is_files: bool
) -> datetime:
    """This object's bookmark from the map a run supplied.

    Absent means absent: the run read the whole table, so an object with no row
    has never had a clean load.
    """

    from ..declaration.metadata import ObjectId
    from ..declaration.model import WeaverDocumentId

    identity = WeaverDocumentId(
        context.item, ObjectId(schema, object), is_files=is_files
    )
    return _aware(context.bookmarks.get(identity)) or sentinel()


def _aware(at: datetime | None) -> datetime | None:
    if at is None:
        return None
    return at if at.tzinfo is not None else at.replace(tzinfo=timezone.utc)


class _CatalogueScope:
    """One open connection to the catalogue, for a standalone load's questions."""

    def __init__(self, connection) -> None:
        self._connection = connection
        self._bookmarks: Mapping | None = None

    def identity(self, *, lakehouse: str, schema: str, object: str, is_files: bool):
        from ..catalogue.state import resolve_installed_object

        return resolve_installed_object(
            self._connection,
            target_name=lakehouse,
            schema=schema,
            object=object,
            is_files=is_files,
        )

    def bookmark(self, identity) -> datetime:
        from ..catalogue.state import read_installed_bookmarks

        if self._bookmarks is None:
            self._bookmarks = read_installed_bookmarks(self._connection)
        return _aware(self._bookmarks.get(identity)) or sentinel()

    def write(self, identity, at: datetime) -> None:
        from ..catalogue.claims import bookmark_row
        from ..catalogue.render import render_keyed_merge
        from ..catalogue.tables import BOOKMARK

        statement = render_keyed_merge(BOOKMARK, [bookmark_row(identity, _aware(at))])
        if statement is not None:
            self._connection.execute(statement)


@contextmanager
def catalogue_scope(catalogue: str):
    """A connection to the named catalogue Warehouse, for as long as it is needed.

    In a Fabric notebook, which is where a standalone load runs: the workspace is
    the one this process is in, and the Session is the one this host offers. A
    process that is not in a Fabric session has no workspace to name, and says so
    rather than reaching for a default.
    """

    from ..catalogue.connection import catalogue_connection
    from ..sessions.host import current_workspace_name, use_or_create_session
    from ..workspaces import Workspace

    name = current_workspace_name()
    if not name:
        raise LoadError(
            f"catalogue={catalogue!r} names a Warehouse in the Fabric workspace "
            "this process is running in, and this process is not in one. Run the "
            "load through `weaver load`, which reaches the workspace from outside."
        )
    workspace = Workspace(workspace=name, catalogue=catalogue)
    with use_or_create_session(None, workspace=workspace) as session:
        yield _CatalogueScope(catalogue_connection(session, workspace))


def with_catalogue(
    context: BookmarkContext | None, catalogue: str | None
) -> BookmarkContext | None:
    """The context a caller's ``catalogue=`` argument implies."""

    if catalogue is None:
        return context
    if context is None:
        return BookmarkContext(catalogue=catalogue)
    return replace(context, catalogue=catalogue)


__all__ = [
    "NO_CATALOGUE_MESSAGE",
    "BookmarkContext",
    "advance",
    "bookmark_of",
    "catalogue_scope",
    "sentinel",
    "with_catalogue",
]
