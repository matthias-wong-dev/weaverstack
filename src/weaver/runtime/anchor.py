"""Anchoring an authored object to the Weaver catalogue.

An object is freestanding or catalogue-anchored::

    My__Table(spark)                               # freestanding
    My__Table(spark, catalogue="Warehouse/Weaver")  # anchored

Anchoring resolves two things once, at construction: the catalogue itself, and
which installed object this is. Both are needed before a load begins — a
``Static`` gate reads a bookmark and an authored ``read()`` may too — and neither
is worth resolving twice.

An orchestrated run has already read the catalogue and knows the identity, so it
anchors the object itself through ``with_catalogue`` and this module is not
involved.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from ..errors import ConfigError

#: What the catalogue is read into when an object anchors itself. Enough to say
#: which installed object this is, and how far it has been loaded.
ANCHOR_TABLES = ("Installation", "Registry", "Bookmark")


def anchored(object: Any, catalogue: str) -> tuple[Any, Any]:
    """The catalogue this object is anchored to, and its identity in it.

    Both resolved now rather than on first use, so a name the catalogue does not
    record fails where the object was constructed instead of part-way through a
    load.
    """

    from ..catalogue.state import catalogue_for
    from ..catalogue.tables import CATALOGUE_TABLES

    wanted = tuple(table for table in CATALOGUE_TABLES if table.name in ANCHOR_TABLES)
    with _session_for(catalogue) as (session, workspace):
        read = catalogue_for(session, workspace, tables=wanted)
        return read, resolved_identity(object, read)


def resolved_identity(object: Any, catalogue: Any):
    """Which installed object this is, according to ``catalogue``."""

    schema, name = object.identity
    return catalogue.installed_object(
        target_name=object.lakehouse.name,
        schema=schema,
        object=name,
        is_files=object._is_files,
    )


@contextmanager
def _session_for(catalogue: str):
    """A Session for the workspace this process is running in.

    Anchoring by name happens where authored code runs, which is a Fabric
    session: the workspace is the one this process is in, and the catalogue is
    the item the caller named in it. A process that is not in a Fabric session
    has no workspace to name and says so rather than reaching for a default.
    """

    from ..sessions.host import current_workspace_name, use_or_create_session
    from ..workspaces import Workspace

    name = current_workspace_name()
    if not name:
        raise ConfigError(
            f"catalogue={catalogue!r} names a Warehouse in the Fabric workspace "
            "this process is running in, and this process is not in one. Run the "
            "load through `weaver load`, which reaches the workspace from outside."
        )
    workspace = Workspace(workspace=name, catalogue=catalogue)
    with use_or_create_session(None, workspace=workspace) as session:
        yield session, workspace


__all__ = ["ANCHOR_TABLES", "anchored", "resolved_identity"]
