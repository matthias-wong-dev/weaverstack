"""Workspace resolution: names to locations.

Fabric is the only workspace, and resolution is arithmetic: nothing here touches
storage, so every location can be inspected before any mutation. Mutation is a
:class:`~weaver.store.Store` concern.

This is the only place that turns a name into a location. Everything
downstream receives resolved locations and never derives them.
"""

from __future__ import annotations

#: The Lakehouse area holding Delta tables. Never written by a user, a Delta
#: target names a Lakehouse and the area follows from the object kind.
TABLES_AREA = "Tables"


# --- choosing an implementation for a workspace -----------------------------------


def resolver_for(workspace):
    """Return the resolver for the current execution environment.

    Inside Fabric, resolution uses NotebookUtils. Desktop resolution uses the
    Fabric REST API.
    """

    try:
        from notebookutils import lakehouse, runtime
    except ImportError:
        pass
    else:
        from .fabric.session import FabricSessionResolver

        return FabricSessionResolver(workspace, lakehouse=lakehouse, runtime=runtime)

    from .fabric.resolution import FabricResolver

    return FabricResolver(workspace)


def store_for(workspace):
    """Return the in-Fabric store.

    ``FabricStore`` uses NotebookUtils. Desktop callers must inject an
    ``OneLakeDfsClient`` instead.
    """

    from .fabric.store import FabricStore

    return FabricStore()
