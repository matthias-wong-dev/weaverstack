"""Workspace resolution — names to locations.

Fabric is the only workspace, and resolution is arithmetic: nothing here touches
storage, so every location can be inspected before any mutation. Mutation is a
:class:`~weaver.store.Store` concern.

This is the only place that knows how a name becomes a location. Everything
downstream receives resolved locations and never derives them.
"""

from __future__ import annotations

#: The Lakehouse area holding Delta tables. Never written by a user — a Delta
#: target names a Lakehouse and the area follows from the object kind.
TABLES_AREA = "Tables"


# --- choosing an implementation for a workspace -----------------------------------


def resolver_for(workspace):
    """The resolver for a workspace in the current host.

    Inside Fabric, resolution goes through NotebookUtils. From a desktop it is
    the REST-backed resolver, and that cross-boundary caller supplies its DFS
    store explicitly.
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
    """The **within-workspace** default store, which needs a Fabric session.

    ``FabricStore`` goes through NotebookUtils and therefore works only inside
    Fabric. A desktop caller crossing in constructs ``OneLakeDfsClient`` and
    injects it explicitly, so DFS is never mistaken for the default.
    """

    from .fabric.store import FabricStore

    return FabricStore()
