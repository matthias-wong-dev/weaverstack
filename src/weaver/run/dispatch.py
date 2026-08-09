"""dispatch_primitive — the one place a run crosses into a real engine.

.. code-block:: text

    RunNode
       ↓
    primitive kind / runtime reference
       ↓
    Session capability
       ↓
    the installed runtime artefact

A narrow function, deliberately, and not a fifth doer. Everything about *when* a
node runs belongs to the Runner; everything about *how* to reach an engine
belongs to the Session. What is left here is the translation between them, which
is small enough that giving it a lifecycle would be inventing one.

It is also the seam a run-cycle test replaces. Nothing here is aware of that: the
Runner calls whatever callable it was given, and a controlled outcome and a real
one arrive by the same route. Which is why a deliberately trivial fixture
artefact needs no special case — the Registry already points a node wherever the
estate says, and a trivial artefact is simply an installed one that does little.
"""

from __future__ import annotations

from ..errors import LoadError
from .resolution import (
    ENDPOINT_REFRESH,
    PYTHON_FOLDER,
    PYTHON_TABLE,
    WAREHOUSE_PROCEDURE,
)


def dispatch_primitive(
    node,
    *,
    session=None,
    state=None,
    resolved=None,
    fault_tolerant: bool = False,
    runtime_scope=None,
    workspace=None,
):
    """Run one installed primitive and return what it reported."""

    if session is None:
        raise LoadError(
            f"{node.node_id} needs a Session to reach {node.physical_target}; "
            "a run with no Session must be given a dispatch of its own"
        )

    kind = node.primitive_kind
    if kind == WAREHOUSE_PROCEDURE:
        return _warehouse_procedure(node, session, workspace, fault_tolerant)
    if kind in (PYTHON_TABLE, PYTHON_FOLDER):
        return _python(node, session, workspace, resolved, fault_tolerant, runtime_scope)
    if kind == ENDPOINT_REFRESH:
        return _endpoint_refresh(node, session, workspace)
    raise LoadError(f"{node.node_id} names unknown primitive kind {kind!r}")


def _warehouse_procedure(node, session, workspace, fault_tolerant: bool):
    """The installed load procedure, called by name over the Session's TDS.

    Asked for by name rather than by result set: the procedure's authored setup
    may run EXEC and return rows of its own, so "the result set it produced" is
    not something a caller can identify. The outputs are, and they are in its
    signature.
    """

    from ..declaration.tsql_load import RESULT_PARAMETERS
    from ..etl import load_procedure_name
    from ..runtime.load_result import LoadResult
    from ..targets import ItemRef, WarehouseTarget

    target = WarehouseTarget(ItemRef(node.physical_target.name))
    sql = session.sql_executor(target, workspace=workspace)
    row = sql.call_procedure(
        load_procedure_name(node.logical_id.object_id),
        inputs=(("fault_tolerant", 1 if fault_tolerant else 0),),
        outputs=RESULT_PARAMETERS,
    )
    return LoadResult.from_row(row)


def _python(node, session, workspace, resolved, fault_tolerant: bool, runtime_scope):
    """Import the deployed module, construct its object, and load it.

    The destination is resolved *here* and handed in, never inferred: an authored
    object with no Lakehouse falls back to the session's attachment, which in an
    orchestrated run is the Weaver control plane. Orchestration runs detached
    from every destination it writes to, so it must always say which one it
    means.

    The import goes through a runtime *context* rather than through ``sys.path``,
    because two Lakehouses may each deploy a ``lib/dates.py`` and ``sys.modules``
    is consulted before any path is searched — so the second estate would
    silently receive the first one's helper.
    """

    from ..etl import LOAD_ROOT
    from ..lakehouse import lakehouse_for
    from ..runtime.python_context import import_deployed_module

    if runtime_scope is None:
        raise LoadError(f"{node.node_id} needs a runtime scope, and this run has none")
    expected = getattr(resolved, "expected_class", None)
    if expected is None:
        raise LoadError(
            f"{node.node_id} names a deployed module whose expected class is unknown"
        )

    from ..targets import ItemRef

    resolver = session.resolver(workspace)
    lakehouse = lakehouse_for(resolver, ItemRef(node.physical_target.name))
    runtime_root = _join(lakehouse.files_root(), *LOAD_ROOT.split("/"))
    relative = f"{node.primitive_object.schema}/{node.primitive_object.object}"
    within = (
        relative[len(LOAD_ROOT) + 1 :] if relative.startswith(LOAD_ROOT) else relative
    )
    context = runtime_scope.context_for(
        # The logical item, not the object: everything one item deployed into one
        # target shares a tree, because that is what its author wrote against.
        logical_item=node.logical_id.item,
        physical_target=node.physical_target,
        runtime_root=runtime_root,
    )
    module = import_deployed_module(
        context, within, expected=expected, node_id=node.node_id
    )
    cls = getattr(module, expected)
    return cls(session.spark(workspace), lakehouse=lakehouse).load(
        fault_tolerant=fault_tolerant
    )


def _endpoint_refresh(node, session, workspace):
    """Refresh one Lakehouse's SQL analytics endpoint. No rows, so no counts."""

    from ..runtime.load_result import LoadResult
    from ..targets import ItemRef

    resolver = session.resolver(workspace)
    refresh = getattr(resolver, "refresh_sql_endpoint", None)
    if refresh is None:
        raise LoadError(f"{node.node_id}: this host cannot refresh a SQL endpoint")
    refresh(ItemRef(node.physical_target.name))
    return LoadResult(succeeded=True)


def _join(root: str, *parts: str) -> str:
    return "/".join([str(root).rstrip("/"), *parts])


def can_refresh(session, workspace=None) -> bool:
    """Whether this host has a SQL analytics endpoint to refresh at all.

    The emulator has none, which is an honest absence rather than a fault — the
    build's own executor skips for the same reason — so the Runner is told and
    skips the node rather than failing it.
    """

    if session is None:
        return False
    return callable(getattr(session.resolver(workspace), "refresh_sql_endpoint", None))


__all__ = ["can_refresh", "dispatch_primitive"]
