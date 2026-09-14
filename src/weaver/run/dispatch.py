"""Dispatch installed runtime primitives through a Session."""

from __future__ import annotations

from .resolution import (
    ENDPOINT_REFRESH,
    ONELAKE_PUBLICATION,
    PYTHON_FOLDER,
    PYTHON_TABLE,
    PYTHON_VALIDATION,
    WAREHOUSE_PROCEDURE,
)
from .result import RunError


def dispatch_primitive(
    node,
    *,
    session=None,
    state=None,
    resolved=None,
    fault_tolerant: bool = False,
    reload: bool = False,
    open_runtime=None,
    workspace=None,
    collect=False,
    publication=None,
):
    """Dispatch one installed primitive.

    The runtime scope opens only for deployed Python modules. ``reload`` is
    passed only to table loads; planning accepts it for no other work.
    """

    if session is None:
        raise RunError(
            f"Cannot run {node.node_id} against {node.physical_target} without a "
            "Session. Provide a Session or a custom dispatch."
        )

    kind = node.primitive_kind
    if getattr(node, "installed", None) is not None:
        return _validation(node, session, workspace, open_runtime, collect)
    if kind == WAREHOUSE_PROCEDURE:
        return _warehouse_procedure(
            node, session, workspace, fault_tolerant, publication, reload
        )
    if kind in (PYTHON_TABLE, PYTHON_FOLDER):
        return _python(
            node, session, workspace, resolved, fault_tolerant, open_runtime, reload
        )
    if kind == ENDPOINT_REFRESH:
        return _endpoint_refresh(node, session, workspace)
    if kind == ONELAKE_PUBLICATION:
        return _onelake_publication(node, session, workspace, publication)
    raise RunError(
        f"Cannot run {node.node_id}: deployed object type {kind!r} is unsupported"
    )


def _validation(node, session, workspace, open_runtime, collect: bool):
    from ..test_execution import primitive_kind, run_installed_validation

    installed = node.installed
    # Only a Lakehouse validation is a deployed module, so only it needs the
    # run's scope, and a Warehouse one must not cause it to be opened. A
    # Warehouse validation is a procedure, and TDS reaches it from here.
    if primitive_kind(installed) != PYTHON_VALIDATION:
        return run_installed_validation(
            installed,
            session=session,
            workspace=workspace,
            runtime_scope=None,
            collect_diagnostics=collect,
        )

    return _scope(open_runtime, node).dispatch_validation(installed, collect=collect)


def _scope(open_runtime, node):
    if open_runtime is None:
        raise RunError(
            f"Cannot run {node.node_id}: no runtime scope is available for its module"
        )
    return open_runtime.get()


def _warehouse_procedure(
    node, session, workspace, fault_tolerant: bool, publication, reload: bool = False
):
    """Call the object's procedure without the self-recording wrapper.

    Output parameters identify its result because authored setup may return
    unrelated result sets.
    """

    from ..declaration.tsql_load import (
        PROCEDURE_RESULT_PARAMETERS,
        logical_result_row,
    )
    from ..etl import load_procedure_name
    from ..runtime.load_result import LoadResult
    from ..targets import ItemRef, WarehouseTarget

    target = WarehouseTarget(ItemRef(node.physical_target.name))
    # Before the procedure, because a barrier behind it cannot see this.
    if publication is not None:
        publication.observe(node, session, workspace)
    sql = session.sql_executor(target, workspace=workspace)
    # `@reload` is named only when set: a procedure installed before reload
    # existed has no such parameter, and an ordinary load of it still runs.
    inputs = (("fault_tolerant", 1 if fault_tolerant else 0),)
    if reload:
        inputs = inputs + (("reload", 1),)
    row = sql.call_procedure(
        load_procedure_name(node.logical_id.object_id),
        inputs=inputs,
        outputs=PROCEDURE_RESULT_PARAMETERS,
    )
    result = LoadResult.from_row(logical_result_row(row))
    if publication is not None:
        publication.settled(node.node_id, result)
    return result


def _onelake_publication(node, session, workspace, publication):
    """Wait for this Warehouse load to become readable by its consumers."""

    from ..runtime.load_result import LoadResult
    from .publication import await_publication

    producer = node.produced_by
    if publication is None or producer is None:
        raise RunError(
            f"{node.node_id} waits on a publication and this run recorded none"
        )
    if not publication.moved(producer):
        # Nothing was written, so nothing is published and no Spark is needed.
        return LoadResult(succeeded=True)
    await_publication(
        node,
        session,
        workspace,
        before=publication.baseline(producer),
        readiness=tuple(node.publication_targets),
    )
    return LoadResult(succeeded=True)


def _python(
    node,
    session,
    workspace,
    resolved,
    fault_tolerant: bool,
    open_runtime,
    reload: bool = False,
):
    """Run a deployed Python primitive in the scope that imports its module."""

    expected = getattr(resolved, "expected_class", None)
    if expected is None:
        raise RunError(
            f"Cannot run {node.node_id}: its deployed module has no expected class. "
            "Rebuild and reinstall the project."
        )

    from ..runtime.load_result import LoadResult

    return LoadResult.from_row(
        _scope(open_runtime, node).dispatch_python(
            node,
            expected_class=expected,
            fault_tolerant=fault_tolerant,
            reload=reload,
        )
    )


def python_primitive(
    *,
    node_id: str,
    logical_item,
    physical_target,
    schema: str,
    object: str,
    expected_class: str,
    fault_tolerant: bool,
    runtime_scope,
    session,
    workspace=None,
    catalogue=None,
    node_identity=None,
    reload: bool = False,
):
    """Import and load a deployed Python primitive.

    Resolve the destination explicitly. The runtime context keeps identically
    named modules deployed by different Lakehouses isolated from ``sys.modules``.
    """

    from ..etl import LOAD_ROOT
    from ..lakehouse import lakehouse_for
    from ..runtime.python_context import import_deployed_module
    from ..targets import ItemRef

    resolver = session.resolver(workspace)
    lakehouse = lakehouse_for(resolver, ItemRef(physical_target.name))
    runtime_root = _join(lakehouse.files_root(), *LOAD_ROOT.split("/"))
    relative = f"{schema}/{object}"
    within = (
        relative[len(LOAD_ROOT) + 1 :] if relative.startswith(LOAD_ROOT) else relative
    )
    context = runtime_scope.context_for(
        # The logical item, not the object: everything one item deployed into one
        # target shares a tree, because that is what its author wrote against.
        logical_item=logical_item,
        physical_target=physical_target,
        runtime_root=runtime_root,
    )
    module = import_deployed_module(
        context, within, expected=expected_class, node_id=node_id
    )
    cls = getattr(module, expected_class)
    primitive = cls(session.spark(workspace), lakehouse=lakehouse)
    # The run's catalogue, and the identity the run already resolved. An object
    # this one constructs inherits the same catalogue and resolves its own
    # identity against it, so `Other__Thing(self)` needs no argument.
    #
    # Asked for rather than passed to the constructor, because `cls(spark,
    # lakehouse=...)` is the whole contract a deployed primitive has to meet. One
    # with nowhere to put a catalogue records nothing and is left alone.
    take = getattr(primitive, "with_catalogue", None)
    if take is not None and catalogue is not None:
        take(catalogue, identity=node_identity)
    # `_load` and never `load`: the run records what settled, centrally and
    # asynchronously, so a primitive that recorded itself would be a second
    # writer of the same row.
    #
    # `reload` is named only when set: a Folder's `_load` does not take it, and
    # planning has already refused a folder reload.
    policy = {"fault_tolerant": fault_tolerant}
    if reload:
        policy["reload"] = True
    return primitive._load(**policy)


def _endpoint_refresh(node, session, workspace):

    from ..runtime.load_result import LoadResult
    from ..targets import ItemRef

    resolver = session.resolver(workspace)
    refresh = getattr(resolver, "refresh_sql_endpoint", None)
    if refresh is None:
        raise RunError(
            f"Cannot refresh the SQL endpoint for {node.physical_target}: this "
            "Session does not support endpoint refresh"
        )
    refresh(ItemRef(node.physical_target.name))
    return LoadResult(succeeded=True)


def _join(root: str, *parts: str) -> str:
    return "/".join([str(root).rstrip("/"), *parts])


def can_refresh(session, workspace=None) -> bool:

    if session is None:
        return False
    return callable(getattr(session.resolver(workspace), "refresh_sql_endpoint", None))


__all__ = ["can_refresh", "dispatch_primitive", "python_primitive"]
