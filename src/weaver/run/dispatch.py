"""Dispatch an installed runtime primitive through Session capabilities.

Runner controls scheduling and Session controls engine access. This module maps
the installed primitive reference to the appropriate Session operation.
"""

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
    open_runtime=None,
    workspace=None,
    collect=False,
    publication=None,
):
    """Run one installed primitive and return what it reported.

    ``open_runtime`` is called only by branches that import a deployed module.
    Warehouse-only runs therefore do not open a Spark runtime.
    """

    if session is None:
        raise RunError(
            f"{node.node_id} needs a Session to reach {node.physical_target}; "
            "a run with no Session must be given a dispatch of its own"
        )

    kind = node.primitive_kind
    if getattr(node, "installed", None) is not None:
        # A validation reads the estate and reports a judgement about it. The
        # Runner treats it as any other node; only the engine differs.
        return _validation(node, session, workspace, open_runtime, collect)
    if kind == WAREHOUSE_PROCEDURE:
        return _warehouse_procedure(
            node, session, workspace, fault_tolerant, publication
        )
    if kind in (PYTHON_TABLE, PYTHON_FOLDER):
        return _python(node, session, workspace, resolved, fault_tolerant, open_runtime)
    if kind == ENDPOINT_REFRESH:
        return _endpoint_refresh(node, session, workspace)
    if kind == ONELAKE_PUBLICATION:
        return _onelake_publication(node, session, workspace, publication)
    raise RunError(f"{node.node_id} names unknown primitive kind {kind!r}")


def _validation(node, session, workspace, open_runtime, collect: bool):
    """One installed Test or Assumption, run where it is installed.

    Delegated rather than reimplemented: what a validation means, comparing
    two sides, counting violations, deciding what a discrepancy is, belongs to
    the validation runtime and is proven against real engines. What belongs here
    is the same thing that belongs here for a load: reaching the engine through
    the Session that owns it.
    """

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
    """This run's scope, opened now because something is about to import."""

    if open_runtime is None:
        raise RunError(f"{node.node_id} needs a runtime scope, and this run has none")
    return open_runtime.get()


def _warehouse_procedure(node, session, workspace, fault_tolerant: bool, publication):
    """The installed load procedure, called by name over the Session's TDS.

    The object's own procedure, not the generic ``_.Load`` wrapper: the run
    records what settled itself, so one row has one writer.

    Asked for by name rather than by result set: the procedure's authored setup
    may run EXEC and return rows of its own, so "the result set it produced" is
    not something a caller can identify. The outputs are, and they are in its
    signature.
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
    row = sql.call_procedure(
        load_procedure_name(node.logical_id.object_id),
        inputs=(("fault_tolerant", 1 if fault_tolerant else 0),),
        outputs=PROCEDURE_RESULT_PARAMETERS,
    )
    result = LoadResult.from_row(logical_result_row(row))
    if publication is not None:
        publication.settled(node.node_id, result)
    return result


def _onelake_publication(node, session, workspace, publication):
    """Wait for this Warehouse load's publication to reach its consumers.

    The Warehouse-side counterpart of the SQL endpoint refresh, and a node for the
    same reasons: its own progress line, its own timing, and a failure that blames
    the boundary rather than the load that already committed.
    """

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


def _python(node, session, workspace, resolved, fault_tolerant: bool, open_runtime):
    """One deployed Python primitive, run where its module can be imported.

    A deployed module is imported inside the session that owns the Spark it will
    use, so on a desktop this crosses. What crosses is the handful of strings
    the import and the construction need, not a serialisation of the Runner's
    node, which carries typed identities the far side has no use for and would
    then have to be kept in step with.

    Which of the two happens is the scope's to answer, not this function's: a
    remote scope dispatches into the interpreter holding it.
    """

    expected = getattr(resolved, "expected_class", None)
    if expected is None:
        raise RunError(
            f"{node.node_id} names a deployed module whose expected class is unknown"
        )

    from ..runtime.load_result import LoadResult

    # The scope answers with the row the primitive reported, in either position.
    # What that row means is settled here, which is the one module a load's
    # vocabulary belongs in.
    return LoadResult.from_row(
        _scope(open_runtime, node).dispatch_python(
            node, expected_class=expected, fault_tolerant=fault_tolerant
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
):
    """Import the deployed module, construct its object, and load it.

    Host-neutral: what differs between a notebook and a Livy interpreter is
    answered by the Session it is given, so both sides of a decomposed run call
    this one implementation.

    The destination is resolved here and handed in, never inferred: an authored
    object with no Lakehouse falls back to the session's attachment, which in an
    orchestrated run decides.

    The import goes through a runtime context rather than ``sys.path``, because
    two Lakehouses may each deploy a ``lib/dates.py`` and ``sys.modules`` is
    consulted before any path is searched.
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
    return primitive._load(fault_tolerant=fault_tolerant)


def _endpoint_refresh(node, session, workspace):
    """Refresh one Lakehouse's SQL analytics endpoint. No rows, so no counts."""

    from ..runtime.load_result import LoadResult
    from ..targets import ItemRef

    resolver = session.resolver(workspace)
    refresh = getattr(resolver, "refresh_sql_endpoint", None)
    if refresh is None:
        raise RunError(f"{node.node_id}: this host cannot refresh a SQL endpoint")
    refresh(ItemRef(node.physical_target.name))
    return LoadResult(succeeded=True)


def _join(root: str, *parts: str) -> str:
    return "/".join([str(root).rstrip("/"), *parts])


def can_refresh(session, workspace=None) -> bool:
    """Whether this host has a SQL analytics endpoint to refresh at all.

    A Warehouse has none of its own, so the Runner is told and skips the node
    rather than failing it.
    """

    if session is None:
        return False
    return callable(getattr(session.resolver(workspace), "refresh_sql_endpoint", None))


__all__ = ["can_refresh", "dispatch_primitive", "python_primitive"]
