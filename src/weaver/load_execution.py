"""Running a resolved load plan, one node at a time, in topological order.

Sequential by design for this phase, and the design does not prevent parallelism
later: the executor asks the graph what is ready and runs it, so the only thing
that would change is how many it runs at once. No concurrency abstraction is
introduced now, because an unused one is a guess about the shape of a problem
nobody has met yet.

**The dispatcher dispatches.** Merge semantics, identity handling, rejection
policy, folder replacement and source comparison all live in the primitives, each
of which is runnable on its own and tested on its own. What happens here is the
translation between one resolved node and one normalised result — and the two
places that translation can go wrong are the two this module is about: reading a
transport's answer, and surviving a transport that does not answer at all.

**Two levels of fault tolerance, and they are not the same thing.** The requested
value is passed *into* the primitive, which is what governs its own rejection
behaviour. Around that, the whole dispatch boundary is wrapped, because the
failures orchestration must survive are the ones no primitive normalises: a
module that will not import, a Warehouse that will not connect, a result that is
not a result. Both feed the same message stream.

**Failure propagation is by graph, never by position.** A node whose upstream
failed is ``blocked`` whatever ``fault_tolerant`` says — tolerance decides
whether *independent* branches continue, and never whether a node may run on a
dependency that did not. Nothing here can execute a node whose upstream failed,
because the check is against the recorded status of its upstream nodes rather
than against a flag.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Mapping

from .errors import LoadError, WeaverError
from .etl import LOAD_ROOT, load_procedure_name
from .load_plan import (
    ENDPOINT_REFRESH,
    PYTHON_FOLDER,
    PYTHON_TABLE,
    SPARK_SQL_FILE,
    WAREHOUSE_PROCEDURE,
)
from .load_report import (
    BLOCKED,
    DEPENDENCY_BLOCKED,
    DISPATCH_EXCEPTION,
    ENDPOINT_REFRESH_FAILURE,
    FAILED,
    MODULE_IMPORT_FAILURE,
    PENDING,
    PRIMITIVE_FAILURE,
    PRIMITIVE_REJECTS,
    RESULT_CONTRACT_INVALID,
    SKIPPED,
    SUCCEEDED,
    SUCCEEDED_WITH_REJECTS,
    LoadMessage,
    LoadNodeReport,
    LoadResult,
    error,
    info,
    warning,
)
from .load_resolution import LoadEnvironment, ResolvedLoadNode, ResolvedLoadPlan
from .locations import Location
from .targets import ItemRef

#: Statuses an upstream node may have and still let its dependants run. Rejects
#: are reported failure of the *rows*, not of the step: the valid work completed
#: and what it wrote is there to be read.
_CLEARS_DOWNSTREAM = frozenset({SUCCEEDED, SUCCEEDED_WITH_REJECTS, SKIPPED})

#: Statuses that propagate. Deliberately narrower than "not cleared": a node that
#: never started because fail-fast stopped the run has not *failed*, so its
#: dependants are unstarted too. Marking them blocked would report a failure that
#: nothing observed and hide which node actually broke.
_PROPAGATES = frozenset({FAILED, BLOCKED})


def execute_load_plan(
    plan: ResolvedLoadPlan,
    *,
    fault_tolerant: bool = False,
    environment: LoadEnvironment,
    dispatch: Callable[..., LoadResult] | None = None,
    on_step: Callable[[LoadNodeReport], None] | None = None,
) -> tuple[LoadNodeReport, ...]:
    """Execute a resolved plan sequentially and report every planned node.

    ``dispatch`` is injectable so orchestration can be proven over prepared nodes
    and fake results — these are orchestration claims, not primitive ones, and a
    test that had to stand up four engines to assert an ordering would be
    asserting the engines.

    ``on_step`` receives each executed step's report as it completes, which is
    how task evidence reaches storage without this module knowing what a file is.
    """

    dispatch = dispatch or dispatch_load_node
    statuses: dict[str, str] = {node.node_id: PENDING for node in plan.nodes}
    reports: dict[str, LoadNodeReport] = {}
    stopped = False

    for resolved in plan.order:
        node = resolved.node
        blocking = _blocking(plan, node.node_id, statuses)
        if blocking:
            reports[node.node_id] = _blocked_report(resolved, blocking)
            statuses[node.node_id] = BLOCKED
            continue
        if stopped:
            # Fail-fast: nothing new is scheduled. This node's own dependencies
            # were fine, so it is not blocked — it simply never started, and
            # saying so is more useful than inventing a failure for it.
            reports[node.node_id] = _pending_report(resolved)
            continue
        if not resolved.valid:
            reports[node.node_id] = _invalid_report(resolved)
            statuses[node.node_id] = FAILED
            if not fault_tolerant:
                stopped = True
            continue
        if resolved.unsupported:
            reports[node.node_id] = _skipped_report(resolved)
            statuses[node.node_id] = SKIPPED
            if on_step is not None:
                on_step(reports[node.node_id])
            continue

        started = _now()
        result, messages = _guarded(
            dispatch,
            resolved,
            fault_tolerant=fault_tolerant,
            environment=environment,
        )
        status = _status_for(result)
        report = LoadNodeReport(
            node_id=node.node_id,
            logical_id=str(node.logical_id) if node.logical_id else None,
            physical_target=str(node.physical_target),
            primitive_kind=node.primitive_kind,
            dispatch_location=resolved.dispatch_location,
            status=status,
            executed=True,
            messages=messages,
            result=result,
            started_at=started,
            finished_at=_now(),
        )
        reports[node.node_id] = report
        statuses[node.node_id] = status
        if on_step is not None:
            on_step(report)
        if status == FAILED and not fault_tolerant:
            stopped = True

    return tuple(reports[node.node.node_id] for node in plan.order)


def _blocking(plan: ResolvedLoadPlan, node_id: str, statuses) -> tuple[str, ...]:
    return tuple(
        sorted(
            upstream
            for upstream in plan.dag.upstream(node_id)
            if statuses.get(upstream) in _PROPAGATES
        )
    )


def _status_for(result: LoadResult) -> str:
    if result.succeeded:
        return SUCCEEDED
    # A primitive that refused rows and was asked to tolerate them wrote the
    # valid ones and reported the refusal. That is not a failed step; a step that
    # failed without refusing anything is.
    return SUCCEEDED_WITH_REJECTS if result.rows_rejected else FAILED


def _guarded(
    dispatch, resolved: ResolvedLoadNode, *, fault_tolerant: bool, environment
) -> tuple[LoadResult, tuple[LoadMessage, ...]]:
    """Dispatch one node, converting anything it throws into a failed result.

    The orchestrator's own fault tolerance, and it is unconditional: an
    unexpected exception becomes data whatever ``fault_tolerant`` says, because
    the run has to record what happened before it decides what to do about it.
    """

    node = resolved.node
    try:
        result = dispatch(
            resolved, fault_tolerant=fault_tolerant, environment=environment
        )
    except LoadError as exc:
        carried = getattr(exc, "result", None)
        result = (
            carried
            if isinstance(carried, LoadResult)
            else LoadResult.failure(str(exc))
        )
        return result, (
            error(
                _failure_code(node.primitive_kind),
                f"{node.node_id} failed: {exc}",
                source=node.primitive_kind,
            ),
        )
    except Exception as exc:  # noqa: BLE001 - the boundary exists to catch these
        return LoadResult.failure(f"{type(exc).__name__}: {exc}"), (
            error(
                DISPATCH_EXCEPTION,
                f"{node.node_id} raised {type(exc).__name__}: {exc}",
                source="load_execution",
            ),
        )
    if not isinstance(result, LoadResult):
        return LoadResult.failure(
            f"the primitive returned {type(result).__name__}, not a load result"
        ), (
            error(
                RESULT_CONTRACT_INVALID,
                f"{node.node_id} returned {type(result).__name__} rather than a "
                "load result",
                source=node.primitive_kind,
            ),
        )
    return result, _result_messages(resolved, result)


def _failure_code(primitive_kind: str) -> str:
    return (
        ENDPOINT_REFRESH_FAILURE
        if primitive_kind == ENDPOINT_REFRESH
        else PRIMITIVE_FAILURE
    )


def _result_messages(
    resolved: ResolvedLoadNode, result: LoadResult
) -> tuple[LoadMessage, ...]:
    node = resolved.node
    if result.succeeded:
        return ()
    if result.rows_rejected:
        return (
            warning(
                PRIMITIVE_REJECTS,
                f"{node.node_id} rejected {result.rows_rejected} row(s): "
                f"{result.error_message}",
                source=node.primitive_kind,
            ),
        )
    return (
        error(
            _failure_code(node.primitive_kind),
            f"{node.node_id} reported failure: {result.error_message}",
            source=node.primitive_kind,
        ),
    )


def _blocked_report(resolved: ResolvedLoadNode, blocking) -> LoadNodeReport:
    return _report(
        resolved,
        BLOCKED,
        (
            error(
                DEPENDENCY_BLOCKED,
                f"{resolved.node_id} did not run: "
                + ", ".join(blocking)
                + " did not complete successfully",
                source="load_execution",
            ),
        ),
    )


def _invalid_report(resolved: ResolvedLoadNode) -> LoadNodeReport:
    return _report(resolved, FAILED, resolved.validation_messages)


def _skipped_report(resolved: ResolvedLoadNode) -> LoadNodeReport:
    return _report(resolved, SKIPPED, resolved.validation_messages)


def _pending_report(resolved: ResolvedLoadNode) -> LoadNodeReport:
    return _report(
        resolved,
        PENDING,
        (
            info(
                DEPENDENCY_BLOCKED,
                f"{resolved.node_id} was not scheduled: an earlier node failed "
                "and this run is not fault tolerant",
                source="load_execution",
            ),
        ),
    )


def _report(resolved: ResolvedLoadNode, status: str, messages) -> LoadNodeReport:
    node = resolved.node
    return LoadNodeReport(
        node_id=node.node_id,
        logical_id=str(node.logical_id) if node.logical_id else None,
        physical_target=str(node.physical_target),
        primitive_kind=node.primitive_kind,
        dispatch_location=resolved.dispatch_location,
        status=status,
        executed=False,
        messages=tuple(messages),
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# --- the dispatch boundary ----------------------------------------------------


def dispatch_load_node(
    resolved: ResolvedLoadNode,
    *,
    fault_tolerant: bool = False,
    environment: LoadEnvironment,
) -> LoadResult:
    """Run one resolved node's installed primitive and return what it reported."""

    kind = resolved.node.primitive_kind
    if kind == WAREHOUSE_PROCEDURE:
        return _dispatch_warehouse_procedure(resolved, fault_tolerant, environment)
    if kind == SPARK_SQL_FILE:
        return _dispatch_spark_sql_file(resolved, fault_tolerant, environment)
    if kind in (PYTHON_TABLE, PYTHON_FOLDER):
        return _dispatch_python(resolved, fault_tolerant, environment)
    if kind == ENDPOINT_REFRESH:
        return _dispatch_endpoint_refresh(resolved, environment)
    raise LoadError(f"{resolved.node_id} names unknown primitive kind {kind!r}")


def _dispatch_warehouse_procedure(
    resolved, fault_tolerant: bool, environment: LoadEnvironment
) -> LoadResult:
    target = resolved.node.physical_target
    sql = environment.sql_for(target)
    if sql is None:
        raise LoadError(
            f"{resolved.node_id} needs a SQL capability for {target}, and this "
            "run has none"
        )
    procedure = load_procedure_name(resolved.node.logical_id.object_id)
    rows = sql.query(
        f"exec {procedure} @fault_tolerant = {1 if fault_tolerant else 0}"
    )
    if not rows:
        raise LoadError(
            f"{procedure} returned no row — a load procedure projects its result"
        )
    return LoadResult.from_row(rows[0])


def _dispatch_spark_sql_file(
    resolved, fault_tolerant: bool, environment: LoadEnvironment
) -> LoadResult:
    from .runtime.spark_load import run_load_program

    if environment.store is None or environment.spark is None:
        raise LoadError(
            f"{resolved.node_id} needs a store and a Spark session, and this run "
            "has neither"
        )
    program = environment.store.read(Location(resolved.dispatch_location))
    return run_load_program(
        environment.spark,
        program.decode("utf-8"),
        fault_tolerant=fault_tolerant,
    )


def _dispatch_python(
    resolved, fault_tolerant: bool, environment: LoadEnvironment
) -> LoadResult:
    """Import the deployed module, construct its object, and load it.

    The destination is resolved *here* and handed in, never inferred: an authored
    object with no Lakehouse falls back to the session's attachment, which in an
    orchestrated run is the Weaver control plane. Orchestration runs detached
    from every destination it writes to, so it must always say which one it
    means.
    """

    from .lakehouse import lakehouse_for

    if environment.spark is None:
        raise LoadError(f"{resolved.node_id} needs a Spark session, and this run has none")
    lakehouse = lakehouse_for(environment.resolver, ItemRef(resolved.node.physical_target.name))
    runtime_root = _join(lakehouse.files_root(), *LOAD_ROOT.split("/"))
    relative = (
        f"{resolved.node.primitive_object.schema}/"
        f"{resolved.node.primitive_object.object}"
    )
    within = relative[len(LOAD_ROOT) + 1 :] if relative.startswith(LOAD_ROOT) else relative
    module = _import_deployed(
        runtime_root, within, expected=resolved.expected_class, node_id=resolved.node_id
    )
    cls = getattr(module, resolved.expected_class)
    return cls(environment.spark, lakehouse=lakehouse).load(
        fault_tolerant=fault_tolerant
    )


def _import_deployed(runtime_root: str, relative: str, *, expected: str, node_id: str):
    """One deployed module, imported from the runtime tree it was deployed into.

    Loaded from its exact file, so the module a node dispatches is unambiguously
    the one at the location the node resolved — but *named* by its position in
    the tree, ``Files.Sales__Seed`` rather than ``Sales__Seed``, because that is
    what other deployed modules import it as. Naming it otherwise would leave two
    module objects for one file, one of them the object nobody imports.

    The tree's root goes on ``sys.path`` first, so a module's own imports resolve
    exactly as they did when it was authored: ``from lib.dates import parse``
    finds ``lib`` where it was written, and ``from Files.Sales__Seed import …``
    finds the folder module through the ordinary machinery.
    """

    import importlib.util
    import sys

    path = _join(runtime_root, *relative.split("/"))
    # Ahead of whatever is already there: a process may hold more than one
    # estate's runtime tree, and the one being dispatched is the one that wins.
    if runtime_root in sys.path:
        sys.path.remove(runtime_root)
    sys.path.insert(0, runtime_root)
    importlib.invalidate_caches()
    name = relative[: -len(".py")].replace("/", ".")
    specification = importlib.util.spec_from_file_location(name, path)
    if specification is None or specification.loader is None:
        raise LoadError(f"{node_id}: no deployed module at {path}")
    module = importlib.util.module_from_spec(specification)
    # Registered before execution so a module that imports itself by name — and a
    # dataclass or pickle that later looks it up — finds the one being run.
    sys.modules[name] = module
    try:
        specification.loader.exec_module(module)
    except FileNotFoundError as exc:
        raise LoadError(f"{node_id}: no deployed module at {path}") from exc
    except Exception as exc:  # noqa: BLE001 - authored code, any failure is data
        raise LoadError(
            f"{node_id}: importing {path} raised {type(exc).__name__}: {exc}"
        ) from exc
    if not hasattr(module, expected):
        raise LoadError(
            f"{node_id}: {path} defines no class {expected!r} — a deployed object "
            "module names its class for its file"
        )
    return module


def _dispatch_endpoint_refresh(resolved, environment: LoadEnvironment) -> LoadResult:
    """Refresh one Lakehouse's SQL analytics endpoint. No rows, so no counts."""

    refresh = getattr(environment.resolver, "refresh_sql_endpoint", None)
    if refresh is None:
        raise LoadError(
            f"{resolved.node_id}: this environment cannot refresh a SQL endpoint"
        )
    refresh(ItemRef(resolved.node.physical_target.name))
    return LoadResult(succeeded=True)


def _join(root: str, *parts: str) -> str:
    return "/".join([root.rstrip("/"), *parts])


__all__ = [
    "dispatch_load_node",
    "execute_load_plan",
]
