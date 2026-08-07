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

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from .errors import LoadError
from .etl import LOAD_ROOT, load_procedure_name
from .load_plan import (
    ENDPOINT_REFRESH,
    PYTHON_FOLDER,
    PYTHON_TABLE,
    WAREHOUSE_PROCEDURE,
)
from .load_report import (
    BLOCKED,
    DEPENDENCY_BLOCKED,
    DISPATCH_EXCEPTION,
    ENDPOINT_REFRESH_FAILURE,
    FAILED,
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

    ``on_step`` receives **every** planned node's report at the moment its final
    status is settled — executed, invalid, skipped, blocked or pending alike —
    which is how durable evidence reaches storage without this module knowing
    what a file is. One record per planned node, written once, in plan order:
    the alternative is a log that says which nodes ran and leaves a reader to
    infer what became of the rest, exactly when inference is least safe.

    **The plan is always classified in full**, even under fail-fast. Stopping
    the loop early would leave nodes with no recorded outcome at all, and
    "nothing was written for it" cannot be told apart from "the run died before
    reaching it". So fail-fast stops *scheduling*, not reporting.
    """

    dispatch = dispatch or dispatch_load_node
    statuses: dict[str, str] = {node.node_id: PENDING for node in plan.nodes}
    reports: dict[str, LoadNodeReport] = {}
    stopped = False

    def record(report: LoadNodeReport, status: str | None = None) -> None:
        reports[report.node_id] = report
        if status is not None:
            statuses[report.node_id] = status
        if on_step is not None:
            on_step(report)

    for resolved in plan.order:
        node = resolved.node
        blocking = _blocking(plan, node.node_id, statuses)
        if blocking:
            record(_blocked_report(resolved, blocking), BLOCKED)
            continue
        if stopped:
            # Fail-fast: nothing new is scheduled. This node's own dependencies
            # were fine, so it is not blocked — it simply never started, and
            # saying so is more useful than inventing a failure for it.
            record(_pending_report(resolved))
            continue
        if not resolved.valid:
            record(_invalid_report(resolved), FAILED)
            if not fault_tolerant:
                stopped = True
            continue
        if resolved.unsupported:
            record(_skipped_report(resolved), SKIPPED)
            continue

        started = _now()
        outcome = _guarded(
            dispatch,
            resolved,
            fault_tolerant=fault_tolerant,
            environment=environment,
        )
        status = _status_for(outcome)
        record(
            LoadNodeReport(
                node_id=node.node_id,
                logical_id=str(node.logical_id) if node.logical_id else None,
                physical_target=str(node.physical_target),
                primitive_kind=node.primitive_kind,
                dispatch_location=resolved.dispatch_location,
                status=status,
                executed=True,
                messages=outcome.messages,
                result=outcome.result,
                started_at=started,
                finished_at=_now(),
            ),
            status,
        )
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


@dataclass(frozen=True)
class DispatchOutcome:
    """What one dispatch produced, and *how* it produced it.

    The second part is the whole reason this exists. A primitive that refuses
    rows raises when it was told not to tolerate them, and the exception carries
    a result whose counts include the rejections — so a reader looking only at
    the result cannot tell a refusal from a tolerated load. Both have
    ``succeeded=False`` and ``rows_rejected > 0``, and they mean opposite things:
    one wrote the valid rows, the other wrote nothing.

    Keeping the exception is what lets :func:`_status_for` answer correctly
    without inferring anything from the counts.
    """

    result: LoadResult
    messages: tuple[LoadMessage, ...] = ()
    exception: Exception | None = None

    @property
    def raised(self) -> bool:
        return self.exception is not None


def _status_for(outcome: DispatchOutcome) -> str:
    # A dispatch that raised is a failed node whatever it was carrying. The
    # target was not modified, so calling it "succeeded with rejects" would
    # report rows that were never written.
    if outcome.raised:
        return FAILED
    if outcome.result.succeeded:
        return SUCCEEDED
    # A primitive that refused rows and was asked to tolerate them wrote the
    # valid ones and *returned* the refusal. That is not a failed step; a step
    # that failed without refusing anything is.
    return SUCCEEDED_WITH_REJECTS if outcome.result.rows_rejected else FAILED


def _guarded(
    dispatch, resolved: ResolvedLoadNode, *, fault_tolerant: bool, environment
) -> DispatchOutcome:
    """Dispatch one node, converting anything it throws into an outcome.

    The orchestrator's own fault tolerance, and it is unconditional: an
    unexpected exception becomes data whatever ``fault_tolerant`` says, because
    the run has to record what happened before it decides what to do about it.
    Deciding is :func:`weaver.load.run_load`'s, once the whole plan is recorded.
    """

    node = resolved.node
    try:
        result = dispatch(
            resolved, fault_tolerant=fault_tolerant, environment=environment
        )
    except LoadError as exc:
        carried = getattr(exc, "result", None)
        return DispatchOutcome(
            result=(
                carried
                if isinstance(carried, LoadResult)
                else LoadResult.failure(str(exc))
            ),
            messages=(
                error(
                    _failure_code(node.primitive_kind),
                    f"{node.node_id} failed: {exc}",
                    source=node.primitive_kind,
                ),
            ),
            exception=exc,
        )
    except Exception as exc:  # noqa: BLE001 - the boundary exists to catch these
        return DispatchOutcome(
            result=LoadResult.failure(f"{type(exc).__name__}: {exc}"),
            messages=(
                error(
                    DISPATCH_EXCEPTION,
                    f"{node.node_id} raised {type(exc).__name__}: {exc}",
                    source="load_execution",
                ),
            ),
            exception=exc,
        )
    if not isinstance(result, LoadResult):
        invalid = LoadResult.failure(
            f"the primitive returned {type(result).__name__}, not a load result"
        )
        return DispatchOutcome(
            result=invalid,
            messages=(
                error(
                    RESULT_CONTRACT_INVALID,
                    f"{node.node_id} returned {type(result).__name__} rather than "
                    "a load result",
                    source=node.primitive_kind,
                ),
            ),
            # Not an exception, but not a dispatch either: nothing ran to
            # completion, so it must never read as a tolerated rejection.
            exception=LoadError(invalid.error_message or "invalid result"),
        )
    return DispatchOutcome(
        result=result, messages=_result_messages(resolved, result)
    )


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


def _dispatch_python(
    resolved, fault_tolerant: bool, environment: LoadEnvironment
) -> LoadResult:
    """Import the deployed module, construct its object, and load it.

    The destination is resolved *here* and handed in, never inferred: an authored
    object with no Lakehouse falls back to the session's attachment, which in an
    orchestrated run is the Weaver control plane. Orchestration runs detached
    from every destination it writes to, so it must always say which one it
    means.

    The import goes through a runtime *context* rather than through
    ``sys.path``, because two Lakehouses may each deploy a ``lib/dates.py`` and
    ``sys.modules`` is consulted before any path is searched — so the second
    estate would silently receive the first one's helper. See
    :mod:`weaver.runtime.python_context`.
    """

    from .lakehouse import lakehouse_for
    from .runtime.python_context import import_deployed_module

    if environment.spark is None:
        raise LoadError(f"{resolved.node_id} needs a Spark session, and this run has none")
    target = resolved.node.physical_target
    lakehouse = lakehouse_for(environment.resolver, ItemRef(target.name))
    runtime_root = _join(lakehouse.files_root(), *LOAD_ROOT.split("/"))
    relative = (
        f"{resolved.node.primitive_object.schema}/"
        f"{resolved.node.primitive_object.object}"
    )
    within = relative[len(LOAD_ROOT) + 1 :] if relative.startswith(LOAD_ROOT) else relative
    context = environment.runtime_scope.context_for(
        # The logical item, not the object: everything one item deployed into one
        # target shares a tree, because that is what its author wrote against.
        logical_item=resolved.node.logical_id.item,
        physical_target=target,
        runtime_root=runtime_root,
    )
    module = import_deployed_module(
        context, within, expected=resolved.expected_class, node_id=resolved.node_id
    )
    cls = getattr(module, resolved.expected_class)
    return cls(environment.spark, lakehouse=lakehouse).load(
        fault_tolerant=fault_tolerant
    )


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
    "DispatchOutcome",
    "dispatch_load_node",
    "execute_load_plan",
]
