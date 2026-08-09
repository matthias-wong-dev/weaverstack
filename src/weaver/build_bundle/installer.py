"""Installing a bundle — validated execution only, never planning.

The installer loads and fully validates a bundle, resolves its targets through
the supplied environment, and runs the sequences as barriers: each completes
before the next starts, one action's failure fails its sequence, and no later
sequence begins. It records exactly one result per action and persists the
report. It never reads the source repository, resolves a dependency or selects a
target — every such decision is already in the bundle.

Build is not load: the installer runs generated create DDL, creates folder
directories, deploys an item's runtime code, and reconciles the target — it never
executes an object's code, and it has no route back to the source repository at
all, because a bundle carries its outputs rather than a second copy of its
inputs. Concurrency starts conservatively:
sequences are serial and actions run serially within a batch, because one shared
local Spark session gives no useful parallel DDL. The manifest still models
independent actions, so a Fabric installer can add session concurrency later
without changing bundle semantics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

from ..errors import InstallError
from ..locations import Location
from ..store import Store
from ..targets import ItemRef
from .bundle import BuildBundle, load_bundle, validate_bundle
from .executors import default_executors
from .executors.base import (
    ActionExecutor,
    InstallationContext,
    ResolvedTarget,
    SkippedExecution,
)
from .models import InstallAction, BuildBatch, BuildPlan, BuildSequence
from .report import (
    FAILED,
    SKIPPED,
    SUCCEEDED,
    ActionResult,
    InstallationReport,
    SequenceResult,
)
from .targets import WAREHOUSE_TARGET, BoundTarget

REPORT_FILENAME = "install-report.yml"


@dataclass
class InstallationEnvironment:
    """Runtime services the installer executes against — no planning inputs.

    ``spark`` is optional so a Folder-only bundle needs no session; a bundle
    with Spark work supplies one. ``sql`` is likewise optional: a Warehouse
    install acquires it **Fabric-natively** from the session identity, and only a
    desktop caller crossing into Fabric injects ``desktop_sql_executor``
    explicitly (``workspace`` is then unnecessary). ``executors`` defaults to the
    built-in registry.
    """

    store: Store
    resolver: Any
    spark: Any = None
    sql: Any = None
    workspace: Any = None
    executors: dict[str, ActionExecutor] = field(default_factory=default_executors)
    #: Set when this environment opened its own Fabric-native SQL, so it closes it.
    _owned_sql: Any = field(default=None, init=False, repr=False)

    def resolve_target(self, bound: BoundTarget) -> ResolvedTarget:
        # The resolver, store and Spark already define the environment the
        # installer is running in, so a target is its item plus that item's
        # physical roots. Resolving here, once, is what stops an executor deriving
        # a path for itself — and what would let one installation address several
        # destination Lakehouses without ever changing the session's own.
        item = ItemRef(bound.item_id)
        return ResolvedTarget(
            bound=bound,
            lakehouse=item,
            location=self._resolved(bound, item, "lakehouse_spark_location"),
            destination=self._resolved(bound, item, "spark_destination"),
        )

    def _resolved(self, bound: BoundTarget, item: ItemRef, method: str):
        """One of the destination's two addresses, where the workspace can give it.

        A Warehouse has neither — it is reached over TDS — and a resolver may not
        implement the method at all. Neither is a failure here, because the
        actions that need an address are Lakehouse actions and each fails
        explicitly, naming the target, when it is missing.
        """

        if bound.kind == WAREHOUSE_TARGET:
            return None
        resolve = getattr(self.resolver, method, None)
        if resolve is None:
            return None
        return resolve(item)

    def sql_for(self, bound: BoundTarget) -> Any:
        """The SQL capability for a Warehouse batch — injected, or Fabric-native.

        Weaver runs in Fabric, so an install against a Warehouse authenticates
        through the session's own identity rather than a desktop connection. The
        executor is opened once per installation and closed with it.
        """

        if self.sql is not None:
            return self.sql
        if bound.kind != WAREHOUSE_TARGET:
            return None
        if self._owned_sql is None:
            from ..fabric.sql import fabric_sql_executor
            from ..targets import WarehouseTarget

            self._owned_sql = fabric_sql_executor(
                WarehouseTarget(warehouse=ItemRef(bound.item_id)), self.workspace
            )
        return self._owned_sql

    def close(self) -> None:
        if self._owned_sql is not None and hasattr(self._owned_sql, "close"):
            self._owned_sql.close()
            self._owned_sql = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _epoch(started: datetime) -> str:
    """This installation's instant, as a Spark timestamp literal.

    Naive rather than offset-carrying: the column is a plain ``timestamp`` and a
    trailing offset would be parsed against the session's zone, which differs
    between a desktop and a Fabric driver. UTC throughout, spelled without one.
    """

    return started.strftime("%Y-%m-%d %H:%M:%S.%f")


def install_bundle(
    bundle: BuildBundle | Location,
    *,
    environment: InstallationEnvironment,
) -> InstallationReport:
    """Validate and run a bundle, returning a complete report."""

    if isinstance(bundle, Location):
        bundle = load_bundle(bundle, store=environment.store)
    else:
        # Preflight even a pre-loaded bundle: the installer trusts nothing it has
        # not just checked.
        validate_bundle(
            bundle.location,
            bundle.plan,
            store=bundle.store or environment.store,
        )

    plan = bundle.plan
    resolved = {target.id: environment.resolve_target(target) for target in plan.targets}

    started = _now()
    # One instant for the whole installation, taken once and handed to every
    # batch. Registry rows are published across several statements — one pair per
    # item — and rows written by one build have to be indistinguishable in age,
    # or an alias and the source it points at could order against each other
    # merely for having been written a few milliseconds apart.
    epoch = _epoch(started)
    sequence_results: list[SequenceResult] = []
    stop = False

    try:
        for sequence in plan.sequences:
            if stop:
                sequence_results.append(_skipped_sequence(sequence))
                continue
            result = _run_sequence(sequence, resolved, bundle, environment, epoch=epoch)
            sequence_results.append(result)
            if result.status == FAILED:
                stop = True
    finally:
        # Release any SQL connection this installation opened for itself.
        environment.close()

    finished = _now()
    report = InstallationReport(
        bundle_id=plan.bundle_id,
        status=FAILED if stop else SUCCEEDED,
        started_at=started,
        finished_at=finished,
        sequences=tuple(sequence_results),
    )
    (bundle.store or environment.store).write(
        bundle.location.join(REPORT_FILENAME), report.to_yaml().encode("utf-8")
    )
    return report


def _run_sequence(
    sequence: BuildSequence,
    resolved: dict[str, ResolvedTarget],
    bundle: BuildBundle,
    environment: InstallationEnvironment,
    *,
    epoch: str | None = None,
) -> SequenceResult:
    action_results: list[ActionResult] = []
    failed = False

    for batch in sequence.batches:
        target = resolved[batch.target_id]
        context = InstallationContext(
            spark=environment.spark,
            resolver=environment.resolver,
            store=environment.store,
            target=target,
            sql=environment.sql_for(target.bound),
            targets=resolved,
            epoch=epoch,
        )
        for action in batch.actions:
            if failed:
                action_results.append(_skipped_action(action, batch))
                continue
            result = _run_action(action, batch, context, bundle, environment)
            action_results.append(result)
            if result.status == FAILED:
                failed = True

    skipped = bool(action_results) and all(
        result.status == SKIPPED for result in action_results
    )
    return SequenceResult(
        number=sequence.number,
        description=sequence.description,
        status=FAILED if failed else SKIPPED if skipped else SUCCEEDED,
        actions=tuple(action_results),
    )


def execute_install_action(
    action: InstallAction,
    payload: bytes | None = None,
    *,
    context: InstallationContext,
    executors: Mapping[str, ActionExecutor] | None = None,
) -> ActionResult:
    """Run one action against one target, with the installer's result semantics.

    The same execution the installer performs, minus everything that is about a
    *bundle*: no loading, no validation, no sequence barriers, no target
    resolution, no report. One action, one payload, one context, one result.

    It exists so the platform boundary can be tested where it actually is. A test
    asking whether Fabric accepts Weaver's generated T-SQL, or whether a created
    Warehouse table shows up in inventory, needs the statement *executed* — not a
    repository parsed, a catalogue read, a bundle planned and an installation
    reported. Those are separately proven, and running them again to reach the
    one question costs a full build.

    A failing action is data here exactly as it is in an installation: the error
    is recorded on the result rather than raised, so the caller asserts on a
    result in both places and the semantics cannot drift apart.
    """

    return _execute(
        action,
        lambda: payload,
        context=context,
        target_id=context.target.bound.id,
        executors=default_executors() if executors is None else executors,
    )


def _run_action(
    action: InstallAction,
    batch: BuildBatch,
    context: InstallationContext,
    bundle: BuildBundle,
    environment: InstallationEnvironment,
) -> ActionResult:
    def load_payload() -> bytes | None:
        if action.payload is None:
            return None
        return (bundle.store or environment.store).read(
            bundle.location.join(*action.payload.split("/"))
        )

    return _execute(
        action,
        load_payload,
        context=context,
        target_id=batch.target_id,
        executors=environment.executors,
    )


def _execute(
    action: InstallAction,
    load_payload,
    *,
    context: InstallationContext,
    target_id: str,
    executors: Mapping[str, ActionExecutor],
) -> ActionResult:
    """The one place an action is run, shared by the installer and by callers.

    ``load_payload`` is deferred rather than passed as bytes because a payload
    that cannot be read is an action failure like any other, recorded with the
    same timing and the same result shape as one whose executor raised.
    """

    started = _now()
    executor = executors.get(action.executor)
    if executor is None:
        return _failed(
            action, target_id, started, InstallError(f"no executor named {action.executor!r}")
        )

    try:
        execution = executor.execute(action, load_payload(), context)
    except Exception as exc:  # a failing action is data, not a crash
        return _failed(action, target_id, started, exc)

    finished = _now()
    skipped = isinstance(execution, SkippedExecution)
    return ActionResult(
        action_id=action.id,
        resource_node_id=action.resource_node_id,
        target_id=target_id,
        executor=action.executor,
        status=SKIPPED if skipped else SUCCEEDED,
        started_at=started,
        finished_at=finished,
        duration_seconds=(finished - started).total_seconds(),
        details=execution.details if skipped else (execution or None),
    )


def _failed(
    action: InstallAction, target_id: str, started: datetime, exc: Exception
) -> ActionResult:
    finished = _now()
    return ActionResult(
        action_id=action.id,
        resource_node_id=action.resource_node_id,
        target_id=target_id,
        executor=action.executor,
        status=FAILED,
        started_at=started,
        finished_at=finished,
        duration_seconds=(finished - started).total_seconds(),
        error_type=type(exc).__name__,
        error_message=str(exc),
    )


def _skipped_action(action: InstallAction, batch: BuildBatch) -> ActionResult:
    return ActionResult(
        action_id=action.id,
        resource_node_id=action.resource_node_id,
        target_id=batch.target_id,
        executor=action.executor,
        status=SKIPPED,
    )


def _skipped_sequence(sequence: BuildSequence) -> SequenceResult:
    actions = tuple(
        _skipped_action(action, batch)
        for batch in sequence.batches
        for action in batch.actions
    )
    return SequenceResult(
        number=sequence.number,
        description=sequence.description,
        status=SKIPPED,
        actions=actions,
    )
