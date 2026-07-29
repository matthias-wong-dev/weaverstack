"""Installing a bundle — validated execution only, never planning.

The installer loads and fully validates a bundle, resolves its targets through
the supplied environment, and runs the sequences as barriers: each completes
before the next starts, one action's failure fails its sequence, and no later
sequence begins. It records exactly one result per action and persists the
report. It never reads the source repository, resolves a dependency or selects a
target — every such decision is already in the bundle.

Build is not load: the installer runs generated create DDL, creates folder
directories, and reconciles the target — it never executes an object's code, so
there is no snapshot on the import path. Concurrency starts conservatively:
sequences are serial and actions run serially within a batch, because one shared
local Spark session gives no useful parallel DDL. The manifest still models
independent actions, so a Fabric installer can add session concurrency later
without changing bundle semantics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ..errors import InstallError
from ..locations import Location
from ..store import Store
from ..targets import ItemRef
from .bundle import BuildBundle, load_bundle, validate_bundle
from .executors import default_executors
from .executors.base import ActionExecutor, InstallationContext, ResolvedTarget
from .models import BuildAction, BuildBatch, BuildPlan, BuildSequence
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
    sequence_results: list[SequenceResult] = []
    stop = False

    try:
        for sequence in plan.sequences:
            if stop:
                sequence_results.append(_skipped_sequence(sequence))
                continue
            result = _run_sequence(sequence, resolved, bundle, environment)
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
) -> SequenceResult:
    action_results: list[ActionResult] = []
    failed = False

    for batch in sequence.batches:
        target = resolved[batch.target_id]
        context = InstallationContext(
            spark=environment.spark,
            resolver=environment.resolver,
            store=environment.store,
            snapshot=bundle.location.join("repository"),
            target=target,
            sql=environment.sql_for(target.bound),
            snapshot_store=bundle.store or environment.store,
        )
        for action in batch.actions:
            if failed:
                action_results.append(_skipped_action(action, batch))
                continue
            result = _run_action(action, batch, context, bundle, environment)
            action_results.append(result)
            if result.status == FAILED:
                failed = True

    return SequenceResult(
        number=sequence.number,
        description=sequence.description,
        status=FAILED if failed else SUCCEEDED,
        actions=tuple(action_results),
    )


def _run_action(
    action: BuildAction,
    batch: BuildBatch,
    context: InstallationContext,
    bundle: BuildBundle,
    environment: InstallationEnvironment,
) -> ActionResult:
    started = _now()
    executor = environment.executors.get(action.executor)
    if executor is None:
        return _failed(
            action, batch, started, InstallError(f"no executor named {action.executor!r}")
        )

    try:
        payload = None
        if action.payload is not None:
            payload = (bundle.store or environment.store).read(
                bundle.location.join(*action.payload.split("/"))
            )
        details = executor.execute(action, payload, context)
    except Exception as exc:  # a failing action is data, not a crash
        return _failed(action, batch, started, exc)

    finished = _now()
    return ActionResult(
        action_id=action.id,
        resource_node_id=action.resource_node_id,
        target_id=batch.target_id,
        executor=action.executor,
        status=SUCCEEDED,
        started_at=started,
        finished_at=finished,
        duration_seconds=(finished - started).total_seconds(),
        details=details or None,
    )


def _failed(action: BuildAction, batch: BuildBatch, started: datetime, exc: Exception) -> ActionResult:
    finished = _now()
    return ActionResult(
        action_id=action.id,
        resource_node_id=action.resource_node_id,
        target_id=batch.target_id,
        executor=action.executor,
        status=FAILED,
        started_at=started,
        finished_at=finished,
        duration_seconds=(finished - started).total_seconds(),
        error_type=type(exc).__name__,
        error_message=str(exc),
    )


def _skipped_action(action: BuildAction, batch: BuildBatch) -> ActionResult:
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
