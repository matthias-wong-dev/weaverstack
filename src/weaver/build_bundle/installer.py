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
from .models import BuildAction, BuildBatch, BuildPlan, BuildSequence, ManagedInventory
from .planner import managed_inventory
from .report import (
    FAILED,
    SKIPPED,
    SUCCEEDED,
    ActionResult,
    InstallationReport,
    SequenceResult,
)
from .targets import BoundTarget, LOCAL_HOST

REPORT_FILENAME = "install-report.yml"


@dataclass
class InstallationEnvironment:
    """Runtime services the installer executes against — no planning inputs.

    ``spark`` is optional so a Folder-only bundle needs no session; a bundle
    with Spark work supplies one. ``executors`` defaults to the built-in
    registry.
    """

    store: Store
    resolver: Any
    spark: Any = None
    executors: dict[str, ActionExecutor] = field(default_factory=default_executors)

    def resolve_target(self, bound: BoundTarget) -> ResolvedTarget:
        if bound.host_kind != LOCAL_HOST:
            raise NotImplementedError(
                f"installing against a {bound.host_kind!r} host is not supported by "
                "build bundle v1"
            )
        return ResolvedTarget(bound=bound, lakehouse=ItemRef(bound.item_id))


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
        validate_bundle(bundle.location, bundle.plan, store=environment.store)

    plan = bundle.plan
    resolved = {target.id: environment.resolve_target(target) for target in plan.targets}
    managed = managed_inventory(plan)

    started = _now()
    sequence_results: list[SequenceResult] = []
    stop = False

    for sequence in plan.sequences:
        if stop:
            sequence_results.append(_skipped_sequence(sequence))
            continue
        result = _run_sequence(sequence, resolved, managed, bundle, environment)
        sequence_results.append(result)
        if result.status == FAILED:
            stop = True

    finished = _now()
    report = InstallationReport(
        bundle_id=plan.bundle_id,
        status=FAILED if stop else SUCCEEDED,
        started_at=started,
        finished_at=finished,
        sequences=tuple(sequence_results),
    )
    environment.store.write(
        bundle.location.join(REPORT_FILENAME), report.to_yaml().encode("utf-8")
    )
    return report


def _run_sequence(
    sequence: BuildSequence,
    resolved: dict[str, ResolvedTarget],
    managed: ManagedInventory,
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
            managed=managed,
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
            payload = environment.store.read(
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
