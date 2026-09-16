"""Validate and execute planned build bundles.

Sequences are barriers. A failed sequence skips all later sequences, and every
planned action receives one result. The installer resolves target capabilities
through the Session; it never reads the source repository or changes the plan.
"""

from __future__ import annotations

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
from .models import BuildBatch, BuildSequence, InstallAction
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


class _Deferred:
    """A Warehouse connection opened by the first executor that uses it."""

    __slots__ = ("_acquire", "_session")

    def __init__(self, acquire) -> None:
        object.__setattr__(self, "_acquire", acquire)
        object.__setattr__(self, "_session", None)

    def _resolved(self):
        if object.__getattribute__(self, "_session") is None:
            object.__setattr__(
                self, "_session", object.__getattribute__(self, "_acquire")()
            )
        return object.__getattribute__(self, "_session")

    def __getattr__(self, name):
        return getattr(self._resolved(), name)

    def __repr__(self) -> str:
        acquired = object.__getattribute__(self, "_session")
        return "<not yet acquired>" if acquired is None else repr(acquired)


class Installer:
    """Execute an already-planned bundle through a Session."""

    def __init__(
        self,
        session,
        *,
        workspace: Any = None,
        executors: dict[str, ActionExecutor] | None = None,
    ) -> None:
        self.session = session
        self.workspace = workspace if workspace is not None else session.workspace
        self.executors = default_executors() if executors is None else executors

    @property
    def store(self) -> Store:
        return self.session.store(self.workspace)

    @property
    def resolver(self) -> Any:
        return self.session.resolver(self.workspace)

    @property
    def spark(self) -> Any:
        return self.session.spark(self.workspace)

    def spark_sql(self):
        session = self.session
        workspace = self.workspace

        def run(statement: str, *, exact_case: bool = False):
            return session.execute_spark_sql(
                statement, exact_case=exact_case, workspace=workspace
            )

        return run

    def spark_sql_batch(self):
        """Run ordered statements in one submission and identifier-case scope."""

        session = self.session
        workspace = self.workspace

        def run(statements, *, exact_case: bool = False):
            return session.execute_spark_sql_batch(
                statements, exact_case=exact_case, workspace=workspace
            )

        return run

    def delta_table_creator(self):
        session = self.session
        workspace = self.workspace

        def create(
            qualified_name,
            columns,
            *,
            identity_column=None,
            column_mapping=True,
        ):
            return session.create_delta_table(
                qualified_name,
                columns,
                identity_column=identity_column,
                column_mapping=column_mapping,
                workspace=workspace,
            )

        return create

    def sql_for(self, bound: BoundTarget) -> Any:
        """Return a deferred Warehouse connection, or ``None`` for a Lakehouse."""

        if bound.kind != WAREHOUSE_TARGET:
            return None
        from ..targets import WarehouseTarget

        return _Deferred(
            lambda: self.session.sql_executor(
                WarehouseTarget(ItemRef(bound.item_id)), workspace=self.workspace
            )
        )

    def resolve_target(self, bound: BoundTarget) -> ResolvedTarget:
        # Resolve once so executors never derive paths or inherit a Lakehouse.
        item = ItemRef(bound.item_id)
        return ResolvedTarget(
            bound=bound,
            lakehouse=item,
            location=self._resolved(bound, item, "lakehouse_spark_location"),
            destination=self._resolved(bound, item, "spark_destination"),
        )

    def _resolved(self, bound: BoundTarget, item: ItemRef, method: str):
        """Resolve a Lakehouse address; Warehouse actions use TDS instead."""

        if bound.kind == WAREHOUSE_TARGET:
            return None
        resolve = getattr(self.resolver, method, None)
        if resolve is None:
            return None
        return resolve(item)

    def install(self, bundle: BuildBundle | Location) -> InstallationReport:
        if isinstance(bundle, Location):
            bundle = load_bundle(bundle, store=self.store)
        else:
            # Revalidate pre-loaded bundles immediately before execution.
            validate_bundle(
                bundle.location, bundle.plan, store=bundle.store or self.store
            )

        plan = bundle.plan
        resolved = {target.id: self.resolve_target(target) for target in plan.targets}

        started = _now()
        # All Registry rows from one build share an instant so shortcut freshness
        # does not depend on statement timing.
        build_datetime = _epoch(started)
        sequence_results: list[SequenceResult] = []
        stop = False

        for sequence in plan.sequences:
            if stop:
                sequence_results.append(_skipped_sequence(sequence))
                continue
            result = _run_sequence(
                sequence, resolved, bundle, self, build_datetime=build_datetime
            )
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
        (bundle.store or self.store).write(
            bundle.location.join(REPORT_FILENAME), report.to_yaml().encode("utf-8")
        )
        return report


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _epoch(started: datetime) -> str:
    """Render UTC without an offset for Spark's zone-sensitive ``timestamp``."""

    return started.strftime("%Y-%m-%d %H:%M:%S.%f")


#: Fabric Warehouse snapshot isolation can abort concurrent DDL and DML that
#: contend on catalogue metadata or rows, so actions within a batch are serial.
_WHY_SERIAL = "concurrent T-SQL deadlocked a real Warehouse; see the note above"


def _sequence_label(sequence: BuildSequence, resolved: dict) -> str:
    text = (sequence.description or "").strip()
    said = text[:1].upper() + text[1:] if text else "Install"
    names: list[str] = []
    for batch in sequence.batches:
        target = resolved.get(batch.target_id)
        name = target.bound.display if target is not None else batch.target_id
        if name not in names:
            names.append(name)
    return f"{said}: {', '.join(names)}" if names else said


def _run_batch(
    batch: BuildBatch,
    context: InstallationContext,
    bundle: BuildBundle,
    installer: "Installer",
) -> list[ActionResult]:
    """Run a batch serially in stable manifest order."""

    return [
        _run_action(action, batch, context, bundle, installer)
        for action in batch.actions
    ]


def _run_sequence(
    sequence: BuildSequence,
    resolved: dict[str, ResolvedTarget],
    bundle: BuildBundle,
    installer: "Installer",
    *,
    build_datetime: str | None = None,
) -> SequenceResult:
    action_results: list[ActionResult] = []
    failed = False

    with installer.session.substep(_sequence_label(sequence, resolved)):
        for batch in sequence.batches:
            target = resolved[batch.target_id]
            context = InstallationContext(
                create_delta_table=installer.delta_table_creator(),
                spark_sql=installer.spark_sql(),
                spark_sql_batch=installer.spark_sql_batch(),
                resolver=installer.resolver,
                store=installer.store,
                target=target,
                sql=installer.sql_for(target.bound),
                targets=resolved,
                build_datetime=build_datetime,
            )
            if failed:
                action_results.extend(
                    _skipped_action(one, batch) for one in batch.actions
                )
                continue
            results = _run_batch(batch, context, bundle, installer)
            action_results.extend(results)
            failed = any(result.status == FAILED for result in results)

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
    """Execute one action; failures populate its ``ActionResult``."""

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
    installer: "Installer",
) -> ActionResult:
    def load_payload() -> bytes | None:
        if action.payload is None:
            return None
        return (bundle.store or installer.store).read(
            bundle.location.join(*action.payload.split("/"))
        )

    return _execute(
        action,
        load_payload,
        context=context,
        target_id=batch.target_id,
        executors=installer.executors,
    )


def _execute(
    action: InstallAction,
    load_payload,
    *,
    context: InstallationContext,
    target_id: str,
    executors: Mapping[str, ActionExecutor],
) -> ActionResult:
    """Run an action, including deferred payload reads, under one result boundary."""

    started = _now()
    executor = executors.get(action.executor)
    if executor is None:
        return _failed(
            action,
            target_id,
            started,
            InstallError(f"no executor named {action.executor!r}"),
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
        source_path=action.source_path,
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
        source_path=action.source_path,
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
        source_path=action.source_path,
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
