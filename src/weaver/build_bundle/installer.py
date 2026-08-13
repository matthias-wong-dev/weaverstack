"""Installing a bundle — validated execution only, never planning.

The installer loads and fully validates a bundle, resolves its targets through
the supplied environment, and runs the sequences as barriers: each completes
before the next starts, one action's failure fails its sequence, and no later
sequence begins. It records exactly one result per action and persists the
report. It never reads the source repository, resolves a dependency or selects a
target — every such decision is already in the bundle.

Build is not load: it runs generated create DDL, creates folder directories,
deploys an item's runtime code and reconciles the target. It never executes an
object's code, and has no route back to the source repository.

Everything runs one at a time. Sequences are serial because a sequence is a
barrier; the actions within a batch are serial too — see :data:`_WHY_SERIAL`.

Every action runs here, whichever position this is. An executor reaches for the
capability its work needs — storage, REST, TDS, or the Session's Spark SQL — and
the Session decides what performing it means.
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
    """A Warehouse connection opened by the first executor that uses it.

    Separates "this batch has one" — which it needs when it is assembled — from
    "one is open", which can wait. Everything is forwarded, so an executor
    cannot tell.
    """

    __slots__ = ("_acquire", "_session")

    def __init__(self, acquire) -> None:
        object.__setattr__(self, "_acquire", acquire)
        object.__setattr__(self, "_session", None)

    def _resolved(self):
        if object.__getattribute__(self, "_session") is None:
            object.__setattr__(self, "_session", object.__getattribute__(self, "_acquire")())
        return object.__getattribute__(self, "_session")

    def __getattr__(self, name):
        return getattr(self._resolved(), name)

    def __repr__(self) -> str:
        acquired = object.__getattribute__(self, "_session")
        return "<not yet acquired>" if acquired is None else repr(acquired)


class Installer:
    """Execute an already-decided bundle against a Session.

    It validates the bundle, resolves its targets, walks the sequences as
    barriers and records one result per action. It never reopens the repository,
    resolves a dependency, chooses a target or replans anything — every such
    decision is already in the bundle.

    Its runtime services come from the Session, and it closes none of them
    because it opened none of them.

    .. code-block:: python

        report = Installer(session).install(bundle)
    """

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

    # --- what an executor is given -----------------------------------------

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
        """Run one Spark SQL statement, from wherever this is running."""

        session = self.session
        workspace = self.workspace

        def run(statement: str, *, exact_case: bool = False):
            return session.execute_spark_sql(
                statement, exact_case=exact_case, workspace=workspace
            )

        return run

    def spark_sql_batch(self):
        """Run several Spark SQL statements as one piece of work.

        Ordered, one submission where they cross, one identifier-case scope.
        """

        session = self.session
        workspace = self.workspace

        def run(statements, *, exact_case: bool = False):
            return session.execute_spark_sql_batch(
                statements, exact_case=exact_case, workspace=workspace
            )

        return run

    def sql_for(self, bound: BoundTarget) -> Any:
        """The Warehouse connection for a batch, opened by the first action
        that runs a statement. ``None`` means this target is not a Warehouse."""

        if bound.kind != WAREHOUSE_TARGET:
            return None
        from ..targets import WarehouseTarget

        return _Deferred(
            lambda: self.session.sql_executor(
                WarehouseTarget(ItemRef(bound.item_id)), workspace=self.workspace
            )
        )

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

    # --- installation -------------------------------------------------------

    def install(self, bundle: BuildBundle | Location) -> InstallationReport:
        """Validate and run a bundle, returning a complete report."""

        if isinstance(bundle, Location):
            bundle = load_bundle(bundle, store=self.store)
        else:
            # Preflight even a pre-loaded bundle: the installer trusts nothing
            # it has not just checked.
            validate_bundle(
                bundle.location, bundle.plan, store=bundle.store or self.store
            )

        plan = bundle.plan
        resolved = {target.id: self.resolve_target(target) for target in plan.targets}

        started = _now()
        # One instant for the whole installation, taken once and handed to every
        # batch. Registry rows are published across several statements — one pair
        # per item — and rows written by one build have to be indistinguishable in
        # age, or an alias and the source it points at could order against each
        # other merely for having been written a few milliseconds apart.
        epoch = _epoch(started)
        sequence_results: list[SequenceResult] = []
        stop = False

        for sequence in plan.sequences:
            if stop:
                sequence_results.append(_skipped_sequence(sequence))
                continue
            result = _run_sequence(sequence, resolved, bundle, self, epoch=epoch)
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
    """This installation's instant, as a Spark timestamp literal.

    Naive rather than offset-carrying: the column is a plain ``timestamp`` and a
    trailing offset would be parsed against the session's zone, which differs
    between a desktop and a Fabric driver. UTC throughout, spelled without one.
    """

    return started.strftime("%Y-%m-%d %H:%M:%S.%f")


#: Why actions within a batch run one at a time.
#:
#: Independent in dependency order is not independent in lock order. Run
#: concurrently, DDL and DML against one Warehouse contend on catalogue metadata
#: and on the rows they touch, and Fabric's snapshot isolation aborts rather
#: than waits:
#:
#: .. code-block:: text
#:
#:     Transaction (Process ID 55) was deadlocked on lock resources
#:     Snapshot isolation transaction aborted due to update conflict
#:
#: Widening this needs a design for that contention and a measurement showing it
#: is worth the complexity; it was not measurably faster on the estate that
#: broke.
_WHY_SERIAL = "concurrent T-SQL deadlocked a real Warehouse; see the note above"



def _sequence_label(sequence: BuildSequence, resolved: dict) -> str:
    """What this sequence is doing, and which items it is doing it to.

    The planner's own description, capitalised, with the targets it touches
    after it. Both halves earn their place: several sequences share a
    description — a dependency layer is built once per layer — and the target is
    what tells them apart.
    """

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
    """One batch's actions, one at a time, in manifest order.

    Serial, and the comment on :data:`_WHY_SERIAL` says why concurrency was
    tried and taken back out. Manifest order, because a report that reordered
    itself would make two runs of one bundle incomparable.
    """

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
    epoch: str | None = None,
) -> SequenceResult:
    action_results: list[ActionResult] = []
    failed = False

    # The sequence is the unit worth naming, and it already carries the words:
    # the planner wrote "publish catalogue dictionaries and installations" when
    # it built these, and nothing was showing it. A batch apiece described its
    # executors instead — "Lakehouse/Weaver: views" for what is plainly the
    # catalogue being updated — which is the mechanism dressed up as an answer.
    # The target belongs on the line, not in the detail. "Build dependency
    # layer" appears once per layer, so four identical lines with four different
    # durations told a reader that four things happened and nothing about which.
    with installer.session.substep(_sequence_label(sequence, resolved)):
        for batch in sequence.batches:
            target = resolved[batch.target_id]
            context = InstallationContext(
                spark_sql=installer.spark_sql(),
                spark_sql_batch=installer.spark_sql_batch(),
                resolver=installer.resolver,
                store=installer.store,
                target=target,
                sql=installer.sql_for(target.bound),
                targets=resolved,
                epoch=epoch,
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
