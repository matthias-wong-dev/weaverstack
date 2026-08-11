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
inputs.

**Concurrency follows the manifest rather than guessing.** Sequences stay
serial, because a sequence is a barrier and that is what it is for. Within a
batch the manifest already says the actions are independent units against one
target, so T-SQL among them runs at once — a Warehouse's round trips are the
thing worth not paying end to end, and running them together reorders nothing a
barrier was protecting.

Only T-SQL. A Spark statement's concurrency is the Fabric session's business
rather than ours, and OneLake writes and REST calls are a later measurement
rather than an assumption. The plan's instruction is to decompose one capability
route at a time and let the timings say what to widen.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
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
    """A capability acquired by the first executor that uses it.

    A batch is handed its capabilities up front, and the expensive ones — a
    Spark session, a Warehouse connection — should not be paid for by actions
    that never touch them. This exists so that "this host has one" and "this
    host has *started* one" stop being the same question: the first is what a
    batch needs to know when it is assembled, the second is answerable later.

    Deliberately not a general lazy proxy. Everything is forwarded, so an
    executor writes ``context.spark.sql(...)`` or ``context.sql.execute(...)``
    exactly as before and cannot tell the difference.
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

    One of Weaver's four doers, and the one with no opinions. It validates the
    bundle, resolves its targets, walks the sequences as barriers and records one
    result per action. It never reopens the repository, resolves a dependency,
    chooses a target, inspects the estate to check whether the Builder was right,
    or replans anything: every such decision is already in the bundle, and an
    installer that could second-guess it would make "who decided?" unanswerable.

    Its runtime services come from the Session — the Spark it runs in, the store
    it reads payloads from, the resolver that turns a target into paths, the
    connection each Warehouse is reached over. It closes none of them, because it
    opened none of them.

    .. code-block:: python

        report = Installer(session).install(bundle)

    This runs where the data is. A console addressing Fabric crosses first, once,
    with the whole bundle; the Installer on the far side is this same object.
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

    def spark_when_needed(self) -> Any:
        """Spark for this host, acquired only if an action actually asks for it.

        A batch is given its capabilities up front, and a Spark session is the
        one that costs seconds to start and that a JVM permits exactly one of.
        Building it eagerly meant a bundle of nothing but file writes, T-SQL and
        an endpoint refresh still started one — paying for a capability none of
        its actions would touch, and, in a process that already had a session,
        failing outright with *Only one SparkContext should be running in this
        JVM*.

        ``None`` still means *this host has no Spark*, which several executors
        read to skip Lakehouse work rather than fail it. That answer is given
        without acquiring anything: whether a host executes here is a property
        of the host, not of having started a session.
        """

        if not self.session.executes_here(self.workspace):
            return None
        return _Deferred(lambda: self.session.spark(self.workspace))

    def spark_sql(self):
        """Ask this host's Spark one question, from wherever this is running.

        The capability an executor needs when it only has a *question* — the
        alias read probe is the case — rather than work that needs a real
        DataFrame. Supplying it is what lets shortcut creation stay on the
        desktop while the probe crosses.
        """

        session = self.session
        workspace = self.workspace

        def ask(statement: str, *, exact_case: bool = False):
            return session.execute_spark_sql(
                statement, exact_case=exact_case, workspace=workspace
            )

        return ask

    def sql_for(self, bound: BoundTarget) -> Any:
        """The Warehouse connection for a batch, from the Session that owns it.

        Deferred for the reason Spark is: a connection is opened by the first
        action that runs a statement, not by assembling the batch that might.
        ``None`` still means *this target is not a Warehouse*, which is a
        structural fact and costs nothing to answer.
        """

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


#: Executors whose actions may run at the same time as each other.
#:
#: T-SQL only, and deliberately. A batch names one target and its actions are
#: independent units by the manifest's own contract, so concurrency here does
#: not reorder anything a sequence barrier was protecting. What stops this being
#: "parallelise everything" is that the others do not benefit the same way: a
#: Spark statement's concurrency is the Fabric session's business rather than
#: ours, and OneLake writes and REST calls are a later measurement, not an
#: assumption.
PARALLEL_EXECUTORS = frozenset({"tsql", "tsql_batch"})

#: How many at once when nothing says otherwise. Bounded rather than unbounded:
#: a Warehouse is a server with a connection pool, not a thread sink.
DEFAULT_PARALLEL_WORKERS = 4


def _parallel_workers(installer: "Installer", target: ResolvedTarget) -> int:
    """How wide this target may go, from its own configuration if it says.

    Per target, because a Warehouse's capacity is a property of that Warehouse —
    which is exactly what `execution.parallel_workers` on a configured target is
    already for.
    """

    workspace = installer.workspace
    declared = None
    warehouses = getattr(workspace, "warehouses", None) or {}
    entry = warehouses.get(target.bound.name) if hasattr(warehouses, "get") else None
    execution = getattr(entry, "execution", None)
    if execution is not None:
        declared = execution.parallel_workers
    if declared is None:
        execution = getattr(workspace, "execution", None)
        declared = getattr(execution, "parallel_workers", None)
    return max(1, declared or DEFAULT_PARALLEL_WORKERS)


def _run_batch(
    batch: BuildBatch,
    context: InstallationContext,
    bundle: BuildBundle,
    installer: "Installer",
) -> list[ActionResult]:
    """One batch's actions, in parallel where that is safe and worth it.

    The manifest already says these are independent — one target, one batch, and
    "actions are independent units, each reported on its own". So running the
    T-SQL among them at once reorders nothing that a barrier was protecting; it
    only stops a Warehouse's round trips being paid end to end.

    Results come back in manifest order however they finished, because a report
    that reordered itself by completion would make two runs of one bundle
    incomparable.

    A failure does not cancel work already in flight. Those actions were going
    to run anyway, their results are true, and reporting them is strictly more
    than reporting that they were skipped — the *sequence* barrier is what stops
    anything downstream.
    """

    from concurrent.futures import ThreadPoolExecutor

    crossing = [action for action in batch.actions if _crosses(action, installer)]
    answers: dict = {}
    started = _now()
    if crossing:
        answers = _run_crossed(crossing, batch, context, bundle, installer)

    def run(action: InstallAction) -> ActionResult:
        if action.id in {one.id for one in crossing}:
            return _crossed_result(action, answers.get(action.id), batch.target_id, started)
        return _run_action(action, batch, context, bundle, installer)

    concurrent = [
        action
        for action in batch.actions
        if action.executor in PARALLEL_EXECUTORS and action not in crossing
    ]
    if len(concurrent) < 2:
        return [run(action) for action in batch.actions]

    workers = min(_parallel_workers(installer, context.target), len(concurrent))
    results: dict[str, ActionResult] = {}
    with ThreadPoolExecutor(
        max_workers=workers, thread_name_prefix="weaver-install"
    ) as pool:
        futures = {pool.submit(run, action): action for action in concurrent}
        for future, action in futures.items():
            results[action.id] = future.result()

    ordered: list[ActionResult] = []
    for action in batch.actions:
        ordered.append(results[action.id] if action.id in results else run(action))
    return ordered


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

    for batch in sequence.batches:
        target = resolved[batch.target_id]
        context = InstallationContext(
            spark=installer.spark_when_needed(),
            spark_sql=installer.spark_sql(),
            resolver=installer.resolver,
            store=installer.store,
            target=target,
            sql=installer.sql_for(target.bound),
            targets=resolved,
            epoch=epoch,
        )
        if failed:
            action_results.extend(_skipped_action(one, batch) for one in batch.actions)
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


def _crosses(action: InstallAction, installer: "Installer") -> bool:
    """Whether this action has to run where the Spark is, rather than here."""

    executor = installer.executors.get(action.executor)
    return bool(getattr(executor, "needs_spark", False)) and not (
        installer.session.executes_here(installer.workspace)
    )


def _run_crossed(
    actions: list,
    batch: BuildBatch,
    context: InstallationContext,
    bundle: BuildBundle,
    installer: "Installer",
) -> dict:
    """Run this batch's Spark actions in the session, in one submission.

    One submission rather than one apiece, and that is a measurement rather than
    a preference: crossing per action cost about four seconds of overhead each,
    so six actions in a small estate paid twenty-four seconds of pure transport.
    The manifest still models independent actions — what is batched is the
    physical effect, not the semantic unit.

    Each action still gets its own result, its own timing and its own status, so
    a reader cannot tell from the report that they shared a trip.
    """

    import base64

    from . import remote
    from ..session.program import RemoteProgram

    workspace = installer.workspace
    store = bundle.store or installer.store
    entries = []
    for action in actions:
        payload = None
        if action.payload is not None:
            payload = base64.b64encode(
                store.read(bundle.location.join(*action.payload.split("/")))
            ).decode()
        entries.append({"action": action.to_mapping(), "payload": payload})

    arguments = {
        "actions": entries,
        "target": context.target.bound.to_mapping(),
        "targets": [one.bound.to_mapping() for one in context.targets.values()],
        "epoch": context.epoch,
    }
    source = (
        "from weaver.workspaces import FabricWorkspace\n"
        "from weaver.build_bundle.remote import install_actions\n"
        "from weaver.session import NotebookSession\n"
        f"workspace = {_workspace_literal(workspace)}\n"
        "session = NotebookSession(workspace=workspace, spark=spark)\n"
        f"emit(install_actions(session=session, workspace=workspace, **{arguments!r}))\n"
    )
    answered = installer.session.execute_python(
        RemoteProgram(
            name="install.spark",
            call=lambda: remote.install_actions(
                session=installer.session, workspace=workspace, **arguments
            ),
            source=source,
            detail=f"{len(actions)} action(s)",
        ),
        workspace=workspace,
    )
    running = 0.0
    placed = {}
    for answer in answered:
        placed[answer["id"]] = {**answer, "offset": running}
        running += float(answer.get("seconds") or 0.0)
    return placed


def _crossed_result(
    action: InstallAction,
    answer: dict | None,
    target_id: str,
    started: datetime,
) -> ActionResult:
    """One remote answer, recorded as the local result shape.

    The duration is the far side's own measurement of *this action*, not the
    submission's. Stamping every action in a batch with the batch's elapsed time
    was the first attempt and it was worse than useless: six actions sharing one
    trip each appeared to have taken the whole trip, so the numbers a reader
    uses to find the slow one all said the same thing.

    ``started_at`` places that duration on the local timeline by accumulating
    the offsets the far side reported, so the actions in a batch read in order
    and their spans do not overlap. It is the remote clock's *duration* on the
    local clock's *origin*, which is the honest composition of what each side
    can actually see.
    """

    seconds = float((answer or {}).get("seconds") or 0.0)
    offset = float((answer or {}).get("offset") or 0.0)
    began = started + timedelta(seconds=offset)
    common = dict(
        action_id=action.id,
        resource_node_id=action.resource_node_id,
        source_path=action.source_path,
        target_id=target_id,
        executor=action.executor,
        started_at=began,
        finished_at=began + timedelta(seconds=seconds),
        duration_seconds=seconds,
    )
    if answer is None:
        return ActionResult(
            status=FAILED,
            error_type="InstallError",
            error_message=f"{action.id} was submitted but nothing came back for it",
            **common,
        )
    if answer.get("failed"):
        return ActionResult(
            status=FAILED,
            error_type=answer.get("error_type"),
            error_message=answer.get("error_message"),
            **common,
        )
    skipped = answer.get("skipped")
    return ActionResult(
        status=SKIPPED if skipped else SUCCEEDED,
        details=answer.get("details"),
        **common,
    )


def _workspace_literal(workspace) -> str:
    return (
        f"FabricWorkspace(workspace={workspace.workspace!r}, "
        f"weaver_lakehouse={workspace.weaver_lakehouse!r}, "
        f"environment={workspace.environment!r})"
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
