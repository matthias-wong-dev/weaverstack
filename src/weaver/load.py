"""The public ``weaver.load(...)`` operation.

The adaptation boundary between the small notebook-facing surface and the
planning, resolution, execution and logging seams beneath it — the load
counterpart of :mod:`weaver.operations`, and deliberately shaped like it:

.. code-block:: python

    weaver.load(["Warehouse/Reporting", "Lakehouse/Curated"])

Typed physical target strings at the boundary; an explicit workspace or workspace
configuration where there is one; the notebook's own context where there is not;
and typed objects retained only as internal seams. What a caller writes here is
what they already write for ``build`` and ``wipe``, because it is parsed by the
same grammar.

**The catalogue is the source, and the repository is not consulted.** By the time
anything is loadable it has been built, and what was built is recorded. Reopening
the declaration would orchestrate what somebody meant to install rather than what
is installed, and the two differ exactly when it matters most.

The order below is the order things must happen in, and each step is somebody
else's module:

.. code-block:: text

    parse targets            weaver.targets
    resolve workspace        weaver.operations
    read the catalogue       weaver.catalogue.state
    observe every target     weaver.run.state
    reverse the bindings     weaver.load_plan
    build the physical DAG   weaver.run.graph
    resolve every node       weaver.run.resolution
    ── dry run stops here ──
    dispatch each primitive  weaver.run.runner
    write task evidence      weaver.task_logging

Nothing executes while catalogue state is still being discovered: the whole plan
is settled, ordered and resolved before the first primitive is dispatched.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .errors import CommandError, LoadError
from .load_plan import (
    ENDPOINT_REFRESH,
    LAKEHOUSE_TARGET,
    WAREHOUSE_TARGET,
    InstalledEstate,
    LoadDag,
    PhysicalTargetRef,
    load_dag,
)
from .load_report import (
    BLOCKED,
    FAILED,
    LoadNodeReport,
    LoadRunReport,
    SEVERITY_ERROR,
    SUCCEEDED,
    SUCCEEDED_WITH_REJECTS,
    final_status,
)
from .targets import (
    DeltaTarget,
    ItemRef,
    WarehouseTarget,
    parse_physical_target,
    physical_item,
)
from .workspaces import FabricWorkspace, LocalWorkspace, Workspace

#: The task type this operation writes evidence under.
TASK_TYPE = "load"


def load(
    targets: str | Sequence[str],
    *,
    workspace: str | Path | Workspace | None = None,
    weaver_lakehouse: str | None = None,
    workspace_config: str | Path | None = None,
    fault_tolerant: bool = False,
    dry_run: bool = False,
    session=None,
) -> LoadRunReport:
    """Load every installed loadable object in the requested physical targets.

    ``targets`` are typed physical items — ``Lakehouse/Curated``,
    ``Warehouse/Reporting`` — and the request means *everything loadable hosted
    there, plus whatever upstream work those objects need*. Object-level
    selection is not part of this phase.

    Every value resolves the same way, and it is the way ``build`` resolves them:

    .. code-block:: text

        an explicit argument
          → a workspace configuration file
            → what the notebook is attached to
              → a configuration error naming what is missing

    So ``workspace=None`` means the current Fabric session, and an explicit
    ``weaver_lakehouse`` stands on its own — a caller who names both the
    workspace and the control Lakehouse needs no configuration file at all.
    """

    values = (targets,) if isinstance(targets, str) else tuple(targets)
    if not values:
        raise CommandError("load needs at least one target")
    requested = tuple(
        parse_physical_target(value, what="load target", error=CommandError)
        for value in values
    )

    from dataclasses import replace

    from .operations import _operation_workspace, _with_inferred_control_lakehouse

    resolved_workspace = _operation_workspace(
        workspace=workspace, workspace_config=workspace_config, session=session
    )
    if weaver_lakehouse is not None:
        # An explicit argument outranks a configured or already-resolved value,
        # so a caller can override what it inferred without rebuilding the
        # Workspace it inferred it into.
        resolved_workspace = replace(
            resolved_workspace,
            weaver_lakehouse=ItemRef.parse(str(weaver_lakehouse)).name,
        )
    resolved_workspace = _with_inferred_control_lakehouse(resolved_workspace)
    if not resolved_workspace.weaver_lakehouse:
        raise CommandError(
            "load needs a Weaver control Lakehouse: pass weaver_lakehouse=, "
            "give one in workspace configuration, or run inside a Fabric "
            "notebook with one attached as the default Lakehouse"
        )
    if isinstance(resolved_workspace, LocalWorkspace) and any(
        isinstance(target, WarehouseTarget) for target in requested
    ):
        raise CommandError(
            "Warehouse targets require a Fabric Workspace; the local emulator has no SQL"
        )

    from .session.host import use_or_create_session

    with use_or_create_session(session, workspace=resolved_workspace) as opened:
        if not opened.executes_here(resolved_workspace):
            raise CommandError(
                "load runs where the data is: call it from a Fabric notebook, "
                "or against a local Workspace"
            )
        return run_load(
            opened,
            workspace=resolved_workspace,
            requested=tuple(_physical_ref(target) for target in requested),
            fault_tolerant=fault_tolerant,
            dry_run=dry_run,
        )


def run_load(
    session,
    *,
    workspace,
    requested: Sequence[PhysicalTargetRef],
    state=None,
    fault_tolerant: bool = False,
    dry_run: bool = False,
) -> LoadRunReport:
    """The whole orchestration path, over a Session.

    Reads the estate once, at a boundary, and hands the Runner a snapshot:

    .. code-block:: text

        physical catalogue + targets  →  RunState  →  Runner  →  RunResult

    Nothing about *when* a node runs lives here any more. What is left is the
    load operation's own business — reading the estate, writing the evidence,
    and rendering the result in the shape a load's readers expect.

    ``state`` is the handover, and a caller that already has one supplies it:
    the estate is a Python representation, so a caller holding an observed
    snapshot should not have to arrange for it to be re-observed. Omitted, this
    reads one — which is what the operation does in production.
    """

    from .run import (
        RunRequest,
        Runner,
        RunState,
        can_refresh,
        dispatch_primitive,
        open_run_log,
    )
    from .run.state import read_installed_catalogue, read_target_inventories

    started = datetime.now(timezone.utc)
    catalogue = (
        state.catalogue
        if state is not None
        else read_installed_catalogue(session=session, workspace=workspace)
    )
    estate = InstalledEstate.from_catalogue(catalogue)
    _refuse_uninstalled_targets(estate, requested)

    if state is None:
        # Only the targets the graph touches: the rest have nothing to be looked
        # up in them. Planning the graph first is what makes that knowable, and
        # the reading happens once, here, so everything below decides against a
        # snapshot rather than against state that is still moving.
        planned = load_dag(estate, targets=requested)
        state = RunState(
            catalogue=catalogue,
            target_inventories=read_target_inventories(
                tuple(node.physical_target for node in planned.nodes),
                session=session,
                workspace=workspace,
            ),
        )
    runner = Runner(
        state,
        RunRequest.load(
            requested, fault_tolerant=fault_tolerant, dry_run=dry_run
        ),
        workspace=workspace,
        can_refresh=can_refresh(session, workspace),
    )

    log = (
        None
        if dry_run
        else open_run_log(session, workspace=workspace, task_type=TASK_TYPE)
    )
    if log is not None:
        log.write_plan(_plan_document(runner.graph, state, requested, dry_run))
    result = runner.run(
        session=session,
        dispatch=dispatch_primitive,
        on_node=(
            None
            if log is None
            else lambda node: log.write_step(_step_type(node), node.to_mapping())
        ),
    )

    report = _as_load_report(result, started=started, task_log=log)
    if log is not None:
        log.write_completion(_completion_document(report))
    if not fault_tolerant and not dry_run:
        _raise_for_failure(report)
    return report


def _as_load_report(result, *, started, task_log) -> LoadRunReport:
    """One RunResult, rendered as the shape a load's readers expect.

    The internal model is one; the public shapes are not. A load reader wants
    rows moved and a task log to point at, and gets exactly the report they got
    before — which is why the CLI, the notebook and the task log did not have to
    learn a new one.
    """

    return LoadRunReport(
        requested=result.requested,
        status=result.status,
        dry_run=result.dry_run,
        fault_tolerant=result.fault_tolerant,
        nodes=tuple(
            LoadNodeReport(
                node_id=node.node_id,
                logical_id=node.logical_id,
                physical_target=node.physical_target,
                primitive_kind=node.primitive_kind,
                dispatch_location=node.dispatch_location,
                status=node.status,
                executed=node.executed,
                messages=tuple(node.messages),
                result=node.result,
                started_at=node.started_at,
                finished_at=node.finished_at,
            )
            for node in result.nodes
        ),
        edges=result.edges,
        order=result.order,
        messages=tuple(result.messages),
        workspace=result.workspace,
        task_id=None if task_log is None else task_log.task_id,
        task_log=None if task_log is None else task_log.root.value,
        started_at=started.isoformat(),
        finished_at=result.finished_at,
    )


def _raise_for_failure(report: LoadRunReport) -> None:
    """Turn an intolerant run's recorded failure into the exception it is.

    **Everything durable is already written.** Every planned node has its final
    record, and the completion document says the task reached a decided outcome
    — so the absence of one still means an interruption rather than an ordinary
    handled failure, which is the distinction a reader depends on.

    Only then does this raise. ``fault_tolerant=False`` is a caller saying *stop
    if anything fails*, and returning an ordinary report would make that
    indistinguishable from success to everyone who did not read it. A tolerant
    run is the opposite instruction and returns its report as it always did.

    The exception carries what the report knew: the failing node's counts, the
    partial report, and where the evidence went.
    """

    failed = [node for node in report.nodes if node.status == FAILED]
    if not failed:
        return
    first = failed[0]
    detail = next(
        (
            message.message
            for message in first.messages
            if message.severity == SEVERITY_ERROR
        ),
        first.result.error_message if first.result is not None else None,
    )
    blocked = sum(1 for node in report.nodes if node.status == BLOCKED)
    raise LoadError(
        f"{first.node_id} failed"
        + (f": {detail}" if detail else "")
        + (f"; {len(failed)} node(s) failed" if len(failed) > 1 else "")
        + (f", {blocked} blocked" if blocked else ""),
        result=first.result,
        report=report,
        task_log=report.task_log,
    )


# --- preflight ----------------------------------------------------------------
#
# One check, and it is about the *catalogue*: nobody ever built into this target,
# so there is no estate to load. Almost always a typo, and reporting it as "no
# work to do" is the single worst answer available, because it looks like
# success.
#
# Whether the physical item still exists is deliberately *not* asked here. That
# check exists to save a desktop the forty seconds of starting a Livy session for
# a request already known to be bad, so it belongs where that cost is paid —
# in the CLI, before the session — and nowhere else. By the time this runs the
# session exists, the saving is spent, and asking again would be paying for an
# answer nobody can act on. See ``weaver_cli.main._refuse_absent_targets``.
#
# An item the workspace no longer holds still fails, and says so: reading its
# inventory raises carrying the cause.


def _refuse_uninstalled_targets(estate: InstalledEstate, requested) -> None:
    """Refuse a requested target the installed estate has never heard of."""

    installed = set(estate.targets)
    unknown = [target for target in requested if target not in installed]
    if not unknown:
        return
    known = ", ".join(str(target) for target in estate.targets) or "none"
    raise CommandError(
        "no installed estate in "
        + ", ".join(str(target) for target in unknown)
        + f" — the catalogue binds no logical item to it. Installed: {known}"
    )


def _step_type(report: LoadNodeReport) -> str:
    """The broad kind a step file's name carries: a load, or a refresh."""

    return "refresh" if report.primitive_kind == ENDPOINT_REFRESH else "load"


def _plan_document(graph, state, requested, dry_run: bool) -> dict:
    """The complete intended task, written once before anything runs.

    Enough on its own to answer what was requested, what Weaver intended to run,
    in what order, against which physical targets and through which installed
    primitives — which is the whole point of writing it before rather than
    after. Every existence answer comes from the snapshot the run was planned
    against, so the record and the decision cannot disagree.
    """

    from . import __version__
    from .run.resolution import resolve

    ordered = graph.order()
    resolutions = {node.node_id: resolve(node, state) for node in ordered}
    return {
        "weaver_version": __version__,
        "requested": [str(target) for target in requested],
        "mode": "dry_run" if dry_run else "execute",
        "order": [node.node_id for node in ordered],
        "edges": [list(edge) for edge in graph.edges],
        "nodes": [
            {
                "node_id": node.node_id,
                "logical_id": str(node.logical_id) if node.logical_id else None,
                "physical_target": str(node.physical_target),
                "physical_object": str(node.physical_object)
                if node.physical_object
                else None,
                "primitive_kind": node.primitive_kind,
                "dispatch_location": resolutions[node.node_id].dispatch_location,
                "target_exists": resolutions[node.node_id].target_present,
                "primitive_exists": resolutions[node.node_id].primitive_present,
            }
            for node in ordered
        ],
        "messages": [message.to_mapping() for message in graph.messages],
    }


def _completion_document(report: LoadRunReport) -> dict:
    """What the run added up to, reconciled from the steps rather than tallied."""

    counted = {status: 0 for status in ("executed", "succeeded", "failed", "blocked")}
    rows = {
        "rows_read": 0,
        "rows_inserted": 0,
        "rows_updated": 0,
        "rows_deleted": 0,
        "rows_rejected": 0,
    }
    for node in report.nodes:
        counted["executed"] += 1 if node.executed else 0
        counted["succeeded"] += 1 if node.status in (
            SUCCEEDED,
            SUCCEEDED_WITH_REJECTS,
        ) else 0
        counted["failed"] += 1 if node.status == "failed" else 0
        counted["blocked"] += 1 if node.status == "blocked" else 0
        if node.result is not None:
            # What the result actually measured. A node that failed without
            # reaching its primitive reports no counts at all, and it
            # contributes none — which is true, because nothing was written.
            for name in rows:
                rows[name] += getattr(node.result, name, 0)
    return {
        "mode": "execute",
        "final_status": report.status,
        "planned_steps": len(report.nodes),
        "executed_steps": counted["executed"],
        "succeeded_steps": counted["succeeded"],
        "failed_steps": counted["failed"],
        "blocked_steps": counted["blocked"],
        "rows": rows,
        "messages": [message.to_mapping() for message in report.messages],
    }


def _physical_ref(target) -> PhysicalTargetRef:
    return PhysicalTargetRef(
        kind=LAKEHOUSE_TARGET if isinstance(target, DeltaTarget) else WAREHOUSE_TARGET,
        name=physical_item(target).name,
    )


# --- acquiring capabilities ---------------------------------------------------
#
# The one part that differs between the emulator, a desktop process and a Fabric
# session. Everything above this line is the same code in all three.


__all__ = ["TASK_TYPE", "load", "run_load"]
