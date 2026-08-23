"""Public ``weaver.load(...)`` entry point and orchestration.

Loads read installed state from the catalogue, construct and resolve a physical
DAG, then dispatch primitives and record what each one did.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from ..errors import CommandError, LoadError
from ..load_plan import (
    ENDPOINT_REFRESH,
    InstalledEstate,
    PhysicalTargetRef,
    lakehouse_names,
)
from ..load_report import (
    BLOCKED,
    FAILED,
    SEVERITY_ERROR,
    SUCCEEDED,
    SUCCEEDED_WITH_REJECTS,
    LoadNodeReport,
    LoadRunReport,
)
from ..targets import (
    parse_physical_target,
)

#: The task type this operation records under. Restated rather than imported,
#: because this module reaches ``weaver.run`` inside the function that needs it;
#: ``tests/targeted/test_run_record_representation.py`` asserts it matches
#: :data:`weaver.run.record.LOAD_TASK`.
TASK_TYPE = "load"


def load(
    targets: str | Sequence[str],
    *,
    names: str | Sequence[str] | None = None,
    workspace: str | None = None,
    catalogue: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    fault_tolerant: bool = False,
    dry_run: bool = False,
    session=None,
) -> LoadRunReport:
    """Load installed objects within the requested physical targets.

    ``targets`` are typed physical items — ``Lakehouse/Curated``,
    ``Warehouse/Reporting`` — and they are a hard execution boundary. With no
    name filter, every loadable object hosted there runs in dependency order;
    dependencies never add an unrequested target.

    ``names`` selects exact installed ``Schema.Object`` loadables inside those
    targets. It is an operator override: only those nodes run, without dependency
    expansion or dependency ordering.

    ``workspace``, ``catalogue`` and ``environment`` are names, resolved as
    ``build`` resolves them; ``session`` is where an already-resolved
    ``Workspace`` travels.

    .. code-block:: text

        an explicit name
          → a workspace configuration file
            → the Session's own workspace
              → what the notebook is attached to
                → a configuration error naming what is missing
    """

    values = (targets,) if isinstance(targets, str) else tuple(targets)
    if not values:
        raise CommandError("load needs at least one target")
    requested = tuple(
        parse_physical_target(value, what="load target", error=CommandError)
        for value in values
    )
    selected_names = _load_names(names)

    from .workspace import operation_workspace

    resolved_workspace = operation_workspace(
        "load",
        workspace=workspace,
        catalogue=catalogue,
        environment=environment,
        workspace_config=workspace_config,
        session=session,
    )

    from ..sessions.host import use_or_create_session

    refs = tuple(PhysicalTargetRef.of(target) for target in requested)

    with use_or_create_session(session, workspace=resolved_workspace) as opened:
        # Fabric attaches a Spark session to a Lakehouse, so a host that crosses
        # needs one of the Lakehouses this load is actually for.
        opened.offer_spark_home(lakehouse_names(refs))
        with opened.task(
            "Load (dry run)" if dry_run else "Load", ", ".join(map(str, requested))
        ):
            return run_load(
                opened,
                workspace=resolved_workspace,
                requested=refs,
                names=selected_names,
                fault_tolerant=fault_tolerant,
                dry_run=dry_run,
            )


def run_load(
    session,
    *,
    workspace,
    requested: Sequence[PhysicalTargetRef],
    names: Sequence[str] = (),
    state=None,
    fault_tolerant: bool = False,
    dry_run: bool = False,
) -> LoadRunReport:
    """Run the catalogue graph through a Session."""

    from ..run import (
        Runner,
        RunRequest,
        RunState,
        can_refresh,
        dispatch_primitive,
        open_run_record,
    )
    from ..run.state import read_installed_catalogue

    started = datetime.now(timezone.utc)
    with session.step("Read catalogue"):
        # One catalogue, read once: what is installed, and how far each object
        # has been loaded. A run of two hundred objects would otherwise be two
        # hundred round trips for one table's contents.
        catalogue = (
            state.catalogue
            if state is not None
            else read_installed_catalogue(session=session, workspace=workspace)
        )
        estate = InstalledEstate.from_catalogue(catalogue)
        _refuse_uninstalled_targets(estate, requested)

    with session.step("Build run graph"):
        if state is None:
            state = RunState(catalogue=catalogue)
        runner = Runner(
            state,
            RunRequest.load(
                requested,
                names=names,
                fault_tolerant=fault_tolerant,
                dry_run=dry_run,
            ),
            workspace=workspace,
            can_refresh=can_refresh(session, workspace),
        )

    # A dry run writes nothing durable: a row for work nobody did would be
    # evidence of a load that never happened, and a bookmark it moved would make
    # the next real load skip a window nothing had read.
    record = (
        None
        if dry_run
        else open_run_record(
            catalogue, workspace=workspace, task_type=TASK_TYPE, session=session
        )
    )
    with session.step("Execute"):
        result = runner.run(
            session=session,
            dispatch=dispatch_primitive,
            on_node=None if record is None else record.settled,
        )

    if record is not None:
        # Before the report, and before any failure is raised: a caller told the
        # load succeeded must be able to rely on what it recorded being durable.
        with session.step("Record what the run did"):
            record.flush()

    report = _as_load_report(result, started=started, record=record)
    if not fault_tolerant and not dry_run:
        _raise_for_failure(report)
    return report


def _as_load_report(result, *, started, record) -> LoadRunReport:
    """One RunResult, rendered as the shape a load's readers expect.

    One internal model, several public shapes. A load reader wants rows moved
    and a workflow to correlate its evidence by.
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
        workflow_id=None if record is None else record.workflow_id,
        started_at=started.isoformat(),
        finished_at=result.finished_at,
    )


def _raise_for_failure(report: LoadRunReport) -> None:
    """Turn an intolerant run's recorded failure into the exception it is.

    Everything durable is written first: every planned node has its final
    record and the completion document says the task reached a decided outcome,
    so a missing completion still means an interruption rather than a handled
    failure.

    Only then does this raise. ``fault_tolerant=False`` means stop if anything
    fails, and an ordinary report would be indistinguishable from success. The
    exception carries the failing node's counts, the partial report, and where
    the evidence went.
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
        workflow_id=report.workflow_id,
    )


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


def _completion_document(report: LoadRunReport, timings=()) -> dict:
    """What the run added up to, reconciled from the steps rather than tallied.

    ``timings`` are the frames this run closed, in closing order. They ride the
    completion document rather than a file of their own: how long a step took is
    a property of that step.
    """

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
        counted["succeeded"] += (
            1
            if node.status
            in (
                SUCCEEDED,
                SUCCEEDED_WITH_REJECTS,
            )
            else 0
        )
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
        "timings": [frame.to_mapping() for frame in timings],
    }


def _load_names(names: str | Sequence[str] | None) -> tuple[str, ...]:
    """Normalise the notebook convenience spelling into the request contract."""

    if names is None:
        return ()
    values = (names,) if isinstance(names, str) else tuple(names)
    if not values:
        raise CommandError("load names= needs at least one Schema.Object")
    return tuple(str(value) for value in values)


__all__ = ["TASK_TYPE", "load", "run_load"]
