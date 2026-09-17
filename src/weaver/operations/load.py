"""Orchestrate the public ``weaver.load(...)`` operation."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from ..catalogue.state import READABLE_TABLES
from ..catalogue.tables import LOAD_STATUS
from ..declaration.model import WeaverItemId
from ..errors import CommandError, LoadError
from ..health import assess_load, resolve_as_of
from ..installed import PYTHON_FOLDER, PYTHON_TABLE, WAREHOUSE_PROCEDURE
from ..load_plan import ENDPOINT_REFRESH, ONELAKE_PUBLICATION
from ..load_report import (
    BLOCKED,
    FAILED,
    PENDING,
    SEVERITY_ERROR,
    SKIPPED,
    SUCCEEDED,
    SUCCEEDED_WITH_REJECTS,
    LoadNodeReport,
    LoadRunReport,
)
from ..targets import lakehouse_names
from .items import requested_items, run_context_lines, run_scope

#: Kept local to avoid importing ``weaver.run`` eagerly; must match ``LOAD_TASK``.
TASK_TYPE = "load"


def load(
    items: str | Sequence[str] | None = None,
    *,
    names: str | Sequence[str] | None = None,
    workspace: str | None = None,
    catalogue: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    fault_tolerant: bool = False,
    dry_run: bool = False,
    reload: bool = False,
    stale: bool = False,
    as_of: str | datetime | None = None,
    session=None,
) -> LoadRunReport:
    """Load the installed objects the named items own.

    ``items`` are installed Weaver items, and they are a hard execution boundary:
    with no name filter every loadable object they own runs in dependency order,
    and a dependency never adds an unnamed item. Naming none loads every item the
    Weaver catalogue records an installation for.

    ``names`` selects installed loadables inside those items. A Lakehouse
    selector carries its area, ``Tables/Schema.Object`` or
    ``Files/Schema.Object``; a Warehouse relation has none, and a bare
    ``Schema.Object`` is accepted where it reaches one object. It is an operator
    override: only those nodes run, without dependency expansion or dependency
    ordering.

    ``reload`` reconstructs each selected table from zero: its ``_.Bookmark`` row
    is removed, its ``_.LoadStatus`` goes to Pending, its target is emptied, and
    the authored load runs. It reaches what this request selected and nothing
    downstream.

    ``stale`` runs the loadables ``weaver health`` reports as not green.
    Selecting nothing is a success. ``as_of`` is the freshness cutoff that
    selection measures against, and it requires ``stale``.

    A supplied ``session`` carries its resolved Workspace and is left open.
    """

    started = datetime.now(timezone.utc)
    requested = requested_items(items, what="load")
    selected_names = _load_names(names)
    _refuse_conflicting_modes(stale=stale, reload=reload, as_of=as_of)
    # Before the workspace is resolved, so a malformed instant is refused
    # without reaching a tenant.
    threshold = resolve_as_of(as_of, started=started)

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

    with use_or_create_session(session, workspace=resolved_workspace) as opened:
        with opened.task(
            "Load (dry run)" if dry_run else "Load",
            ", ".join(map(str, requested)) or "every installed item",
        ) as frame:
            report = run_load(
                opened,
                workspace=resolved_workspace,
                items=requested,
                names=selected_names,
                fault_tolerant=fault_tolerant,
                dry_run=dry_run,
                reload=reload,
                stale=stale,
                as_of=threshold,
            )
            frame.failed = not report.succeeded
            return report


def _refuse_conflicting_modes(*, stale: bool, reload: bool, as_of) -> None:
    if stale and reload:
        raise CommandError("--reload and --stale cannot be used together")
    if as_of is not None and not stale:
        raise CommandError("--as-of requires --stale")


def run_load(
    session,
    *,
    workspace,
    items: Sequence[WeaverItemId],
    names: Sequence[str] = (),
    state=None,
    fault_tolerant: bool = False,
    dry_run: bool = False,
    reload: bool = False,
    stale: bool = False,
    as_of: datetime | None = None,
) -> LoadRunReport:
    """Run the catalogue graph through a Session.

    The catalogue is read before Spark starts because it holds the physical
    Lakehouse attachment and may show that the installation is missing.

    ``stale`` assesses the catalogue this read and runs the subjects that are
    not green. ``as_of`` is the freshness cutoff, resolved by the caller.
    """

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
        # Read installation and load state together to avoid one trip per object.
        catalogue = (
            state.catalogue
            if state is not None
            else read_installed_catalogue(
                session=session,
                workspace=workspace,
                # Stale selection also needs current load state.
                tables=(*READABLE_TABLES, LOAD_STATUS) if stale else None,
            )
        )
        # Resolve an empty scope against the catalogue just read.
        items, installed = run_scope(
            catalogue.dag(), items, what="load", catalogue=workspace.catalogue
        )
    session.report(run_context_lines(workspace, items, installed))

    selected = None
    if stale:
        from .mirror import mirrored_source

        # Mirrored load state is recorded at its source. Only local descendants
        # are eligible for stale selection.
        source = mirrored_source(
            catalogue,
            workspace=workspace,
            session=session,
            operation="load --stale",
            tables=(LOAD_STATUS,),
        )
        selected = assess_load(
            catalogue,
            as_of=as_of if as_of is not None else resolve_as_of(None, started=started),
            items=items,
            source=source,
        ).unsettled_identities()

    # Fabric requires a Lakehouse attachment before Spark starts.
    session.offer_spark_home(lakehouse_names(installed.values()))

    with session.step("Build run graph"):
        if state is None:
            state = RunState(catalogue=catalogue)
        runner = Runner(
            state,
            RunRequest.load(
                items,
                names=names,
                selected=selected,
                fault_tolerant=fault_tolerant,
                dry_run=dry_run,
                reload=reload,
            ),
            workspace=workspace,
            can_refresh=can_refresh(session, workspace),
        )
        if reload:
            # Refuse unsupported work before execution or dry-run reporting.
            _refuse_unsupported_reload(runner.plan())

    # A dry run must not create run evidence or move bookmarks.
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
            before_node=None if record is None or not reload else _reset_before(record),
        )

    if record is not None:
        # Flush durable evidence before reporting success or raising failure.
        with session.step("Record what the run did"):
            record.flush()

    report = _as_load_report(result, started=started, record=record)
    if not fault_tolerant and not dry_run:
        _raise_for_failure(report)
    return report


#: The primitive kinds a reload can reconstruct.
RELOADABLE_KINDS = (WAREHOUSE_PROCEDURE, PYTHON_TABLE)


def _refuse_unsupported_reload(graph) -> None:
    refused = sorted(
        node.node_id for node in graph.nodes if node.primitive_kind == PYTHON_FOLDER
    )
    if refused:
        raise CommandError(
            "reload supports tables only; this selection includes folders: "
            + ", ".join(refused)
            + ". Select the tables by name, or load without reload."
        )


def _reset_before(record):
    """Invalidate each reloaded object's load state as the run reaches it.

    Per node, so a node the run never dispatches keeps its bookmark.
    """

    def reset(node) -> None:
        if node.primitive_kind in RELOADABLE_KINDS and node.logical_id is not None:
            record.reset(node.logical_id)

    return reset


def _as_load_report(result, *, started, record) -> LoadRunReport:
    return LoadRunReport(
        requested=result.requested,
        status=result.status,
        dry_run=result.dry_run,
        fault_tolerant=result.fault_tolerant,
        reload=result.reload,
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
    """Raise only after the run's final state has been recorded durably."""

    failed = [node for node in report.nodes if node.status == FAILED]
    if not failed:
        return
    first = failed[0]
    finding = next(
        (message for message in first.messages if message.severity == SEVERITY_ERROR),
        None,
    )
    detail = (
        finding.message
        if finding is not None
        else first.result.error_message
        if first.result is not None
        else None
    )
    subject = first.logical_id or first.physical_target
    summaries = "; ".join(_status_summaries(report))
    raise LoadError(
        f"{_step_type(first).title()} failed for {subject}"
        + (f": {detail}" if detail else "")
        + (f"; {summaries}" if summaries else ""),
        result=first.result,
        report=report,
        workflow_id=report.workflow_id,
        executor=None if finding is None else finding.executor,
    )


#: What a step file is called, for the kinds that are not a load.
_STEP_TYPES = {ENDPOINT_REFRESH: "refresh", ONELAKE_PUBLICATION: "publication"}


def _step_type(report: LoadNodeReport) -> str:
    return _STEP_TYPES.get(report.primitive_kind, "load")


SUMMARY_STATUSES = (
    SUCCEEDED,
    SUCCEEDED_WITH_REJECTS,
    FAILED,
    BLOCKED,
    PENDING,
    SKIPPED,
)


def status_counts(report: LoadRunReport) -> dict[str, dict[str, int]]:
    """Count detailed rows by step category and status."""

    grouped = {"load": {status: 0 for status in SUMMARY_STATUSES}}
    for node in report.nodes:
        kind = _step_type(node)
        counts = grouped.setdefault(kind, {status: 0 for status in SUMMARY_STATUSES})
        if node.status in counts:
            counts[node.status] += 1
    return grouped


def _status_summaries(report: LoadRunReport) -> tuple[str, ...]:
    grouped = status_counts(report)
    lines = []
    for kind in ("load", "refresh", "publication"):
        counts = grouped.get(kind)
        if counts is None or not any(counts.values()):
            continue
        values = ", ".join(
            f"{counts[status]} {status.replace('_', ' ')}"
            for status in SUMMARY_STATUSES
            if counts[status]
        )
        lines.append(f"{kind.title()} summary: {values}")
    return tuple(lines)


def _completion_document(report: LoadRunReport, timings=()) -> dict:
    """Reconcile totals and closing-order timings from the run's steps."""

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
            # A node that never reached its primitive contributes no row counts.
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
    if names is None:
        return ()
    values = (names,) if isinstance(names, str) else tuple(names)
    if not values:
        raise CommandError("load names= must contain at least one load selector")
    return tuple(str(value) for value in values)


__all__ = ["SUMMARY_STATUSES", "TASK_TYPE", "load", "run_load", "status_counts"]
