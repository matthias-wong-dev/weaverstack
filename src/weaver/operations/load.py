"""Orchestrate the public ``weaver.load(...)`` operation."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from ..catalogue.state import READABLE_TABLES
from ..catalogue.tables import LOAD_STATUS
from ..declaration.model import SEMANTIC_MODEL, WeaverItemId
from ..errors import CommandError, LoadError
from ..health import assess_load, resolve_as_of
from ..installed import (
    PYTHON_FOLDER,
    PYTHON_TABLE,
    SEMANTIC_REFRESH,
    WAREHOUSE_PROCEDURE,
)
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
    warning,
)
from ..targets import lakehouse_names
from .items import requested_items, run_context_lines, run_scope

#: Kept local to avoid importing ``weaver.run`` eagerly; must match ``LOAD_TASK``.
TASK_TYPE = "load"

#: Stale objects a load here does not run, because they are mirrored.
STALE_MIRRORED = "stale_mirrored"


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
    ignore_stability_threshold: bool = False,
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

    ``ignore_stability_threshold`` waives the declared delete and update
    limits for this invocation, for when a very large change is the correct
    answer. It waives nothing else: null and unique key checks still reject
    rows, fault tolerance is unchanged, and the selection, declarations and
    bookmarks are untouched.

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
        needs_catalogue=False,
    )
    if not resolved_workspace.catalogue:
        _refuse_without_catalogue(
            requested, names=selected_names, stale=stale, reload=reload
        )

    from ..sessions.host import use_or_create_session

    with use_or_create_session(session, workspace=resolved_workspace) as opened:
        with opened.task(
            "Load (dry run)" if dry_run else "Load",
            ", ".join(map(str, requested)) or "every installed item",
        ) as frame:
            state = None
            if not resolved_workspace.catalogue:
                from ..run import RunState

                with opened.step("Resolve semantic models"):
                    state = RunState(
                        catalogue=uncatalogued_semantic_models(
                            requested, workspace=resolved_workspace, session=opened
                        )
                    )
            report = run_load(
                opened,
                workspace=resolved_workspace,
                items=requested,
                state=state,
                names=selected_names,
                fault_tolerant=fault_tolerant,
                dry_run=dry_run,
                reload=reload,
                ignore_stability_threshold=ignore_stability_threshold,
                stale=stale,
                as_of=threshold,
            )
            frame.failed = not report.succeeded
            return report


def _refuse_without_catalogue(requested, *, names, stale, reload) -> None:
    others = sorted(str(i) for i in requested if i.item_type != SEMANTIC_MODEL)
    if not requested or others:
        raise CommandError(
            "Loading "
            + (", ".join(others) or "every installed item")
            + " needs a Weaver catalogue: pass catalogue='Warehouse/Weaver', or "
            "give one in workspace configuration. Without one, name the semantic "
            "models to refresh"
        )
    if names or stale or reload:
        raise CommandError(
            "--name, --stale and --reload need a Weaver catalogue; without one, "
            "load refreshes the named semantic models"
        )


def uncatalogued_semantic_models(items, *, workspace, session):
    """Installed rows for semantic models in a workspace with no catalogue.

    Without a catalogue nothing records an installation, so each named model is
    resolved in the workspace and refreshed as it stands.
    """

    from ..catalogue.state import Catalogue
    from ..targets import physical_item

    rows = {}
    for item in items:
        name = (
            physical_item(workspace.target_for(item)).name
            if item in workspace.configured_items
            else item.item_name
        )
        model = session.resolve_item(name, item_type=SEMANTIC_MODEL)
        identity = {
            "item_type": item.item_type,
            "item_name": item.item_name,
            "schema_name": "",
            "object_name": "",
        }
        rows[item] = {
            "Installation": (
                {
                    "item_type": item.item_type,
                    "item_name": item.item_name,
                    "target_name": name,
                    "workspace_id": model.workspace_id,
                    "item_id": model.id,
                },
            ),
            "Registry": (
                {
                    **identity,
                    "object_type": "semantic_model",
                    "object_role": "data",
                    "signature": "uncatalogued",
                },
            ),
            "SemanticModel": (
                {**identity, "signature": "uncatalogued", "definition": "{}"},
            ),
        }
    return Catalogue(rows)


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
    ignore_stability_threshold: bool = False,
    stale: bool = False,
    as_of: datetime | None = None,
) -> LoadRunReport:
    """Run the catalogue graph through a Session.

    The catalogue is read before Spark starts because it holds the physical
    Lakehouse attachment and may show that the installation is missing.

    ``stale`` assesses the catalogue this read and runs the subjects that are
    not green. ``as_of`` is the freshness cutoff, resolved by the caller.
    """

    from ..run import RunRequest, RunState
    from ..run.entry import run_load_in_fabric
    from ..run.runner import needs_spark
    from ..run.state import read_installed_catalogue
    from ..sessions.program import FabricRun

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
    left = ()
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
        assessment = assess_load(
            catalogue,
            as_of=as_of if as_of is not None else resolve_as_of(None, started=started),
            items=items,
            source=source,
        )
        selected = assessment.unsettled_identities()
        left = _mirrored_behind(assessment, workspace)

    # Fabric requires a Lakehouse attachment before Spark starts.
    session.offer_spark_home(lakehouse_names(installed.values()), workspace=workspace)

    request = RunRequest.load(
        items,
        names=names,
        selected=selected,
        fault_tolerant=fault_tolerant,
        dry_run=dry_run,
        reload=reload,
        ignore_stability_threshold=ignore_stability_threshold,
    )
    with session.step("Build run graph"):
        if state is None:
            state = RunState(catalogue=catalogue)
        runner = load_runner(session, workspace, state, request)
        if reload:
            # Refuse unsupported work before execution or dry-run reporting.
            _refuse_unsupported_reload(runner.plan())

    run = FabricRun(
        name="load",
        needs_spark=not dry_run and needs_spark(runner.graph),
        call=lambda here: execute_load(
            here, workspace=workspace, runner=runner, started=started
        ),
        entry=run_load_in_fabric,
        arguments=lambda: {
            "catalogue": state.catalogue.to_mapping(),
            "request": request.to_mapping(),
            "started": started.isoformat(),
        },
        decode=LoadRunReport.from_mapping,
    )
    report = session.execute_run(run, workspace=workspace)
    if left:
        report = replace(report, messages=(*report.messages, *left))
    if not fault_tolerant and not dry_run:
        _raise_for_failure(report)
    return report


def _mirrored_behind(assessment, workspace) -> tuple:
    """Say which stale objects a load here leaves, because they are mirrored."""

    behind = [subject for subject in assessment.unsettled() if subject.node.is_mirrored]
    if not behind:
        return ()
    count = len(behind)
    noun = "object is" if count == 1 else "objects are"
    return (
        warning(
            STALE_MIRRORED,
            f"{count} mirrored {noun} behind in {workspace.mirror}.",
        ),
    )


def load_runner(session, workspace, state, request):
    """The Runner for a load, for the Session that executes it."""

    from ..run import Runner, can_refresh

    return Runner(
        state,
        request,
        workspace=workspace,
        can_refresh=can_refresh(session, workspace),
    )


def execute_load(session, *, workspace, runner, started) -> LoadRunReport:
    """Execute a planned load and record what it did, where ``session`` runs."""

    from ..run import dispatch_primitive, open_run_record
    from ..run.runner import Lanes

    # A dry run must not create run evidence or move bookmarks, and a workspace
    # with no catalogue has nowhere to record it.
    record = (
        None
        if runner.request.dry_run or not workspace.catalogue
        else open_run_record(
            runner.state.catalogue,
            workspace=workspace,
            task_type=TASK_TYPE,
            session=session,
        )
    )
    with session.step("Execute"):
        result = runner.run(
            session=session,
            dispatch=dispatch_primitive,
            on_node=None if record is None else record.settled,
            before_node=None
            if record is None or not runner.request.reload
            else _reset_before(record),
            # Independent branches of a load run at once.
            lanes=Lanes.configured(workspace),
        )

    if record is not None:
        # Flush durable evidence before reporting success or raising failure.
        with session.step("Record what the run did"):
            record.flush()

    return _as_load_report(result, started=started, record=record)


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
        ignore_stability_threshold=result.ignore_stability_threshold,
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
    named = f"{_step_type(first).title()} failed for {subject}"
    counted = f"; {summaries}" if summaries else ""
    raise LoadError(
        named + (f": {detail}" if detail else "") + counted,
        result=first.result,
        report=report,
        workflow_id=report.workflow_id,
        executor=None if finding is None else finding.executor,
        # The node's own detail is in the report. A caller that presented it
        # says only which node failed and how the run finished.
        summary=named + counted,
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
    if report.nodes and all(
        node.primitive_kind == SEMANTIC_REFRESH for node in report.nodes
    ):
        rows = {}
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
