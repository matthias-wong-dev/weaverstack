"""Public ``weaver.test(...)`` operation.

Catalogue mode runs installed validations and records their results. File mode
runs validations from the project folder against deployed objects and records
nothing. Reports distinguish failed validations from validations that could not
be evaluated.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from ..declaration.model import WeaverItemId
from ..errors import CommandError
from ..selection import matching, name_patterns
from ..targets import lakehouse_names
from ..test_report import (
    FAILED,
    INVALID,
    ValidationNodeReport,
    ValidationRunReport,
    run_status,
)
from .items import requested_items, run_context_lines, run_scope
from .project import Project
from .workspace import operation_workspace

#: Kept local to avoid importing ``weaver.run`` eagerly; must match ``TEST_TASK``.
TASK_TYPE = "test"


def test(
    items: str | Sequence[str] | None = None,
    *,
    names: str | Sequence[str] | None = None,
    files: str | Path | Sequence[str | Path] | None = None,
    source=None,
    workspace: str | None = None,
    catalogue: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    dry_run: bool = False,
    session=None,
) -> ValidationRunReport:
    """Run the Tests and Assumptions of the named items.

    With a Weaver catalogue this is catalogue mode: it runs the validations
    installed for the items and records TestStatus and Log. ``files``, or a
    workspace with no catalogue, selects file mode: validations are read from
    the project folder at ``source`` and run against the deployed objects, and
    nothing is recorded. An Expected source resolves through workspace
    configuration ``targets:`` in file mode.

    Naming no item runs every installed item in catalogue mode and every item in
    the project folder in file mode. ``PowerBI`` and ``PowerBI/<project>`` name
    the semantic models of the project folder.

    ``names`` are regular expressions, each matched against a whole validation
    ``Schema.Object``, ignoring case. Each must match something, and only the
    matches run, with their diagnostic rows.

    ``files`` are validation files, directories or glob patterns. A file the
    project folder declares runs against its own item; any other file runs
    against the one item named.

    ``source`` defaults to the current directory or Notebook Resources.

    A completed run returns its report whatever the validations found. The
    report distinguishes findings from validations that could not be evaluated.
    """

    selected_names = tuple(text for text, _ in name_patterns(names, error=CommandError))
    selected_files = _files(files)
    resolved = operation_workspace(
        "test",
        workspace=workspace,
        catalogue=catalogue,
        environment=environment,
        workspace_config=workspace_config,
        session=session,
        needs_catalogue=False,
    )
    project = Project(source, resolved)
    requested = requested_items(items, what="test", project=project)
    from_files = bool(selected_files) or not resolved.catalogue

    from ..sessions.host import use_or_create_session

    with use_or_create_session(session, workspace=resolved) as opened:
        with opened.task(
            "Test (dry run)" if dry_run else "Test",
            ", ".join(map(str, requested))
            or ("every item" if from_files else "every installed item"),
        ) as frame:
            if from_files:
                report = run_source_test(
                    opened,
                    workspace=resolved,
                    project=project,
                    items=requested,
                    names=selected_names,
                    files=selected_files,
                    dry_run=dry_run,
                )
            else:
                report = run_test(
                    opened,
                    workspace=resolved,
                    items=requested,
                    names=selected_names,
                    dry_run=dry_run,
                )
            frame.failed = not report.succeeded
            return report


def _files(files) -> tuple[str, ...]:
    if files is None:
        return ()
    values = (files,) if isinstance(files, (str, Path)) else tuple(files)
    return tuple(str(value) for value in values)


def run_test(
    session,
    *,
    workspace,
    items: Sequence[WeaverItemId],
    names: Sequence[str] = (),
    state=None,
    dry_run: bool = False,
) -> ValidationRunReport:
    """Run installed validations through a prepared Session, and record them.

    ``state`` lets a caller provide an already-read catalogue snapshot.

    The catalogue is read before Spark starts because it holds the physical
    target.
    """

    from ..run import RunRequest, RunState
    from ..run.entry import run_test_in_fabric
    from ..run.runner import needs_spark
    from ..run.state import read_installed_catalogue
    from ..sessions.program import FabricRun

    with session.step("Read catalogue"):
        if state is None:
            state = RunState(
                catalogue=read_installed_catalogue(session=session, workspace=workspace)
            )
        # Resolve an empty scope against the catalogue just read.
        items, installed = run_scope(
            state.catalogue.dag(), items, what="test", catalogue=workspace.catalogue
        )
    session.report(run_context_lines(workspace, items, installed))
    targets = tuple(installed[item] for item in items)

    _require_lakehouse_environment(
        session, workspace=workspace, targets=targets, dry_run=dry_run
    )
    # Fabric requires a Lakehouse attachment before Spark starts.
    session.offer_spark_home(lakehouse_names(targets), workspace=workspace)
    started = datetime.now(timezone.utc)

    request = RunRequest.test(
        items,
        names=names,
        dry_run=dry_run,
        # Validations are independent; a finding does not block the rest.
        fault_tolerant=True,
    )
    with session.step("Build run graph"):
        runner = validation_runner(workspace, state, request)

    run = FabricRun(
        name="test",
        needs_spark=not dry_run and needs_spark(runner.graph),
        call=lambda here: execute_test(
            here, workspace=workspace, runner=runner, started=started
        ),
        entry=run_test_in_fabric,
        arguments=lambda: {
            "catalogue": state.catalogue.to_mapping(),
            "request": request.to_mapping(),
            "started": started.isoformat(),
        },
        decode=_decoded,
    )
    return session.execute_run(run, workspace=workspace)


def run_source_test(
    session,
    *,
    workspace,
    project,
    items: Sequence[WeaverItemId],
    names: Sequence[str] = (),
    files: Sequence[str] = (),
    dry_run: bool = False,
) -> ValidationRunReport:
    """Run validations from source through a prepared Session, recording nothing."""

    from ..run.entry import run_source_test_in_fabric
    from ..sessions.program import FabricRun
    from ..test_file import (
        file_validations,
        project_validations,
    )
    from .items import uncatalogued_target

    with session.step("Read validations"):
        validations = (
            file_validations(files, project=project, items=items)
            if files
            else project_validations(project, items)
        )
        validations = _named(validations, names)
    targets = tuple(
        dict.fromkeys(uncatalogued_target(workspace, each.item) for each in validations)
    )
    _require_lakehouse_environment(
        session, workspace=workspace, targets=targets, dry_run=dry_run
    )
    session.offer_spark_home(lakehouse_names(targets), workspace=workspace)

    started = datetime.now(timezone.utc)
    run = FabricRun(
        name="test",
        needs_spark=not dry_run and bool(lakehouse_names(targets)),
        call=lambda here: execute_source_test(
            here,
            workspace=workspace,
            validations=validations,
            started=started,
            dry_run=dry_run,
            collect=bool(names or files),
        ),
        entry=run_source_test_in_fabric,
        arguments=lambda: {
            "validations": [each.to_mapping() for each in validations],
            "started": started.isoformat(),
            "dry_run": dry_run,
            "collect": bool(names or files),
        },
        decode=_decoded,
        records_catalogue=False,
    )
    return session.execute_run(run, workspace=workspace)


def execute_source_test(
    session, *, workspace, validations, started, dry_run=False, collect=False
) -> ValidationRunReport:
    from ..test_file import source_validation_nodes

    with session.step("Execute"):
        nodes = source_validation_nodes(
            session,
            workspace=workspace,
            validations=validations,
            started=started,
            dry_run=dry_run,
            collect=collect,
        )
    return _reported(nodes=nodes, started=started, workflow_id=None)


def _named(validations, names: Sequence[str]):
    patterns = name_patterns(names, error=CommandError)
    if not patterns:
        return validations
    known = ", ".join(sorted(each.qualified for each in validations)) or "none"
    return matching(
        patterns,
        validations,
        name=lambda validation: validation.qualified,
        unmatched=lambda text: CommandError(
            f"no validation to run matches '{text}'. Validations: {known}"
        ),
    )


def validation_runner(workspace, state, request):
    """The Runner for a test run."""

    from ..run import Runner

    return Runner(state, request, workspace=workspace)


def execute_test(session, *, workspace, runner, started) -> ValidationRunReport:
    """Execute a planned test run and record what it found, where ``session`` runs."""

    from ..run import open_run_record
    from ..run.runner import Lanes

    record = (
        None
        if runner.request.dry_run
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
            # Return diagnostics only for validations selected by name.
            dispatch=_dispatch_collecting(collect=bool(runner.request.names)),
            on_node=None if record is None else record.settled,
            # Validations are independent, so they run at once.
            lanes=Lanes.configured(workspace),
        )
    if record is not None:
        with session.step("Record what the run did"):
            record.flush()

    return _reported(
        nodes=tuple(_as_validation_node(node) for node in result.nodes),
        started=started,
        workflow_id=None if record is None else record.workflow_id,
    )


def _decoded(carried: dict) -> ValidationRunReport:
    """A report Fabric returned, with the diagnostic rows that crossed beside it."""

    from dataclasses import replace

    report = ValidationRunReport.from_mapping(carried)
    diagnostics = carried.get("diagnostics") or {}
    return replace(
        report,
        nodes=tuple(
            replace(node, diagnostics=tuple(diagnostics[node.logical_id]))
            if node.logical_id in diagnostics
            else node
            for node in report.nodes
        ),
    )


def _dispatch_collecting(*, collect: bool):
    from ..run import dispatch_primitive

    def dispatch(node, **asked):
        return dispatch_primitive(node, collect=collect, **asked)

    return dispatch


def _require_lakehouse_environment(session, *, workspace, targets, dry_run: bool):
    """Require an Environment before planning a desktop Lakehouse run."""

    from ..sessions.base import CLIENT

    if dry_run or workspace.environment or not lakehouse_names(targets):
        return
    if session.position(workspace) != CLIENT:
        return
    target = next(target for target in targets if target.is_lakehouse)
    raise CommandError(
        f"{target} requires a Fabric Environment with Weaver installed. Pass "
        "--environment <Environment | Workspace/Environment>, or set environment "
        "in workspace configuration."
    )


def _as_validation_node(node) -> ValidationNodeReport:
    """Render work status as passed, failed or invalid validation state."""

    from ..run.result import INVALID as RUN_INVALID
    from ..run.result import SUCCEEDED, VALIDATED
    from ..test_report import PASSED, PLANNED

    if node.status == VALIDATED:
        status = PLANNED
    elif node.status == SUCCEEDED:
        status = PASSED
    elif node.status == RUN_INVALID or getattr(node, "raised", False):
        # A validation that could not run is invalid, not a data finding.
        status = INVALID
    else:
        status = FAILED
    result = getattr(node.result, "result", node.result)
    if status == INVALID and not _has_result_for_validation(node.role, result):
        result = _failed_validation_result(node, result)
    return ValidationNodeReport(
        logical_id=node.logical_id,
        kind=node.role or "Test",
        physical_target=node.physical_target,
        primitive_kind=node.primitive_kind,
        dispatch_location=str(getattr(node, "dispatch_location", None) or ""),
        status=status,
        executed=node.executed,
        messages=tuple(
            message.message if hasattr(message, "message") else str(message)
            for message in node.messages
        ),
        result=result,
        diagnostics=getattr(node.result, "diagnostics", None),
        started_at=node.started_at,
        finished_at=node.finished_at,
    )


def _has_result_for_validation(kind, result) -> bool:
    from ..declaration.metadata import ASSUMPTION

    field = "violation_count" if kind == ASSUMPTION else "missing_count"
    return hasattr(result, field)


def _failed_validation_result(node, result):
    from ..declaration.metadata import ASSUMPTION
    from ..runtime.validation_result import AssumptionResult, TestResult

    message = getattr(result, "error_message", None) or "could not run"
    if node.role == ASSUMPTION:
        return AssumptionResult.failed_to_run(message)
    return TestResult.failed_to_run(message)


def _reported(
    *,
    nodes: Sequence[ValidationNodeReport],
    started: datetime,
    workflow_id: str | None,
) -> ValidationRunReport:
    return ValidationRunReport(
        status=run_status(nodes),
        nodes=tuple(nodes),
        workflow_id=workflow_id,
        started_at=started.isoformat(),
        finished_at=datetime.now(timezone.utc).isoformat(),
    )


__all__ = ["TASK_TYPE", "run_source_test", "run_test", "test"]
