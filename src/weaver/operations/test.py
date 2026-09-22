"""Public ``weaver.test(...)`` operation.

Item and named runs select from installed catalogue state. A file run compiles a
source validation without installing or publishing it. Reports distinguish failed
validations from validations that could not be evaluated.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from ..declaration.model import WeaverItemId
from ..errors import CommandError
from ..targets import lakehouse_names
from ..test_report import (
    FAILED,
    INVALID,
    ValidationNodeReport,
    ValidationRunReport,
    run_status,
)
from .items import requested_items, run_context_lines, run_scope
from .workspace import operation_workspace

#: Kept local to avoid importing ``weaver.run`` eagerly; must match ``TEST_TASK``.
TASK_TYPE = "test"


def test(
    items: str | Sequence[str] | None = None,
    *,
    name: str | None = None,
    file: str | Path | None = None,
    workspace: str | None = None,
    catalogue: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    dry_run: bool = False,
    session=None,
) -> ValidationRunReport:
    """Run the installed validations the named items own.

    Naming no item runs every item the Weaver catalogue records an installation
    for.

    ``name`` runs one installed validation and includes its diagnostic rows.
    ``file`` compiles and runs an uninstalled source file against exactly one
    item. The two options are mutually exclusive.

    A completed run returns its report whatever the validations found. The
    report distinguishes findings from validations that could not be evaluated.
    """

    if name is not None and file is not None:
        raise CommandError("test accepts either name= or file=, not both")

    requested = requested_items(items, what="test")
    if file is not None and not requested:
        raise CommandError(
            "test file= requires one installed item: Lakehouse/Name or Warehouse/Name"
        )
    resolved = operation_workspace(
        "test",
        workspace=workspace,
        catalogue=catalogue,
        environment=environment,
        workspace_config=workspace_config,
        session=session,
    )

    from ..sessions.host import use_or_create_session

    with use_or_create_session(session, workspace=resolved) as opened:
        with opened.task(
            "Test (dry run)" if dry_run else "Test",
            ", ".join(map(str, requested)) or "every installed item",
        ) as frame:
            report = run_test(
                opened,
                workspace=resolved,
                items=requested,
                name=name,
                file=file,
                dry_run=dry_run,
            )
            frame.failed = not report.succeeded
            return report


def run_test(
    session,
    *,
    workspace,
    items: Sequence[WeaverItemId],
    name: str | None = None,
    file: str | Path | None = None,
    state=None,
    dry_run: bool = False,
) -> ValidationRunReport:
    """Run validations through a prepared Session.

    ``state`` lets a caller provide an already-read catalogue snapshot.

    The catalogue is read before Spark starts because it holds the physical
    target, including for a file run.
    """

    from ..run import Runner, RunRequest, RunState
    from ..run.state import read_installed_catalogue

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
    session.offer_spark_home(lakehouse_names(targets))
    started = datetime.now(timezone.utc)

    if file is not None:
        from ..test_file import source_file_node

        node = source_file_node(
            session,
            targets=targets,
            path=Path(file),
            started=started,
            dry_run=dry_run,
        )
        # Source-file runs publish no estate evidence.
        return _reported(
            nodes=(node,),
            started=started,
            workflow_id=None,
        )

    with session.step("Build run graph"):
        runner = Runner(
            state,
            RunRequest.test(
                items,
                name=name,
                dry_run=dry_run,
                # Validations are independent; a finding does not block the rest.
                fault_tolerant=True,
            ),
            workspace=workspace,
        )
    from ..run import open_run_record

    record = (
        None
        if dry_run
        else open_run_record(
            state.catalogue,
            workspace=workspace,
            task_type=TASK_TYPE,
            session=session,
        )
    )
    with session.step("Execute"):
        result = runner.run(
            session=session,
            # Return diagnostics only when one validation was requested.
            dispatch=_dispatch_collecting(collect=name is not None),
            on_node=None if record is None else record.settled,
        )
    if record is not None:
        with session.step("Record what the run did"):
            record.flush()

    return _reported(
        nodes=tuple(_as_validation_node(node) for node in result.nodes),
        started=started,
        workflow_id=None if record is None else record.workflow_id,
    )


def _dispatch_collecting(*, collect: bool):
    from ..run import dispatch_primitive

    def dispatch(node, **asked):
        return dispatch_primitive(node, collect=collect, **asked)

    return dispatch


def _require_lakehouse_environment(session, *, workspace, targets, dry_run: bool):
    """Require an Environment before planning a desktop Lakehouse run."""

    from ..sessions.base import ACROSS_BOUNDARY

    if dry_run or workspace.environment or not lakehouse_names(targets):
        return
    if session.position(workspace) != ACROSS_BOUNDARY:
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


__all__ = ["TASK_TYPE", "run_test", "test"]
