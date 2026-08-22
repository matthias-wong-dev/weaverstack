"""Public ``weaver.test(...)`` entry point and orchestration.

Target and named runs use installed catalogue state. File runs compile a source
validation without installing or publishing it. Reports distinguish failed
validations from validations that could not be evaluated.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from ..errors import CommandError, ValidationError
from ..load_plan import PhysicalTargetRef, lakehouse_names
from ..targets import parse_physical_target
from ..test_report import (
    FAILED,
    INVALID,
    ValidationNodeReport,
    ValidationRunReport,
    run_status,
)
from .workspace import operation_workspace

TASK_TYPE = "test"


def test(
    targets: str | Sequence[str],
    *,
    name: str | None = None,
    file: str | Path | None = None,
    workspace: str | None = None,
    catalogue: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    dry_run: bool = False,
    strict: bool = False,
    session=None,
) -> ValidationRunReport:
    """Run the installed validation in the requested physical targets.

    ``name`` runs one installed validation and returns its diagnostic rows
    alongside its counts; ``file`` compiles and runs a source file without
    installing it. Mutually exclusive: one names something the estate has, the
    other something it may not.
    """

    if name is not None and file is not None:
        raise CommandError(
            "test takes name= or file=, not both — one runs what is installed "
            "and the other runs a source file that may not be"
        )

    requested = _requested(targets)
    resolved = operation_workspace(
        "test",
        workspace=workspace,
        catalogue=catalogue,
        environment=environment,
        workspace_config=workspace_config,
        session=session,
    )
    refs = tuple(PhysicalTargetRef.of(target) for target in requested)

    from ..sessions.host import use_or_create_session

    with use_or_create_session(session, workspace=resolved) as opened:
        # Fabric attaches a Spark session to a Lakehouse, so a host that crosses
        # needs one of the Lakehouses this run is actually for.
        opened.offer_spark_home(lakehouse_names(refs))
        with opened.task(
            "Test (dry run)" if dry_run else "Test", ", ".join(map(str, refs))
        ):
            return run_test(
                opened,
                workspace=resolved,
                requested=refs,
                name=name,
                file=file,
                dry_run=dry_run,
                strict=strict,
            )


def run_test(
    session,
    *,
    workspace,
    requested: Sequence[PhysicalTargetRef],
    name: str | None = None,
    file: str | Path | None = None,
    state=None,
    dry_run: bool = False,
    strict: bool = False,
) -> ValidationRunReport:
    """The whole orchestration path, over a prepared session.

    Separated from :func:`test` as :func:`weaver.operations.load.run_load` is: workspace
    resolution and capability acquisition differ between positions, and none of
    them changes the orchestration.

    ``state`` lets a caller provide an already-read catalogue snapshot.
    """

    _require_lakehouse_environment(
        session, workspace=workspace, requested=requested, dry_run=dry_run
    )
    started = datetime.now(timezone.utc)

    if file is not None:
        from ..test_file import source_file_node

        node = source_file_node(
            session,
            requested=requested,
            path=Path(file),
            started=started,
            dry_run=dry_run,
        )
        # Source-file runs use the same report but publish no estate evidence.
        return _reported(
            nodes=(node,),
            requested=requested,
            started=started,
            strict=strict,
            selection=str(file),
            workflow_id=None,
        )

    from ..run import Runner, RunRequest, RunState
    from ..run.state import read_installed_catalogue

    with session.step("Read catalogue"):
        if state is None:
            state = RunState(
                catalogue=read_installed_catalogue(session=session, workspace=workspace)
            )
    with session.step("Build run graph"):
        runner = Runner(
            state,
            RunRequest.test(
                requested,
                name=name,
                dry_run=dry_run,
                # Validations are independent: each reads the estate and
                # reports, and none produces what another consumes. One that
                # fails is a finding rather than a reason to stop asking the
                # others.
                fault_tolerant=True,
            ),
            workspace=workspace,
        )
    from ..run import open_run_log

    log = (
        None
        if dry_run
        else open_run_log(
            state.catalogue,
            workspace=workspace,
            task_type=TASK_TYPE,
            session=session,
        )
    )
    with session.step("Execute"):
        result = runner.run(
            session=session,
            # Evidence for a caller who asked about one validation; counts alone
            # for a whole-target run, which must not transfer diagnostic rows.
            dispatch=_dispatch_collecting(collect=name is not None),
            on_node=None if log is None else log.submit,
        )
    if log is not None:
        with session.step("Record what the run did"):
            state.catalogue.flush()

    return _reported(
        nodes=tuple(_as_validation_node(node) for node in result.nodes),
        requested=requested,
        started=started,
        strict=strict,
        selection=name,
        workflow_id=None if log is None else log.workflow_id,
    )


def _dispatch_collecting(*, collect: bool):
    """The one crossing, told whether this run was asked to show its evidence."""

    from ..run import dispatch_primitive

    def dispatch(node, **asked):
        return dispatch_primitive(node, collect=collect, **asked)

    return dispatch


def _require_lakehouse_environment(session, *, workspace, requested, dry_run: bool):
    """Fail before planning when a desktop Lakehouse run cannot start."""

    from ..sessions.base import ACROSS_BOUNDARY

    if dry_run or workspace.environment or not lakehouse_names(requested):
        return
    if session.position(workspace) != ACROSS_BOUNDARY:
        return
    target = next(target for target in requested if target.is_lakehouse)
    raise CommandError(
        f"{target} requires a Fabric Environment with Weaver installed. Pass "
        "--environment <name>, or set environment in workspace configuration."
    )


def _as_validation_node(node) -> ValidationNodeReport:
    """One run node, in the vocabulary a validation's readers use.

    A validation passes or fails rather than succeeding: a judgement about data
    rather than about work. One internal model, two public shapes.
    """

    from ..run.result import INVALID as RUN_INVALID
    from ..run.result import SUCCEEDED, VALIDATED
    from ..test_report import PASSED, PLANNED

    if node.status == VALIDATED:
        status = PLANNED
    elif node.status == SUCCEEDED:
        status = PASSED
    elif node.status == RUN_INVALID or getattr(node, "raised", False):
        # A check that could not be *evaluated* is invalid, not failed. A Test
        # that was never installed, or whose procedure threw, found nothing —
        # and reading that as "found no discrepancies" is the one answer a
        # validation must never give.
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
    """Whether a result carries the counts for this validation kind."""

    from ..declaration.metadata import ASSUMPTION

    field = "violation_count" if kind == ASSUMPTION else "missing_count"
    return hasattr(result, field)


def _failed_validation_result(node, result):
    """Give an invalid validation a result in its own result vocabulary."""

    from ..declaration.metadata import ASSUMPTION
    from ..runtime.validation_result import AssumptionResult, TestResult

    message = getattr(result, "error_message", None) or "could not run"
    if node.role == ASSUMPTION:
        return AssumptionResult.failed_to_run(message)
    return TestResult.failed_to_run(message)


def _reported(
    *,
    nodes: Sequence[ValidationNodeReport],
    requested: Sequence[PhysicalTargetRef],
    started: datetime,
    strict: bool,
    selection: str | None,
    workflow_id: str | None,
) -> ValidationRunReport:
    """Assemble the report and raise only if asked."""

    status = run_status(nodes)
    report = ValidationRunReport(
        status=status,
        nodes=tuple(nodes),
        workflow_id=workflow_id,
        started_at=started.isoformat(),
        finished_at=datetime.now(timezone.utc).isoformat(),
    )
    if strict and status in (FAILED, INVALID):
        raise ValidationError(
            _failure_message(report),
        )
    return report


def _failure_message(report: ValidationRunReport) -> str:
    parts = []
    for node in report.invalid_nodes:
        parts.append(f"{node.logical_id} could not be evaluated")
    for node in report.failed_nodes:
        result = node.result
        found = (
            f"{getattr(result, 'violation_count', 0)} violation(s)"
            if hasattr(result, "violation_count")
            else f"{getattr(result, 'failure_count', 0)} discrepancy row(s)"
        )
        parts.append(f"{node.logical_id} found {found}")
    return "; ".join(parts)


def _requested(targets: str | Sequence[str]):
    values = (targets,) if isinstance(targets, str) else tuple(targets)
    if not values:
        raise CommandError("test needs at least one target")
    return tuple(
        parse_physical_target(value, what="test target", error=CommandError)
        for value in values
    )


__all__ = ["TASK_TYPE", "run_test", "test"]
