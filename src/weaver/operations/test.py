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
from ..load_plan import PhysicalTargetRef
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

    ``state`` is the same preflight snapshot :func:`weaver.operations.load.run_load` takes,
    so a caller that has already read the estate is not made to read it twice.
    """

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
        # Through the same reporting as an installed run, so `--dry-run` and
        # `strict` mean the same thing. A file run publishes nothing, which
        # `_reported` handles by writing no task log.
        return _reported(
            session,
            workspace=workspace,
            nodes=(node,),
            requested=requested,
            started=started,
            dry_run=dry_run,
            strict=strict,
            selection=str(file),
            durable=False,
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
    with session.step("Execute"):
        result = runner.run(
            session=session,
            # Evidence for a caller who asked about one validation; counts alone
            # for a whole-target run, which must not transfer diagnostic rows.
            dispatch=_dispatch_collecting(collect=name is not None),
        )

    return _reported(
        session,
        workspace=workspace,
        nodes=tuple(_as_validation_node(node) for node in result.nodes),
        requested=requested,
        started=started,
        dry_run=dry_run,
        strict=strict,
        selection=name,
    )


def _dispatch_collecting(*, collect: bool):
    """The one crossing, told whether this run was asked to show its evidence."""

    from ..run import dispatch_primitive

    def dispatch(node, **asked):
        return dispatch_primitive(node, collect=collect, **asked)

    return dispatch


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


def _reported(
    session,
    *,
    workspace,
    nodes: Sequence[ValidationNodeReport],
    requested: Sequence[PhysicalTargetRef],
    started: datetime,
    dry_run: bool,
    strict: bool,
    selection: str | None,
    durable: bool = True,
) -> ValidationRunReport:
    """Write the evidence, assemble the report, and raise only if asked.

    ``durable`` is false for a run over source that was never installed: a
    developer's loop rather than an estate event, so the estate's evidence
    records nothing it does not have.
    """

    status = run_status(nodes)
    from ..run import open_run_log

    log = (
        None
        if dry_run or not durable
        else open_run_log(session, workspace=workspace, task_type=TASK_TYPE)
    )
    if log is not None:
        log.write_plan(
            {
                "requested": [str(target) for target in requested],
                "selection": selection,
                "planned": [node.logical_id for node in nodes],
                "started_at": started.isoformat(),
            }
        )
        for node in nodes:
            # The mapping, never the node — a mapping has no diagnostics on it,
            # so no discrepancy row can reach a durable record by accident.
            log.write_step(node.kind.casefold(), node.to_mapping())

    report = ValidationRunReport(
        status=status,
        nodes=tuple(nodes),
        task_log=None if log is None else log.root.value,
        started_at=started.isoformat(),
        finished_at=datetime.now(timezone.utc).isoformat(),
    )
    if log is not None:
        log.write_completion({"status": status, **report.totals()})
    if strict and status in (FAILED, INVALID):
        raise ValidationError(_failure_message(report), )
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
