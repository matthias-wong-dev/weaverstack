"""The public ``weaver.test(...)`` operation.

The validation counterpart of :mod:`weaver.load`, and deliberately shaped like
it: typed physical target strings at the boundary, the same workspace resolution,
the same session, the same task log machinery.

.. code-block:: python

    weaver.test("Lakehouse/Sales")
    weaver.test("Lakehouse/Sales", name="Sales.OrdersReconcile")
    weaver.test("Lakehouse/Sales", file="tests/Sales.OrdersReconcile.sql")

**The installed catalogue is authoritative and the repository is not reopened**,
for whole-target and ``name=`` runs. By the time anything is runnable it has been
built, and what was built is recorded.

``file=`` is the exception, and is the only mode that reads source: it compiles a
file with the same compiler a build uses and executes the result without
installing it, so a developer can run a validation they have not committed. It
publishes nothing.

**There is no ``weaver.assumption(...)``.** One operation runs both kinds,
because a caller asking "does this estate hold up?" is not asking two questions.

**A failing validation is a returned report, not an exception.** A run that
found discrepancies did its job; raising would make the evidence harder to reach
than ignoring it. A validation that could not be *evaluated* is a different
outcome, reported as ``invalid`` — and ``strict=True`` is how a caller asks for
either to be raised.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .errors import CommandError, ValidationError
from .load_plan import PhysicalTargetRef, WAREHOUSE_TARGET
from .targets import ItemRef, WarehouseTarget, parse_physical_target
from .test_plan import InstalledValidation, ValidationEstate, validation_order
from .test_report import (
    FAILED,
    INVALID,
    ValidationNodeReport,
    ValidationRunReport,
    run_status,
)
from .workspaces import FabricWorkspace, LocalWorkspace, Workspace

TASK_TYPE = "test"


def test(
    targets: str | Sequence[str],
    *,
    name: str | None = None,
    file: str | Path | None = None,
    workspace: str | Path | Workspace | None = None,
    weaver_lakehouse: str | None = None,
    workspace_config: str | Path | None = None,
    dry_run: bool = False,
    strict: bool = False,
    session=None,
) -> ValidationRunReport:
    """Run the installed validation in the requested physical targets.

    ``name`` runs one installed validation and returns its diagnostic rows
    alongside its counts; ``file`` compiles and runs a source file without
    installing it. They are mutually exclusive: one names something the estate
    has and the other names something it may not, and a request that meant both
    would have to decide which won.
    """

    if name is not None and file is not None:
        raise CommandError(
            "test takes name= or file=, not both — one runs what is installed "
            "and the other runs a source file that may not be"
        )

    requested = _requested(targets)
    resolved = _resolve_workspace(
        workspace=workspace,
        weaver_lakehouse=weaver_lakehouse,
        workspace_config=workspace_config,
        requested=requested,
        session=session,
    )
    refs = tuple(_physical_ref(target) for target in requested)

    from .session.host import use_or_create_session

    with use_or_create_session(session, workspace=resolved) as opened:
        if not opened.executes_here(resolved):
            raise CommandError(
                "test runs where the data is: call it from a Fabric notebook, "
                "or against a local Workspace"
            )
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

    Separated from :func:`test` for the reason :func:`weaver.load.run_load` is:
    workspace resolution and capability acquisition differ between the desktop,
    the emulator and a Fabric session, and none of them changes the
    orchestration itself.

    ``state`` is the same preflight snapshot :func:`weaver.load.run_load` takes,
    for the same reason: reading the estate is a boundary act, and a caller who
    has already read it — or who is describing one deliberately — should not
    have the run read it again behind them.
    """

    started = datetime.now(timezone.utc)

    if file is not None:
        from .test_file import source_file_node

        node = source_file_node(
            session,
            requested=requested,
            path=Path(file),
            started=started,
            dry_run=dry_run,
        )
        # Through the same reporting as an installed run, so `--dry-run` means
        # the same thing and `strict` raises on the same outcomes. What differs
        # is only that a file run publishes nothing, which `_reported` handles
        # by writing no task log when there is nothing installed to record
        # against.
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

    from .run import RunRequest, Runner, RunState

    from .run.state import read_installed_catalogue

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
                # Validations are independent by construction: each reads the
                # estate and reports, and none produces what another consumes.
                # One that fails is a finding, not a reason to stop asking the
                # others — and "everything I did not get to" is the least useful
                # answer a run that was asked what is wrong with an estate could
                # give.
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

    from .run import dispatch_primitive

    def dispatch(node, **asked):
        return dispatch_primitive(node, collect=collect, **asked)

    return dispatch


def _as_validation_node(node) -> ValidationNodeReport:
    """One run node, in the vocabulary a validation's readers use.

    A validation does not "succeed" — it passes or fails, which is a judgement
    about data rather than about work. One internal model does not mean one
    public shape, and this is where the two meet.
    """

    from .run.result import INVALID as RUN_INVALID
    from .run.result import SUCCEEDED, VALIDATED
    from .test_report import PASSED, PLANNED

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

    ``durable`` is false for a run over source that was never installed. Such a
    run is a developer's loop rather than an estate event, and a task log
    claiming otherwise would put a record of something the estate does not have
    into the estate's own evidence.
    """

    status = run_status(nodes)
    from .run import open_run_log

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


def _resolve_workspace(
    *,
    workspace: str | Path | Workspace | None,
    weaver_lakehouse: str | None,
    workspace_config: str | Path | None,
    requested,
    session=None,
) -> Workspace:
    from dataclasses import replace

    from .operations import _operation_workspace, _with_inferred_control_lakehouse

    resolved = _operation_workspace(
        workspace=workspace, workspace_config=workspace_config, session=session
    )
    if weaver_lakehouse is not None:
        resolved = replace(
            resolved,
            weaver_lakehouse=ItemRef.parse(str(weaver_lakehouse)).name,
        )
    resolved = _with_inferred_control_lakehouse(resolved)
    if not resolved.weaver_lakehouse:
        raise CommandError(
            "test needs a Weaver control Lakehouse: pass weaver_lakehouse=, give "
            "one in workspace configuration, or run inside a Fabric notebook "
            "with one attached as the default Lakehouse"
        )
    if isinstance(resolved, LocalWorkspace) and any(
        isinstance(target, WarehouseTarget) for target in requested
    ):
        raise CommandError(
            "Warehouse targets require a Fabric Workspace; the local emulator has no SQL"
        )
    return resolved


def _physical_ref(target) -> PhysicalTargetRef:
    from .load import _physical_ref as reference

    return reference(target)


__all__ = ["TASK_TYPE", "run_test", "test"]
