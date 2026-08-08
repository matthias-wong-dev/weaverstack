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
from .load import LoadSession, _load_session
from .load_plan import PhysicalTargetRef, WAREHOUSE_TARGET
from .targets import ItemRef, WarehouseTarget, parse_physical_target
from .test_execution import execute_validations
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
    )
    refs = tuple(_physical_ref(target) for target in requested)

    with _load_session(resolved, requested) as session:
        return run_test(
            session,
            requested=refs,
            name=name,
            file=file,
            dry_run=dry_run,
            strict=strict,
        )


def run_test(
    session: LoadSession,
    *,
    requested: Sequence[PhysicalTargetRef],
    name: str | None = None,
    file: str | Path | None = None,
    dry_run: bool = False,
    strict: bool = False,
) -> ValidationRunReport:
    """The whole orchestration path, over a prepared session.

    Separated from :func:`test` for the reason :func:`weaver.load.run_load` is:
    workspace resolution and capability acquisition differ between the desktop,
    the emulator and a Fabric session, and none of them changes the
    orchestration itself.
    """

    started = datetime.now(timezone.utc)

    if file is not None:
        from .test_file import run_source_file

        return run_source_file(
            session, requested=requested, path=Path(file), started=started
        )

    estate = ValidationEstate.from_catalogue(session.read_catalogue())
    if name is not None:
        selected: tuple[InstalledValidation, ...] = (estate.named(name, requested),)
    else:
        selected = validation_order(estate.for_targets(requested))

    environment = _environment(session, selected)
    try:
        nodes = execute_validations(
            selected,
            environment=environment,
            # Evidence for a caller who asked about one validation; counts alone
            # for a whole-target run, which must not transfer diagnostic rows.
            collect_diagnostics=name is not None,
            dry_run=dry_run,
        )
    finally:
        environment.runtime_scope.close()

    return _reported(
        session,
        nodes=nodes,
        requested=requested,
        started=started,
        dry_run=dry_run,
        strict=strict,
        selection=name,
    )


def _reported(
    session: LoadSession,
    *,
    nodes: Sequence[ValidationNodeReport],
    requested: Sequence[PhysicalTargetRef],
    started: datetime,
    dry_run: bool,
    strict: bool,
    selection: str | None,
) -> ValidationRunReport:
    """Write the evidence, assemble the report, and raise only if asked."""

    status = run_status(nodes)
    log = None if dry_run else session.open_log(TASK_TYPE)
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


def _environment(session: LoadSession, selected: Sequence[InstalledValidation]):
    """Runtime services for the targets these validations actually live in.

    No inventories are read. A load asks what physically exists because it is
    about to write; a validation reads what is there, and a target that is not
    there fails its own dispatch with a message about the thing that was
    missing.
    """

    from .load_resolution import LoadEnvironment

    for validation in selected:
        if validation.target.kind == WAREHOUSE_TARGET:
            session._warehouse_sql(validation.target.name)  # noqa: SLF001 - one seam
    return LoadEnvironment(
        resolver=session.resolver,
        store=session.store,
        spark=session.spark,
        sql=session._sql,  # noqa: SLF001 - the session owns what it opened
        workspace=session.workspace,
    )


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
) -> Workspace:
    from dataclasses import replace

    from .operations import _operation_workspace, _with_inferred_control_lakehouse

    resolved = _operation_workspace(
        workspace=workspace, workspace_config=workspace_config
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
