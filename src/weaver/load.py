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
    reverse the bindings     weaver.load_plan
    build the physical DAG   weaver.load_plan
    resolve every node       weaver.load_resolution
    ── dry run stops here ──
    execute sequentially     weaver.load_execution
    write task evidence      weaver.task_logging

Nothing executes while catalogue state is still being discovered: the whole plan
is settled, ordered and resolved before the first primitive is dispatched.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .errors import CommandError, LoadError
from .load_execution import execute_load_plan
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
from .load_resolution import LoadEnvironment, dry_run_reports, resolve_load_plan
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
        workspace=workspace, workspace_config=workspace_config
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

    with _load_session(resolved_workspace, requested) as session:
        return run_load(
            session,
            requested=tuple(_physical_ref(target) for target in requested),
            fault_tolerant=fault_tolerant,
            dry_run=dry_run,
        )


def run_load(
    session: "LoadSession",
    *,
    requested: Sequence[PhysicalTargetRef],
    fault_tolerant: bool = False,
    dry_run: bool = False,
) -> LoadRunReport:
    """The whole orchestration path, over a prepared session.

    Separated from :func:`load` so the composition can be driven without the
    workspace resolution and capability acquisition in front of it — which is
    what the desktop, the emulator and a Fabric session each do differently and
    none of them changes about the orchestration itself.
    """

    started = datetime.now(timezone.utc)
    estate = InstalledEstate.from_catalogue(session.read_catalogue())
    _refuse_uninstalled_targets(estate, requested)
    dag = load_dag(estate, targets=requested)
    environment = session.environment(dag)
    _refuse_absent_targets(dag, environment)
    plan = resolve_load_plan(dag, environment=environment)

    common = {
        "requested": tuple(str(target) for target in requested),
        "dry_run": dry_run,
        "fault_tolerant": fault_tolerant,
        "edges": dag.edges,
        "order": tuple(node.node_id for node in dag.order()),
        "messages": dag.messages,
        "workspace": str(session.workspace.workspace),
        "started_at": started.isoformat(),
    }

    if dry_run:
        nodes = dry_run_reports(plan)
        return LoadRunReport(
            **common,
            nodes=nodes,
            status=final_status(nodes, dry_run=True),
            finished_at=datetime.now(timezone.utc).isoformat(),
        )

    log = session.open_log()
    if log is not None:
        log.write_plan(_plan_document(plan, common))
    nodes = execute_load_plan(
        plan,
        fault_tolerant=fault_tolerant,
        environment=environment,
        on_step=(
            None
            if log is None
            else lambda report: log.write_step(_step_type(report), report.to_mapping())
        ),
    )
    status = final_status(nodes, dry_run=False)
    report = LoadRunReport(
        **common,
        nodes=nodes,
        status=status,
        task_id=None if log is None else log.task_id,
        task_log=None if log is None else log.root.value,
        finished_at=datetime.now(timezone.utc).isoformat(),
    )
    if log is not None:
        log.write_completion(_completion_document(report))
    if not fault_tolerant:
        _raise_for_failure(report)
    return report


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
# Both checks run before anything is dispatched, and in a dry run as well as a
# real one — a mistyped target is a mistyped target whichever was asked for.
#
# The two failures they separate look alike and are not:
#
#     not installed          nobody ever built into this target. Almost always a
#                            typo, and reporting it as "no work to do" is the
#                            single worst answer available: it looks like success.
#
#     physically missing     the catalogue says the estate is there and the
#                            workspace disagrees. A deleted Lakehouse, a wiped
#                            Warehouse, a restored control plane.
#
# Deferring either into per-node failures would turn one clear refusal into a
# scattering of identical ones, each naming an object rather than the target that
# is actually absent.


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


def _refuse_absent_targets(dag: LoadDag, environment: LoadEnvironment) -> None:
    """Refuse when a target this plan needs cannot be read.

    Every target the *graph* touches, not only the ones asked for: a load of a
    Warehouse may depend on a Lakehouse upstream, and a run that discovered its
    dependency was gone halfway through would already have written to the
    Warehouse.
    """

    needed = tuple(dict.fromkeys(node.physical_target for node in dag.nodes))
    absent = [target for target in needed if environment.inventory(target) is None]
    if not absent:
        return
    raise LoadError(
        "the catalogue says these are installed, but they could not be read: "
        + ", ".join(str(target) for target in absent)
        + " — the estate and the workspace disagree, so nothing was loaded"
    )


def _step_type(report: LoadNodeReport) -> str:
    """The broad kind a step file's name carries: a load, or a refresh."""

    return "refresh" if report.primitive_kind == ENDPOINT_REFRESH else "load"


def _plan_document(plan, common: Mapping[str, Any]) -> dict:
    """The complete intended task, written once before anything runs.

    Enough on its own to answer what was requested, what Weaver intended to run,
    in what order, against which physical targets and through which installed
    primitives — which is the whole point of writing it before rather than after.
    """

    from . import __version__

    return {
        "weaver_version": __version__,
        "requested": list(common["requested"]),
        "mode": "dry_run" if common["dry_run"] else "execute",
        "fault_tolerant": common["fault_tolerant"],
        "workspace": common["workspace"],
        "started_at": common["started_at"],
        "order": list(common["order"]),
        "edges": [list(edge) for edge in common["edges"]],
        "nodes": [
            {
                "node_id": resolved.node.node_id,
                "logical_id": str(resolved.node.logical_id)
                if resolved.node.logical_id
                else None,
                "physical_target": str(resolved.node.physical_target),
                "physical_object": str(resolved.node.physical_object)
                if resolved.node.physical_object
                else None,
                "primitive_kind": resolved.node.primitive_kind,
                "dispatch_location": resolved.dispatch_location,
                "target_exists": resolved.target_exists,
                "primitive_exists": resolved.primitive_exists,
            }
            for resolved in plan.order
        ],
        "messages": [message.to_mapping() for message in common["messages"]],
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
            for name in rows:
                rows[name] += getattr(node.result, name)
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


class LoadSession:
    """The capabilities one load run needs, acquired for its host.

    Owns what it opened and closes it — a Spark session locally, a TDS connection
    per Warehouse — and nothing it was given.
    """

    def __init__(self, workspace: Workspace, requested, *, spark=None, store=None) -> None:
        self.workspace = workspace
        self.requested = tuple(requested)
        self.spark = spark
        self.store = store
        self._sql: dict[str, Any] = {}
        self._opened: list[Any] = []

    # --- context ------------------------------------------------------------

    def __enter__(self) -> "LoadSession":
        return self

    def __exit__(self, *exc) -> bool:
        for opened in reversed(self._opened):
            close = getattr(opened, "close", None)
            if close is not None:
                close()
        self._opened.clear()
        return False

    # --- what orchestration asks for ----------------------------------------

    @property
    def resolver(self):
        from .resolution import resolver_for

        return resolver_for(self.workspace)

    def read_catalogue(self):
        """The installed catalogue, read from the Weaver control Lakehouse."""

        from .catalogue.state import read_installed_catalogue
        from .spark import SparkCatalogue

        if self.spark is None:
            raise LoadError(
                "reading the installed catalogue needs a Spark session"
            )
        return read_installed_catalogue(
            SparkCatalogue(
                self.spark,
                self.resolver.spark_destination(ItemRef(self.workspace.weaver_lakehouse)),
            )
        )

    def environment(self, dag: LoadDag) -> LoadEnvironment:
        """Runtime services plus the physical state every planned target is in.

        The inventory read happens once, here, and the whole of resolution then
        runs against frozen state — the same discipline a build follows, and for
        the same reason: a decision made against state that is still moving is a
        decision nobody can reproduce.
        """

        targets = tuple(dict.fromkeys(node.physical_target for node in dag.nodes))
        inventories = {}
        for target in targets:
            observed = self._inventory(target)
            if observed is not None:
                inventories[str(target)] = observed
        return LoadEnvironment(
            resolver=self.resolver,
            inventories=inventories,
            store=self.store,
            spark=self.spark,
            sql=self._sql,
            workspace=self.workspace,
        )

    def open_log(self):
        from .task_logging import log_folder, open_task_log

        if self.store is None:
            raise LoadError("writing a task log needs a store")
        return open_task_log(
            task_type=TASK_TYPE,
            folder=log_folder(self.resolver, ItemRef(self.workspace.weaver_lakehouse)),
            store=self.store,
        )

    # --- reading physical state ---------------------------------------------

    def _inventory(self, target: PhysicalTargetRef):
        from .build_bundle.prune import (
            read_lakehouse_inventory,
            read_warehouse_inventory,
        )
        from .build_bundle.targets import LakehouseBinding, WarehouseBinding

        if target.kind == LAKEHOUSE_TARGET:
            bound = LakehouseBinding(ItemRef(target.name)).to_bound_target()
            try:
                return read_lakehouse_inventory(
                    bound, resolver=self.resolver, store=self.store, spark=self.spark
                )
            except Exception:
                # A target that cannot be read is a target that is not there, as
                # far as this run is concerned. Resolution says so per node,
                # naming the target, rather than the whole run failing on a read.
                return None
        bound = WarehouseBinding(ItemRef(target.name)).to_bound_target()
        sql = self._warehouse_sql(target.name)
        if sql is None:
            return None
        try:
            return read_warehouse_inventory(bound, sql=sql)
        except Exception:
            return None

    def _warehouse_sql(self, name: str):
        if name in self._sql:
            return self._sql[name]
        executor = self._open_warehouse_sql(name)
        if executor is not None:
            self._sql[name] = executor
        return executor

    def _open_warehouse_sql(self, name: str):
        from .operations import _inside_fabric_session

        target = WarehouseTarget(ItemRef(name))
        if not isinstance(self.workspace, FabricWorkspace):
            return None
        if _inside_fabric_session(self.workspace):
            from .fabric.sql import fabric_sql_executor

            executor = fabric_sql_executor(target, self.workspace)
        else:
            from .fabric import desktop_sql_executor

            executor = desktop_sql_executor(target, self.workspace)
        self._opened.append(executor)
        return executor


def _load_session(workspace: Workspace, requested) -> LoadSession:
    """A session with this host's Spark and store already acquired."""

    from .operations import _active_spark, _inside_fabric_session, _operation_store

    if isinstance(workspace, LocalWorkspace):
        from .spark import local_delta_session
        from .store import FilesystemStore

        session = local_delta_session(workspace)
        spark = session.__enter__()
        opened = LoadSession(workspace, requested, spark=spark, store=FilesystemStore())
        opened._opened.append(_Closing(lambda: session.__exit__(None, None, None)))
        return opened
    if not _inside_fabric_session(workspace):
        raise CommandError(
            "load runs where the data is: call it from a Fabric notebook, or "
            "against a local Workspace"
        )
    from .resolution import store_for

    return LoadSession(
        workspace, requested, spark=_active_spark(), store=store_for(workspace)
    )


class _Closing:
    """Adapts an arbitrary teardown into the ``close()`` the session expects."""

    def __init__(self, teardown) -> None:
        self._teardown = teardown

    def close(self) -> None:
        self._teardown()


__all__ = ["LoadSession", "TASK_TYPE", "load", "run_load"]
