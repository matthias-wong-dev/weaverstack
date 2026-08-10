"""RunState — what the estate was, when the run was planned against it.

The runtime twin of :class:`~weaver.build_bundle.workflow.BuildState`, and for
the same reason. A Runner decides what runs, in what order, and what is blocked;
it must not be the thing that discovers whether a Warehouse is still there,
because a decision made against state that is still moving is a decision nobody
can reproduce.

.. code-block:: text

    physical catalogue  → Catalogue
    physical targets    → TargetInventory
                        ↓
                     RunState
                        ↓
                      Runner

So the reading happens once, at a boundary, above the Runner — and everything
the Runner does afterwards is Python. A run-cycle test constructs one of these
directly and needs no estate at all.

**Inventories are keyed by the target's public spelling** — ``Lakehouse/Raw_LH``
— because that is what a caller wrote and what a report prints. A key that could
not be read back would put a second vocabulary between the request and the
answer. A target with no entry is a target that is not there, which is a
different failure from a graph that is wrong about one, and the Runner says
which.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from ..build_bundle.prune import TargetInventory
from ..catalogue.state import Catalogue
from .result import RunError


@dataclass(frozen=True)
class RunState:
    """The installed estate as one Python handover: what is claimed, and what is there."""

    catalogue: Catalogue
    target_inventories: Mapping[str, TargetInventory] = field(default_factory=dict)

    def inventory(self, target) -> TargetInventory | None:
        """What was observed at one physical target, or None if it is absent."""

        return self.target_inventories.get(str(target))

    def observed(self, target) -> bool:
        return str(target) in self.target_inventories

    def to_mapping(self) -> dict:
        return {
            "format_version": 1,
            "catalogue": self.catalogue.to_mapping(),
            "target_inventories": [
                {"target": target, "inventory": inventory.to_mapping()}
                for target, inventory in sorted(self.target_inventories.items())
            ],
        }

    @classmethod
    def from_mapping(cls, mapping) -> "RunState":
        from .result import RunError

        version = mapping.get("format_version")
        if version != 1:
            raise RunError(
                f"unsupported run state format_version {version!r}; expected 1"
            )
        return cls(
            catalogue=Catalogue.from_mapping(mapping["catalogue"]),
            target_inventories={
                entry["target"]: TargetInventory.from_mapping(entry["inventory"])
                for entry in mapping.get("target_inventories", ())
            },
        )



# --- reading one out of the estate --------------------------------------------
#
# The boundary above the Runner: these are what a *caller* does before there is
# a run at all. They live beside `RunState` because what they exist to produce
# is a `RunState`, and separating the two meant a reader had to find both to
# see where the snapshot came from.

def read_run_state(targets, *, session, workspace=None) -> RunState:
    """The installed catalogue and every requested target, as one handover."""

    workspace = workspace if workspace is not None else session.workspace
    return RunState(
        catalogue=read_installed_catalogue(session=session, workspace=workspace),
        target_inventories=read_target_inventories(
            targets, session=session, workspace=workspace
        ),
    )


def read_installed_catalogue(*, session, workspace=None):
    """What Weaver knows it installed, read from the control Lakehouse.

    The catalogue lives in Delta tables in the Weaver Lakehouse, so reading it
    needs Spark — which a desktop process does not have. It asks for the read
    as a whole instead, and what comes back is the same
    :class:`~weaver.catalogue.state.Catalogue` either way.

    That is the shape the whole decomposition turns on: **remote state is read
    once, at a boundary, and represented locally as ordinary Python.** Above
    here, nothing knows or cares which side of the wire the rows came from.
    """

    from ..catalogue.state import Catalogue
    from ..session.program import RemoteProgram

    workspace = workspace if workspace is not None else session.workspace
    if workspace is None or not workspace.weaver_lakehouse:
        raise RunError("a run needs a Workspace with a Weaver Lakehouse")

    body = (
        "from weaver.workspaces import FabricWorkspace\n"
        "from weaver.run.state import _catalogue_here\n"
        "from weaver.session import NotebookSession\n"
        f"workspace = {_workspace_literal(workspace)}\n"
        "session = NotebookSession(workspace=workspace, spark=spark)\n"
        "emit(_catalogue_here(session=session, workspace=workspace).to_mapping())\n"
    )
    payload = session.execute_python(
        RemoteProgram(
            name="read_catalogue",
            call=lambda: _catalogue_here(
                session=session, workspace=workspace
            ).to_mapping(),
            source=body,
            detail=str(workspace.weaver_lakehouse),
        ),
        workspace=workspace,
    )
    return Catalogue.from_mapping(payload)


def _catalogue_here(*, session, workspace):
    """The catalogue read by a host that already has Spark."""

    from ..catalogue.state import read_installed_catalogue as read
    from ..spark import SparkCatalogue
    from ..targets import ItemRef

    return read(
        SparkCatalogue(
            session.spark(workspace),
            session.resolver(workspace).spark_destination(
                ItemRef(workspace.weaver_lakehouse)
            ),
        )
    )


def _workspace_literal(workspace) -> str:
    return (
        f"FabricWorkspace(workspace={workspace.workspace!r}, "
        f"weaver_lakehouse={workspace.weaver_lakehouse!r}, "
        f"environment={workspace.environment!r})"
    )


def read_target_inventories(targets, *, session, workspace=None) -> dict:
    """What each requested physical target actually holds, right now.

    Split by what the reading *needs*, not by what the target is. A Warehouse
    inventory is a T-SQL question, and TDS reaches a Warehouse from anywhere —
    so it is asked from here whichever host is running. A Lakehouse inventory
    needs the Spark catalogue, so on a desktop it crosses.

    Every Lakehouse crosses in **one** program rather than one apiece. A
    submission costs seconds and the statements inside it cost almost nothing,
    so a call per target would make observing three Lakehouses three times the
    price of observing one — and, worse, three observations of three different
    moments presented as one snapshot.
    """

    from ..build_bundle.prune import TargetInventory, read_warehouse_inventory
    from ..build_bundle.targets import WarehouseBinding
    from ..load_plan import LAKEHOUSE_TARGET
    from ..session.program import RemoteProgram
    from ..targets import ItemRef, WarehouseTarget

    workspace = workspace if workspace is not None else session.workspace
    ordered = list(dict.fromkeys(targets))
    inventories: dict = {}

    for target in ordered:
        if target.kind == LAKEHOUSE_TARGET:
            continue
        try:
            bound = WarehouseBinding(ItemRef(target.name)).to_bound_target()
            inventories[str(target)] = read_warehouse_inventory(
                bound,
                sql=session.sql_executor(
                    WarehouseTarget(ItemRef(target.name)), workspace=workspace
                ),
            )
        except Exception as exc:  # noqa: BLE001 - re-raised with its cause
            raise _unreadable(target, exc) from exc

    delta = [target for target in ordered if target.kind == LAKEHOUSE_TARGET]
    lakehouses = [target.name for target in delta]
    if not lakehouses:
        return inventories

    body = (
        "from weaver.workspaces import FabricWorkspace\n"
        "from weaver.run.state import _lakehouse_inventories_here\n"
        "from weaver.session import NotebookSession\n"
        f"workspace = {_workspace_literal(workspace)}\n"
        "session = NotebookSession(workspace=workspace, spark=spark)\n"
        f"emit(_lakehouse_inventories_here({lakehouses!r}, session=session, "
        "workspace=workspace))\n"
    )
    try:
        observed = session.execute_python(
            RemoteProgram(
                name="read_inventories",
                call=lambda: _lakehouse_inventories_here(
                    lakehouses, session=session, workspace=workspace
                ),
                source=body,
                detail=", ".join(lakehouses),
            ),
            workspace=workspace,
        )
    except Exception as exc:  # noqa: BLE001 - re-raised with its cause
        # Every target the one crossing was reading. A batched read cannot say
        # which of them the failure belongs to, and naming one would be picking
        # a culprit rather than reporting what happened.
        raise _unreadable(", ".join(str(target) for target in delta), exc) from exc

    for target in ordered:
        if target.kind != LAKEHOUSE_TARGET:
            continue
        inventories[str(target)] = TargetInventory.from_mapping(observed[target.name])
    return inventories


def _lakehouse_inventories_here(names, *, session, workspace) -> dict:
    """Every named Lakehouse's inventory, by a host that has Spark."""

    from ..build_bundle.prune import read_lakehouse_inventory
    from ..build_bundle.targets import LakehouseBinding
    from ..targets import ItemRef

    resolver = session.resolver(workspace)
    store = session.store(workspace)
    spark = session.spark(workspace)
    return {
        name: read_lakehouse_inventory(
            LakehouseBinding(ItemRef(name)).to_bound_target(),
            resolver=resolver,
            store=store,
            spark=spark,
        ).to_mapping()
        for name in names
    }


def _unreadable(target, exc: Exception) -> RunError:
    return RunError(
        f"{target}: the catalogue says it is installed, but its "
        f"inventory could not be read: {type(exc).__name__}: {exc}"
    )


def open_run_log(session, *, workspace=None, task_type: str):
    """Where this run's evidence goes — the sink, opened at the boundary.

    Downstream of the Runner by construction: a run is correct without one, and
    this is called by the operation that wants a durable record rather than by
    the thing doing the work. ``task_type`` is what the record says it was,
    because a load and a validation need the same capabilities and are not the
    same event.
    """

    from ..targets import ItemRef
    from ..task_logging import log_folder, open_task_log

    workspace = workspace if workspace is not None else session.workspace
    if workspace is None or not workspace.weaver_lakehouse:
        raise RunError("writing a task log needs a Workspace with a Weaver Lakehouse")
    return open_task_log(
        task_type=task_type,
        folder=log_folder(
            session.resolver(workspace), ItemRef(workspace.weaver_lakehouse)
        ),
        store=session.store(workspace),
    )

__all__ = [
    "RunState",
    "open_run_log",
    "read_installed_catalogue",
    "read_run_state",
    "read_target_inventories",
]
