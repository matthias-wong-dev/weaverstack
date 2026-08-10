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
    """What Weaver knows it installed, read from the control Lakehouse."""

    from ..catalogue.state import read_installed_catalogue as read
    from ..spark import SparkCatalogue
    from ..targets import ItemRef

    workspace = workspace if workspace is not None else session.workspace
    if workspace is None or not workspace.weaver_lakehouse:
        raise RunError("a run needs a Workspace with a Weaver Lakehouse")
    return read(
        SparkCatalogue(
            session.spark(workspace),
            session.resolver(workspace).spark_destination(
                ItemRef(workspace.weaver_lakehouse)
            ),
        )
    )


def read_target_inventories(targets, *, session, workspace=None) -> dict:
    """What each requested physical target actually holds, right now."""

    from ..build_bundle.prune import read_lakehouse_inventory, read_warehouse_inventory
    from ..build_bundle.targets import LakehouseBinding, WarehouseBinding
    from ..load_plan import LAKEHOUSE_TARGET
    from ..targets import ItemRef, WarehouseTarget

    workspace = workspace if workspace is not None else session.workspace
    inventories: dict = {}
    for target in dict.fromkeys(targets):
        try:
            if target.kind == LAKEHOUSE_TARGET:
                bound = LakehouseBinding(ItemRef(target.name)).to_bound_target()
                observed = read_lakehouse_inventory(
                    bound,
                    resolver=session.resolver(workspace),
                    store=session.store(workspace),
                    spark=session.spark(workspace),
                )
            else:
                bound = WarehouseBinding(ItemRef(target.name)).to_bound_target()
                observed = read_warehouse_inventory(
                    bound,
                    sql=session.sql_executor(
                        WarehouseTarget(ItemRef(target.name)), workspace=workspace
                    ),
                )
        except Exception as exc:  # noqa: BLE001 - re-raised with its cause
            raise RunError(
                f"{target}: the catalogue says it is installed, but its "
                f"inventory could not be read: {type(exc).__name__}: {exc}"
            ) from exc
        inventories[str(target)] = observed
    return inventories


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
