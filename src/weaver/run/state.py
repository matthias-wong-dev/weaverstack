"""Observed catalogue and target inventories used to plan a run.

RunState is read at an operation boundary and then used as an immutable planning
input by Runner.
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

    The catalogue is Delta tables in the Weaver Lakehouse, so reading it is
    Spark SQL. The statements go through the Session and the rows are assembled
    here: above this, nothing knows which side of a boundary they came from.
    """

    from ..build_bundle.workflow import session_catalogue
    from ..catalogue.state import read_installed_catalogue as read
    from ..targets import ItemRef

    workspace = workspace if workspace is not None else session.workspace
    if workspace is None or not workspace.catalogue:
        raise RunError("a run needs a Workspace with a Weaver Lakehouse")

    return read(
        session_catalogue(session, workspace, workspace.catalogue_item)
    )


def read_target_inventories(targets, *, session, workspace=None) -> dict:
    """What each requested physical target actually holds, right now.

    Split by what the reading *needs*, not by what the target is. A Warehouse
    inventory is a T-SQL question; a Lakehouse inventory is storage plus Spark
    SQL for the views. Both reach their target from wherever this runs.
    """

    from ..build_bundle.prune import read_warehouse_inventory
    from ..build_bundle.targets import WarehouseBinding
    from ..load_plan import LAKEHOUSE_TARGET
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

    from ..build_bundle.prune import read_lakehouse_inventory
    from ..build_bundle.targets import LakehouseBinding
    from ..build_bundle.workflow import session_catalogue

    resolver = session.resolver(workspace)
    store = session.transport_store(workspace)
    for target in ordered:
        if target.kind != LAKEHOUSE_TARGET:
            continue
        item = ItemRef(target.name)
        try:
            inventories[str(target)] = read_lakehouse_inventory(
                LakehouseBinding(item).to_bound_target(),
                resolver=resolver,
                store=store,
                catalogue=session_catalogue(session, workspace, item),
            )
        except Exception as exc:  # noqa: BLE001 - re-raised with its cause
            raise _unreadable(target, exc) from exc
    return inventories


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
    if workspace is None or not workspace.catalogue:
        raise RunError("writing a task log needs a Workspace with a Weaver Lakehouse")
    return open_task_log(
        task_type=task_type,
        folder=log_folder(
            session.resolver(workspace), workspace.catalogue_item
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
