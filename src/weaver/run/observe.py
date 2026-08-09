"""Reading the estate a run will be planned against, once, before it is planned.

The runtime counterpart of ``read_build_state``, and the boundary the Runner is
deliberately on the far side of:

.. code-block:: text

    physical catalogue  → Catalogue   ┐
    physical targets    → Inventory   ├→ RunState → Runner
                                      ┘

Only the targets the graph touches are read, because only they have anything to
be looked up in them — and the read happens once, so everything above it decides
against a snapshot rather than against state that is still moving.

The two halves fail differently and say so. A catalogue that cannot be read
stops the run outright: a run planned from a half-read catalogue would silently
be a smaller run. An inventory that cannot be read for one target is reported
with its cause — a deleted item, an expired credential, an unavailable endpoint
and a defect in the reader are four different problems with four different
fixes, and a reader told only "missing" is sent to check the one thing that may
be fine.
"""

from __future__ import annotations

from .contract import RunError
from .state import RunState


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
    "open_run_log",
    "read_installed_catalogue",
    "read_run_state",
    "read_target_inventories",
]
