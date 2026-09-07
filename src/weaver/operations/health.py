"""Public ``weaver.health(...)`` entry point.

Gathers what :mod:`weaver.health` evaluates: the installed catalogue including
its current status tables, the ``_.LoadStatistic`` rows behind current
``_.LoadStatus`` state, and the physical inventory of each selected Lakehouse or
Warehouse.

Every read is over TDS or OneLake. Health executes no authored load or test
Python, so it takes no Environment, and a Warehouse-only request starts no Livy
session.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from ..catalogue.tables import (
    DEPENDENCY,
    FOLDER_DICTIONARY,
    INSTALLATION,
    LOAD_STATUS,
    REGISTRY,
    SHORTCUT,
    TABLE_DICTIONARY,
    TEST_DICTIONARY,
    TEST_STATUS,
)
from ..declaration.model import WeaverItemId
from ..health import HealthReport, assess, resolve_as_of
from .items import installed_targets, requested_items
from .workspace import operation_workspace

#: What health reads of the catalogue, table by table. Each one is read over
#: TDS, so a table nothing consults is a round trip nobody needed.
#:
#: The installed graph is built from the first seven, and the two status tables
#: hold current lifecycle state. ``_.TableDictionary`` and ``_.FolderDictionary``
#: serve both: the graph reads whether an object is Static, and Build health
#: reads what is declared and not certified.
#:
#: ``_.Bookmark`` is absent: it is the loader's execution cursor. So are the
#: dictionaries describing columns and keys, and ``_.LoadStatistic``, read only
#: as the window matching current ``_.LoadStatus`` state.
HEALTH_TABLES = (
    INSTALLATION,
    REGISTRY,
    TABLE_DICTIONARY,
    FOLDER_DICTIONARY,
    TEST_DICTIONARY,
    DEPENDENCY,
    SHORTCUT,
    LOAD_STATUS,
    TEST_STATUS,
)


def health(
    items: str | Sequence[str] | None = None,
    *,
    as_of: str | datetime | None = None,
    workspace: str | None = None,
    catalogue: str | None = None,
    workspace_config: str | Path | None = None,
    inventories: bool = True,
    session=None,
) -> HealthReport:
    """The installed estate's operational health.

    ``items`` restricts the subjects reported on. With none, every target the
    catalogue binds an item to. Managed ancestry outside the selection is still
    read, because whether a selected object is behind its sources is a question
    about the whole graph.

    ``as_of`` is the instant a settled load must be no older than. It defaults
    to one day before the operation started. An aware datetime or an ISO-8601
    string carrying an offset is accepted; a naive one is refused, because a
    health report crosses timezones.

    ``inventories`` reads each selected target's physical state, so a certified
    object that is not there is reported. Turning it off leaves Build health to
    what the catalogue contradicts about itself.
    """

    started = datetime.now(timezone.utc)
    requested = requested_items(items, what="health")
    resolved = operation_workspace(
        "health",
        workspace=workspace,
        catalogue=catalogue,
        workspace_config=workspace_config,
        session=session,
    )
    from ..sessions.host import use_or_create_session

    with use_or_create_session(session, workspace=resolved) as opened:
        with opened.task("Health", ", ".join(map(str, requested)) or "whole estate"):
            return run_health(
                opened,
                workspace=resolved,
                items=requested,
                as_of=resolve_as_of(as_of, started=started),
                generated_at=started,
                inventories=inventories,
            )


def run_health(
    session,
    *,
    workspace,
    items: Sequence[WeaverItemId] = (),
    as_of: datetime,
    generated_at: datetime,
    inventories: bool = True,
) -> HealthReport:
    """The whole gathering path, over a prepared session.

    Separated from :func:`health` as :func:`weaver.operations.load.run_load` is:
    workspace resolution and capability acquisition differ between positions,
    and neither changes what health evaluates.
    """

    from ..catalogue.connection import catalogue_connection
    from ..catalogue.state import read_installed_catalogue

    connection = catalogue_connection(session, workspace)
    with session.step("Read catalogue"):
        # Current state and the statistics explaining it, in one read.
        # Everything below reasons from this catalogue and asks the Warehouse
        # nothing further.
        catalogue = read_installed_catalogue(
            connection, tables=HEALTH_TABLES, load_history=True
        )

    dag = catalogue.dag()
    # Item to target, from the same `_.Installation` a load resolves through.
    # Inventories and the report's subjects are physical: that is where the
    # objects are.
    selected = (
        dag.targets
        if not items
        else tuple(
            dict.fromkeys(
                installed_targets(dag, items, catalogue=workspace.catalogue).values()
            )
        )
    )
    read = {}
    if inventories:
        with session.step("Read installed objects"):
            read = _inventories(session, workspace=workspace, targets=selected, dag=dag)

    return assess(
        catalogue,
        as_of=as_of,
        generated_at=generated_at,
        targets=selected if items else None,
        inventories=read,
    )


def _inventories(session, *, workspace, targets, dag):
    """Each selected target's physical state, read the way a build reads it.

    A Warehouse answers over TDS and a Lakehouse over its storage, so neither
    starts a Spark session. A Lakehouse is read without a Spark catalogue, so
    its views are not listed; see
    :meth:`weaver.health._Assessment._absent_from_inventory`.
    """

    from ..build_bundle.prune import read_lakehouse_inventory, read_warehouse_inventory
    from ..build_bundle.targets import BoundTarget
    from ..targets import ItemRef, WarehouseTarget

    bound_items = {}
    for item, target in dag.installations.items():
        bound_items.setdefault(target, item)

    found = {}
    for target in targets:
        item = bound_items.get(target)
        bound = BoundTarget(
            id=f"{target.kind}-{target.name}",
            kind=target.kind,
            item_id=target.name,
            item_name=target.name,
            workspace_name=workspace.workspace,
            logical_item_type=None if item is None else item.item_type,
            logical_item_name=None if item is None else item.item_name,
        )
        if target.is_lakehouse:
            found[target] = read_lakehouse_inventory(
                bound,
                resolver=session.resolver(workspace),
                store=session.transport_store(workspace),
            )
        else:
            found[target] = read_warehouse_inventory(
                bound,
                sql=session.sql_executor(
                    WarehouseTarget(warehouse=ItemRef.parse(target.name)),
                    workspace=workspace,
                ),
            )
    return found


__all__ = ["HEALTH_TABLES", "health", "run_health"]
