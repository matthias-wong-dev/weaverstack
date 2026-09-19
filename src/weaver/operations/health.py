"""Public ``weaver.health(...)`` operation.

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
    MIRROR,
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

#: Catalogue tables needed for topology and current lifecycle state.
HEALTH_TABLES = (
    INSTALLATION,
    REGISTRY,
    TABLE_DICTIONARY,
    FOLDER_DICTIONARY,
    TEST_DICTIONARY,
    DEPENDENCY,
    SHORTCUT,
    MIRROR,
    LOAD_STATUS,
    TEST_STATUS,
)

#: Mirrored catalogues contribute only current load state.
SOURCE_TABLES = (LOAD_STATUS,)


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
    """Report the installed estate's operational health.

    ``items`` restricts the subjects reported on. With none, every target the
    catalogue binds an item to. Managed ancestry outside the selection is still
    read, because whether a selected object is behind its sources is a question
    about the whole graph.

    ``as_of`` is the oldest acceptable settled load time and defaults to one day
    before the operation started. It must include a timezone.

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
    from ..catalogue.connection import catalogue_connection
    from ..catalogue.state import read_installed_catalogue

    connection = catalogue_connection(session, workspace)
    with session.step("Read catalogue"):
        # Read current state and its supporting statistics together.
        catalogue = read_installed_catalogue(
            connection, tables=HEALTH_TABLES, load_history=True
        )

    source = None
    if catalogue.mirrors:
        from .mirror import mirrored_source

        with session.step("Read the mirrored catalogue"):
            # Mirrored load state comes from the catalogue where rows are written.
            source = mirrored_source(
                catalogue,
                workspace=workspace,
                session=session,
                operation="health",
                tables=SOURCE_TABLES,
                history=True,
            )

    dag = catalogue.dag()
    # Report physical targets bound by the catalogue's installations.
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
        items=tuple(items) or None,
        inventories=read,
        source=source,
    )


def _inventories(session, *, workspace, targets, dag):
    """Read each selected target's physical state without starting Spark.

    Warehouse state comes over TDS and Lakehouse state from storage. Lakehouse
    views are therefore not listed; see
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


__all__ = ["HEALTH_TABLES", "SOURCE_TABLES", "health", "run_health"]
