"""Public ``weaver.health(...)`` entry point.

Gathers what :mod:`weaver.health` evaluates: the installed catalogue including
its current status tables, one bounded window of ``_.Log`` and
``_.LoadStatistic``, and the physical inventory of each selected Lakehouse or
Warehouse.

Every read is over TDS or OneLake. Health executes no authored load or test
Python, so it takes no Environment, and a Warehouse-only request starts no Livy
session.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Sequence

from ..catalogue.tables import (
    LOAD_STATUS,
    READABLE_TABLES,
    TEST_STATUS,
)
from ..errors import CommandError
from ..health import DEFAULT_AGE_HOURS, HealthReport, assess
from ..targets import PhysicalTargetRef, parse_physical_target
from .workspace import operation_workspace

#: What health reads of the catalogue: what a run reads, plus the two current
#: status tables, which a run writes and never asks about. The history tables
#: are absent: they grow with the estate's age and are read as one bounded
#: window instead.
HEALTH_TABLES = READABLE_TABLES + (LOAD_STATUS, TEST_STATUS)


def health(
    targets: str | Sequence[str] | None = None,
    *,
    as_of: str | datetime | None = None,
    workspace: str | None = None,
    catalogue: str | None = None,
    workspace_config: str | Path | None = None,
    inventories: bool = True,
    session=None,
) -> HealthReport:
    """The installed estate's operational health.

    ``targets`` restricts the subjects reported on. With none, every physical
    target the catalogue binds an item to. Managed ancestry outside the
    selection is still read, because whether a selected object is behind its
    sources is a question about the whole graph.

    ``as_of`` is the instant a settled load must be no older than. It defaults
    to one day before the operation started. An aware datetime or an ISO-8601
    string carrying an offset is accepted; a naive one is refused, because a
    health report crosses timezones.

    ``inventories`` reads each selected target's physical state, so a certified
    object that is not there is reported. Turning it off leaves Build health to
    what the catalogue contradicts about itself.
    """

    started = datetime.now(timezone.utc)
    requested = _requested(targets)
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
                requested=requested,
                as_of=_as_of(as_of, started=started),
                generated_at=started,
                inventories=inventories,
            )


def run_health(
    session,
    *,
    workspace,
    requested: Sequence[PhysicalTargetRef] = (),
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
        # Current state and the bounded window of recent activity, in one read.
        # Everything below reasons from this catalogue and asks the Warehouse
        # nothing further.
        catalogue = read_installed_catalogue(
            connection, tables=HEALTH_TABLES, load_history=True
        )

    selected = _selected(catalogue, requested)
    read = {}
    if inventories:
        with session.step("Read installed objects"):
            read = _inventories(
                session, workspace=workspace, targets=selected, dag=catalogue.dag()
            )

    return assess(
        catalogue,
        as_of=as_of,
        generated_at=generated_at,
        targets=selected if requested else None,
        inventories=read,
    )


def _selected(catalogue, requested) -> tuple[PhysicalTargetRef, ...]:
    """The targets this report is about, refusing one the catalogue does not bind."""

    installed = catalogue.dag().targets
    if not requested:
        return installed
    unknown = [target for target in requested if target not in installed]
    if unknown:
        known = ", ".join(str(target) for target in installed) or "none"
        raise CommandError(
            "no installed estate in "
            + ", ".join(str(target) for target in unknown)
            + f". The catalogue binds no logical item to it. Installed: {known}"
        )
    return tuple(dict.fromkeys(requested))


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


def _requested(targets) -> tuple[PhysicalTargetRef, ...]:
    if targets is None:
        return ()
    values = (targets,) if isinstance(targets, str) else tuple(targets)
    return tuple(
        PhysicalTargetRef.of(
            parse_physical_target(value, what="health target", error=CommandError)
        )
        for value in values
    )


def _as_of(value, *, started: datetime) -> datetime:
    """The instant this report measures freshness against, always UTC.

    A naive datetime is refused. A report that named an instant without a zone
    would mean a different moment on every machine that read it.
    """

    if value is None:
        return started - timedelta(hours=DEFAULT_AGE_HOURS)
    if isinstance(value, str):
        text = value.strip()
        try:
            value = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            raise CommandError(
                f"as-of must be an ISO-8601 instant with a zone, got {text!r}"
            ) from None
    if not isinstance(value, datetime):
        raise CommandError(
            f"as-of must be a datetime or an ISO-8601 string, got "
            f"{type(value).__name__}"
        )
    if value.tzinfo is None:
        raise CommandError(
            "as-of must carry a timezone, so the instant it names is the same "
            "on every machine that reads the report"
        )
    return value.astimezone(timezone.utc)


__all__ = ["HEALTH_TABLES", "health", "run_health"]
