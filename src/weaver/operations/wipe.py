"""Emptying a physical item, and the catalogue claims that leaves behind.

A wipe is the one operation that removes rather than builds, so it is kept
apart from the build it usually precedes: what they share is how a workspace is
resolved, and that lives in :mod:`weaver.operations.workspace`.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from ..errors import CommandError
from ..locations import Location
from ..targets import (
    ItemRef,
    WarehouseTarget,
    parse_physical_target,
    physical_item,
    physical_kind,
)
from ..workspaces import Workspace
from .workspace import operation_workspace


@dataclass(frozen=True)
class WipeTarget:
    item_type: str
    item: ItemRef

    @classmethod
    def parse(cls, text: str) -> "WipeTarget":
        target = parse_physical_target(text, what="wipe target", error=CommandError)
        return cls(item_type=physical_kind(target), item=physical_item(target))

    @property
    def physical_name(self) -> str:
        return self.item.name

    def __str__(self) -> str:
        return f"{self.item_type}/{self.item}"


def _unbind_target_names(
    targets: Iterable[str],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Parse unbind selection through the same typed grammar used by wipe."""

    parsed = tuple(WipeTarget.parse(target) for target in targets)
    return (
        tuple(
            target.physical_name for target in parsed if target.item_type == "Lakehouse"
        ),
        tuple(
            target.physical_name for target in parsed if target.item_type == "Warehouse"
        ),
    )


@dataclass(frozen=True)
class WipeReport:
    target: str
    location: Location
    removed: tuple[str, ...]
    dry_run: bool = False

    @property
    def count(self) -> int:
        return len(self.removed)

    def to_mapping(self) -> dict:
        return {
            "target": self.target,
            "location": self.location.value,
            "removed": list(self.removed),
            "dry_run": self.dry_run,
        }


@dataclass(frozen=True)
class WipeResult:
    workspace: str
    reports: tuple[WipeReport, ...]
    unbound: Mapping | None = None
    dry_run: bool = False

    @property
    def count(self) -> int:
        return sum(report.count for report in self.reports)

    def to_mapping(self) -> dict:
        return {
            "workspace": self.workspace,
            "reports": [report.to_mapping() for report in self.reports],
            "unbound": dict(self.unbound) if self.unbound is not None else None,
            "dry_run": self.dry_run,
        }


def wipe(
    targets: str | Iterable[str],
    *,
    workspace: str | None = None,
    catalogue: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    unbind_from: str | None = None,
    dry_run: bool = False,
    session=None,
) -> WipeResult:
    """Empty one or more whole Lakehouse or Warehouse items.

    Takes a Session as the other operations do: a wipe resolves the same item
    names, reaches the same OneLake paths and opens the same Warehouse
    connections as the build before it. It needs no Builder and no Runner.
    """

    values = (targets,) if isinstance(targets, str) else tuple(targets)
    parsed = tuple(WipeTarget.parse(value) for value in values)
    if not parsed:
        raise CommandError("wipe needs at least one target")
    resolved_workspace = operation_workspace(
        "wipe",
        workspace=workspace,
        catalogue=catalogue,
        environment=environment,
        workspace_config=workspace_config,
        session=session,
        # A wipe empties a physical item, which needs no control plane. The
        # catalogue only comes into it if `--unbind-from` asks for the claims.
        needs_catalogue=False,
    )
    from ..sessions.host import use_or_create_session

    with use_or_create_session(session, workspace=resolved_workspace) as opened:
        # Named for what it is. A dry run reads the estate and decides, which
        # takes real time and is worth seeing; what it must not do is present
        # itself as the removal.
        with opened.task(
            "Wipe (dry run)" if dry_run else "Wipe", ", ".join(map(str, parsed))
        ):
            storage_targets = tuple(t for t in parsed if t.item_type == "Lakehouse")
            store = opened.store(resolved_workspace) if storage_targets else None
            reports: list[WipeReport] = []
            for target in parsed:
                with opened.step(str(target)):
                    reports.extend(
                        _wipe_one(
                            target,
                            resolved_workspace,
                            store=store,
                            dry_run=dry_run,
                            session=opened,
                        )
                    )

            unbound = None
            control = unbind_from or resolved_workspace.catalogue
            # Compared as item names, because the two arrive spelled
            # differently: `unbind_from` names an item and the workspace's
            # catalogue is typed. Wiping the catalogue itself skips the unbind
            # entirely — deleting rows from tables that are about to be removed
            # is work nobody needs.
            control_name = str(control).rpartition("/")[2] if control else None
            whole_lakehouses = {
                target.physical_name
                for target in parsed
                if target.item_type == "Lakehouse"
            }
            if not dry_run and control and control_name not in whole_lakehouses:
                # `unbind_from` names an item; the workspace field is typed.
                # Both mean one Lakehouse, so the field is written typed.
                catalogue_workspace = replace(
                    resolved_workspace,
                    catalogue=control
                    if "/" in str(control)
                    else f"Lakehouse/{control}",
                )
                with opened.step("Unbind catalogue claims"):
                    unbound = _unbind_physical_targets(
                        catalogue_workspace, parsed, session=opened
                    )

            return WipeResult(
                workspace=str(resolved_workspace.workspace),
                reports=tuple(reports),
                unbound=unbound,
                dry_run=dry_run,
            )


def _wipe_one(target: WipeTarget, workspace, *, store, dry_run, session):
    from ..physical_wipe import wipe_lakehouse, wipe_sql_target

    if target.item_type == "Lakehouse":
        low = wipe_lakehouse(
            target.item, workspace, store=store, dry_run=dry_run, session=session
        )
        return tuple(
            WipeReport(
                target=str(target),
                location=report.location,
                removed=report.removed,
                dry_run=dry_run,
            )
            for report in low
        )

    report = WipeReport(
        target=str(target),
        location=Location(f"warehouse://{target.item.name}"),
        removed=("all user-created SQL objects",),
        dry_run=dry_run,
    )
    if dry_run:
        return (report,)
    warehouse = WarehouseTarget(target.item)
    # The Session's connection, reused and closed with the Session. A wipe that
    # opened its own would pay for a Warehouse the build before it had already
    # connected to — and would close it before the load after it connects again.
    wipe_sql_target(
        warehouse, workspace, sql=session.sql_executor(warehouse, workspace=workspace)
    )
    return (report,)


def _unbind_physical_targets(
    workspace: Workspace, targets: Sequence[WipeTarget], *, session=None
):
    """The catalogue claims a set of wiped targets leaves behind."""

    return unbind_catalogue_claims(
        workspace,
        lakehouses=sorted(
            {
                target.physical_name
                for target in targets
                if target.item_type == "Lakehouse"
            }
        ),
        warehouses=sorted(
            {
                target.physical_name
                for target in targets
                if target.item_type == "Warehouse"
            }
        ),
        session=session,
    )


def unbind_catalogue_claims(
    workspace: Workspace, *, lakehouses, warehouses, session=None
) -> dict:
    """Remove catalogue claims for named physical targets.

    Two callers want it: ``weaver unbind``, and the tail of a ``wipe`` that
    emptied a target the catalogue still claims. Reading and deleting are both
    Spark SQL, so the statements go through the Session.
    """

    from ..build_bundle.workflow import session_catalogue
    from ..sessions.host import use_or_create_session
    from ..unbind import unbind_targets

    with use_or_create_session(session, workspace=workspace) as opened:
        if not opened.executes_here(workspace) and not workspace.environment:
            from ..fabric.livy import missing_environment

            raise CommandError(missing_environment(workspace))
        catalogue = session_catalogue(opened, workspace, workspace.catalogue_item)
        return unbind_targets(
            catalogue, lakehouses=lakehouses, warehouses=warehouses
        ).to_mapping()
