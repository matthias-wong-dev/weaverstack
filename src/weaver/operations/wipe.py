"""Emptying physical items, and the catalogue state that decides what a wipe means.

A wipe answers one question: am I pointed at the estate I intend to destroy?
It is kept apart from the build it usually precedes; what they share is how a
workspace is resolved, and that lives in :mod:`weaver.operations.workspace`.

Three shapes, one vocabulary:

- naming targets wipes exactly those physical items, and nothing else;
- naming no target with a catalogue wipes the estate that catalogue holds: every
  target its Installation rows name, plus the catalogue Warehouse itself;
- ``--unbind`` wipes named targets while preserving the catalogue, so its claims
  must be removed for it by name.

See ``design/cli-usage.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
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
    #: How the catalogue stood to this wipe: ``"removed with the estate"``,
    #: ``"preserved; claims unbound"`` or ``None`` where no catalogue resolved.
    catalogue_role: str | None = None

    @property
    def count(self) -> int:
        return sum(report.count for report in self.reports)

    def to_mapping(self) -> dict:
        return {
            "workspace": self.workspace,
            "reports": [report.to_mapping() for report in self.reports],
            "unbound": dict(self.unbound) if self.unbound is not None else None,
            "catalogue_role": self.catalogue_role,
            "dry_run": self.dry_run,
        }


def wipe(
    targets: str | Iterable[str] = (),
    *,
    workspace: str | None = None,
    catalogue: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    unbind: bool = False,
    dry_run: bool = False,
    session=None,
) -> WipeResult:
    """Empty physical items: exactly those named, or the whole estate.

    Named targets are wiped and nothing else: the catalogue's claims are left
    alone unless ``unbind`` asks for their removal by name. Naming no target
    wipes the estate the resolved catalogue holds - every target its
    Installation rows name, plus the catalogue Warehouse itself, whose removal
    takes its claims with it.

    Takes a Session as the other operations do: a wipe resolves the same item
    names, reaches the same OneLake paths and opens the same Warehouse
    connections as the build before it. It needs no Builder and no Runner.
    """

    values = (targets,) if isinstance(targets, str) else tuple(targets)
    parsed = tuple(WipeTarget.parse(value) for value in values)
    if unbind and not parsed:
        raise CommandError(
            "unbind preserves the catalogue while named targets are wiped, "
            "so it needs at least one target."
        )
    resolved_workspace = operation_workspace(
        "wipe",
        workspace=workspace,
        catalogue=catalogue,
        environment=environment,
        workspace_config=workspace_config,
        session=session,
        # A wipe empties physical items, which needs no catalogue. One that
        # resolves decides what an untargeted wipe means.
        needs_catalogue=False,
    )
    if not parsed:
        parsed = _estate_targets(resolved_workspace, session=session)
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
            catalogue_role = _catalogue_role(resolved_workspace, parsed, unbind)
            # Claims for a catalogue that is itself being wiped go with it; the
            # unbind removes what a preserved catalogue still holds.
            unbind_targets_parsed = tuple(
                target
                for target in parsed
                if (target.item_type, target.physical_name.casefold())
                != _catalogue_key(resolved_workspace)
            )
            if unbind and not dry_run and unbind_targets_parsed:
                with opened.step("Unbind catalogue claims"):
                    unbound = _unbind_physical_targets(
                        resolved_workspace, unbind_targets_parsed, session=opened
                    )

            return WipeResult(
                workspace=str(resolved_workspace.workspace),
                reports=tuple(reports),
                unbound=unbound,
                catalogue_role=catalogue_role,
                dry_run=dry_run,
            )


def _catalogue_key(workspace: Workspace) -> tuple[str, str] | None:
    """The resolved catalogue as a ``(kind, folded name)`` pair, or ``None``."""

    if not workspace.catalogue:
        return None
    kind, _, name = workspace.catalogue.partition("/")
    return (kind, name.casefold())


def _catalogue_role(
    workspace: Workspace, targets: Sequence[WipeTarget], unbind: bool
) -> str | None:
    """How the catalogue stands to this wipe, as the preflight states it."""

    catalogue = workspace.catalogue
    if not catalogue:
        return None
    wiped = {(target.item_type, target.physical_name.casefold()) for target in targets}
    if _catalogue_key(workspace) in wiped:
        return "removed with the estate"
    if unbind:
        return "preserved; claims unbound"
    return "preserved"


def _estate_targets(workspace: Workspace, *, session=None) -> tuple[WipeTarget, ...]:
    """The estate one catalogue holds, catalogue first.

    The Installation rows say what is bound; the catalogue Warehouse itself
    completes the estate, and its removal takes its claims with it.
    """

    from ..catalogue.connection import catalogue_connection
    from ..catalogue.reader import read_table
    from ..catalogue.tables import INSTALLATION
    from ..sessions.host import use_or_create_session

    if not workspace.catalogue:
        raise CommandError(
            "Wiping a whole estate needs a Weaver catalogue: pass "
            "catalogue='Warehouse/Weaver', or give one in workspace "
            "configuration."
        )
    with use_or_create_session(session, workspace=workspace) as opened:
        connection = catalogue_connection(opened, workspace)
        rows = read_table(connection, INSTALLATION)

    kind, _, name = workspace.catalogue.partition("/")
    parsed = [WipeTarget(item_type=kind, item=ItemRef(name))]
    seen = {(kind, name.casefold())}
    for row in rows:
        target = WipeTarget(
            item_type=str(row.get("item_type") or ""),
            item=ItemRef(str(row.get("target_name") or "")),
        )
        if target.item_type not in ("Lakehouse", "Warehouse"):
            raise CommandError(
                f"The catalogue holds an installation of {target.item_type!r} "
                f"for {target.item}, which is not a wipeable item kind."
            )
        if (target.item_type, target.physical_name.casefold()) in seen:
            continue
        seen.add((target.item_type, target.physical_name.casefold()))
        parsed.append(target)
    return tuple(parsed)


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
    # connected to, and would close it before the load after it connects again.
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

    The caller is a wipe run with ``unbind``: physical targets were wiped while
    the catalogue was preserved, so the claims those targets held are removed
    here by name. Reading and deleting are both T-SQL against
    the catalogue Warehouse, so neither needs Spark and the statements go
    through the Session.
    """

    from ..catalogue.connection import catalogue_connection
    from ..sessions.host import use_or_create_session
    from ..unbind import unbind_targets

    with use_or_create_session(session, workspace=workspace) as opened:
        catalogue = catalogue_connection(opened, workspace)
        return unbind_targets(
            catalogue, lakehouses=lakehouses, warehouses=warehouses
        ).to_mapping()
