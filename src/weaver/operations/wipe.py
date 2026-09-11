"""Emptying the physical estate, and what that leaves in the catalogue.

A wipe is the one operation that removes rather than builds, so it is kept
apart from the build it usually precedes: what they share is how a workspace is
resolved, and that lives in :mod:`weaver.operations.workspace`.

Two decisions, made separately. Target selection names the physical items to
empty: the ones given, or the estate the catalogue's ``_.Installation`` rows
describe. Catalogue disposition says what happens to the catalogue itself.
:data:`REMOVE` empties it last, :data:`UNBIND` keeps it and deletes its claims
for the emptied targets, :data:`LEAVE` touches it not at all.

:func:`plan_wipe` settles both and returns a frozen :class:`WipePlan`, and
:func:`wipe` executes one. The plan a caller shows is the plan it runs.
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

#: Empty the resolved catalogue as well, last of all.
REMOVE = "remove"
#: Keep the resolved catalogue, and delete its claims for the emptied targets.
UNBIND = "unbind"
#: Leave the catalogue alone.
LEAVE = "leave"

CATALOGUE_ACTIONS = (REMOVE, UNBIND, LEAVE)

#: The two physical item types, as the catalogue and the target grammar spell
#: them.
LAKEHOUSE = "Lakehouse"
WAREHOUSE = "Warehouse"

#: What an item-level outcome says happened to a physical item.
EMPTIED = "emptied"
PRESERVED = "preserved"

#: A removal that took a pointer away, as the low-level reports spell it.
SHORTCUT_PREFIX = "shortcut:"

#: The coarse counts an emptied item carries, in the order they are reported.
COUNTED = ("tables", "folders", "shortcuts")


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


@dataclass(frozen=True)
class WipePlan:
    """The estate one wipe empties, and what it does with the catalogue.

    ``targets`` is in execution order, and a catalogue being removed is last:
    the catalogue is the index describing this estate, so it stays readable
    while the targets it names are emptied.
    """

    workspace: Workspace
    targets: tuple[WipeTarget, ...]
    catalogue: str | None
    catalogue_action: str
    unbound: tuple[str, ...] = ()

    def is_catalogue(self, target: WipeTarget) -> bool:
        """Whether this target is the catalogue this plan resolved."""

        return (
            self.catalogue is not None
            and str(target).casefold() == self.catalogue.casefold()
        )

    def describe(self) -> str:
        """The estate this wipe is pointed at, as the question a person answers.

        Fabric items, and what happens to each. No table, view, schema,
        shortcut path or SQL object name: the question is which estate, and an
        inventory answers a different one.
        """

        names = [str(target) for target in self.targets] + [self.catalogue or ""]
        width = max(len(name) for name in names)
        lines = [f"Wipe on {self.workspace.workspace}", "", "Empty"]
        for target in self.targets:
            note = "  catalogue" if self.is_catalogue(target) else ""
            lines.append(f"  {str(target).ljust(width)}{note}".rstrip())
        lines.append("")
        lines.append("Catalogue")
        if self.catalogue is None:
            lines.append("  none resolved")
            return "\n".join(lines)
        held = self.catalogue.ljust(width)
        if self.catalogue_action == REMOVE:
            lines.append(f"  {held}  emptied last")
        elif self.catalogue_action == UNBIND:
            claims = ", ".join(self.unbound)
            lines.append(f"  {held}  preserved; claims for {claims} unbound")
        else:
            lines.append(f"  {held}  untouched")
        return "\n".join(lines)

    def to_mapping(self) -> dict:
        return {
            "workspace": str(self.workspace.workspace),
            "targets": [
                {"target": str(target), "catalogue": self.is_catalogue(target)}
                for target in self.targets
            ],
            "catalogue": self.catalogue,
            "catalogue_action": self.catalogue_action,
            "unbound": list(self.unbound),
        }


@dataclass(frozen=True)
class WipeReport:
    """What emptying one area of one physical item removed, or would remove."""

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
class WipeItemResult:
    """One physical Fabric item, and what the wipe did to it."""

    target: str
    outcome: str
    is_catalogue: bool = False
    unbound: bool = False
    counts: Mapping[str, int] = None  # type: ignore[assignment]
    reports: tuple[WipeReport, ...] = ()

    def __post_init__(self) -> None:
        if self.counts is None:
            object.__setattr__(self, "counts", {})

    def describe(self) -> str:
        """One line: the item, what happened to it, and the coarse counts.

        ASCII, because a Windows console runs on the system codepage and this
        is the last thing printed after an estate was emptied.
        """

        outcome = f"catalogue {self.outcome}" if self.is_catalogue else self.outcome
        words = [outcome]
        if self.unbound:
            words.append("claims unbound")
        counted = self.counts or {}
        words.extend(f"{counted[word]} {word}" for word in COUNTED if counted.get(word))
        return f"{self.target:<28}{', '.join(words)}"

    def to_mapping(self) -> dict:
        return {
            "target": self.target,
            "outcome": self.outcome,
            "catalogue": self.is_catalogue,
            "unbound": self.unbound,
            "counts": dict(self.counts or {}),
        }


@dataclass(frozen=True)
class WipeResult:
    workspace: str
    items: tuple[WipeItemResult, ...] = ()
    reports: tuple[WipeReport, ...] = ()
    unbound: Mapping | None = None
    plan: WipePlan | None = None
    dry_run: bool = False

    @property
    def count(self) -> int:
        return sum(report.count for report in self.reports)

    def to_mapping(self) -> dict:
        return {
            "workspace": self.workspace,
            "plan": None if self.plan is None else self.plan.to_mapping(),
            "items": [item.to_mapping() for item in self.items],
            "unbound": dict(self.unbound) if self.unbound is not None else None,
            "dry_run": self.dry_run,
        }


def plan_wipe(
    targets: str | Iterable[str] = (),
    *,
    workspace: str | None = None,
    catalogue: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    unbind: bool = False,
    catalogue_action: str | None = None,
    session=None,
) -> WipePlan:
    """The estate one wipe empties, settled before anything is removed.

    Naming targets selects exactly those physical items. Naming none discovers
    the estate from the resolved catalogue's ``_.Installation`` rows.

    ``unbind`` asks for the catalogue to be kept and its claims for the emptied
    targets deleted. ``catalogue_action`` names the disposition outright, which
    is how an internal caller empties one Warehouse without treating a
    catalogue as an estate index.
    """

    values = (targets,) if isinstance(targets, str) else tuple(targets)
    selected = tuple(dict.fromkeys(WipeTarget.parse(value) for value in values))
    resolved = operation_workspace(
        "wipe",
        workspace=workspace,
        catalogue=catalogue,
        environment=environment,
        workspace_config=workspace_config,
        session=session,
        # A named target is emptied whether or not a catalogue exists. Estate
        # discovery needs one, and says so below.
        needs_catalogue=False,
    )
    resolved_catalogue = resolved.catalogue or None
    action = _catalogue_action(
        catalogue_action,
        unbind=unbind,
        catalogue=resolved_catalogue,
        selected=selected,
    )

    if selected:
        discovered = selected
    else:
        if resolved_catalogue is None:
            raise CommandError(
                "wipe needs targets or a Weaver catalogue to discover them "
                "from: name Lakehouse/Name or Warehouse/Name, pass "
                "catalogue='Warehouse/Weaver', or give one in workspace "
                "configuration"
            )
        from ..sessions.host import use_or_create_session

        with use_or_create_session(session, workspace=resolved) as opened:
            with opened.task("Read the installed estate", resolved_catalogue):
                discovered = _installed_estate(resolved, session=opened)

    ordered = _execution_order(discovered, catalogue=resolved_catalogue, action=action)
    unbound = ()
    if action == UNBIND:
        unbound = tuple(
            str(target)
            for target in ordered
            if resolved_catalogue is None
            or str(target).casefold() != resolved_catalogue.casefold()
        )
    return WipePlan(
        workspace=resolved,
        targets=ordered,
        catalogue=resolved_catalogue,
        catalogue_action=action,
        unbound=unbound,
    )


def _catalogue_action(
    named: str | None, *, unbind: bool, catalogue: str | None, selected
) -> str:
    """What this invocation does with the catalogue it resolved."""

    if named is not None:
        if named not in CATALOGUE_ACTIONS:
            raise CommandError(
                "catalogue_action is one of "
                f"{', '.join(CATALOGUE_ACTIONS)}, got {named!r}"
            )
        if unbind and named != UNBIND:
            raise CommandError(
                f"unbind asks for {UNBIND!r} and catalogue_action says {named!r}"
            )
        return named
    if not unbind:
        return REMOVE if catalogue else LEAVE
    if catalogue is None:
        raise CommandError(
            "--unbind keeps a catalogue and removes its claims, and this "
            "command resolved none: pass --catalogue Warehouse/Weaver, or give "
            "one in workspace configuration"
        )
    if not selected:
        raise CommandError(
            "--unbind removes the claims for the targets it emptied, so it "
            "needs them named: pass the targets, or leave --unbind off to wipe "
            "the whole estate including its catalogue"
        )
    if any(str(target).casefold() == catalogue.casefold() for target in selected):
        raise CommandError(
            f"--unbind keeps {catalogue}, and this command also names it as a "
            "target to empty"
        )
    return UNBIND


def _execution_order(
    targets: Sequence[WipeTarget], *, catalogue: str | None, action: str
) -> tuple[WipeTarget, ...]:
    """The targets in the order they are emptied, the catalogue last.

    The catalogue describes this estate, so it stays readable while the targets
    it names are emptied, and a wipe stopped part way can be run again.
    """

    if action != REMOVE or catalogue is None:
        return tuple(targets)
    folded = catalogue.casefold()
    rest = tuple(target for target in targets if str(target).casefold() != folded)
    return rest + (WipeTarget.parse(catalogue),)


def _installed_estate(workspace: Workspace, *, session) -> tuple[WipeTarget, ...]:
    """The physical targets this catalogue's installations are bound to.

    Read from ``_.Installation``. What workspace configuration declares is what
    a build would install, and what a wipe empties is what one did.
    """

    from ..catalogue.connection import catalogue_connection

    return installed_targets(catalogue_connection(session, workspace))


def installed_targets(catalogue) -> tuple[WipeTarget, ...]:
    """The distinct physical targets one catalogue's installations name."""

    from ..catalogue.reader import read_table
    from ..catalogue.tables import INSTALLATION

    found: dict[str, WipeTarget] = {}
    for row in read_table(catalogue, INSTALLATION):
        name = row.get("target_name")
        if not name:
            continue
        # Through the target grammar, so a row naming something other than a
        # Lakehouse or a Warehouse is reported as the catalogue row it is.
        target = WipeTarget.parse(f"{row['item_type']}/{str(name).strip()}")
        found.setdefault(str(target).casefold(), target)
    return tuple(found[key] for key in sorted(found))


def wipe(
    targets: str | Iterable[str] = (),
    *,
    plan: WipePlan | None = None,
    workspace: str | None = None,
    catalogue: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    unbind: bool = False,
    catalogue_action: str | None = None,
    dry_run: bool = False,
    session=None,
) -> WipeResult:
    """Empty the physical items one :class:`WipePlan` names.

    ``plan`` runs an already-settled plan, which is how a caller that showed one
    to a person empties the estate it showed. The selectors are the convenience
    form and build a plan through :func:`plan_wipe` first.

    Takes a Session as the other operations do: a wipe resolves the same item
    names, reaches the same OneLake paths and opens the same Warehouse
    connections as the build before it. It needs no Builder and no Runner.
    """

    if plan is None:
        plan = plan_wipe(
            targets,
            workspace=workspace,
            catalogue=catalogue,
            environment=environment,
            workspace_config=workspace_config,
            unbind=unbind,
            catalogue_action=catalogue_action,
            session=session,
        )
    elif targets:
        raise CommandError("wipe takes a plan or a target selection, not both")

    resolved = plan.workspace

    from ..sessions.host import use_or_create_session

    with use_or_create_session(session, workspace=resolved) as opened:
        # Named for what it is. A dry run reads the estate and decides, which
        # takes real time and is worth seeing; what it must not do is present
        # itself as the removal.
        with opened.task(
            "Wipe (dry run)" if dry_run else "Wipe",
            ", ".join(str(target) for target in plan.targets),
        ):
            storage = any(target.item_type == LAKEHOUSE for target in plan.targets)
            store = opened.store(resolved) if storage else None
            items: list[WipeItemResult] = []
            reports: list[WipeReport] = []
            for target in plan.targets:
                with opened.step(str(target)):
                    produced = _wipe_one(
                        target, resolved, store=store, dry_run=dry_run, session=opened
                    )
                reports.extend(produced)
                items.append(
                    WipeItemResult(
                        target=str(target),
                        outcome=EMPTIED,
                        is_catalogue=plan.is_catalogue(target),
                        counts=_counts(produced),
                        reports=produced,
                    )
                )

            unbound = None
            if plan.catalogue_action == UNBIND and plan.unbound:
                if not dry_run:
                    with opened.step("Unbind catalogue claims"):
                        unbound = _unbind_physical_targets(
                            resolved,
                            tuple(WipeTarget.parse(value) for value in plan.unbound),
                            session=opened,
                        )
                items.append(
                    WipeItemResult(
                        target=plan.catalogue,
                        outcome=PRESERVED,
                        is_catalogue=True,
                        unbound=True,
                    )
                )

            return WipeResult(
                workspace=str(resolved.workspace),
                items=tuple(items),
                reports=tuple(reports),
                unbound=unbound,
                plan=plan,
                dry_run=dry_run,
            )


def _counts(reports: Sequence[WipeReport]) -> dict[str, int]:
    """Coarse counts for one item, from what its areas reported removing.

    A shortcut is a pointer taken away and the rest are directories deleted, so
    they are counted apart. A Warehouse reports no names and counts nothing:
    Weaver's Warehouse wipe drops object types by enumerating them, and there
    is no list of what it dropped to count.
    """

    counted: dict[str, int] = {}
    for report in reports:
        shortcuts = sum(
            1 for name in report.removed if name.startswith(SHORTCUT_PREFIX)
        )
        rest = report.count - shortcuts
        word = "tables" if report.target.startswith("delta:") else "folders"
        if rest:
            counted[word] = counted.get(word, 0) + rest
        if shortcuts:
            counted["shortcuts"] = counted.get("shortcuts", 0) + shortcuts
    return counted


def _wipe_one(target: WipeTarget, workspace, *, store, dry_run, session):
    from ..physical_wipe import wipe_lakehouse, wipe_sql_target

    if target.item_type == LAKEHOUSE:
        low = wipe_lakehouse(
            target.item, workspace, store=store, dry_run=dry_run, session=session
        )
        return tuple(
            WipeReport(
                target=report.target,
                location=report.location,
                removed=report.removed,
                dry_run=dry_run,
            )
            for report in low
        )

    report = WipeReport(
        target=str(target),
        location=Location(f"warehouse://{target.item.name}"),
        removed=(),
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
                if target.item_type == LAKEHOUSE
            }
        ),
        warehouses=sorted(
            {
                target.physical_name
                for target in targets
                if target.item_type == WAREHOUSE
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
    T-SQL against the catalogue Warehouse, so neither needs Spark and the
    statements go through the Session.
    """

    from ..catalogue.connection import catalogue_connection
    from ..sessions.host import use_or_create_session
    from ..unbind import unbind_targets

    with use_or_create_session(session, workspace=workspace) as opened:
        catalogue = catalogue_connection(opened, workspace)
        return unbind_targets(
            catalogue, lakehouses=lakehouses, warehouses=warehouses
        ).to_mapping()
