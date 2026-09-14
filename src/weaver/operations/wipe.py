"""Empty physical items and settle their catalogue claims.

Two decisions, made separately. Target selection names the physical items to
empty: the ones given, or the estate the catalogue's ``_.Installation`` rows
describe. Catalogue disposition says what happens to the catalogue itself.

.. code-block:: text

    REMOVE          the named targets, and the catalogue last
    UNBIND          the named targets; the catalogue kept, its claims for them
                    deleted, and never itself a target
    LEAVE           no catalogue resolved
    PHYSICAL_ONLY   exactly the named targets, and no catalogue behaviour

The CLI uses ``REMOVE`` and ``UNBIND``. ``PHYSICAL_ONLY`` is for an internal
operation emptying one named item without reading it as a catalogue.

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

#: Empty the named targets and then the resolved catalogue.
REMOVE = "remove"
#: Empty the named targets, keep the resolved catalogue, and delete its claims
#: for them. The catalogue is never one of the targets.
UNBIND = "unbind"
#: No catalogue resolved, so there is nothing to do with one.
LEAVE = "leave"
#: Empty exactly the named targets without reading or changing a catalogue.
PHYSICAL_ONLY = "physical-only"

CATALOGUE_ACTIONS = (REMOVE, UNBIND, LEAVE, PHYSICAL_ONLY)

#: Physical item types as spelled by the catalogue and target grammar.
LAKEHOUSE = "Lakehouse"
WAREHOUSE = "Warehouse"

EMPTIED = "emptied"
PRESERVED = "preserved"

#: Prefix used by low-level reports for removed shortcuts.
SHORTCUT_PREFIX = "shortcut:"

#: The coarse counts an emptied item carries, in the order they are reported.
COUNTED = ("entries", "shortcuts")


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
    """The estate to empty and the catalogue disposition.

    ``targets`` is in execution order, and a catalogue being removed is last:
    it remains readable while the targets it describes are emptied.
    """

    workspace: Workspace
    targets: tuple[WipeTarget, ...]
    catalogue: str | None
    catalogue_action: str
    unbound: tuple[str, ...] = ()

    def is_catalogue(self, target: WipeTarget) -> bool:
        return (
            self.catalogue is not None
            and str(target).casefold() == self.catalogue.casefold()
        )

    @property
    def empties_the_catalogue(self) -> bool:
        return any(self.is_catalogue(target) for target in self.targets)

    def describe(self) -> str:
        """Describe the Fabric items and catalogue disposition."""

        names = [str(target) for target in self.targets] + [self.catalogue or ""]
        width = max(len(name) for name in names)
        lines = [f"Wipe on {self.workspace.workspace}", "", "Empty"]
        for target in self.targets:
            note = "  catalogue" if self.is_catalogue(target) else ""
            lines.append(f"  {str(target).ljust(width)}{note}".rstrip())
        lines.append("")
        lines.append("Catalogue")
        lines.append(f"  {self._catalogue_line(width)}")
        return "\n".join(lines)

    def _catalogue_line(self, width: int) -> str:
        if self.catalogue is None:
            return "none resolved"
        held = self.catalogue.ljust(width)
        if self.empties_the_catalogue:
            if self.catalogue_action == REMOVE:
                return f"{held}  emptied last"
            # This named target was not read or changed as a catalogue.
            return f"{held}  emptied as a named target; no claims removed"
        if self.unbound:
            claims = ", ".join(self.unbound)
            return f"{held}  preserved; claims for {claims} unbound"
        return f"{held}  preserved; no claims removed"

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
    """Entries removed, or to be removed, from one item area."""

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
    """The outcome for one physical Fabric item."""

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
        """Describe the item outcome in ASCII for Windows console compatibility."""

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
    """Wipe outcomes by physical Fabric item.

    ``items`` is the result. ``reports`` is the per-area detail underneath it,
    kept for a caller that wants the names an area removed.
    """

    workspace: str
    items: tuple[WipeItemResult, ...] = ()
    reports: tuple[WipeReport, ...] = ()
    unbound: Mapping | None = None
    plan: WipePlan | None = None
    dry_run: bool = False

    @property
    def emptied(self) -> tuple[str, ...]:
        """Physical items emptied, in execution order."""

        return tuple(item.target for item in self.items if item.outcome == EMPTIED)

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
    """Settle the estate and catalogue disposition before removing anything.

    Naming targets selects exactly those physical items. Naming none discovers
    the estate from the resolved catalogue's ``_.Installation`` rows.

    ``unbind`` keeps the catalogue and deletes its claims for the emptied
    targets. Internal callers can set ``catalogue_action`` directly.
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
                "wipe needs named targets or a Weaver catalogue for estate "
                "discovery. Name Lakehouse/Name or Warehouse/Name, pass "
                "catalogue='Warehouse/Weaver', or configure a catalogue."
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
    """Validate the requested catalogue disposition."""

    action = _requested_action(named, unbind=unbind, catalogue=catalogue)
    if action == UNBIND:
        _refuse_unusable_unbind(catalogue=catalogue, selected=selected)
    if action == LEAVE and catalogue is not None:
        # LEAVE over a resolved catalogue could leave claims for emptied targets.
        # PHYSICAL_ONLY is the explicit path that ignores those claims.
        raise CommandError(
            f"catalogue_action={LEAVE!r} requires no resolved catalogue, but "
            f"{catalogue} resolved. Use {PHYSICAL_ONLY!r} to empty named targets "
            "without reading the catalogue."
        )
    if action == PHYSICAL_ONLY and not selected:
        raise CommandError("a physical-only wipe requires named targets")
    return action


def _requested_action(named: str | None, *, unbind: bool, catalogue: str | None) -> str:
    if named is None:
        if not unbind:
            return REMOVE if catalogue else LEAVE
        return UNBIND
    if named not in CATALOGUE_ACTIONS:
        raise CommandError(
            f"catalogue_action is one of {', '.join(CATALOGUE_ACTIONS)}, got {named!r}"
        )
    if unbind and named != UNBIND:
        raise CommandError(
            f"unbind asks for {UNBIND!r} and catalogue_action says {named!r}"
        )
    return named


def _refuse_unusable_unbind(*, catalogue: str | None, selected) -> None:
    """Require one preserved catalogue and named non-catalogue targets."""

    if catalogue is None:
        raise CommandError(
            "--unbind requires a catalogue to preserve and update. Pass "
            "--catalogue Warehouse/Weaver or configure a catalogue."
        )
    if not selected:
        raise CommandError(
            "--unbind requires named targets. Pass the targets, or omit "
            "--unbind to wipe the whole estate including its catalogue."
        )
    if any(str(target).casefold() == catalogue.casefold() for target in selected):
        raise CommandError(
            f"--unbind preserves {catalogue}, so it cannot also be a target"
        )


def _execution_order(
    targets: Sequence[WipeTarget], *, catalogue: str | None, action: str
) -> tuple[WipeTarget, ...]:
    """Keep the catalogue readable until last so an interrupted wipe can resume."""

    if action != REMOVE or catalogue is None:
        return tuple(targets)
    folded = catalogue.casefold()
    rest = tuple(target for target in targets if str(target).casefold() != folded)
    return rest + (WipeTarget.parse(catalogue),)


def _installed_estate(workspace: Workspace, *, session) -> tuple[WipeTarget, ...]:
    """Read installed physical targets from ``_.Installation``."""

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
        # Use the target grammar so invalid catalogue rows are reported precisely.
        target = WipeTarget.parse(f"{row['item_type']}/{str(name).strip()}")
        found.setdefault(str(target).casefold(), target)
    return tuple(found[key] for key in sorted(found))


#: Arguments that cannot override a settled destructive plan.
PLANNING_ARGUMENTS = (
    "targets",
    "workspace",
    "catalogue",
    "environment",
    "workspace_config",
    "unbind",
    "catalogue_action",
)


def _refuse_planning_arguments(**given) -> None:
    supplied = sorted(name for name in PLANNING_ARGUMENTS if given.get(name))
    if not supplied:
        return
    raise CommandError(
        "wipe accepts a settled plan or planning arguments, not both. "
        f"Pass {', '.join(supplied)} to plan_wipe or omit them."
    )


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
    """Empty the physical items named by a :class:`WipePlan`.

    Pass either a settled ``plan`` or arguments from which :func:`plan_wipe`
    builds one. ``session`` and ``dry_run`` may accompany either form.
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
    else:
        _refuse_planning_arguments(
            targets=targets,
            workspace=workspace,
            catalogue=catalogue,
            environment=environment,
            workspace_config=workspace_config,
            unbind=unbind,
            catalogue_action=catalogue_action,
        )

    resolved = plan.workspace

    from ..sessions.host import use_or_create_session

    with use_or_create_session(session, workspace=resolved) as opened:
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
    """Count named shortcuts and other entries reported for one item.

    Warehouse wipes report no removed names and therefore contribute no counts.
    """

    counted: dict[str, int] = {}
    for report in reports:
        shortcuts = sum(
            1 for name in report.removed if name.startswith(SHORTCUT_PREFIX)
        )
        entries = report.count - shortcuts
        if entries:
            counted["entries"] = counted.get("entries", 0) + entries
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
    wipe_sql_target(
        warehouse, workspace, sql=session.sql_executor(warehouse, workspace=workspace)
    )
    return (report,)


def _unbind_physical_targets(
    workspace: Workspace, targets: Sequence[WipeTarget], *, session=None
):
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
    """Remove catalogue claims for named physical targets over T-SQL."""

    from ..catalogue.connection import catalogue_connection
    from ..sessions.host import use_or_create_session
    from ..unbind import unbind_targets

    with use_or_create_session(session, workspace=workspace) as opened:
        catalogue = catalogue_connection(opened, workspace)
        return unbind_targets(
            catalogue, lakehouses=lakehouses, warehouses=warehouses
        ).to_mapping()
