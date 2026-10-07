"""Remove a catalogue's claims for physical items and leave the items as they are.

:func:`plan_unbind` reads the catalogue's installations and the workspace's
items, so the plan says which logical items stop being managed and whether each
physical item still exists. :func:`unbind` deletes the claims and changes
nothing in Fabric.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping

from ..errors import CommandError
from ..workspaces import Workspace
from .wipe import WipeTarget
from .workspace import operation_workspace


@dataclass(frozen=True)
class UnbindPlan:
    """The targets whose claims are removed, as the catalogue and Fabric hold them."""

    workspace: Workspace
    catalogue: str
    targets: tuple[WipeTarget, ...]
    #: Logical items the catalogue records in each target, by target.
    installed: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    #: Targets that still exist in the workspace.
    present: frozenset[str] = frozenset()

    @property
    def still_in_fabric(self) -> tuple[str, ...]:
        return tuple(str(t) for t in self.targets if str(t) in self.present)

    def _named(self, target: WipeTarget) -> str:
        items = [i for i in self.installed.get(str(target), ()) if i != str(target)]
        return f"{', '.join(items)} → {target}" if items else str(target)

    def describe(self) -> str:
        names = [self._named(target) for target in self.targets]
        width = max(len(name) for name in [*names, self.catalogue])
        lines = [f"Unbind on {self.workspace.workspace}", "", "Forget"]
        for target, name in zip(self.targets, names):
            state = (
                "still in Fabric" if str(target) in self.present else "not in Fabric"
            )
            lines.append(f"  {name.ljust(width)}  {state}")
        lines += [
            "",
            "Catalogue",
            f"  {self.catalogue.ljust(width)}  preserved; claims removed",
        ]
        return "\n".join(lines)

    def to_mapping(self) -> dict:
        return {
            "workspace": str(self.workspace.workspace),
            "catalogue": self.catalogue,
            "targets": [
                {
                    "target": str(target),
                    "items": list(self.installed.get(str(target), ())),
                    "in_fabric": str(target) in self.present,
                }
                for target in self.targets
            ],
        }


@dataclass(frozen=True)
class UnbindResult:
    plan: UnbindPlan
    logical_items: tuple[str, ...] = ()
    dry_run: bool = False

    def to_mapping(self) -> dict:
        return {
            **self.plan.to_mapping(),
            "unbound": list(self.logical_items),
            "dry_run": self.dry_run,
        }


def plan_unbind(
    targets: str | Iterable[str],
    *,
    workspace: str | None = None,
    catalogue: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    session=None,
) -> UnbindPlan:
    """Settle which claims an unbind removes, and whether each item still exists."""

    values = (targets,) if isinstance(targets, str) else tuple(targets)
    selected = tuple(dict.fromkeys(WipeTarget.parse(value) for value in values))
    if not selected:
        raise CommandError(
            "unbind needs at least one target: Lakehouse/Name, Warehouse/Name or "
            "SemanticModel/Name."
        )
    resolved = operation_workspace(
        "unbind",
        workspace=workspace,
        catalogue=catalogue,
        environment=environment,
        workspace_config=workspace_config,
        session=session,
    )
    for target in selected:
        if str(target).casefold() == resolved.catalogue.casefold():
            raise CommandError(
                f"{target} is the catalogue. Use weaver wipe to remove a catalogue."
            )

    from ..catalogue.connection import catalogue_connection
    from ..sessions.host import use_or_create_session
    from .wipe import installations

    with use_or_create_session(session, workspace=resolved) as opened:
        with opened.task("Read the installed estate", resolved.catalogue):
            recorded = installations(catalogue_connection(opened, resolved))
            items = opened.resolver(resolved).discover()

    installed: dict[str, list[str]] = {}
    for item, target in recorded:
        installed.setdefault(str(target).casefold(), []).append(item)
    unclaimed = [str(t) for t in selected if str(t).casefold() not in installed]
    if unclaimed:
        raise CommandError(
            f"{resolved.catalogue} records no item in {', '.join(unclaimed)}. "
            "Check the target names."
        )
    existing = {(item.type, item.name.casefold()) for item in items}
    return UnbindPlan(
        workspace=resolved,
        catalogue=resolved.catalogue,
        targets=selected,
        installed={str(t): tuple(installed[str(t).casefold()]) for t in selected},
        present=frozenset(
            str(t)
            for t in selected
            if (t.item_type, t.physical_name.casefold()) in existing
        ),
    )


def unbind(
    targets: str | Iterable[str] = (),
    *,
    plan: UnbindPlan | None = None,
    workspace: str | None = None,
    catalogue: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    dry_run: bool = False,
    session=None,
) -> UnbindResult:
    """Delete the catalogue's claims for the plan's targets.

    Pass either a settled ``plan`` or arguments from which :func:`plan_unbind`
    builds one. Nothing in Fabric changes.
    """

    if plan is None:
        plan = plan_unbind(
            targets,
            workspace=workspace,
            catalogue=catalogue,
            environment=environment,
            workspace_config=workspace_config,
            session=session,
        )
    elif targets or workspace or catalogue or environment or workspace_config:
        raise CommandError(
            "unbind accepts a settled plan or planning arguments, not both."
        )
    if dry_run:
        return UnbindResult(plan=plan, dry_run=True)

    from ..catalogue.connection import catalogue_connection
    from ..catalogue.unbind import plan_claim_deletion
    from ..sessions.host import use_or_create_session
    from ..wipe_plan import wipe_mutation_plan
    from .wipe import (
        LAKEHOUSE,
        SEMANTIC_MODEL,
        UNBIND,
        WAREHOUSE,
        WipePlan,
    )

    def names(kind):
        return sorted({t.physical_name for t in plan.targets if t.item_type == kind})

    with use_or_create_session(session, workspace=plan.workspace) as opened:
        with opened.task("Unbind", ", ".join(map(str, plan.targets))):
            claims = plan_claim_deletion(
                catalogue_connection(opened, plan.workspace),
                lakehouses=names(LAKEHOUSE),
                warehouses=names(WAREHOUSE),
                semantic_models=names(SEMANTIC_MODEL),
            )
            # An unbind is a wipe of no targets: only the claim deletion runs.
            mutation, payloads = wipe_mutation_plan(
                WipePlan(
                    workspace=plan.workspace,
                    targets=(),
                    catalogue=plan.catalogue,
                    catalogue_action=UNBIND,
                ),
                unbind_statements=claims.statements,
            )
            report = opened.execute_mutation(mutation, payloads)
    failed = [
        f"{result.action_id}: {result.error or result.status}"
        for result in report.results
        if result.status != "succeeded"
    ]
    if failed:
        raise CommandError("the unbind did not complete: " + "; ".join(failed))
    return UnbindResult(plan=plan, logical_items=claims.logical_items)


__all__ = ["UnbindPlan", "UnbindResult", "plan_unbind", "unbind"]
