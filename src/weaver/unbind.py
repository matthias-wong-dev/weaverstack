"""Remove catalogue installations for explicitly named physical targets."""

from __future__ import annotations

from dataclasses import dataclass

from .catalogue.reader import read_table
from .catalogue.reconcile import prune_installation
from .catalogue.render import InstallationScope, InstallationScopes
from .catalogue.tables import INSTALLATION
from .declaration.model import LAKEHOUSE, WAREHOUSE
from .targets import ItemRef


@dataclass(frozen=True)
class UnbindResult:
    targets: tuple[str, ...]
    logical_items: tuple[str, ...]
    statements: tuple[str, ...]

    def to_mapping(self) -> dict[str, object]:
        return {
            "targets": list(self.targets),
            "logical_items": list(self.logical_items),
            "statements": len(self.statements),
        }


def plan_unbind(
    catalogue,
    *,
    lakehouses=(),
    warehouses=(),
) -> UnbindResult:
    """Render complete catalogue deletion without inspecting physical targets."""

    selected = {(LAKEHOUSE, ItemRef.parse(value).name) for value in lakehouses} | {
        (WAREHOUSE, ItemRef.parse(value).name) for value in warehouses
    }
    rows = read_table(catalogue, INSTALLATION)
    scopes = sorted(
        {
            InstallationScope(str(row["item_type"]), str(row["item_name"]))
            for row in rows
            if (str(row["item_type"]), str(row["target_name"])) in selected
        },
        key=str,
    )
    # One pass over the tables for every selected installation, rather than one
    # pass per installation: a DELETE costs a Delta transaction whether it
    # removes one row or all of them, so the statement count is the cost.
    statements = (
        prune_installation(
            InstallationScopes(tuple(scopes)), destination=catalogue.destination
        )
        if scopes
        else ()
    )
    targets = tuple(f"{item_type}/{name}" for item_type, name in sorted(selected))
    return UnbindResult(
        targets=targets,
        logical_items=tuple(map(str, scopes)),
        statements=statements,
    )


def unbind_targets(catalogue, *, lakehouses=(), warehouses=()) -> UnbindResult:
    """Execute target-directed catalogue deletion and touch no physical target."""

    result = plan_unbind(catalogue, lakehouses=lakehouses, warehouses=warehouses)
    for statement in result.statements:
        catalogue.sql(statement)
    return result
