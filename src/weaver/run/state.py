"""RunState — what the estate was, when the run was planned against it.

The runtime twin of :class:`~weaver.build_bundle.workflow.BuildState`, and for
the same reason. A Runner decides what runs, in what order, and what is blocked;
it must not be the thing that discovers whether a Warehouse is still there,
because a decision made against state that is still moving is a decision nobody
can reproduce.

.. code-block:: text

    physical catalogue  → Catalogue
    physical targets    → TargetInventory
                        ↓
                     RunState
                        ↓
                      Runner

So the reading happens once, at a boundary, above the Runner — and everything
the Runner does afterwards is Python. A run-cycle test constructs one of these
directly and needs no estate at all.

**Inventories are keyed by the target's public spelling** — ``Lakehouse/Raw_LH``
— because that is what a caller wrote and what a report prints. A key that could
not be read back would put a second vocabulary between the request and the
answer. A target with no entry is a target that is not there, which is a
different failure from a graph that is wrong about one, and the Runner says
which.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from ..build_bundle.prune import TargetInventory
from ..catalogue.state import Catalogue


@dataclass(frozen=True)
class RunState:
    """The installed estate as one Python handover: what is claimed, and what is there."""

    catalogue: Catalogue
    target_inventories: Mapping[str, TargetInventory] = field(default_factory=dict)

    def inventory(self, target) -> TargetInventory | None:
        """What was observed at one physical target, or None if it is absent."""

        return self.target_inventories.get(str(target))

    def observed(self, target) -> bool:
        return str(target) in self.target_inventories

    def to_mapping(self) -> dict:
        return {
            "format_version": 1,
            "catalogue": self.catalogue.to_mapping(),
            "target_inventories": [
                {"target": target, "inventory": inventory.to_mapping()}
                for target, inventory in sorted(self.target_inventories.items())
            ],
        }

    @classmethod
    def from_mapping(cls, mapping) -> "RunState":
        from .contract import RunError

        version = mapping.get("format_version")
        if version != 1:
            raise RunError(
                f"unsupported run state format_version {version!r}; expected 1"
            )
        return cls(
            catalogue=Catalogue.from_mapping(mapping["catalogue"]),
            target_inventories={
                entry["target"]: TargetInventory.from_mapping(entry["inventory"])
                for entry in mapping.get("target_inventories", ())
            },
        )


__all__ = ["RunState"]
