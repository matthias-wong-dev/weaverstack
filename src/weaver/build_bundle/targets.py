"""Serialisable physical target descriptors.

A build request supplies live host objects; a bundle must not. The planner
converts each supplied binding into a :class:`BoundTarget` — a flat, stable
descriptor carrying exactly what an installer needs to resolve the physical
destination, and nothing that ties the bundle to the process that wrote it.

For a local target ``item_id`` is the logical Lakehouse name and there is no
``workspace_id``; for Fabric it will carry the concrete workspace and item IDs
the executor addresses. Either way the descriptor is plain data — no Python host
object is ever serialised into a bundle.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from ..targets import ItemRef

#: Target kinds a bound target may name. They mirror the SES target kinds but
#: live here because a bundle is read without importing the SES vocabulary.
LAKEHOUSE_TARGET = "lakehouse"
WAREHOUSE_TARGET = "warehouse"

#: Host kinds a descriptor may carry.
LOCAL_HOST = "local"
FABRIC_HOST = "fabric"


@dataclass(frozen=True)
class BoundTarget:
    """One physical destination, as flat serialisable data.

    ``id`` is the manifest-local identifier a batch names. ``kind`` says whether
    it is a Lakehouse or a Warehouse; ``host_kind`` whether it is local or
    Fabric. The remaining fields address the item, with those that only Fabric
    needs left absent locally.
    """

    id: str
    kind: str
    host_kind: str
    item_id: str
    workspace_id: str | None = None
    sql_endpoint_id: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        mapping: dict[str, Any] = {
            "id": self.id,
            "kind": self.kind,
            "host_kind": self.host_kind,
            "item_id": self.item_id,
        }
        if self.workspace_id is not None:
            mapping["workspace_id"] = self.workspace_id
        if self.sql_endpoint_id is not None:
            mapping["sql_endpoint_id"] = self.sql_endpoint_id
        return mapping

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "BoundTarget":
        return cls(
            id=mapping["id"],
            kind=mapping["kind"],
            host_kind=mapping["host_kind"],
            item_id=mapping["item_id"],
            workspace_id=mapping.get("workspace_id"),
            sql_endpoint_id=mapping.get("sql_endpoint_id"),
        )


# --- input bindings ----------------------------------------------------------
#
# What a caller supplies to the planner. These carry a live identity (an
# ItemRef, and for Fabric the workspace/item ids); the planner converts them into
# the flat BoundTarget above so no live host object is serialised into a bundle.


@dataclass(frozen=True)
class LakehouseBinding:
    """A bound destination Lakehouse for Folder and Delta materialisation."""

    lakehouse: ItemRef
    host_kind: str = LOCAL_HOST
    workspace_id: str | None = None
    #: The concrete Fabric item id; locally the logical Lakehouse name serves.
    item_id: str | None = None

    def to_bound_target(self) -> BoundTarget:
        return BoundTarget(
            id=f"{LAKEHOUSE_TARGET}-{self.lakehouse.name}",
            kind=LAKEHOUSE_TARGET,
            host_kind=self.host_kind,
            item_id=self.item_id or self.lakehouse.name,
            workspace_id=self.workspace_id,
        )


@dataclass(frozen=True)
class WarehouseBinding:
    """A bound destination Warehouse. Present so the boundary is visible; v1
    installation of Warehouse work is not supported and raises."""

    warehouse: ItemRef
    host_kind: str = FABRIC_HOST
    workspace_id: str | None = None
    item_id: str | None = None
    sql_endpoint_id: str | None = None

    def to_bound_target(self) -> BoundTarget:
        return BoundTarget(
            id=f"{WAREHOUSE_TARGET}-{self.warehouse.name}",
            kind=WAREHOUSE_TARGET,
            host_kind=self.host_kind,
            item_id=self.item_id or self.warehouse.name,
            workspace_id=self.workspace_id,
            sql_endpoint_id=self.sql_endpoint_id,
        )


@dataclass(frozen=True)
class TargetBindings:
    """The optional physical bindings a build is projected onto."""

    lakehouse: LakehouseBinding | None = None
    warehouse: WarehouseBinding | None = None

    @property
    def bound_target_kinds(self) -> frozenset[str]:
        kinds = set()
        if self.lakehouse is not None:
            kinds.add(LAKEHOUSE_TARGET)
        if self.warehouse is not None:
            kinds.add(WAREHOUSE_TARGET)
        return frozenset(kinds)
