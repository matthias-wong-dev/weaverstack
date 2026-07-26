"""Serialisable physical target descriptors.

A build request supplies live host objects; a bundle must not. The planner
converts each supplied binding into a :class:`BoundTarget` — a flat, stable
descriptor carrying exactly what an installer needs to resolve the physical
destination, and nothing that ties the bundle to the process that wrote it.

There is no host kind here. Weaver has one real host, Fabric; local execution is
an emulation of it for development, not a second kind the bundle contract records.
A target names an item — a Lakehouse or a Warehouse — by the identifiers the
installer resolves it with; where the installer is running (in a Fabric session,
or in-process locally) is supplied by its environment, not frozen into the bundle.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from ..targets import ItemRef

#: Target kinds a bound target may name. They mirror the SES target kinds but
#: live here because a bundle is read without importing the SES vocabulary.
LAKEHOUSE_TARGET = "lakehouse"
WAREHOUSE_TARGET = "warehouse"


@dataclass(frozen=True)
class BoundTarget:
    """One physical destination, as flat serialisable data.

    ``id`` is the manifest-local identifier a batch names. ``kind`` says whether
    it is a Lakehouse or a Warehouse. ``item_id`` names the item, with the
    optional Fabric identifiers alongside; the installer resolves the item
    through its own environment.
    """

    id: str
    kind: str
    item_id: str
    #: The item's resolved display name. Carried alongside ``item_id`` because on
    #: Fabric the id is a GUID: the catalogue records which item an installation is
    #: bound to, and a GUID would make that record unreadable. It is a *record*,
    #: never identity — resolution goes through ``item_id``.
    item_name: str | None = None
    workspace_id: str | None = None
    sql_endpoint_id: str | None = None

    @property
    def name(self) -> str:
        """The readable name, falling back to the id when none was carried."""

        return self.item_name or self.item_id

    def to_mapping(self) -> dict[str, Any]:
        mapping: dict[str, Any] = {
            "id": self.id,
            "kind": self.kind,
            "item_id": self.item_id,
        }
        if self.item_name is not None:
            mapping["item_name"] = self.item_name
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
            item_id=mapping["item_id"],
            item_name=mapping.get("item_name"),
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
    workspace_id: str | None = None
    #: The concrete Fabric item id; locally the logical Lakehouse name serves.
    item_id: str | None = None

    def to_bound_target(self) -> BoundTarget:
        return BoundTarget(
            id=f"{LAKEHOUSE_TARGET}-{self.lakehouse.name}",
            kind=LAKEHOUSE_TARGET,
            item_id=self.item_id or self.lakehouse.name,
            item_name=self.lakehouse.name,
            workspace_id=self.workspace_id,
        )


@dataclass(frozen=True)
class WarehouseBinding:
    """A bound destination Warehouse. Present so the boundary is visible; v1
    installation of Warehouse work is not supported and raises."""

    warehouse: ItemRef
    workspace_id: str | None = None
    item_id: str | None = None
    sql_endpoint_id: str | None = None

    def to_bound_target(self) -> BoundTarget:
        return BoundTarget(
            id=f"{WAREHOUSE_TARGET}-{self.warehouse.name}",
            kind=WAREHOUSE_TARGET,
            item_id=self.item_id or self.warehouse.name,
            item_name=self.warehouse.name,
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
