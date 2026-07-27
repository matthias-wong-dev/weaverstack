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
from ..errors import BuildError
from ..declaration.model import LAKEHOUSE, WAREHOUSE, WeaverItemId

#: Target kinds a bound target may name. They mirror the Weaver document target kinds but
#: live here because a bundle is read without importing the Weaver document vocabulary.
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
    logical_item_type: str | None = None
    logical_item_name: str | None = None

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
        if self.logical_item_type is not None:
            mapping["logical_item_type"] = self.logical_item_type
        if self.logical_item_name is not None:
            mapping["logical_item_name"] = self.logical_item_name
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
            logical_item_type=mapping.get("logical_item_type"),
            logical_item_name=mapping.get("logical_item_name"),
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
class ItemBinding:
    """One exact logical Weaver item bound to one typed physical item."""

    item: WeaverItemId
    target: LakehouseBinding | WarehouseBinding

    def __post_init__(self) -> None:
        expected = LAKEHOUSE if isinstance(self.target, LakehouseBinding) else WAREHOUSE
        if self.item.item_type != expected:
            raise BuildError(
                f"logical item {self.item} requires a {self.item.item_type} binding, "
                f"not {type(self.target).__name__}"
            )

    def to_bound_target(self) -> BoundTarget:
        physical = self.target.to_bound_target()
        logical_slug = f"{self.item.item_type}-{self.item.item_name}"
        return BoundTarget(
            id=f"{logical_slug}--{physical.id}",
            kind=physical.kind,
            item_id=physical.item_id,
            item_name=physical.item_name,
            workspace_id=physical.workspace_id,
            sql_endpoint_id=physical.sql_endpoint_id,
            logical_item_type=self.item.item_type,
            logical_item_name=self.item.item_name,
        )


@dataclass(frozen=True)
class ItemBindings:
    """The sparse logical-to-physical bindings for one coordinated build."""

    entries: tuple[ItemBinding, ...]

    def __post_init__(self) -> None:
        seen: set[WeaverItemId] = set()
        for binding in self.entries:
            if binding.item in seen:
                raise BuildError(f"logical item is bound more than once: {binding.item}")
            seen.add(binding.item)

    @property
    def by_item(self) -> Mapping[WeaverItemId, ItemBinding]:
        return {binding.item: binding for binding in self.entries}


def parse_item_binding(text: str) -> ItemBinding:
    """Parse ``ItemType/LogicalName=PhysicalName`` at the public boundary."""

    if not isinstance(text, str) or text.count("=") != 1:
        raise BuildError(
            "an item binding must be ItemType/LogicalName=PhysicalName"
        )
    logical_text, physical_text = (part.strip() for part in text.split("=", 1))
    if not logical_text or not physical_text:
        raise BuildError(
            "an item binding must be ItemType/LogicalName=PhysicalName"
        )
    item = WeaverItemId.parse(logical_text)
    physical = ItemRef.parse(physical_text)
    target = (
        LakehouseBinding(physical)
        if item.item_type == LAKEHOUSE
        else WarehouseBinding(physical)
    )
    return ItemBinding(item, target)
