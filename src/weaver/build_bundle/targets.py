"""Define serialisable physical target descriptors and planner bindings.

Bundles contain flat identifiers and display names, not live workspace objects
or host information. The installer's Session supplies where execution runs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping

from ..declaration.model import LAKEHOUSE, WAREHOUSE, WeaverItemId
from ..errors import BuildError
from ..targets import (
    LAKEHOUSE_TARGET,
    WAREHOUSE_TARGET,
    ItemRef,
    physical_item,
    physical_kind,
)

if TYPE_CHECKING:
    from ..spark import FabricSparkTarget


@dataclass(frozen=True)
class BoundTarget:
    """A physical destination as flat serialisable data.

    ``id`` is local to the manifest. Fabric ids resolve items through REST and
    OneLake; display names render Spark's four-part object names.
    """

    id: str
    kind: str
    item_id: str
    #: Resolved display name for readable catalogue records; identity remains item_id.
    item_name: str | None = None
    workspace_id: str | None = None
    workspace_name: str | None = None
    sql_endpoint_id: str | None = None
    logical_item_type: str | None = None
    logical_item_name: str | None = None

    @property
    def spark_target(self) -> "FabricSparkTarget":
        """How Fabric Spark addresses this Lakehouse.

        A Warehouse has none: its objects are named over TDS by the connection
        the statement runs on.
        """

        from ..spark import FabricSparkTarget

        if self.kind == WAREHOUSE_TARGET:
            raise BuildError(
                f"{self.display} is a Warehouse and has no Spark destination"
            )
        if not self.workspace_name:
            raise BuildError(
                f"cannot render a Fabric Spark statement for {self.display} "
                "without a workspace display name"
            )
        return FabricSparkTarget(workspace=self.workspace_name, lakehouse=self.name)

    @property
    def name(self) -> str:
        return self.item_name or self.item_id

    @property
    def display(self) -> str:
        kind = (self.kind or "").strip()
        return f"{kind.title()}/{self.name}" if kind else str(self.name)

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
        if self.workspace_name is not None:
            mapping["workspace_name"] = self.workspace_name
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
            workspace_name=mapping.get("workspace_name"),
            sql_endpoint_id=mapping.get("sql_endpoint_id"),
            logical_item_type=mapping.get("logical_item_type"),
            logical_item_name=mapping.get("logical_item_name"),
        )


# Planner inputs, converted to flat BoundTarget values before serialisation.


@dataclass(frozen=True)
class LakehouseBinding:
    kind = LAKEHOUSE_TARGET

    lakehouse: ItemRef
    workspace_id: str | None = None
    #: The workspace's display name, which four-part Spark naming is spelled with.
    workspace_name: str | None = None
    #: The concrete Fabric item id.
    item_id: str | None = None

    @property
    def item(self) -> ItemRef:
        return self.lakehouse

    @property
    def physical_kind(self) -> str:
        return LAKEHOUSE

    def to_bound_target(self) -> BoundTarget:
        return BoundTarget(
            id=f"{self.kind}-{self.lakehouse.name}",
            kind=self.kind,
            item_id=self.item_id or self.lakehouse.name,
            item_name=self.lakehouse.name,
            workspace_id=self.workspace_id,
            workspace_name=self.workspace_name,
        )


@dataclass(frozen=True)
class WarehouseBinding:
    """A bound destination Warehouse, reached over TDS.

    Also what the Weaver catalogue is bound to: ``_`` lives in a Warehouse, and
    a build addresses it exactly as it addresses any other Warehouse target.
    """

    kind = WAREHOUSE_TARGET

    warehouse: ItemRef
    workspace_id: str | None = None
    workspace_name: str | None = None
    item_id: str | None = None
    sql_endpoint_id: str | None = None

    @property
    def item(self) -> ItemRef:
        return self.warehouse

    @property
    def physical_kind(self) -> str:
        return WAREHOUSE

    def to_bound_target(self) -> BoundTarget:
        return BoundTarget(
            id=f"{self.kind}-{self.warehouse.name}",
            kind=self.kind,
            item_id=self.item_id or self.warehouse.name,
            item_name=self.warehouse.name,
            workspace_id=self.workspace_id,
            workspace_name=self.workspace_name,
            sql_endpoint_id=self.sql_endpoint_id,
        )


@dataclass(frozen=True)
class ItemBinding:
    item: WeaverItemId
    target: LakehouseBinding | WarehouseBinding

    def __post_init__(self) -> None:
        if self.item.item_type != self.target.physical_kind:
            raise BuildError(
                f"{self.item} requires a {self.item.item_type} binding, "
                f"not a {self.target.physical_kind} one"
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
            workspace_name=physical.workspace_name,
            sql_endpoint_id=physical.sql_endpoint_id,
            logical_item_type=self.item.item_type,
            logical_item_name=self.item.item_name,
        )


@dataclass(frozen=True)
class ItemBindings:
    """The sparse logical-to-physical bindings for one coordinated build."""

    entries: tuple[ItemBinding, ...]

    def __post_init__(self) -> None:
        from ..catalogue.builtin import BUILTIN_ITEM

        seen: set[WeaverItemId] = set()
        physical: set[tuple[str, str]] = set()
        for binding in self.entries:
            if binding.item in seen:
                raise BuildError(f"item is bound more than once: {binding.item}")
            seen.add(binding.item)
            if binding.item == BUILTIN_ITEM:
                continue
            target = binding.target
            key = (target.physical_kind, target.item.name)
            if key in physical:
                raise BuildError(
                    f"{key[0]}/{key[1]} cannot hold two items. Each item is "
                    "reconciled against the target's complete inventory, so bind "
                    "each item to a different physical target"
                )
            physical.add(key)

    @property
    def by_item(self) -> Mapping[WeaverItemId, ItemBinding]:
        return {binding.item: binding for binding in self.entries}


def effective_item_bindings(
    bindings: ItemBindings, *, control_item: "ItemRef | str", workspace_name: str
) -> ItemBindings:
    """Add the mandatory package-owned catalogue item binding.

    ``control_item`` is the catalogue Warehouse item. ``workspace_name`` is
    required to render its four-part object names.
    """

    if not workspace_name:
        raise BuildError(
            "the catalogue binding requires the workspace display name used in "
            "four-part naming"
        )

    from ..catalogue.builtin import BUILTIN_ITEM

    builtin = BUILTIN_ITEM
    if builtin in bindings.by_item:
        raise BuildError(
            "Warehouse/_weaver is bound implicitly and must not be selected"
        )
    return ItemBindings(
        bindings.entries
        + (
            ItemBinding(
                builtin,
                WarehouseBinding(
                    control_item
                    if isinstance(control_item, ItemRef)
                    else ItemRef(str(control_item)),
                    workspace_name=workspace_name,
                ),
            ),
        )
    )


def parse_build_item(text: str, *, workspace=None) -> ItemBinding:
    """Parse one build item: ``LOGICAL`` or ``LOGICAL=PHYSICAL``.

    Without ``=``, the physical target comes from workspace configuration. Both
    sides are typed and the two types must agree.
    """

    if not isinstance(text, str):
        raise BuildError(f"a build item must be a string, got {type(text).__name__}")
    if text.count("=") > 1:
        raise BuildError(_BUILD_ITEM_GRAMMAR + f", got {text!r}")
    logical_text, separator, physical_text = text.partition("=")
    logical_text = logical_text.strip()
    physical_text = physical_text.strip()
    if not logical_text or (separator and not physical_text):
        raise BuildError(_BUILD_ITEM_GRAMMAR + f", got {text!r}")

    item = _parse_logical_item(logical_text)
    if separator:
        physical_type, physical = _parse_physical_target(physical_text)
        if physical_type != item.item_type:
            raise BuildError(
                f"{item} cannot be built into {physical_text}; both must be "
                f"{item.item_type}"
            )
    else:
        if workspace is None:
            raise BuildError(
                f"build item {logical_text!r} needs a Workspace configuration "
                f"entry or an explicit ={item.item_type}/<physical name>"
            )
        target = workspace.target_for(item)
        physical_type, physical = physical_kind(target), physical_item(target)

    workspace_name = getattr(workspace, "workspace", None)
    binding = (
        LakehouseBinding(physical, workspace_name=workspace_name)
        if physical_type == LAKEHOUSE
        else WarehouseBinding(physical, workspace_name=workspace_name)
    )
    return ItemBinding(item, binding)


#: What a malformed build item is told to write instead.
_BUILD_ITEM_GRAMMAR = (
    "a build item must be Lakehouse/Landing or Lakehouse/Landing=Lakehouse/Landing_Dev"
)


def _parse_logical_item(text: str) -> WeaverItemId:
    from ..errors import IdentityError

    try:
        return WeaverItemId.parse(text)
    except IdentityError:
        raise BuildError(
            f"a build item names a logical Weaver item as {LAKEHOUSE}/Name or "
            f"{WAREHOUSE}/Name, got {text!r}"
        ) from None


def _parse_physical_target(text: str) -> tuple[str, ItemRef]:
    from ..targets import parse_physical_target

    target = parse_physical_target(
        text, what="build item's physical target", error=BuildError
    )
    return physical_kind(target), physical_item(target)
