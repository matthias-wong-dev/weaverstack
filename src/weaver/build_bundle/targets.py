"""Serialisable physical target descriptors.

A build request supplies live workspace objects; a bundle must not. The planner
converts each supplied binding into a :class:`BoundTarget` — a flat, stable
descriptor carrying exactly what an installer needs to resolve the physical
destination, and nothing that ties the bundle to the process that wrote it.

There is no workspace kind here. Weaver has one workspace, Fabric. A target names
an item — a Lakehouse or a Warehouse — by the identifiers the installer resolves
it with, and by the display names Fabric Spark spells a four-part object name
with. Where the installer runs is supplied by its Session, not frozen into the
bundle.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping

from ..declaration.model import LAKEHOUSE, WAREHOUSE, WeaverItemId
from ..errors import BuildError
from ..targets import ItemRef

if TYPE_CHECKING:  # names used only in annotations
    from ..spark import FabricSparkTarget

#: Target kinds a bound target may name. They mirror the Weaver document target kinds but
#: live here because a bundle is read without importing the Weaver document vocabulary.
LAKEHOUSE_TARGET = "lakehouse"
WAREHOUSE_TARGET = "warehouse"


@dataclass(frozen=True)
class BoundTarget:
    """One physical destination, as flat serialisable data.

    ``id`` is the manifest-local identifier a batch names. ``kind`` says whether
    it is a Lakehouse or a Warehouse. ``item_id`` names the item, with the
    Fabric identifiers alongside; the installer resolves the item through its
    own environment.

    Both halves of the workspace identity are carried, and both are used. The
    id resolves the item over REST and OneLake; the display name is what Fabric
    Spark spells a four-part object name with, so a build cannot render a
    statement without it.
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
                f"{self.display} carries no workspace name, so a Fabric Spark "
                "statement for it cannot be named"
            )
        return FabricSparkTarget(workspace=self.workspace_name, lakehouse=self.name)

    @property
    def name(self) -> str:
        """The readable name, falling back to the id when none was carried."""

        return self.item_name or self.item_id

    @property
    def display(self) -> str:
        """What to call this target on screen: ``Lakehouse/Sales``.

        The *physical* item, always. A logical name is the estate's own
        vocabulary and some of it is internal — the control Lakehouse is the
        logical item ``_weaver``, which means nothing to somebody watching a
        build write to a Lakehouse they know as ``Weaver``. ``id`` is worse
        still: ``Lakehouse-_weaver--lakehouse-Weaver``.
        """

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


# --- input bindings ----------------------------------------------------------
#
# What a caller supplies to the planner. These carry a live identity (an
# ItemRef, and for Fabric the workspace/item ids); the planner converts them into
# the flat BoundTarget above so no live workspace object is serialised into a bundle.


@dataclass(frozen=True)
class LakehouseBinding:
    """A bound destination Lakehouse for Folder and Delta materialisation."""

    #: Which of the two a caller is holding, declared rather than inferred from
    #: whichever item field the class happens to carry.
    kind = LAKEHOUSE_TARGET

    lakehouse: ItemRef
    workspace_id: str | None = None
    #: The workspace's display name, which four-part Spark naming is spelled with.
    workspace_name: str | None = None
    #: The concrete Fabric item id.
    item_id: str | None = None

    @property
    def item(self) -> ItemRef:
        """The bound item, under a name that does not presume its kind."""

        return self.lakehouse

    @property
    def physical_kind(self) -> str:
        """The kind as the target grammar spells it, for a message or a lookup."""

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
        """The bound item, under a name that does not presume its kind."""

        return self.warehouse

    @property
    def physical_kind(self) -> str:
        """The kind as the target grammar spells it, for a message or a lookup."""

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
    """One exact logical Weaver item bound to one typed physical item."""

    item: WeaverItemId
    target: LakehouseBinding | WarehouseBinding

    def __post_init__(self) -> None:
        if self.item.item_type != self.target.physical_kind:
            raise BuildError(
                f"logical item {self.item} requires a {self.item.item_type} binding, "
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
        seen: set[WeaverItemId] = set()
        physical: set[tuple[str, str]] = set()
        for binding in self.entries:
            if binding.item in seen:
                raise BuildError(
                    f"logical item is bound more than once: {binding.item}"
                )
            seen.add(binding.item)
            target = binding.target
            key = (target.physical_kind, target.item.name)
            if key in physical:
                raise BuildError(
                    f"physical {key[0]} target is bound more than once: {key[1]}"
                )
            physical.add(key)

    @property
    def by_item(self) -> Mapping[WeaverItemId, ItemBinding]:
        return {binding.item: binding for binding in self.entries}


def effective_item_bindings(
    bindings: ItemBindings, *, control_item: "ItemRef | str", workspace_name: str
) -> ItemBindings:
    """Add the mandatory package-owned catalogue item binding.

    ``control_item`` is the Warehouse the catalogue lives in — the item itself
    rather than the workspace's typed ``catalogue`` value, because what a
    binding needs is a name it can resolve.

    ``workspace_name`` is required rather than optional, because the binding
    this adds is the one every build renders its catalogue statements against.
    A caller that omitted it produced a catalogue target that could not name an
    object, and the failure surfaced inside Fabric several steps later.
    """

    if not workspace_name:
        raise BuildError(
            "the catalogue binding needs the workspace's display name, "
            "which four-part naming is spelled with"
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


def parse_item_binding(text: str, *, workspace=None) -> ItemBinding:
    """Parse a typed physical selector with an optional logical override.

    ``Lakehouse/Sales`` uses the configured default. The self-contained form
    ``Lakehouse/SalesDev=Sales`` needs no configured target declaration: the
    physical Lakehouse ``SalesDev`` holds the logical item ``Sales``. The
    left-hand side is typed and supplies the type for both, so the logical item
    is named alone.

    That is why a typed right-hand side is refused rather than accepted and
    checked. ``Lakehouse/SalesDev=Warehouse/Sales`` is not a binding whose
    types disagree — it is a sentence that cannot be written.
    """

    if not isinstance(text, str) or text.count("=") > 1:
        raise BuildError(
            "a binding must be Lakehouse/Physical or Lakehouse/Physical=LogicalName"
        )
    physical_text, separator, logical_text = text.partition("=")
    physical_text = physical_text.strip()
    logical_text = logical_text.strip()
    if not physical_text or (separator and not logical_text):
        raise BuildError(
            "a binding must be Lakehouse/Physical or Lakehouse/Physical=LogicalName"
        )

    physical_type, physical = _parse_physical_item(physical_text)
    if separator:
        # The left-hand side supplies the type for both, so the right names the
        # logical item alone. Spelling it `=Lakehouse/Sales` would say the type
        # twice and let the two disagree.
        if "/" in logical_text:
            raise BuildError(
                f"a binding's logical item is named without a type: write "
                f"{physical_text}={logical_text.rpartition('/')[2]} rather than "
                f"{physical_text}={logical_text}. The physical side already says "
                f"this is a {physical_type}"
            )
        item = WeaverItemId(physical_type, logical_text)
    else:
        if workspace is None:
            raise BuildError(
                f"binding {physical_text!r} needs a Workspace configuration default "
                "or an explicit =LogicalName"
            )
        item = workspace.declaration_for(physical_type, physical.name).item
    if item.item_type != physical_type:
        raise BuildError(
            f"physical {physical_text} cannot be bound to logical {item}; "
            f"both must be {physical_type}"
        )
    workspace_name = getattr(workspace, "workspace", None)
    target = (
        LakehouseBinding(physical, workspace_name=workspace_name)
        if physical_type == LAKEHOUSE
        else WarehouseBinding(physical, workspace_name=workspace_name)
    )
    return ItemBinding(item, target)


def _parse_physical_item(text: str) -> tuple[str, ItemRef]:
    """The binding's physical half, through the grammar every operation shares.

    The logical item types and the grammar's spellings happen to be the same two
    words, so the kind is used directly rather than translated.
    """

    from ..targets import parse_physical_target, physical_item, physical_kind

    target = parse_physical_target(
        text, what="binding physical target", error=BuildError
    )
    return physical_kind(target), physical_item(target)
