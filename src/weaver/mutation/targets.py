"""Frozen physical target descriptors."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping

from ..errors import BuildError
from ..targets import LAKEHOUSE_TARGET
from .serialization import checked_mapping

_KIND_NAMES = {
    "lakehouse": "Lakehouse",
    "warehouse": "Warehouse",
    "semanticmodel": "SemanticModel",
    "report": "Report",
}

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

        if self.kind != LAKEHOUSE_TARGET:
            raise BuildError(f"{self.display} has no Spark destination")
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
        if not kind:
            return str(self.name)
        return f"{_KIND_NAMES.get(kind.casefold(), kind.title())}/{self.name}"

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
        checked_mapping(mapping, cls)
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
