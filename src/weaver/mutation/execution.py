"""Frozen execution identity without runtime resources."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping

from .serialization import checked_mapping

if TYPE_CHECKING:
    from ..build_bundle.execution import ExecutionIdentity


@dataclass(frozen=True)
class BundleEnvironment:
    """The Fabric Environment a bundle's remote programs are published to.

    ``workspace`` preserves a qualified ``Workspace/Environment`` reference;
    ``None`` means the workload workspace owns it.
    """

    name: str
    workspace: str | None = None
    item_id: str | None = None

    @property
    def reference(self) -> str:
        return f"{self.workspace}/{self.name}" if self.workspace else self.name

    def to_mapping(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "workspace": self.workspace,
            "item_id": self.item_id,
        }

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "BundleEnvironment":
        checked_mapping(mapping, cls)
        return cls(
            name=mapping["name"],
            workspace=mapping.get("workspace"),
            item_id=mapping.get("item_id"),
        )


@dataclass(frozen=True)
class MutationExecution:
    """Where a frozen bundle installs, and what it needs to get there.

    ``catalogue_target_id`` and ``spark_home_target_id`` name targets in the same
    manifest, so the catalogue and the Spark attachment cannot drift from the
    destinations the plan was generated against. ``spark_home_target_id`` is
    ``None`` when no planned action needs Spark.
    """

    workspace_name: str
    catalogue_target_id: str | None = None
    workspace_id: str | None = None
    environment: BundleEnvironment | None = None
    spark_home_target_id: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "workspace_name": self.workspace_name,
            "workspace_id": self.workspace_id,
            "catalogue_target_id": self.catalogue_target_id,
            "environment": (
                None if self.environment is None else self.environment.to_mapping()
            ),
            "spark_home_target_id": self.spark_home_target_id,
        }

    @classmethod
    def of(
        cls,
        identity: ExecutionIdentity,
        *,
        catalogue_target_id: str,
        spark_home_target_id: str | None,
    ) -> "MutationExecution":
        return cls(
            workspace_name=identity.workspace_name,
            catalogue_target_id=catalogue_target_id,
            workspace_id=identity.workspace_id,
            environment=identity.environment,
            spark_home_target_id=spark_home_target_id,
        )

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "MutationExecution":
        checked_mapping(mapping, cls)
        environment = mapping.get("environment")
        return cls(
            workspace_name=mapping["workspace_name"],
            catalogue_target_id=mapping["catalogue_target_id"],
            workspace_id=mapping.get("workspace_id"),
            environment=(
                None
                if environment is None
                else BundleEnvironment.from_mapping(environment)
            ),
            spark_home_target_id=mapping.get("spark_home_target_id"),
        )
