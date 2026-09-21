"""Immutable BuildBundle plan, sequence, batch, and action types.

Sequences are execution barriers, batches bind actions to one target, and every
type serialises to the canonical manifest.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from ..catalogue.runtime_state import (
    RuntimeStateEstablishment,
    RuntimeStateInvalidation,
)
from .changes import TargetChange
from .execution import BundleExecution
from .incremental import BuildSelection
from .targets import BoundTarget

#: Action kinds. Create kinds build structure; prune kinds reconcile the target.
CREATE_SCHEMA = "create_schema"
CREATE_SHORTCUT = "create_shortcut"
BUILD_FOLDER = "build_folder"
BUILD_TABLE = "build_table"
BUILD_VIEW = "build_view"

#: Refresh the SQL analytics endpoint before dependent items read its metadata.
REFRESH_SQL_ENDPOINT = "refresh_sql_endpoint"

#: Runtime artefacts installed in an item's final layer.
WRITE_FILE = "write_file"
BUILD_PROCEDURE = "build_procedure"

#: Rebuild drops remove desired objects before their selected definition is recreated.
DROP_FOLDER = "drop_folder"
DROP_TABLE = "drop_table"
DROP_VIEW = "drop_view"
#: The drop for an identity the previous build installed as a Lakehouse pointer.
#: A OneLake shortcut is a read-write window into the item it points at, so it
#: comes off through the shortcut API; a ``DROP TABLE`` or a directory removal
#: would reach that item's data.
DROP_SHORTCUT = "drop_shortcut"

#: Prior Registry claims determine runtime artefact removals.
DELETE_FILE = "delete_file"
DROP_PROCEDURE = "drop_procedure"

#: Prune kinds. Each names one frozen drop the build computed against the target:
#: a Spark SQL DROP for a table/view/schema, a directory removal for a folder.
PRUNE_TABLE = "prune_table"
PRUNE_VIEW = "prune_view"
PRUNE_SCHEMA = "prune_schema"
PRUNE_FOLDER = "prune_folder"

#: Target-bound, payloadless endpoint refresh after Delta changes.
REFRESH_SQL_ENDPOINT = "refresh_sql_endpoint"

#: Catalogue actions target the central Warehouse. Claim deletion precedes
#: physical work; publication follows it with Registry last.
DELETE_CATALOGUE_CLAIMS = "delete_catalogue_claims"
PUBLISH_CATALOGUE = "publish_catalogue"
PUBLISH_REGISTRY = "publish_registry"
#: Remove current-state rows for retired and replaced object incarnations before
#: physical work.
RECONCILE_RUNTIME_STATE = "reconcile_runtime_state"
CATALOGUE_KINDS = frozenset(
    {
        DELETE_CATALOGUE_CLAIMS,
        PUBLISH_CATALOGUE,
        PUBLISH_REGISTRY,
        RECONCILE_RUNTIME_STATE,
    }
)

#: Reasons a repository node is not in the plan.
OMIT_TARGET_UNBOUND = "target_unbound"
OMIT_DEPENDS_ON_OMITTED = "depends_on_omitted_node"
OMIT_UNSUPPORTED_EXECUTOR = "unsupported_executor"
#: A shortcut for which current bindings provide no physical form.
OMIT_SHORTCUT_UNSUPPORTED = "shortcut_unsupported"
OMISSION_REASONS = frozenset(
    {
        OMIT_TARGET_UNBOUND,
        OMIT_DEPENDS_ON_OMITTED,
        OMIT_UNSUPPORTED_EXECUTOR,
        OMIT_SHORTCUT_UNSUPPORTED,
    }
)


@dataclass(frozen=True)
class OmittedNode:
    """A repository node the projection left out, and why."""

    node_id: str
    reason: str
    detail: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        mapping: dict[str, Any] = {"node_id": self.node_id, "reason": self.reason}
        if self.detail is not None:
            mapping["detail"] = self.detail
        return mapping

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "OmittedNode":
        return cls(
            node_id=mapping["node_id"],
            reason=mapping["reason"],
            detail=mapping.get("detail"),
        )


@dataclass(frozen=True)
class InstallAction:
    """One independently executable unit.

    ``payload`` is a bundle-relative path and ``payload_sha256`` protects its
    contents. ``source_path`` preserves the authored relative path for failure
    reporting; it is never reconstructed from generated names.

    ``awaits_name_release`` marks a dropped shortcut whose name this plan reuses
    for an owned object. Fabric may stop listing the shortcut before OneLake
    releases its namespace.
    """

    id: str
    kind: str
    resource_node_id: str | None
    executor: str
    payload: str | None
    payload_sha256: str | None
    source_path: str | None = None
    awaits_name_release: bool = False

    def to_mapping(self) -> dict[str, Any]:
        mapping: dict[str, Any] = {
            "id": self.id,
            "kind": self.kind,
            "resource_node_id": self.resource_node_id,
            "executor": self.executor,
            "payload": self.payload,
            "payload_sha256": self.payload_sha256,
        }
        if self.source_path is not None:
            # Omitted when absent rather than written as null: the canonical
            # plan.yml is what bundle_id hashes, and a key that appeared on
            # every action would change the id of every bundle that has no
            # authored source to name.
            mapping["source_path"] = self.source_path
        if self.awaits_name_release:
            # Omitted when false, for the reason ``source_path`` is.
            mapping["awaits_name_release"] = True
        return mapping

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "InstallAction":
        return cls(
            id=mapping["id"],
            kind=mapping["kind"],
            resource_node_id=mapping.get("resource_node_id"),
            executor=mapping["executor"],
            payload=mapping.get("payload"),
            payload_sha256=mapping.get("payload_sha256"),
            source_path=mapping.get("source_path"),
            awaits_name_release=bool(mapping.get("awaits_name_release", False)),
        )


@dataclass(frozen=True)
class BuildBatch:
    id: str
    target_id: str
    actions: tuple[InstallAction, ...]

    def to_mapping(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "target_id": self.target_id,
            "actions": [action.to_mapping() for action in self.actions],
        }

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "BuildBatch":
        return cls(
            id=mapping["id"],
            target_id=mapping["target_id"],
            actions=tuple(
                InstallAction.from_mapping(a) for a in mapping.get("actions", ())
            ),
        )


@dataclass(frozen=True)
class BuildSequence:
    """One barrier. Every batch here completes before the next sequence starts."""

    number: int
    description: str
    batches: tuple[BuildBatch, ...]

    def to_mapping(self) -> dict[str, Any]:
        return {
            "number": self.number,
            "description": self.description,
            "batches": [batch.to_mapping() for batch in self.batches],
        }

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "BuildSequence":
        return cls(
            number=mapping["number"],
            description=mapping["description"],
            batches=tuple(
                BuildBatch.from_mapping(b) for b in mapping.get("batches", ())
            ),
        )


@dataclass(frozen=True)
class BuildPlan:
    """A whole deployment, fully bound and ordered."""

    format_version: int
    bundle_id: str
    repository_name: str
    repository_signature: str
    targets: tuple[BoundTarget, ...]
    sequences: tuple[BuildSequence, ...]
    selection: BuildSelection
    #: Where this bundle installs. Frozen at build time and never supplied by
    #: the caller who installs it.
    execution: BundleExecution
    omitted_nodes: tuple[OmittedNode, ...] = ()
    #: Added and removed objects by target id. This is part of the manifest and
    #: bundle identity, so the certified summary cannot change independently.
    target_changes: Mapping[str, tuple[TargetChange, ...]] = field(default_factory=dict)
    #: Current-state rows invalidated by the plan, declared beside the action so
    #: in-memory application does not parse DML.
    runtime_state: tuple[RuntimeStateInvalidation, ...] = ()
    runtime_state_established: tuple[RuntimeStateEstablishment, ...] = ()

    def to_mapping(self) -> dict[str, Any]:
        mapping = {
            "format_version": self.format_version,
            "bundle_id": self.bundle_id,
            "repository_name": self.repository_name,
            "repository_signature": self.repository_signature,
            "targets": [target.to_mapping() for target in self.targets],
            "execution": self.execution.to_mapping(),
            "sequences": [sequence.to_mapping() for sequence in self.sequences],
            "omitted_nodes": [node.to_mapping() for node in self.omitted_nodes],
            "target_changes": {
                target_id: [change.to_mapping() for change in changes]
                for target_id, changes in sorted(self.target_changes.items())
            },
            "runtime_state": [one.to_mapping() for one in self.runtime_state],
            "runtime_state_established": [
                one.to_mapping() for one in self.runtime_state_established
            ],
        }
        mapping["selection"] = self.selection.to_mapping()
        return mapping

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "BuildPlan":
        return cls(
            format_version=mapping["format_version"],
            bundle_id=mapping["bundle_id"],
            repository_name=mapping["repository_name"],
            repository_signature=mapping["repository_signature"],
            targets=tuple(
                BoundTarget.from_mapping(t) for t in mapping.get("targets", ())
            ),
            sequences=tuple(
                BuildSequence.from_mapping(s) for s in mapping.get("sequences", ())
            ),
            selection=BuildSelection.from_mapping(mapping["selection"]),
            execution=BundleExecution.from_mapping(mapping["execution"]),
            omitted_nodes=tuple(
                OmittedNode.from_mapping(n) for n in mapping.get("omitted_nodes", ())
            ),
            target_changes={
                target_id: tuple(TargetChange.from_mapping(c) for c in changes)
                for target_id, changes in mapping.get("target_changes", {}).items()
            },
            runtime_state=tuple(
                RuntimeStateInvalidation.from_mapping(one)
                for one in mapping.get("runtime_state", ())
            ),
            runtime_state_established=tuple(
                RuntimeStateEstablishment.from_mapping(one)
                for one in mapping.get("runtime_state_established", ())
            ),
        )

    @property
    def target_ids(self) -> frozenset[str]:
        return frozenset(target.id for target in self.targets)

    def actions(self):
        """Every action, in manifest order."""

        for sequence in self.sequences:
            for batch in sequence.batches:
                for action in batch.actions:
                    yield sequence, batch, action
