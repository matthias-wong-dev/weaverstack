"""Immutable BuildBundle plan, sequence, batch, and action types.

Sequences are execution barriers, batches bind actions to one target, and every
type serialises to the canonical manifest.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .changes import TargetChange
from .incremental import BuildSelection
from .targets import BoundTarget

#: Action kinds. Create kinds build structure; prune kinds reconcile the target.
CREATE_SCHEMA = "create_schema"
CREATE_SHORTCUT = "create_shortcut"
BUILD_FOLDER = "build_folder"
BUILD_TABLE = "build_table"
BUILD_VIEW = "build_view"

#: One Lakehouse's SQL analytics endpoint catching up with the Delta mutations
#: just made in it. It closes an item's physical work: a dependent item's
#: Warehouse view or OneLake shortcut reads that endpoint's metadata, so it must
#: not be created while the endpoint still describes the previous shape.
REFRESH_SQL_ENDPOINT = "refresh_sql_endpoint"

#: Load kinds — what an item's final layer installs. ``write_file`` puts one
#: deployed module or generated statement into the runtime tree;
#: ``build_procedure`` creates or replaces one generated load procedure.
WRITE_FILE = "write_file"
BUILD_PROCEDURE = "build_procedure"

#: Rebuild drops remove desired objects before their selected definition is recreated.
DROP_FOLDER = "drop_folder"
DROP_TABLE = "drop_table"
DROP_VIEW = "drop_view"

#: Removals of load artefacts whose source has stopped claiming them. Distinct
#: from prune because they come from the *catalogue* rather than from a diff
#: against the target: the previous Registry row says what was installed and
#: where, so a deleted or renamed source produces the removal without anything
#: having to enumerate the runtime tree.
DELETE_FILE = "delete_file"
DROP_PROCEDURE = "drop_procedure"

#: Prune kinds. Each names one frozen drop the build computed against the target:
#: a Spark SQL DROP for a table/view/schema, a directory removal for a folder.
PRUNE_TABLE = "prune_table"
PRUNE_VIEW = "prune_view"
PRUNE_SCHEMA = "prune_schema"
PRUNE_FOLDER = "prune_folder"

#: Refresh the SQL analytics endpoint for one Lakehouse after its Delta tables
#: have changed. The action is target-bound and payloadless: the planner decides
#: which Lakehouses need it, while the executor performs or explicitly skips it
#: for the current host.
REFRESH_SQL_ENDPOINT = "refresh_sql_endpoint"

#: Catalogue kinds. These write the central catalogue in its Warehouse
#: rather than the destination. Claim deletion leads physical work; batched
#: publication concludes it, with Registry visibly last in the manifest.
DELETE_CATALOGUE_CLAIMS = "delete_catalogue_claims"
PUBLISH_CATALOGUE = "publish_catalogue"
PUBLISH_REGISTRY = "publish_registry"
#: Bring ``_.Bookmark`` into line with what this build will leave installed:
#: remove the rows of objects it no longer loads, and reset the ones it rebuilds.
#: It leads physical work for the reason claim deletion does — see
#: :mod:`weaver.build_bundle.bookmarks`.
RECONCILE_BOOKMARKS = "reconcile_bookmarks"
CATALOGUE_KINDS = frozenset(
    {
        DELETE_CATALOGUE_CLAIMS,
        PUBLISH_CATALOGUE,
        PUBLISH_REGISTRY,
        RECONCILE_BOOKMARKS,
    }
)

#: Reasons a repository node is not in the plan. A missing target is visible,
#: not a mysterious absence.
OMIT_TARGET_UNBOUND = "target_unbound"
OMIT_DEPENDS_ON_OMITTED = "depends_on_omitted_node"
OMIT_UNSUPPORTED_EXECUTOR = "unsupported_executor"
#: A shortcut the current bindings give no physical form. The planner decides this
#: — never the installer, which may only run a shortcut action already frozen for
#: it — and records it so the absence is a stated decision rather than a gap.
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

    ``payload`` is a bundle-relative path to the generated definition, or None
    for an action that carries no payload (an explicit no-op). ``payload_sha256``
    hashes that payload so corruption is caught before anything runs.

    ``source_path`` is the authored repository file this action came from,
    relative to the repository root, for actions that have one. It exists so a
    failure can name the file the developer should open:

    .. code-block:: text

        Error installing Warehouse/Reporting/Sales.CustomerRevenue
        Source: Warehouse/Reporting/Sales.CustomerRevenue.sql

    Carried from where the authored file was parsed, never reconstructed from
    ``id`` or a payload name: several authored files can compile to one deployed
    spelling, so a path derived later would be a guess.

    An action with no authored source — a shortcut, an endpoint refresh, a prune,
    a catalogue publication — has None.
    """

    id: str
    kind: str
    resource_node_id: str | None
    executor: str
    payload: str | None
    payload_sha256: str | None
    source_path: str | None = None

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
        )


@dataclass(frozen=True)
class BuildBatch:
    """A group of actions against exactly one target."""

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
    omitted_nodes: tuple[OmittedNode, ...] = ()
    #: What this plan will *mean* for each bound target, keyed by target id —
    #: the objects it adds and removes. Part of the manifest, and therefore of
    #: the bundle identity, so the summary a reviewer reads is the summary the
    #: installation was certified with. A sibling file outside the hash could be
    #: edited after certification, which is the thing frozen payloads exist to
    #: prevent.
    target_changes: Mapping[str, tuple[TargetChange, ...]] = field(default_factory=dict)

    def to_mapping(self) -> dict[str, Any]:
        mapping = {
            "format_version": self.format_version,
            "bundle_id": self.bundle_id,
            "repository_name": self.repository_name,
            "repository_signature": self.repository_signature,
            "targets": [target.to_mapping() for target in self.targets],
            "sequences": [sequence.to_mapping() for sequence in self.sequences],
            "omitted_nodes": [node.to_mapping() for node in self.omitted_nodes],
            "target_changes": {
                target_id: [change.to_mapping() for change in changes]
                for target_id, changes in sorted(self.target_changes.items())
            },
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
            omitted_nodes=tuple(
                OmittedNode.from_mapping(n) for n in mapping.get("omitted_nodes", ())
            ),
            target_changes={
                target_id: tuple(TargetChange.from_mapping(c) for c in changes)
                for target_id, changes in mapping.get("target_changes", {}).items()
            },
        )

    # --- convenience views ------------------------------------------------

    @property
    def target_ids(self) -> frozenset[str]:
        return frozenset(target.id for target in self.targets)

    def actions(self):
        """Every action, in manifest order."""

        for sequence in self.sequences:
            for batch in sequence.batches:
                for action in batch.actions:
                    yield sequence, batch, action
