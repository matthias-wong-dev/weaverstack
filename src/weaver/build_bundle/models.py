"""The build manifest — immutable plan, sequence, batch and action types.

A :class:`BuildPlan` is the whole deployment, fully bound: every executable
identifies one physical target and carries everything an installer needs. It is
produced once, by the planner, and thereafter only read.

The execution shape is deliberately flat and explicit:

- **sequences are barriers.** They run in order; the next begins only when every
  action in the current one has succeeded.
- **batches are target-bound.** A batch names exactly one target, so a physical
  destination appears once per batch rather than being repeated inside actions.
- **actions are independent units.** Each has its own payload (where one is
  required), and is reported on its own.

Every type serialises to and from plain mappings, which is what the canonical
``plan.yml`` and the ``bundle_id`` hash are built from — see :mod:`weaver.build_bundle.bundle`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .targets import BoundTarget

#: Reasons a repository node is not in the plan. A missing target is visible,
#: not a mysterious absence.
OMIT_TARGET_UNBOUND = "target_unbound"
OMIT_DEPENDS_ON_OMITTED = "depends_on_omitted_node"
OMIT_UNSUPPORTED_EXECUTOR = "unsupported_executor"
OMISSION_REASONS = frozenset(
    {OMIT_TARGET_UNBOUND, OMIT_DEPENDS_ON_OMITTED, OMIT_UNSUPPORTED_EXECUTOR}
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
class BuildAction:
    """One independently executable unit.

    ``payload`` is a bundle-relative path to the generated definition, or None
    for an action that carries no payload (an explicit no-op). ``payload_sha256``
    hashes that payload so corruption is caught before anything runs.
    """

    id: str
    kind: str
    resource_node_id: str | None
    executor: str
    payload: str | None
    payload_sha256: str | None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "resource_node_id": self.resource_node_id,
            "executor": self.executor,
            "payload": self.payload,
            "payload_sha256": self.payload_sha256,
        }

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "BuildAction":
        return cls(
            id=mapping["id"],
            kind=mapping["kind"],
            resource_node_id=mapping.get("resource_node_id"),
            executor=mapping["executor"],
            payload=mapping.get("payload"),
            payload_sha256=mapping.get("payload_sha256"),
        )


@dataclass(frozen=True)
class BuildBatch:
    """A group of actions against exactly one target."""

    id: str
    target_id: str
    actions: tuple[BuildAction, ...]

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
            actions=tuple(BuildAction.from_mapping(a) for a in mapping.get("actions", ())),
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
            batches=tuple(BuildBatch.from_mapping(b) for b in mapping.get("batches", ())),
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
    omitted_nodes: tuple[OmittedNode, ...] = ()

    def to_mapping(self) -> dict[str, Any]:
        return {
            "format_version": self.format_version,
            "bundle_id": self.bundle_id,
            "repository_name": self.repository_name,
            "repository_signature": self.repository_signature,
            "targets": [target.to_mapping() for target in self.targets],
            "sequences": [sequence.to_mapping() for sequence in self.sequences],
            "omitted_nodes": [node.to_mapping() for node in self.omitted_nodes],
        }

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "BuildPlan":
        return cls(
            format_version=mapping["format_version"],
            bundle_id=mapping["bundle_id"],
            repository_name=mapping["repository_name"],
            repository_signature=mapping["repository_signature"],
            targets=tuple(BoundTarget.from_mapping(t) for t in mapping.get("targets", ())),
            sequences=tuple(BuildSequence.from_mapping(s) for s in mapping.get("sequences", ())),
            omitted_nodes=tuple(
                OmittedNode.from_mapping(n) for n in mapping.get("omitted_nodes", ())
            ),
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
