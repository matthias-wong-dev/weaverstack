"""Frozen physical mutation representation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .execution import MutationExecution
from .serialization import (
    checked_mapping,
    decoded_items,
    freeze_value,
    owned_items,
    owned_strings,
    thaw_value,
)
from .targets import BoundTarget


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
            # Absent optional fields stay omitted to preserve format-4 identity.
            mapping["source_path"] = self.source_path
        if self.awaits_name_release:
            # Omitted when false, for the reason ``source_path`` is.
            mapping["awaits_name_release"] = True
        return mapping

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "InstallAction":
        checked_mapping(mapping, cls)
        if type(mapping.get("awaits_name_release", False)) is not bool:
            from ..errors import BuildError

            raise BuildError("awaits_name_release must be a boolean")
        return cls(
            id=mapping["id"],
            kind=mapping["kind"],
            resource_node_id=mapping.get("resource_node_id"),
            executor=mapping["executor"],
            payload=mapping.get("payload"),
            payload_sha256=mapping.get("payload_sha256"),
            source_path=mapping.get("source_path"),
            awaits_name_release=mapping.get("awaits_name_release", False),
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
        checked_mapping(mapping, cls)
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
        checked_mapping(mapping, cls)
        return cls(
            number=mapping["number"],
            description=mapping["description"],
            batches=tuple(
                BuildBatch.from_mapping(b) for b in mapping.get("batches", ())
            ),
        )


@dataclass(frozen=True)
class PhysicalScope:
    """A target-relative scope; an empty path covers the whole target."""

    target_id: str
    path: str

    def to_mapping(self):
        return {"target_id": self.target_id, "path": self.path}

    @classmethod
    def from_mapping(cls, mapping):
        checked_mapping(mapping, cls)
        return cls(**mapping)


@dataclass(frozen=True)
class ResultReference:
    action_id: str
    result_type: str

    def to_mapping(self):
        return {"action_id": self.action_id, "result_type": self.result_type}

    @classmethod
    def from_mapping(cls, mapping):
        checked_mapping(mapping, cls)
        return cls(**mapping)


@dataclass(frozen=True)
class DriverContract:
    """A frozen extension contract to match against an execution driver."""

    executor: str
    payload_extension: str | None
    produces: str | None = None
    consumes: str | None = None
    starts_operation: bool = False
    settles_operation: bool = False

    def to_mapping(self):
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_mapping(cls, mapping):
        checked_mapping(mapping, cls)
        return cls(**mapping)


@dataclass(frozen=True, kw_only=True)
class MutationAction(InstallAction):
    target_id: str
    depends_on: tuple[str, ...]
    result_from: ResultReference | None = None
    certifies: tuple[str, ...] = ()
    resources: tuple[str, ...] = ()
    exclusions: tuple[str, ...] = ()
    writes: tuple[PhysicalScope, ...] = ()
    destructive_scopes: tuple[PhysicalScope, ...] = ()

    def __post_init__(self) -> None:
        dependencies = owned_strings(self.depends_on, what="depends_on")
        if len(dependencies) != len(set(dependencies)):
            from ..errors import BuildError

            raise BuildError(f"action {self.id!r} has a duplicate dependency")
        object.__setattr__(self, "depends_on", tuple(sorted(dependencies)))
        for name in ("certifies", "resources", "exclusions"):
            object.__setattr__(
                self, name, owned_strings(getattr(self, name), what=name)
            )
        for name in ("writes", "destructive_scopes"):
            object.__setattr__(
                self, name, owned_items(getattr(self, name), PhysicalScope)
            )

    def to_mapping(self) -> dict[str, Any]:
        mapping = super().to_mapping()
        mapping["depends_on"] = sorted(self.depends_on)
        mapping["result_from"] = (
            None if self.result_from is None else self.result_from.to_mapping()
        )
        for name in ("certifies", "resources", "exclusions"):
            mapping[name] = list(getattr(self, name))
        for name in ("writes", "destructive_scopes"):
            mapping[name] = [s.to_mapping() for s in getattr(self, name)]
        return mapping

    @classmethod
    def from_mapping(cls, mapping, *, target_id):
        checked_mapping(mapping, cls, exclude=("target_id",))
        values = dict(mapping)
        if values.get("result_from") is not None:
            values["result_from"] = ResultReference.from_mapping(values["result_from"])
        for name in ("writes", "destructive_scopes"):
            values[name] = decoded_items(
                values.get(name, ()), PhysicalScope.from_mapping
            )
        return cls(**values, target_id=target_id)


@dataclass(frozen=True)
class MutationBatch(BuildBatch):
    """Target grouping with owned action members."""

    def __post_init__(self) -> None:
        object.__setattr__(self, "actions", owned_items(self.actions, MutationAction))

    @classmethod
    def from_mapping(cls, mapping):
        checked_mapping(mapping, cls)
        return cls(
            id=mapping["id"],
            target_id=mapping["target_id"],
            actions=decoded_items(
                mapping["actions"],
                lambda a: MutationAction.from_mapping(
                    a, target_id=mapping["target_id"]
                ),
            ),
        )


@dataclass(frozen=True)
class MutationSequence(BuildSequence):
    """Presentation grouping whose actions carry their execution edges."""

    def __post_init__(self) -> None:
        object.__setattr__(self, "batches", owned_items(self.batches, MutationBatch))

    @classmethod
    def from_mapping(cls, mapping):
        checked_mapping(mapping, cls)
        return cls(
            number=mapping["number"],
            description=mapping["description"],
            batches=decoded_items(mapping["batches"], MutationBatch.from_mapping),
        )


@dataclass(frozen=True)
class MutationPlan:
    targets: tuple[BoundTarget, ...]
    sequences: tuple[MutationSequence, ...]
    execution: MutationExecution
    bundle_id: str = ""
    format_version: int = 5
    build_envelope: Mapping[str, Any] | None = None
    driver_contracts: tuple[DriverContract, ...] = ()
    required_completion: tuple[str, ...] = ()
    protected_scopes: tuple[PhysicalScope, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "targets", owned_items(self.targets, BoundTarget))
        object.__setattr__(
            self, "sequences", owned_items(self.sequences, MutationSequence)
        )
        object.__setattr__(self, "build_envelope", freeze_value(self.build_envelope))
        object.__setattr__(
            self, "driver_contracts", owned_items(self.driver_contracts, DriverContract)
        )
        object.__setattr__(
            self, "protected_scopes", owned_items(self.protected_scopes, PhysicalScope)
        )
        object.__setattr__(
            self,
            "required_completion",
            owned_strings(self.required_completion, what="required_completion"),
        )
        from .validation import validate_mutation_plan

        validate_mutation_plan(self)

    def actions(self):
        for sequence in self.sequences:
            for batch in sequence.batches:
                for action in batch.actions:
                    yield sequence, batch, action

    @property
    def target_ids(self):
        return frozenset(target.id for target in self.targets)

    def to_mapping(self):
        return {
            "format_version": self.format_version,
            "bundle_id": self.bundle_id,
            "targets": [t.to_mapping() for t in self.targets],
            "sequences": [s.to_mapping() for s in self.sequences],
            "execution": self.execution.to_mapping(),
            "build_envelope": thaw_value(self.build_envelope),
            "driver_contracts": [c.to_mapping() for c in self.driver_contracts],
            "required_completion": list(self.required_completion),
            "protected_scopes": [s.to_mapping() for s in self.protected_scopes],
        }

    @classmethod
    def from_mapping(cls, mapping):
        checked_mapping(mapping, cls, required=("bundle_id", "format_version"))
        return cls(
            targets=decoded_items(mapping["targets"], BoundTarget.from_mapping),
            sequences=decoded_items(
                mapping["sequences"], MutationSequence.from_mapping
            ),
            execution=MutationExecution.from_mapping(mapping["execution"]),
            bundle_id=mapping["bundle_id"],
            format_version=mapping["format_version"],
            build_envelope=mapping.get("build_envelope"),
            driver_contracts=decoded_items(
                mapping.get("driver_contracts", ()), DriverContract.from_mapping
            ),
            required_completion=mapping.get("required_completion", ()),
            protected_scopes=decoded_items(
                mapping.get("protected_scopes", ()), PhysicalScope.from_mapping
            ),
        )
