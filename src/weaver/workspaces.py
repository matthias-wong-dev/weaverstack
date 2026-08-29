"""Workspace configuration.

A Workspace identifies where resources live: one Microsoft Fabric workspace, by
name. It does not say where Weaver code executes. Desktop or notebook is a
Session question.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Mapping

from .declaration.model import LAKEHOUSE, WeaverItemId
from .errors import ConfigError
from .targets import validate_name

if TYPE_CHECKING:  # names used only in annotations
    from .targets import ItemRef

#: Where the Weaver catalogue lives. A Warehouse: catalogue state is read and
#: written over TDS, and needs no Spark session to reach it.
CATALOGUE_KIND = "Warehouse"

CLI_AREA = "cli"


@dataclass(frozen=True)
class EnvironmentRef:
    """A Fabric Environment name and its optional owning workspace."""

    workspace: str | None
    name: str

    def __post_init__(self) -> None:
        if self.workspace is not None:
            object.__setattr__(
                self,
                "workspace",
                validate_name(self.workspace, what="Environment workspace"),
            )
        object.__setattr__(
            self, "name", validate_name(self.name, what="Environment name")
        )

    @classmethod
    def parse(cls, value: object) -> "EnvironmentRef":
        """Parse ``Environment`` or ``Workspace/Environment``."""

        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            raise ConfigError(
                "environment must be a string, got " + type(value).__name__
            )
        parts = value.strip().split("/")
        if len(parts) == 1:
            return cls(workspace=None, name=parts[0])
        if len(parts) == 2:
            return cls(workspace=parts[0], name=parts[1])
        raise ConfigError(
            "environment must be 'Environment' or 'Workspace/Environment', "
            f"got {value!r}"
        )

    def owner(self, workload_workspace: str) -> str:
        """Return the workspace that owns this Environment."""

        return self.workspace or validate_name(
            workload_workspace, what="workload workspace"
        )

    def __str__(self) -> str:
        return f"{self.workspace}/{self.name}" if self.workspace else self.name


def _catalogue_value(value: object) -> str:
    """One ``Warehouse/Name`` catalogue, checked and returned as written."""

    if not isinstance(value, str) or "/" not in value:
        raise ConfigError(
            f"catalogue must be typed as '{CATALOGUE_KIND}/Name', got {value!r}"
        )
    kind, _, name = value.partition("/")
    if kind != CATALOGUE_KIND:
        raise ConfigError(
            f"catalogue must name a {CATALOGUE_KIND}, for example "
            f"{CATALOGUE_KIND}/Weaver; got {value!r}"
        )
    return f"{CATALOGUE_KIND}/{validate_name(name, what='catalogue')}"


@dataclass(frozen=True)
class ExecutionSettings:
    """Technical parallelism settings for a Workspace or one Weaver item."""

    parallel_workers: int | None = None

    def __post_init__(self) -> None:
        workers = self.parallel_workers
        if workers is not None and (
            isinstance(workers, bool) or not isinstance(workers, int) or workers < 1
        ):
            raise ConfigError("parallel_workers must be a positive integer")


@dataclass(frozen=True)
class TargetDeclaration:
    """Where one Weaver item deploys in this environment.

    The item is the mapping key in :attr:`Workspace.targets`, and its type
    decides the physical kind, so this carries one display name.
    """

    #: The environment-specific Fabric item display name.
    physical: str
    execution: ExecutionSettings = field(default_factory=ExecutionSettings)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "physical", validate_name(self.physical, what="physical target")
        )

    def target_for(self, item: WeaverItemId):
        """The typed physical target this declaration names for ``item``."""

        from .targets import DeltaTarget, ItemRef, WarehouseTarget

        ref = ItemRef(self.physical)
        return DeltaTarget(ref) if item.item_type == LAKEHOUSE else WarehouseTarget(ref)


def _target_declarations(
    declarations: Mapping[WeaverItemId, TargetDeclaration],
) -> Mapping[WeaverItemId, TargetDeclaration]:
    """Validate one item-keyed target mapping.

    Two items naming one physical target is accepted here. A build into a target
    another item is installed to is refused there.
    """

    resolved: dict[WeaverItemId, TargetDeclaration] = {}
    for key, declaration in dict(declarations).items():
        item = key if isinstance(key, WeaverItemId) else WeaverItemId.parse(str(key))
        if not isinstance(declaration, TargetDeclaration):
            raise ConfigError(f"targets[{str(item)!r}] must be a TargetDeclaration")
        resolved[item] = declaration
    return MappingProxyType(resolved)


@dataclass(frozen=True, kw_only=True)
class Workspace:
    """One Microsoft Fabric workspace, and the configuration it carries.

    Identifies where the resources are. It does not say where Weaver's own code
    runs. Desktop or notebook is a Session question.
    """

    workspace: str
    environment: EnvironmentRef | str | None = None
    #: Where the Weaver catalogue lives, typed: ``Warehouse/Weaver``. Typed so the
    #: value says which kind of item it names rather than relying on the field's
    #: name to imply it.
    catalogue: str | None = None
    execution: ExecutionSettings = field(default_factory=ExecutionSettings)
    #: Where each Weaver item is deployed in this environment, keyed by the item.
    targets: Mapping[WeaverItemId, TargetDeclaration] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "workspace", validate_name(self.workspace, what="workspace")
        )
        if self.environment is not None:
            object.__setattr__(
                self,
                "environment",
                EnvironmentRef.parse(self.environment),
            )
        if self.catalogue is not None:
            object.__setattr__(self, "catalogue", _catalogue_value(self.catalogue))
        if not isinstance(self.execution, ExecutionSettings):
            raise ConfigError("execution must be ExecutionSettings")
        object.__setattr__(self, "targets", _target_declarations(self.targets))

    @property
    def catalogue_item(self) -> "ItemRef":
        """The catalogue as a resolvable item, or a failure saying it is unset.

        Callers name the item rather than re-parsing the typed string, so where
        the Weaver catalogue lives is read in one place.
        """

        from .targets import ItemRef

        if not self.catalogue:
            raise ConfigError(
                "this Workspace names no catalogue; pass catalogue="
                "'Warehouse/Weaver' or set it in workspace configuration"
            )
        return ItemRef(self.catalogue.split("/", 1)[1])

    def target_for(self, item: WeaverItemId):
        """Where this configuration deploys one item, typed. A build's answer."""

        return self._declaration(item).target_for(item)

    def _declaration(self, item: WeaverItemId) -> TargetDeclaration:
        declaration = self.targets.get(item)
        if declaration is None:
            raise ConfigError(
                f"{item} has no physical target in this Workspace configuration. "
                f"Add a targets: entry for {item}, or name the target as "
                f"{item}={item.item_type}/<physical name>."
            )
        return declaration

    @property
    def configured_items(self) -> tuple[WeaverItemId, ...]:
        """Every item this configuration deploys, in identity order."""

        return tuple(sorted(self.targets, key=str))

    @property
    def configured_lakehouses(self) -> tuple[str, ...]:
        """The Lakehouses this configuration deploys into, sorted.

        What a Livy session falls back to when no operation offered it one.
        """

        return tuple(
            sorted(
                {
                    declaration.physical
                    for item, declaration in self.targets.items()
                    if item.item_type == LAKEHOUSE
                }
            )
        )

    def settings_for(self, item: WeaverItemId) -> ExecutionSettings:
        """Parallelism for one Weaver item, or the workspace's own.

        Keyed by the item an operation selected. A physical Warehouse two items
        may deploy to carries no settings of its own, so what one item declares
        reaches only that item's own work.
        """

        declaration = self.targets.get(item)
        if declaration is None or declaration.execution.parallel_workers is None:
            return self.execution
        return declaration.execution
