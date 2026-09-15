"""Workspace configuration identifies Fabric resources, not where Weaver runs."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Mapping

from .declaration.model import LAKEHOUSE, WeaverItemId
from .errors import ConfigError
from .targets import validate_name

if TYPE_CHECKING:
    from .targets import ItemRef

# The catalogue is a Warehouse so its state is available over TDS without Spark.
CATALOGUE_KIND = "Warehouse"

CLI_AREA = "cli"


@dataclass(frozen=True)
class EnvironmentRef:
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
        return self.workspace or validate_name(
            workload_workspace, what="workload workspace"
        )

    def __str__(self) -> str:
        return f"{self.workspace}/{self.name}" if self.workspace else self.name


@dataclass(frozen=True)
class CatalogueRef:
    """A catalogue Warehouse, optionally qualified by its workspace."""

    workspace: str | None
    name: str

    def __post_init__(self) -> None:
        if self.workspace is not None:
            object.__setattr__(
                self,
                "workspace",
                validate_name(self.workspace, what="catalogue workspace"),
            )
        object.__setattr__(self, "name", validate_name(self.name, what="catalogue"))

    @classmethod
    def parse(cls, value: object) -> "CatalogueRef":
        """Parse ``Warehouse/Name`` or ``Workspace/Warehouse/Name``."""

        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            raise ConfigError(
                f"a catalogue address must be a string, got {type(value).__name__}"
            )
        parts = value.strip().split("/")
        if len(parts) == 2:
            workspace, kind, name = None, parts[0], parts[1]
        elif len(parts) == 3:
            workspace, kind, name = parts[0], parts[1], parts[2]
        else:
            raise ConfigError(
                f"a catalogue address must be '{CATALOGUE_KIND}/Name' or "
                f"'Workspace/{CATALOGUE_KIND}/Name', got {value!r}"
            )
        if kind != CATALOGUE_KIND:
            raise ConfigError(
                f"a catalogue address must name a {CATALOGUE_KIND}, for example "
                f"{CATALOGUE_KIND}/Weaver; got {value!r}"
            )
        return cls(workspace=workspace, name=name)

    @property
    def item(self) -> "ItemRef":
        from .targets import ItemRef

        return ItemRef(self.name)

    def owner(self, workload_workspace: str) -> str:
        return self.workspace or validate_name(
            workload_workspace, what="workload workspace"
        )

    def is_local_to(self, workload_workspace: str) -> bool:
        return (
            self.owner(workload_workspace).casefold()
            == (workload_workspace or "").casefold()
        )

    @property
    def local(self) -> "CatalogueRef":
        return CatalogueRef(workspace=None, name=self.name)

    def __str__(self) -> str:
        typed = f"{CATALOGUE_KIND}/{self.name}"
        return f"{self.workspace}/{typed}" if self.workspace else typed


def _catalogue_value(value: object) -> str:
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
    parallel_workers: int | None = None

    def __post_init__(self) -> None:
        workers = self.parallel_workers
        if workers is not None and (
            isinstance(workers, bool) or not isinstance(workers, int) or workers < 1
        ):
            raise ConfigError("parallel_workers must be a positive integer")


@dataclass(frozen=True)
class TargetDeclaration:
    """A Fabric item name; the key in ``Workspace.targets`` supplies its type."""

    physical: str
    execution: ExecutionSettings = field(default_factory=ExecutionSettings)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "physical", validate_name(self.physical, what="physical target")
        )

    def target_for(self, item: WeaverItemId):
        from .targets import DeltaTarget, ItemRef, WarehouseTarget

        ref = ItemRef(self.physical)
        return DeltaTarget(ref) if item.item_type == LAKEHOUSE else WarehouseTarget(ref)


def _target_declarations(
    declarations: Mapping[WeaverItemId, TargetDeclaration],
) -> Mapping[WeaverItemId, TargetDeclaration]:
    """Allow shared targets here; builds reject conflicting installations."""

    resolved: dict[WeaverItemId, TargetDeclaration] = {}
    for key, declaration in dict(declarations).items():
        item = key if isinstance(key, WeaverItemId) else WeaverItemId.parse(str(key))
        if not isinstance(declaration, TargetDeclaration):
            raise ConfigError(f"targets[{str(item)!r}] must be a TargetDeclaration")
        resolved[item] = declaration
    return MappingProxyType(resolved)


@dataclass(frozen=True, kw_only=True)
class Workspace:
    """Fabric resource configuration, independent of where Weaver runs."""

    workspace: str
    environment: EnvironmentRef | str | None = None
    #: Typed as ``Warehouse/Name``.
    catalogue: str | None = None
    #: The catalogue whose installed state ``weaver mirror`` reproduces here.
    mirror: "CatalogueRef | str | None" = None
    execution: ExecutionSettings = field(default_factory=ExecutionSettings)
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
        if self.mirror is not None:
            object.__setattr__(self, "mirror", CatalogueRef.parse(self.mirror))
        if not isinstance(self.execution, ExecutionSettings):
            raise ConfigError("execution must be ExecutionSettings")
        object.__setattr__(self, "targets", _target_declarations(self.targets))

    @property
    def catalogue_item(self) -> "ItemRef":
        from .targets import ItemRef

        if not self.catalogue:
            raise ConfigError(
                "No catalogue is configured for this Workspace. Pass catalogue="
                "'Warehouse/Weaver' or set it in workspace configuration"
            )
        return ItemRef(self.catalogue.split("/", 1)[1])

    @property
    def catalogue_ref(self) -> CatalogueRef:
        return CatalogueRef(workspace=self.workspace, name=self.catalogue_item.name)

    def target_for(self, item: WeaverItemId):
        return self._declaration(item).target_for(item)

    def _declaration(self, item: WeaverItemId) -> TargetDeclaration:
        declaration = self.targets.get(item)
        if declaration is None:
            raise ConfigError(
                f"No target is configured for {item} in this Workspace. "
                f"Add a targets: entry for {item}, or name the target as "
                f"{item}={item.item_type}/<physical name>."
            )
        return declaration

    @property
    def configured_items(self) -> tuple[WeaverItemId, ...]:
        return tuple(sorted(self.targets, key=str))

    @property
    def configured_lakehouses(self) -> tuple[str, ...]:
        """Lakehouses a Livy session may use when an operation names none."""

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
        """Use item-specific parallelism when set, otherwise the workspace default."""

        declaration = self.targets.get(item)
        if declaration is None or declaration.execution.parallel_workers is None:
            return self.execution
        return declaration.execution
