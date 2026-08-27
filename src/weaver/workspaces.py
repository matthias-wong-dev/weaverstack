"""Workspace configuration.

A Workspace identifies where resources live: one Microsoft Fabric workspace, by
name. It does not say where Weaver code executes. Desktop or notebook is a
Session question.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Mapping

from .declaration.model import LAKEHOUSE, WAREHOUSE, WeaverItemId
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
    """Technical parallelism settings for a Workspace or one physical item."""

    parallel_workers: int | None = None

    def __post_init__(self) -> None:
        workers = self.parallel_workers
        if workers is not None and (
            isinstance(workers, bool) or not isinstance(workers, int) or workers < 1
        ):
            raise ConfigError("parallel_workers must be a positive integer")


@dataclass(frozen=True)
class TargetDeclaration:
    """A configured physical target and its default logical Weaver item."""

    item: WeaverItemId
    execution: ExecutionSettings = field(default_factory=ExecutionSettings)


def _target_declarations(
    declarations: Mapping[str, TargetDeclaration], *, item_type: str, field_name: str
) -> Mapping[str, TargetDeclaration]:
    resolved: dict[str, TargetDeclaration] = {}
    for physical_name, declaration in dict(declarations).items():
        name = validate_name(physical_name, what=f"{field_name} key")
        if not isinstance(declaration, TargetDeclaration):
            raise ConfigError(f"{field_name}[{name!r}] must be a TargetDeclaration")
        if declaration.item.item_type != item_type:
            raise ConfigError(
                f"{field_name}[{name!r}] must name a {item_type} item, "
                f"got {declaration.item}"
            )
        resolved[name] = declaration
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
    lakehouses: Mapping[str, TargetDeclaration] = field(default_factory=dict)
    warehouses: Mapping[str, TargetDeclaration] = field(default_factory=dict)

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
        object.__setattr__(
            self,
            "lakehouses",
            _target_declarations(
                self.lakehouses, item_type=LAKEHOUSE, field_name="lakehouses"
            ),
        )
        object.__setattr__(
            self,
            "warehouses",
            _target_declarations(
                self.warehouses, item_type=WAREHOUSE, field_name="warehouses"
            ),
        )

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

    def declaration_for(self, item_type: str, physical_name: str) -> TargetDeclaration:
        declarations = self.lakehouses if item_type == LAKEHOUSE else self.warehouses
        try:
            return declarations[physical_name]
        except KeyError as exc:
            plural = "Lakehouses" if item_type == LAKEHOUSE else "Warehouses"
            raise ConfigError(
                f"physical target {plural}/{physical_name} is not configured"
            ) from exc

    def settings_for_warehouse(self, name: str) -> ExecutionSettings:
        declaration = self.warehouses.get(name)
        return declaration.execution if declaration else self.execution
