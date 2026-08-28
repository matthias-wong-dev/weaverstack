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
    """One logical Weaver item, and the physical Fabric item it deploys to.

    The logical item carries the type, so the physical half is one display name.
    ``Lakehouse/Landing`` deploys to a Lakehouse and ``Warehouse/Curated`` to a
    Warehouse.
    """

    item: WeaverItemId
    #: The environment-specific Fabric item display name.
    physical: str
    execution: ExecutionSettings = field(default_factory=ExecutionSettings)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "physical",
            validate_name(self.physical, what=f"physical target for {self.item}"),
        )

    @property
    def target(self):
        """The typed physical target this declaration names.

        A :class:`weaver.targets.DeltaTarget` for a Lakehouse item and a
        :class:`weaver.targets.WarehouseTarget` for a Warehouse one. The type
        comes from the logical key, which is why one mapping serves both.
        """

        from .targets import DeltaTarget, ItemRef, WarehouseTarget

        item = ItemRef(self.physical)
        if self.item.item_type == LAKEHOUSE:
            return DeltaTarget(item)
        return WarehouseTarget(item)


def _target_declarations(
    declarations: Mapping[WeaverItemId, TargetDeclaration],
) -> Mapping[WeaverItemId, TargetDeclaration]:
    """Validate one logical-keyed target mapping.

    Two logical items may name one physical item. The mapping says where each
    logical item is deployed, and a physical Lakehouse or Warehouse hosts as
    many logical items as an estate puts in it. An address two logical objects
    both claim inside one of them is a physical collision, refused where an
    operation has to address it: see :attr:`weaver.installed.InstalledDag.ambiguous`.
    """

    resolved: dict[WeaverItemId, TargetDeclaration] = {}
    for key, declaration in dict(declarations).items():
        item = key if isinstance(key, WeaverItemId) else WeaverItemId.parse(str(key))
        if not isinstance(declaration, TargetDeclaration):
            raise ConfigError(f"targets[{str(item)!r}] must be a TargetDeclaration")
        if declaration.item != item:
            raise ConfigError(
                f"targets[{str(item)!r}] declares {declaration.item}; the key and "
                "the declaration must name one logical item"
            )
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
    #: Where each logical Weaver item is deployed in this environment, keyed by
    #: the logical item. One mapping rather than two, because the key's item type
    #: already says whether the physical half is a Lakehouse or a Warehouse.
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
        """The typed physical target this configuration deploys one item to.

        The build half of the logical-to-physical question. Load and test read
        the installed answer from the catalogue instead: see
        :meth:`weaver.installed.InstalledDag.target_for`.
        """

        declaration = self.targets.get(item)
        if declaration is None:
            raise ConfigError(
                f"{item} has no physical target in this Workspace configuration. "
                f"Add a targets: entry for {item}, or name the target as "
                f"{item}={item.item_type}/<physical name>."
            )
        return declaration.target

    @property
    def configured_items(self) -> tuple[WeaverItemId, ...]:
        """Every logical item this configuration deploys, in identity order."""

        return tuple(sorted(self.targets, key=str))

    @property
    def configured_lakehouses(self) -> tuple[str, ...]:
        """The physical Lakehouse names this configuration deploys into.

        Sorted, so a workspace answers the same way twice. What a Livy session
        falls back to when no operation offered it a Lakehouse to attach to.
        """

        return tuple(
            sorted(
                {
                    declaration.physical
                    for declaration in self.targets.values()
                    if declaration.item.item_type == LAKEHOUSE
                }
            )
        )

    def settings_for_warehouse(self, name: str) -> ExecutionSettings:
        """Parallelism for one physical Warehouse, or the workspace's own.

        Keyed by the physical name because parallelism is a property of the
        connection. Where two logical items share a Warehouse and declare
        different settings, the lower worker count wins.
        """

        workers = [
            declaration.execution.parallel_workers
            for declaration in self.targets.values()
            if declaration.item.item_type == WAREHOUSE
            and declaration.physical == name
            and declaration.execution.parallel_workers is not None
        ]
        if not workers:
            return self.execution
        return ExecutionSettings(parallel_workers=min(workers))
