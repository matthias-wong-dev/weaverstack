"""Workspace configuration and the two execution environments Weaver supports.

A Workspace identifies where resources live.  For Fabric that identity is a
Microsoft Fabric workspace name; for the local emulator it is the path to the
folder that stands in for one.  It does not say where Weaver code executes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from .declaration.model import LAKEHOUSE, WAREHOUSE, WeaverItemId
from .errors import ConfigError
from .targets import validate_name

WEAVER_ITEMS_AREA = "weaver_items"
BUILD_BUNDLES_AREA = "build_bundles"
FABRIC = "fabric"
LOCAL = "local"
WORKSPACE_TYPES = (FABRIC, LOCAL)


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
    """The common configuration for one Fabric workspace or local emulator."""

    environment: str | None = None
    weaver_lakehouse: str | None = None
    execution: ExecutionSettings = field(default_factory=ExecutionSettings)
    lakehouses: Mapping[str, TargetDeclaration] = field(default_factory=dict)
    warehouses: Mapping[str, TargetDeclaration] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.environment is not None:
            object.__setattr__(
                self,
                "environment",
                validate_name(self.environment, what="environment"),
            )
        if self.weaver_lakehouse is not None:
            object.__setattr__(
                self,
                "weaver_lakehouse",
                validate_name(self.weaver_lakehouse, what="weaver_lakehouse"),
            )
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
    def workspace_type(self) -> str:
        raise NotImplementedError

    @property
    def supports_sql(self) -> bool:
        raise NotImplementedError

    def declaration_for(self, item_type: str, physical_name: str) -> TargetDeclaration:
        declarations = self.lakehouses if item_type == LAKEHOUSE else self.warehouses
        try:
            return declarations[physical_name]
        except KeyError as exc:
            plural = "Lakehouses" if item_type == LAKEHOUSE else "Warehouses"
            raise ConfigError(
                f"physical target {plural}/{physical_name} is not configured"
            ) from exc


@dataclass(frozen=True, kw_only=True)
class FabricWorkspace(Workspace):
    """One Microsoft Fabric workspace."""

    workspace: str

    def __post_init__(self) -> None:
        super().__post_init__()
        object.__setattr__(
            self, "workspace", validate_name(self.workspace, what="workspace")
        )

    @property
    def workspace_type(self) -> str:
        return FABRIC

    @property
    def supports_sql(self) -> bool:
        return True

    def settings_for_warehouse(self, name: str) -> ExecutionSettings:
        declaration = self.warehouses.get(name)
        return declaration.execution if declaration else self.execution


@dataclass(frozen=True, kw_only=True)
class LocalWorkspace(Workspace):
    """A filesystem folder standing in for one Fabric workspace."""

    workspace: Path

    def __post_init__(self) -> None:
        super().__post_init__()
        value = self.workspace
        if isinstance(value, str):
            if not value.strip():
                raise ConfigError("workspace path must not be empty")
            value = Path(value.strip())
        elif not isinstance(value, Path):
            raise ConfigError(
                f"workspace must be a path, got {type(value).__name__}"
            )
        object.__setattr__(self, "workspace", value.expanduser())

    @property
    def workspace_type(self) -> str:
        return LOCAL

    @property
    def supports_sql(self) -> bool:
        return False
