"""Workspace configuration.

A Workspace identifies where resources live: one Microsoft Fabric workspace, by
name. It does not say where Weaver code executes — desktop or notebook is a
Session question.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from .declaration.model import LAKEHOUSE, WAREHOUSE, WeaverItemId
from .errors import ConfigError
from .targets import validate_name

#: The one item type a catalogue may name today. Typed values are accepted for
#: both so the grammar is stable, and a Warehouse catalogue is refused with the
#: reason rather than an error about parsing.
CATALOGUE_KIND = "Lakehouse"

WEAVER_ITEMS_AREA = "weaver_items"
BUILD_BUNDLES_AREA = "build_bundles"
CLI_AREA = "cli"


def _catalogue_value(value: object) -> str:
    """One ``Lakehouse/Name`` catalogue, checked and returned as written."""

    if not isinstance(value, str) or "/" not in value:
        raise ConfigError(
            f"catalogue must be typed as '{CATALOGUE_KIND}/Name', got {value!r}"
        )
    kind, _, name = value.partition("/")
    if kind != CATALOGUE_KIND:
        raise ConfigError(
            f"the catalogue is a {CATALOGUE_KIND} today, so {value!r} cannot be "
            "used; moving it to a Warehouse is separate work"
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
    """The configuration one workspace carries, whatever it is addressed for."""

    environment: str | None = None
    #: Where the control plane lives, typed: ``Lakehouse/Weaver``. Typed because
    #: the catalogue is a Lakehouse today and a later migration may make it a
    #: Warehouse — and when it does, this value changes without the name
    #: changing again.
    catalogue: str | None = None
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
        if self.catalogue is not None:
            object.__setattr__(
                self, "catalogue", _catalogue_value(self.catalogue)
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
    def catalogue_item(self) -> "ItemRef":
        """The catalogue as a resolvable item, or a failure saying it is unset.

        Callers name the item rather than re-parsing the typed string, so where
        the control plane lives is read in one place.
        """

        from .targets import ItemRef

        if not self.catalogue:
            raise ConfigError(
                "this Workspace names no catalogue; pass catalogue="
                "'Lakehouse/Weaver' or set it in workspace configuration"
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


@dataclass(frozen=True, kw_only=True)
class FabricWorkspace(Workspace):
    """One Microsoft Fabric workspace."""

    workspace: str

    def __post_init__(self) -> None:
        super().__post_init__()
        object.__setattr__(
            self, "workspace", validate_name(self.workspace, what="workspace")
        )

    def settings_for_warehouse(self, name: str) -> ExecutionSettings:
        declaration = self.warehouses.get(name)
        return declaration.execution if declaration else self.execution

