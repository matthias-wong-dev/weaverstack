"""Hosts — the fourth level of the four-level model.

A host is the environment in which level-three items exist: a Fabric workspace,
or a local root directory standing in for one. It is the only level that is
written down and named, because it is the only level that cannot be identified
from inside itself.

A host determines *where work executes*, not where it was requested. Building
``--to MyFabric`` runs in that workspace whether it was invoked from a Fabric
notebook or from a desktop shell; only the transport differs. Building
``--to MyLocal`` runs locally.

Every host is constructible directly::

    FabricHost(workspace="Analytics", weaver_lakehouse="Weaver")

Configuration files are a convenience that name such instances — see
:mod:`weaver.config`. Nothing may be expressible in the file that is not
expressible here.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from .errors import ConfigError
from .targets import validate_name

#: Installed repositories live beneath the Weaver Lakehouse Files area at this
#: relative root. It is a convention, not configuration: a folder pushed here
#: becomes an installed repository.
REPOS_AREA = "repos"


@dataclass(frozen=True)
class WarehouseSettings:
    """Per-Warehouse technical settings.

    Attached to a Warehouse by its real name. This does not define or alias the
    Warehouse — it exists regardless — so a missing entry is normal.
    """

    degrees_of_parallelism: int | None = None

    def __post_init__(self) -> None:
        dop = self.degrees_of_parallelism
        if dop is None:
            return
        if isinstance(dop, bool) or not isinstance(dop, int) or dop < 1:
            raise ConfigError("degrees_of_parallelism must be a positive integer")


@dataclass(frozen=True, kw_only=True)
class Host:
    """Base host record.

    ``alias`` is the name a host was configured under, carried only so errors
    can say which host they mean. A directly-constructed host has none.
    """

    alias: str | None = None
    weaver_lakehouse: str | None = None

    def __post_init__(self) -> None:
        if self.weaver_lakehouse is not None:
            object.__setattr__(
                self,
                "weaver_lakehouse",
                validate_name(self.weaver_lakehouse, what="weaver_lakehouse"),
            )

    @property
    def supports_sql(self) -> bool:
        raise NotImplementedError

    @classmethod
    def configurable_keys(cls) -> frozenset[str]:
        """Keys accepted in a ``hosts:`` entry for this host type.

        Derived from the record rather than hand-maintained, so a new field is
        configurable the moment it exists. ``alias`` is excluded because it is
        the mapping key itself.
        """

        return frozenset({"type"} | {f.name for f in fields(cls) if f.name != "alias"})


@dataclass(frozen=True, kw_only=True)
class FabricHost(Host):
    """One Microsoft Fabric workspace."""

    workspace: str
    fabric_environment: str | None = None
    warehouse_config: Mapping[str, WarehouseSettings] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__post_init__()
        object.__setattr__(self, "workspace", validate_name(self.workspace, what="workspace"))
        if self.fabric_environment is not None:
            object.__setattr__(
                self,
                "fabric_environment",
                validate_name(self.fabric_environment, what="fabric_environment"),
            )
        settings = {
            validate_name(name, what="warehouse_config key"): value
            for name, value in dict(self.warehouse_config).items()
        }
        for name, value in settings.items():
            if not isinstance(value, WarehouseSettings):
                raise ConfigError(f"warehouse_config[{name!r}] must be WarehouseSettings")
        object.__setattr__(self, "warehouse_config", MappingProxyType(settings))

    @property
    def supports_sql(self) -> bool:
        return True

    def settings_for_warehouse(self, name: str) -> WarehouseSettings:
        return self.warehouse_config.get(name, WarehouseSettings())


@dataclass(frozen=True, kw_only=True)
class LocalHost(Host):
    """A root directory standing in for a workspace.

    Each level-three item is a subdirectory holding ``Files/`` and ``Tables/``.
    There is no local SQL implementation, so Folder and Delta work locally and
    Warehouse work does not.
    """

    root: Path

    def __post_init__(self) -> None:
        super().__post_init__()
        if isinstance(self.root, str):
            if not self.root.strip():
                raise ConfigError("root must not be empty")
            object.__setattr__(self, "root", Path(self.root.strip()))
        elif not isinstance(self.root, Path):
            raise ConfigError(f"root must be a path, got {type(self.root).__name__}")
        object.__setattr__(self, "root", Path(self.root).expanduser())

    @property
    def supports_sql(self) -> bool:
        return False
