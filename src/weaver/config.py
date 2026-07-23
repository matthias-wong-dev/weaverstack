"""The ``hosts:`` file — a named lookup of host records.

The file is a convenience, never a layer. It holds nothing the host
constructors cannot express, so everything here is optional: a caller may build
a host directly and never write a file at all.

::

    hosts:
      MyFabric:
        type: Fabric
        workspace: Analytics
        weaver_lakehouse: Weaver
        fabric_environment: Weaver_Env
        warehouse_config:
          Reporting:
            degrees_of_parallelism: 8

      MyLocal:
        type: Local
        root: .local
        weaver_lakehouse: Weaver

A host entry is a dictionary of keyword arguments under a name. Unknown keys are
rejected rather than ignored, and the accepted set is derived from the host
records themselves (:meth:`weaver.hosts.Host.configurable_keys`), so a new host
field becomes configurable without touching this module.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .errors import ConfigError
from .hosts import FabricHost, Host, LocalHost, WarehouseSettings

HOST_TYPES: Mapping[str, type[Host]] = {
    "Fabric": FabricHost,
    "Local": LocalHost,
}


def load_hosts(path: str | Path) -> dict[str, Host]:
    """Load a hosts file. Relative local roots resolve against its directory."""

    import yaml

    config_path = Path(path).expanduser().resolve()
    if not config_path.is_file():
        raise ConfigError(f"hosts file not found: {config_path}")
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    return parse_hosts(payload, base_dir=config_path.parent)


def parse_hosts(payload: Any, base_dir: str | Path | None = None) -> dict[str, Host]:
    """Parse an already-loaded hosts mapping."""

    if not isinstance(payload, dict):
        raise ConfigError("hosts config must be a mapping")

    raw_hosts = payload.get("hosts")
    if not isinstance(raw_hosts, dict) or not raw_hosts:
        raise ConfigError("hosts config must define a non-empty 'hosts' mapping")

    return {
        str(alias): _parse_host(str(alias), raw, base_dir)
        for alias, raw in raw_hosts.items()
    }


def _parse_host(alias: str, raw: Any, base_dir: str | Path | None) -> Host:
    if not isinstance(raw, dict):
        raise ConfigError(f"host {alias!r} must be a mapping")

    type_value = raw.get("type")
    if type_value not in HOST_TYPES:
        raise ConfigError(
            f"host {alias!r} type must be one of {', '.join(sorted(HOST_TYPES))}, "
            f"got {type_value!r}"
        )
    host_class = HOST_TYPES[type_value]

    unknown = set(raw) - set(host_class.configurable_keys())
    if unknown:
        raise ConfigError(
            f"host {alias!r} has keys that do not belong in a {type_value} host: "
            + ", ".join(sorted(unknown))
        )

    kwargs = {key: value for key, value in raw.items() if key != "type"}
    if "warehouse_config" in kwargs:
        kwargs["warehouse_config"] = _parse_warehouse_config(alias, kwargs["warehouse_config"])
    if host_class is LocalHost:
        kwargs["root"] = _resolve_root(alias, kwargs.get("root"), base_dir)

    try:
        return host_class(alias=alias, **kwargs)
    except TypeError as exc:  # missing a required field
        raise ConfigError(f"host {alias!r} is incomplete: {exc}") from exc


def _parse_warehouse_config(alias: str, raw: Any) -> dict[str, WarehouseSettings]:
    if not isinstance(raw, dict):
        raise ConfigError(f"host {alias!r} warehouse_config must be a mapping")

    settings: dict[str, WarehouseSettings] = {}
    for name, values in raw.items():
        if values is None:
            values = {}
        if not isinstance(values, dict):
            raise ConfigError(
                f"host {alias!r} warehouse_config[{name!r}] must be a mapping"
            )
        allowed = {f for f in WarehouseSettings.__dataclass_fields__}
        unknown = set(values) - allowed
        if unknown:
            raise ConfigError(
                f"host {alias!r} warehouse_config[{name!r}] has unknown keys: "
                + ", ".join(sorted(unknown))
            )
        settings[str(name)] = WarehouseSettings(**values)
    return settings


def _resolve_root(alias: str, root: Any, base_dir: str | Path | None) -> Path:
    if root is None:
        raise ConfigError(f"host {alias!r} must define 'root'")
    if not isinstance(root, str) or not root.strip():
        raise ConfigError(f"host {alias!r} root must be a non-empty string")
    path = Path(root.strip()).expanduser()
    if not path.is_absolute() and base_dir is not None:
        path = Path(base_dir) / path
    return path
