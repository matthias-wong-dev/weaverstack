"""Parse one Workspace configuration file into one Workspace value."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .declaration.model import LAKEHOUSE, WAREHOUSE, WeaverItemId
from .errors import ConfigError
from .workspaces import (
    ExecutionSettings,
    TargetDeclaration,
    Workspace,
)

_KEYS = {
    "workspace",
    "environment",
    "catalogue",
    "execution",
    "lakehouses",
    "warehouses",
}


def load_workspace(path: str | Path) -> Workspace:
    """Load one Workspace file."""

    import yaml

    config_path = Path(path).expanduser().resolve()
    if not config_path.is_file():
        raise ConfigError(f"Workspace configuration not found: {config_path}")
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    return parse_workspace(payload, base_dir=config_path.parent)


def parse_workspace(payload: Any, base_dir: str | Path | None = None) -> Workspace:
    """Parse a one-Workspace mapping."""

    if not isinstance(payload, dict):
        raise ConfigError("Workspace configuration must be a mapping")
    unknown = set(payload) - _KEYS
    if unknown:
        raise ConfigError(
            "Workspace configuration has unknown keys: " + ", ".join(sorted(unknown))
        )
    if "workspace" not in payload:
        raise ConfigError("Workspace configuration must define 'workspace'")

    common = {
        "workspace": payload["workspace"],
        "environment": payload.get("environment"),
        "catalogue": payload.get("catalogue"),
        "execution": _execution(payload.get("execution"), where="execution"),
        "lakehouses": _targets(payload.get("lakehouses"), item_type=LAKEHOUSE),
        "warehouses": _targets(payload.get("warehouses"), item_type=WAREHOUSE),
    }
    try:
        return Workspace(**common)
    except TypeError as exc:
        raise ConfigError(f"Workspace configuration is incomplete: {exc}") from exc


def resolve_workspace(
    *,
    workspace: str | None = None,
    environment: str | None = None,
    catalogue: str | None = None,
    workspace_config: str | Path | None = None,
) -> Workspace:
    """Apply CLI-over-configuration precedence and return one Workspace."""

    configured = load_workspace(workspace_config) if workspace_config else None
    resolved_identity = (
        workspace
        if workspace is not None
        else (configured.workspace if configured is not None else None)
    )
    if resolved_identity is None:
        raise ConfigError(
            "A Workspace is required. Use --workspace or --workspace-config."
        )

    common = {
        "workspace": resolved_identity,
        "environment": environment
        if environment is not None
        else (configured.environment if configured is not None else None),
        "catalogue": catalogue
        if catalogue is not None
        else (configured.catalogue if configured is not None else None),
        "execution": configured.execution
        if configured is not None
        else ExecutionSettings(),
        "lakehouses": configured.lakehouses if configured is not None else {},
        "warehouses": configured.warehouses if configured is not None else {},
    }
    return Workspace(**common)


def _execution(raw: Any, *, where: str) -> ExecutionSettings:
    if raw is None:
        return ExecutionSettings()
    if not isinstance(raw, dict):
        raise ConfigError(f"{where} must be a mapping")
    unknown = set(raw) - {"parallel_workers"}
    if unknown:
        raise ConfigError(f"{where} has unknown keys: " + ", ".join(sorted(unknown)))
    return ExecutionSettings(parallel_workers=raw.get("parallel_workers"))


def _targets(raw: Any, *, item_type: str) -> dict[str, TargetDeclaration]:
    field_name = "lakehouses" if item_type == LAKEHOUSE else "warehouses"
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"{field_name} must be a mapping")
    declarations: dict[str, TargetDeclaration] = {}
    for physical_name, value in raw.items():
        where = f"{field_name}[{physical_name!r}]"
        if isinstance(value, str):
            item_text = value
            execution = ExecutionSettings()
        elif isinstance(value, dict):
            unknown = set(value) - {"item", "execution"}
            if unknown:
                raise ConfigError(
                    f"{where} has unknown keys: " + ", ".join(sorted(unknown))
                )
            if "item" not in value:
                raise ConfigError(f"{where} must define 'item'")
            item_text = value["item"]
            execution = _execution(value.get("execution"), where=f"{where}.execution")
        else:
            raise ConfigError(f"{where} must be an item name or mapping")
        try:
            item = WeaverItemId.parse(item_text)
        except (TypeError, ValueError) as exc:
            raise ConfigError(
                f"{where} has invalid logical item {item_text!r}"
            ) from exc
        if item.item_type != item_type:
            raise ConfigError(f"{where} must name a {item_type} item, got {item}")
        declarations[str(physical_name)] = TargetDeclaration(item, execution)
    return declarations
