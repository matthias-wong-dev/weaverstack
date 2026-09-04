"""Parse one Workspace configuration file into one Workspace value."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .declaration.model import WeaverItemId
from .errors import ConfigError, IdentityError
from .workspaces import (
    EnvironmentRef,
    ExecutionSettings,
    TargetDeclaration,
    Workspace,
)

_KEYS = {
    "workspace",
    "environment",
    "catalogue",
    "execution",
    "targets",
}

#: The physical-keyed sections ``targets:`` replaced.
_RETIRED_KEYS = ("lakehouses", "warehouses")


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
    retired = [key for key in _RETIRED_KEYS if key in payload]
    if retired:
        raise ConfigError(
            ", ".join(f"{key}:" for key in retired)
            + " is replaced by one item-keyed targets: mapping. Write 'targets:' "
            "with entries such as 'Lakehouse/Landing: Landing_Dev'."
        )
    unknown = set(payload) - _KEYS
    if unknown:
        raise ConfigError(
            "Workspace configuration has unknown keys: " + ", ".join(sorted(unknown))
        )
    if "workspace" not in payload:
        raise ConfigError("Workspace configuration must define 'workspace'")

    try:
        return Workspace(
            workspace=_text(payload["workspace"], where="workspace"),
            environment=payload.get("environment"),
            catalogue=payload.get("catalogue"),
            execution=_execution(payload.get("execution"), where="execution"),
            targets=_targets(payload.get("targets")),
        )
    except TypeError as exc:
        raise ConfigError(f"Workspace configuration is incomplete: {exc}") from exc
    except IdentityError as exc:
        # A name the identity rules reject, surfaced in the vocabulary of the
        # file it was written in.
        raise ConfigError(f"Workspace configuration is invalid: {exc}") from exc


#: The configuration file a project keeps in its own root.
DEFAULT_FILE = "workspace-config.yml"


def discovered_workspace_config(directory: str | Path | None = None) -> Path | None:
    """The project's own configuration file, when the working directory holds one.

    What lets ``weaver build`` run from a project root with nothing else on the
    command line. It is the last resort: an explicit ``--workspace`` or
    ``--workspace-config``, and a Session's own workspace, are all consulted
    first.
    """

    candidate = Path(directory or Path.cwd()) / DEFAULT_FILE
    return candidate if candidate.is_file() else None


def resolve_workspace(
    *,
    workspace: str | None = None,
    environment: EnvironmentRef | str | None = None,
    catalogue: str | None = None,
    workspace_config: str | Path | None = None,
) -> Workspace:
    """Apply CLI-over-configuration precedence and return one Workspace."""

    if workspace is None and workspace_config is None:
        workspace_config = discovered_workspace_config()
    configured = load_workspace(workspace_config) if workspace_config else None
    resolved_identity = (
        workspace
        if workspace is not None
        else (configured.workspace if configured is not None else None)
    )
    if resolved_identity is None:
        raise ConfigError(
            f"A Workspace is required. Use --workspace or --workspace-config, "
            f"or run from a project directory holding {DEFAULT_FILE}."
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
        "targets": configured.targets if configured is not None else {},
    }
    return Workspace(**common)


def _text(value: Any, *, where: str) -> str:
    """One configured scalar name, or a ConfigError naming the field.

    The identity rules answer what a name may contain. This answers whether a
    name was written at all, so the message says which key is wrong.
    """

    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{where} must be a non-empty string, got {value!r}")
    return value


def _execution(raw: Any, *, where: str) -> ExecutionSettings:
    if raw is None:
        return ExecutionSettings()
    if not isinstance(raw, dict):
        raise ConfigError(f"{where} must be a mapping")
    unknown = set(raw) - {"parallel_workers"}
    if unknown:
        raise ConfigError(f"{where} has unknown keys: " + ", ".join(sorted(unknown)))
    return ExecutionSettings(parallel_workers=raw.get("parallel_workers"))


def _targets(raw: Any) -> dict[WeaverItemId, TargetDeclaration]:
    """Parse ``targets:``, which maps one Weaver item to one Fabric item name.

    .. code-block:: yaml

        targets:
          Lakehouse/Landing: Landing_Dev
          Warehouse/Curated:
            name: Curated_Dev
            execution:
              parallel_workers: 4
    """

    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError("targets must be a mapping")
    declarations: dict[WeaverItemId, TargetDeclaration] = {}
    for written, value in raw.items():
        where = f"targets[{written!r}]"
        try:
            item = WeaverItemId.parse(written)
        except (IdentityError, TypeError, ValueError) as exc:
            raise ConfigError(
                f"{where} is not an item identity such as Lakehouse/Landing"
            ) from exc
        if isinstance(value, str):
            physical = _text(value, where=where)
            execution = ExecutionSettings()
        elif isinstance(value, dict):
            unknown = set(value) - {"name", "execution"}
            if unknown:
                raise ConfigError(
                    f"{where} has unknown keys: " + ", ".join(sorted(unknown))
                )
            if "name" not in value:
                raise ConfigError(f"{where} must define 'name'")
            physical = _text(value["name"], where=f"{where}.name")
            execution = _execution(value.get("execution"), where=f"{where}.execution")
        else:
            raise ConfigError(f"{where} must be a physical item name or mapping")
        declarations[item] = TargetDeclaration(physical, execution)
    return declarations
