"""Generate the configuration and folders for a new Weaver project.

``workspace-config.yml`` names the Fabric workspace, Environment, catalogue
Warehouse and item bindings. ``workflow.yml`` names the command sequences. The
generated files use the same parsers as authored projects.

The catalogue Warehouse holds Weaver's `_` schema and no authored objects, so it
gets no folder here. Item folders are empty when no example was asked for, and a
`.gitkeep` keeps them in version control.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..declaration.model import LAKEHOUSE, WAREHOUSE
from ..errors import CommandError
from ..targets import validate_name

WORKSPACE_CONFIG_FILE = "workspace-config.yml"
WORKFLOW_FILE = "workflow.yml"

# Entries inherit the workspace resolved for the workflow.
WORKFLOW_NAME = "full"

KEEP_FILE = ".gitkeep"


@dataclass(frozen=True)
class ProjectRequest:
    workspace: str
    catalogue: str
    environment: str
    lakehouse: str | None = None
    warehouse: str | None = None
    example: bool = False

    def __post_init__(self) -> None:
        for field in ("workspace", "catalogue", "environment"):
            object.__setattr__(
                self, field, validate_name(getattr(self, field), what=field)
            )
        for field in ("lakehouse", "warehouse"):
            value = getattr(self, field)
            if value is not None:
                object.__setattr__(self, field, validate_name(value, what=field))
        for kind, value in (
            ("Lakehouse", self.lakehouse),
            ("Warehouse", self.warehouse),
            ("Warehouse", self.catalogue),
            ("Environment", self.environment),
        ):
            if value is not None:
                validate_fabric_name(value, kind)
        if self.lakehouse is None and self.warehouse is None:
            raise CommandError(
                "Choose a Lakehouse, a Warehouse, or both. A project with "
                "neither has nowhere to build into."
            )

    @property
    def catalogue_reference(self) -> str:
        return f"{WAREHOUSE}/{self.catalogue}"

    @property
    def items(self) -> tuple[str, ...]:
        """The Weaver items this project declares, Lakehouse first."""

        chosen = []
        if self.lakehouse:
            chosen.append(f"{LAKEHOUSE}/{self.lakehouse}")
        if self.warehouse:
            chosen.append(f"{WAREHOUSE}/{self.warehouse}")
        return tuple(chosen)


def project_files(request: ProjectRequest) -> dict[str, str]:
    files = {
        WORKSPACE_CONFIG_FILE: _workspace_config(request),
        WORKFLOW_FILE: _workflow(),
        "README.md": _readme(request),
    }
    if request.lakehouse and not request.example:
        files[f"{LAKEHOUSE}/{request.lakehouse}/Files/{KEEP_FILE}"] = ""
        files[f"{LAKEHOUSE}/{request.lakehouse}/Tables/{KEEP_FILE}"] = ""
    if request.warehouse and not request.example:
        files[f"{WAREHOUSE}/{request.warehouse}/{KEEP_FILE}"] = ""
    return files


def _workspace_config(request: ProjectRequest) -> str:
    lines = [
        f"workspace: {_scalar(request.workspace)}",
        f"environment: {_scalar(request.environment)}",
        f"catalogue: {_scalar(request.catalogue_reference)}",
        "",
        "targets:",
    ]
    if request.lakehouse:
        lines.append(f"  {LAKEHOUSE}/{request.lakehouse}: {_scalar(request.lakehouse)}")
    if request.warehouse:
        lines.append(f"  {WAREHOUSE}/{request.warehouse}: {_scalar(request.warehouse)}")
    return "\n".join(lines) + "\n"


def _workflow() -> str:
    return """workflows:
  full:
    - build
    - load
    - test
    - health
  load-only:
    - load
    - test
    - health
  build-only:
    - build
  wipe-all:
    - wipe
"""


def _readme(request: ProjectRequest) -> str:
    return f"""# Weaver project

This project describes data objects in the Microsoft Fabric workspace
`{request.workspace}`.

## Project files

`workspace-config.yml` names the workspace, catalogue Warehouse, Environment
and the physical target for each project item.

`Environment/{request.environment}.Environment` defines the Python runtime. Add
packages to its `Libraries/PublicLibraries/environment.yml`. Fabric compute
settings and custom libraries are kept in the same Environment definition.

`workflow.yml` defines repeatable workflows: `full`, `load-only`, `build-only`
and `wipe-all`. Wipe removes all user objects from the configured targets and
asks for confirmation.

## The basic workflow

```bash
weaver build
weaver load
weaver test
weaver health
```

Or run the sequence in one session:

```bash
weaver workflow full
```

Build makes Fabric structures match the project. Load runs the data work.

## The Weaver catalogue

Warehouse/{request.catalogue} holds Weaver's build, load and test state. The
first build creates those catalogue tables.

## The Environment

Publish before running Python work, and after changing packages:

```bash
weaver fabric environment publish --path Environment/{request.environment}.Environment
```

Publishing can take several minutes. Spark SQL needs a Lakehouse; Python work
that imports Weaver also needs the published Environment.

## Working interactively

```bash
weaver session
```

Commands inside the session reuse Fabric connections and the Spark session.

## Try the example

Choose the Sales example during initialisation to include its source files. Run
build, load and test together with `weaver workflow full`. To try the example
later, initialise a new project folder with the example.

## Check connectivity

```bash
weaver doctor --workspace "{request.workspace}"
```

Doctor checks authentication, REST, OneLake, TDS and Spark in the workspace.
`weaver health` reports the installed project's state.
"""


def _scalar(value: str) -> str:
    """Quote a YAML scalar only when needed for a lossless read."""

    import yaml

    return yaml.safe_dump(value, default_flow_style=True).strip().rstrip("...").strip()


def validate_fabric_name(name: str, kind: str) -> str:
    """Validate known Fabric item-name constraints before creation."""

    import re

    validate_name(name, what=f"Fabric {kind} name")
    # Fabric's Lakehouse creation guide limits display names to 123 characters.
    maximum = 123 if kind == "Lakehouse" else 256
    invalid = len(name) > maximum or any(ord(character) < 32 for character in name)
    if kind == "Lakehouse":
        invalid = invalid or re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", name) is None
    if invalid:
        raise CommandError(
            f"{name!r} is not a valid Fabric {kind} name. Choose another {kind} name."
        )
    return name
