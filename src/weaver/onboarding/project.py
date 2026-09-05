"""The configuration and folders a new Weaver project starts with.

Two files carry the project: `workspace-config.yml` names the Fabric workspace,
the Environment, the catalogue Warehouse and the item bindings, and `compose.yml`
names the build, load and test sequence. Both are read back by the parsers that
read a user's own project, `weaver.config.load_workspace` and
`weaver_cli.compose.load_composition`.

The catalogue Warehouse holds Weaver's `_` schema and no authored objects, so it
gets no folder here. Item folders are empty when no example was asked for, and a
`.gitkeep` keeps them in version control.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..declaration.model import LAKEHOUSE, WAREHOUSE
from ..errors import CommandError
from ..targets import validate_name

#: The project files a generated repository is described by.
WORKSPACE_CONFIG_FILE = "workspace-config.yml"
COMPOSE_FILE = "compose.yml"

#: The composition a generated project starts with. Entries take the workspace
#: the composition resolved, so none of them repeats it.
COMPOSITION_NAME = "full"

#: Kept so an empty item folder survives a commit.
KEEP_FILE = ".gitkeep"


@dataclass(frozen=True)
class ProjectRequest:
    """The names one initialise run was given, after defaults are applied."""

    workspace: str
    catalogue: str
    environment: str
    lakehouse: str | None = None
    warehouse: str | None = None
    example: bool = False

    def __post_init__(self) -> None:
        """Validate every supplied name before a path is built from any of them.

        These names become Fabric items and directories under the destination,
        so they go through the identity rules first. A name carrying a separator
        would otherwise reach the filesystem before the generated project was
        parsed and refused.
        """

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
        """The catalogue as workspace configuration writes it, typed."""

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
    """Every project file this request produces, as relative path to text."""

    files = {
        WORKSPACE_CONFIG_FILE: _workspace_config(request),
        COMPOSE_FILE: _composition(),
        "README.md": _readme(request),
    }
    if request.lakehouse and not request.example:
        files[f"{LAKEHOUSE}/{request.lakehouse}/Files/{KEEP_FILE}"] = ""
        files[f"{LAKEHOUSE}/{request.lakehouse}/Tables/{KEEP_FILE}"] = ""
    if request.warehouse and not request.example:
        files[f"{WAREHOUSE}/{request.warehouse}/{KEEP_FILE}"] = ""
    return files


def _workspace_config(request: ProjectRequest) -> str:
    """The project's one workspace configuration file."""

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


def _composition() -> str:
    """The build, load and test sequence a new project runs as one command."""

    return """compose:
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
    """Local instructions for operating the generated project."""

    return f"""# Welcome to Weaver

This project describes data objects in the Microsoft Fabric workspace
`{request.workspace}`.

## The important files

`workspace-config.yml` names the workspace, catalogue Warehouse, Environment
and the physical target for each repository item.

`Environment/{request.environment}.Environment` defines the Python runtime.
Add packages to `Libraries/PublicLibraries/environment.yml` there. Fabric
compute settings and custom libraries are kept beside it.

`compose.yml` defines repeatable workflows: `full`, `load-only`, `build-only`
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
weaver compose full
```

Build makes Fabric structures match the repository. Load runs the data work.
A build can be run independently.

## The Catalogue

Warehouse/{request.catalogue} holds Weaver's build, load and test state in its
`_` schema. The first build creates those catalogue tables.

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

```bash
weaver add-example
weaver compose full
```

Adding the example writes source files. Build, load and test run separately.

## Check connectivity

```bash
weaver doctor --workspace "{request.workspace}"
```

Doctor checks authentication, REST, OneLake, TDS and Spark in the workspace.
`weaver health` reports the installed project's state.
"""


def _scalar(value: str) -> str:
    """One YAML scalar, quoted where the plain form would not read back."""

    import yaml

    return yaml.safe_dump(value, default_flow_style=True).strip().rstrip("...").strip()


def validate_fabric_name(name: str, kind: str) -> str:
    """Validate known Fabric item-name constraints before provisioning."""

    import re

    validate_name(name, what=f"Fabric {kind} name")
    invalid = len(name) > 256 or any(ord(character) < 32 for character in name)
    if kind == "Lakehouse":
        invalid = invalid or re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", name) is None
    if invalid:
        raise CommandError(
            f"{name!r} is not a valid Fabric {kind} name. Choose another {kind} name."
        )
    return name
