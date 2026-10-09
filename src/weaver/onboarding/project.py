"""Generate the configuration and folders for a new Weaver project.

``workspace-config.yml`` names the Fabric workspace, Environment, catalogue
Warehouse and item bindings. ``workflow.yml`` names the command sequences. The
generated files use the same parsers as authored projects.

The catalogue Warehouse holds Weaver's `_` schema and no authored objects, so it
gets no folder here. Item folders are empty when no example was asked for, and a
`.gitkeep` keeps them in version control. Power BI projects go under `PowerBI/`.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..declaration.model import LAKEHOUSE, REPORT, SEMANTIC_MODEL, WAREHOUSE
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
    semantic_model: str | None = None
    #: Power BI items found in the project folder.
    source_items: tuple[str, ...] = ()
    #: Lakehouse and Warehouse items the project folder already declares.
    adopted_items: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for field in ("workspace", "catalogue", "environment"):
            object.__setattr__(
                self, field, validate_name(getattr(self, field), what=field)
            )
        for field in ("lakehouse", "warehouse", "semantic_model"):
            value = getattr(self, field)
            if value is not None:
                object.__setattr__(self, field, validate_name(value, what=field))
        for kind, value in (
            ("Lakehouse", self.lakehouse),
            ("Warehouse", self.warehouse),
            ("SemanticModel", self.semantic_model),
            ("Warehouse", self.catalogue),
            ("Environment", self.environment),
        ):
            if value is not None:
                validate_fabric_name(value, kind)
        if (
            self.lakehouse is None
            and self.warehouse is None
            and self.semantic_model is None
            and not self.source_items
        ):
            raise CommandError(
                "Choose a Lakehouse, a Warehouse, or a SemanticModel for the project."
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
        if self.semantic_model:
            chosen.append(f"{SEMANTIC_MODEL}/{self.semantic_model}")
        return tuple(dict.fromkeys(chosen + list(self.source_items)))


def build_commands(request: ProjectRequest) -> tuple[str, ...]:
    """The project's Build commands: sources first, then Power BI items."""

    items = (*request.items, *request.adopted_items)
    if not any(item.split("/", 1)[0] in {SEMANTIC_MODEL, REPORT} for item in items):
        return ("build",)
    sources = [
        kind
        for kind in (LAKEHOUSE, WAREHOUSE)
        if any(item.startswith(f"{kind}/") for item in items)
    ]
    first = ("build " + " ".join(f"--item {kind}" for kind in sources),)
    return (*(first if sources else ()), "build --item PowerBI")


def project_files(request: ProjectRequest) -> dict[str, str]:
    files = {
        WORKSPACE_CONFIG_FILE: _workspace_config(request),
        WORKFLOW_FILE: _workflow(build_commands(request)),
        "README.md": _readme(request),
    }
    if request.lakehouse and not request.example:
        files[f"{LAKEHOUSE}/{request.lakehouse}/Files/{KEEP_FILE}"] = ""
        files[f"{LAKEHOUSE}/{request.lakehouse}/Tables/{KEEP_FILE}"] = ""
    if request.warehouse and not request.example:
        files[f"{WAREHOUSE}/{request.warehouse}/{KEEP_FILE}"] = ""
    if request.semantic_model:
        files[f"PowerBI/{request.semantic_model}/{request.semantic_model}.tmdl"] = (
            semantic_extension()
        )
    elif not request.source_items:
        files[f"PowerBI/{KEEP_FILE}"] = ""
    return files


def semantic_extension():
    from importlib.resources import files

    return (
        files("weaver")
        .joinpath("fragments/semantic-extension.tmdl")
        .read_text(encoding="utf-8")
    )


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
    if request.semantic_model:
        lines.append(
            f"  {SEMANTIC_MODEL}/{request.semantic_model}: {_scalar(request.semantic_model)}"
        )
    for item in request.source_items:
        lines.append(f"  {item}: {_scalar(item.partition('/')[2])}")
    return "\n".join(lines) + "\n"


def _workflow(builds) -> str:
    steps = "".join(f"    - {build}\n" for build in builds)
    return f"""workflows:
  full:
{steps}    - load
    - test
    - health
  load-only:
    - load
    - test
    - health
  build-only:
{steps}  wipe-all:
    - wipe
"""


def _readme(request: ProjectRequest) -> str:
    builds = "\n".join(f"weaver {build}" for build in build_commands(request))
    return f"""# Weaver project

This project describes data objects in the Microsoft Fabric workspace
`{request.workspace}`.

## Project files

`workspace-config.yml` names the workspace, catalogue Warehouse, Environment
and the physical target for each project item.

`Environment/{request.environment}.Environment` defines the Python runtime. Add
packages to its `Libraries/PublicLibraries/environment.yml`. Fabric compute
settings and custom libraries are kept in the same Environment definition.

`PowerBI/<project>/` holds Power BI Desktop projects. Build creates their
semantic models and Reports in Fabric.

`workflow.yml` defines repeatable workflows: `full`, `load-only`, `build-only`
and `wipe-all`. Wipe removes all user objects from the configured targets and
asks for confirmation.

## The basic workflow

```bash
{builds}
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

Publish before building a Lakehouse or running Python work, and after changing
packages:

```bash
weaver fabric environment publish --path Environment/{request.environment}.Environment
```

Publishing can take several minutes. Lakehouse builds and Python work run in
Fabric Spark with this Environment.

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
