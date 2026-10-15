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
    catalogue: str | None
    environment: str | None
    lakehouse: str | None = None
    warehouse: str | None = None
    example: str | None = None
    semantic_model: str | None = None
    #: Power BI items found in the project folder.
    source_items: tuple[str, ...] = ()
    #: Lakehouse and Warehouse items the project folder already declares.
    adopted_items: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.example is not None and self.example not in ("Parcel", "Sales"):
            raise CommandError(
                "Unknown example. Choose Parcel or Sales, or omit --example."
            )
        for field in ("workspace", "catalogue", "environment"):
            value = getattr(self, field)
            if field == "workspace" or value is not None:
                object.__setattr__(self, field, validate_name(value, what=field))
        for field in ("lakehouse", "warehouse", "semantic_model"):
            value = getattr(self, field)
            if field == "workspace" or value is not None:
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
    def catalogue_reference(self) -> str | None:
        return f"{WAREHOUSE}/{self.catalogue}" if self.catalogue else None

    @property
    def example_model(self) -> str | None:
        """The native example project selected explicitly with --semantic-model."""

        return self.semantic_model if self.example else None

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
        if self.example_model:
            chosen.append(f"{SEMANTIC_MODEL}/{self.example_model}")
            chosen.append(f"{REPORT}/{self.example_model}")
            for kind, selected in (
                (LAKEHOUSE, self.lakehouse),
                (WAREHOUSE, self.warehouse),
            ):
                if selected:
                    chosen.append(f"{SEMANTIC_MODEL}/{self.example_model}{kind}")
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
        WORKFLOW_FILE: _workflow(
            build_commands(request), catalogue=bool(request.catalogue)
        ),
        "README.md": _readme(request),
    }
    if request.lakehouse and not request.example:
        files[f"{LAKEHOUSE}/{request.lakehouse}/Files/{KEEP_FILE}"] = ""
        files[f"{LAKEHOUSE}/{request.lakehouse}/Tables/{KEEP_FILE}"] = ""
    if request.warehouse and not request.example:
        files[f"{WAREHOUSE}/{request.warehouse}/{KEEP_FILE}"] = ""
    if request.example_model:
        # The example writes its own Power BI project.
        return files
    if (
        request.semantic_model
        and f"SemanticModel/{request.semantic_model}" not in request.source_items
    ):
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
    lines = [f"workspace: {_scalar(request.workspace)}"]
    if request.environment:
        lines.append(f"environment: {_scalar(request.environment)}")
    if request.catalogue:
        lines.append(f"catalogue: {_scalar(request.catalogue_reference)}")
    lines.extend(["", "targets:"])
    for item in request.items:
        lines.append(f"  {item}: {_scalar(item.partition('/')[2])}")
    return "\n".join(lines) + "\n"


def _workflow(builds, *, catalogue=True) -> str:
    steps = "".join(f"    - {build}\n" for build in builds)
    recorded = "    - test\n    - health\n" if catalogue else ""
    return f"""workflows:
  full:
{steps}    - load
{recorded}  load-only:
    - load
{recorded}  build-only:
{steps}  wipe-all:
    - wipe
"""


def _readme(request: ProjectRequest) -> str:
    builds = "\n".join(f"weaver {build}" for build in build_commands(request))
    recorded = "\nweaver test\nweaver health" if request.catalogue else ""
    supporting = ""
    if request.catalogue:
        supporting += f"""## The Weaver catalogue

Warehouse/{request.catalogue} holds Weaver's build, load and test state. The
first build creates those catalogue tables.

"""
    if request.environment:
        supporting += f"""## The Environment

`Environment/{request.environment}.Environment` defines the Python runtime.
Add packages to `Libraries/PublicLibraries/environment.yml`. Publish before
building a Lakehouse or running Python work, and after changing packages:

```bash
weaver fabric environment publish --path Environment/{request.environment}.Environment
```

Publication can take several minutes.

"""
    return f"""# Weaver project

This project describes items in the Microsoft Fabric workspace
`{request.workspace}`.

`workspace-config.yml` names the workspace and physical targets. Data projects
also bind a catalogue Warehouse and Fabric Environment. `PowerBI/<project>/`
holds native model and Report sources; Build creates those items in the service.

## Run the project

```bash
weaver check
{builds}
weaver load{recorded}
```

Or run `weaver workflow full`. Build installs definitions; Load runs data work
and refreshes models. Reports have no separate Load or Test.

{supporting}## Work interactively

Run `weaver session` to reuse connections across commands.

## Extend the project

Use `--example Parcel` or `--example Sales` with the items you select. Add
`--semantic-model NAME` for an inline model and linked Report. Re-run Initialise
against this folder to add an item: existing sources, including edits, and
physical bindings remain in place. Managed-source models are additional
definitions, not replacements for the inline sample model.

Native models and Reports can start without a Lakehouse or Warehouse pipeline.
When a data item is added, Initialise adds its supporting configuration.

`workflow.yml` also includes `build-only`, `load-only` and `wipe-all`.
Wipe is destructive: it removes user objects in selected managed data targets
and asks for confirmation. Keep irreplaceable sources outside managed targets.
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
