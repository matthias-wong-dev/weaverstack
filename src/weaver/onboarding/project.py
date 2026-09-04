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
        for field, value in (
            ("workspace", self.workspace),
            ("catalogue", self.catalogue),
            ("environment", self.environment),
        ):
            if not isinstance(value, str) or not value.strip():
                raise CommandError(f"{field} must be a name")
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
        "# One file describes one Fabric workspace.",
        "#",
        "# Weaver items are the keys under targets:, and each value is the Fabric",
        "# item it is built into. Point the project at another workspace by",
        "# copying this file and changing the names.",
        f"workspace: {_scalar(request.workspace)}",
        f"environment: {_scalar(request.environment)}",
        "",
        "# Weaver keeps its own tables in the _ schema of this Warehouse, and",
        "# owns nothing else in it.",
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

    return (
        "# A named sequence, run in one session:\n"
        "#\n"
        f"#   weaver compose {COMPOSITION_NAME}\n"
        "#\n"
        "# Each entry takes the workspace the composition resolved, so none of\n"
        "# them names it again. Naming no item builds, loads and tests every\n"
        "# item workspace-config.yml declares.\n"
        "compose:\n"
        f"  {COMPOSITION_NAME}:\n"
        "    - build\n"
        "    - load\n"
        "    - test\n"
    )


def _scalar(value: str) -> str:
    """One YAML scalar, quoted where the plain form would not read back."""

    import yaml

    return yaml.safe_dump(value, default_flow_style=True).strip().rstrip("...").strip()
