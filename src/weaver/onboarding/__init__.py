"""The files `weaver initialise` writes into a new project.

Three generators, each producing text and nothing else: the project's own
configuration, the Fabric Environment definition, and the optional Sales
example. Nothing here reaches Fabric or the filesystem beyond the destination it
is given, so what a run would write can be listed without writing it.

The generated project is parsed by the same readers a user's own project is, and
`weaver.initialise` validates it that way before anything is created.
"""

from __future__ import annotations

from .environment import environment_definition_files
from .example import example_files
from .project import (
    COMPOSE_FILE,
    WORKSPACE_CONFIG_FILE,
    ProjectRequest,
    project_files,
)

__all__ = [
    "COMPOSE_FILE",
    "WORKSPACE_CONFIG_FILE",
    "ProjectRequest",
    "environment_definition_files",
    "example_files",
    "project_files",
]
