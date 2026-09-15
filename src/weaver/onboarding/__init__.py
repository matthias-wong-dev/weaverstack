"""Generate files for ``weaver initialise`` without writing them.

``weaver.initialise`` validates the generated project with the authored-project
readers before creating anything.
"""

from __future__ import annotations

from .environment import environment_definition_files
from .example import example_files
from .project import (
    WORKFLOW_FILE,
    WORKSPACE_CONFIG_FILE,
    ProjectRequest,
    project_files,
)

__all__ = [
    "WORKFLOW_FILE",
    "WORKSPACE_CONFIG_FILE",
    "ProjectRequest",
    "environment_definition_files",
    "example_files",
    "project_files",
]
