"""The Fabric Environment definition a new project ships.

A `<Name>.Environment` directory in Microsoft Fabric's own format, holding
`.platform` and the external library list. The library list includes `weaverstack`. Publishing installs the declared
packages for Livy sessions and Fabric notebooks attached to the Environment.

No `Setting/Sparkcompute.yml` is written. Fabric applies the workspace's Spark
settings when a definition declares none, and Weaver pins no runtime version.
Add the file to the generated directory to declare compute of your own.
"""

from __future__ import annotations

import json

from ..fabric.environment_definition import (
    DIRECTORY_SUFFIX,
    EXTERNAL_LIBRARIES,
    PLATFORM,
)

#: Where a generated project keeps its Environment definition.
ENVIRONMENT_DIRECTORY = "Environment"

#: The git integration schema a Fabric item definition declares.
_PLATFORM_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/"
    "platformProperties/2.0.0/schema.json"
)

_LIBRARIES = "dependencies:\n  - pip:\n      - weaverstack\n"


def environment_definition_files(name: str) -> dict[str, str]:
    """One Environment definition, as project-relative path to text."""

    root = f"{ENVIRONMENT_DIRECTORY}/{name}{DIRECTORY_SUFFIX}"
    platform = {
        "$schema": _PLATFORM_SCHEMA,
        "metadata": {"type": "Environment", "displayName": name},
        "config": {"version": "2.0"},
    }
    return {
        f"{root}/{PLATFORM}": json.dumps(platform, indent=2) + "\n",
        f"{root}/{EXTERNAL_LIBRARIES}": _LIBRARIES,
    }


def environment_directory(name: str) -> str:
    """The project-relative directory `weaver fabric environment publish` takes."""

    return f"{ENVIRONMENT_DIRECTORY}/{name}{DIRECTORY_SUFFIX}"
