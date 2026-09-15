"""Generate a Fabric Environment definition for a new project.

The ``<Name>.Environment`` directory uses Fabric's format and includes
``weaverstack`` in its external libraries.

No ``Setting/Sparkcompute.yml`` is written. Fabric applies the workspace's Spark
settings when a definition declares none, and Weaver pins no runtime version.
"""

from __future__ import annotations

import json

from ..fabric.environment_definition import (
    DIRECTORY_SUFFIX,
    EXTERNAL_LIBRARIES,
    PLATFORM,
)

ENVIRONMENT_DIRECTORY = "Environment"

_PLATFORM_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/"
    "platformProperties/2.0.0/schema.json"
)

_LIBRARIES = "dependencies:\n  - pip:\n      - weaverstack\n"


def environment_definition_files(name: str) -> dict[str, str]:
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
    return f"{ENVIRONMENT_DIRECTORY}/{name}{DIRECTORY_SUFFIX}"
