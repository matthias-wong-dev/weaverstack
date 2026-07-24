"""Weaver build — planning a repository into a bundle, and installing it.

The boundary is deliberate::

    SesRepository -> BuildPlanner -> BuildBundle -> BundleInstaller -> hosts

The planner owns every decision — projection, ordering, executable generation,
certification. The installer owns execution only: it validates a bundle and runs
it, and never reads the source repository, resolves a dependency or selects a
target. This package grows one checkpoint at a time; only the intended entry
points are exported.
"""

from __future__ import annotations

from .bundle import (
    BuildBundle,
    compute_bundle_id,
    load_bundle,
    plan_from_yaml,
    plan_to_yaml,
    write_bundle,
)
from .models import (
    BuildAction,
    BuildBatch,
    BuildPlan,
    BuildSequence,
    OmittedNode,
)
from .installer import InstallationEnvironment, install_bundle
from .planner import Projection, generate_build_bundle, project
from .report import InstallationReport
from .targets import (
    BoundTarget,
    LakehouseBinding,
    TargetBindings,
    WarehouseBinding,
)

__all__ = [
    "BoundTarget",
    "LakehouseBinding",
    "WarehouseBinding",
    "TargetBindings",
    "OmittedNode",
    "BuildAction",
    "BuildBatch",
    "BuildSequence",
    "BuildPlan",
    "BuildBundle",
    "compute_bundle_id",
    "load_bundle",
    "write_bundle",
    "plan_to_yaml",
    "plan_from_yaml",
    "Projection",
    "project",
    "generate_build_bundle",
    "InstallationEnvironment",
    "install_bundle",
    "InstallationReport",
]
