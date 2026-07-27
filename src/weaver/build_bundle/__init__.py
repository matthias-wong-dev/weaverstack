"""Weaver build — planning a declaration into a bundle, and installing it.

The boundary is deliberate::

    WeaverRepository -> planner -> BuildBundle -> installer -> hosts

The planner owns every decision — item selection, ordering, executable
generation, certification. The installer owns execution only: it validates a
bundle and runs it, and never reads the source declaration, resolves a
dependency or selects a target.
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
from .planner import generate_item_build_bundle
from .report import InstallationReport
from .workflow import (
    ItemBuildResult,
    build_item_repository,
    install_bundle_archive,
    materialise_bundle_archive,
    materialise_tree,
    persist_bundle_archive,
    timestamped_archive_name,
)
from .targets import (
    BoundTarget,
    LakehouseBinding,
    ItemBinding,
    ItemBindings,
    parse_item_binding,
    WarehouseBinding,
)

__all__ = [
    "BoundTarget",
    "LakehouseBinding",
    "ItemBinding",
    "ItemBindings",
    "parse_item_binding",
    "WarehouseBinding",
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
    "generate_item_build_bundle",
    "InstallationEnvironment",
    "install_bundle",
    "InstallationReport",
    "ItemBuildResult",
    "build_item_repository",
    "materialise_tree",
    "persist_bundle_archive",
    "materialise_bundle_archive",
    "install_bundle_archive",
    "timestamped_archive_name",
]
