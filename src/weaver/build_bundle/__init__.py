"""Weaver build — planning a declaration into a bundle, and installing it.

The boundary is deliberate::

    WeaverRepository -> planner -> BuildBundle -> installer -> workspaces

The planner owns every decision — item selection, ordering, executable
generation, certification. The installer owns execution only: it validates a
bundle and runs it, and never reads the source declaration, resolves a
dependency or selects a target.
"""

from __future__ import annotations

from .builder import Builder
from .bundle import (
    BuildBundle,
    compute_bundle_id,
    load_bundle,
    plan_from_yaml,
    plan_to_yaml,
    write_bundle,
)
from .incremental import BuildSelection, Impact, determine_impact
from .installer import Installer, execute_install_action
from .models import (
    BuildBatch,
    BuildPlan,
    BuildSequence,
    InstallAction,
    OmittedNode,
)
from .physical import RenderedAction, render_document_build_action
from .planner import PlannedItem, generate_item_build_bundle, plan_item_build
from .report import InstallationReport
from .targets import (
    BoundTarget,
    ItemBinding,
    ItemBindings,
    LakehouseBinding,
    WarehouseBinding,
    effective_item_bindings,
    parse_item_binding,
)
from .workflow import (
    BuildState,
    ItemBuildResult,
    build_item_repository,
    build_item_repository_source,
    catalogue_items_for_build,
    install_bundle_archive,
    materialise_bundle_archive,
    materialise_tree,
    persist_bundle_archive,
    prepare_repository,
    read_build_state,
    timestamped_archive_name,
    validate_build_request,
)

__all__ = [
    "BoundTarget",
    "LakehouseBinding",
    "ItemBinding",
    "ItemBindings",
    "parse_item_binding",
    "effective_item_bindings",
    "WarehouseBinding",
    "OmittedNode",
    "InstallAction",
    "BuildBatch",
    "BuildSequence",
    "BuildPlan",
    "BuildBundle",
    "Impact",
    "BuildSelection",
    "determine_impact",
    # The three narrow seams: one document rendered, one item planned, one
    # action executed. Each is the lowest layer that can answer its own
    # question, so a failure localises there rather than in a whole build.
    "RenderedAction",
    "render_document_build_action",
    "PlannedItem",
    "plan_item_build",
    "execute_install_action",
    "compute_bundle_id",
    "load_bundle",
    "write_bundle",
    "plan_to_yaml",
    "plan_from_yaml",
    "generate_item_build_bundle",
    "Builder",
    "Installer",
    "InstallationReport",
    "ItemBuildResult",
    "BuildState",
    "build_item_repository",
    "build_item_repository_source",
    "materialise_tree",
    "prepare_repository",
    "catalogue_items_for_build",
    "read_build_state",
    "validate_build_request",
    "persist_bundle_archive",
    "materialise_bundle_archive",
    "install_bundle_archive",
    "timestamped_archive_name",
]
