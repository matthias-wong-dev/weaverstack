"""The intentionally small notebook-facing product namespace."""

from __future__ import annotations

import weaver
from weaver.errors import CommandError, WeaverError


def test_version_is_exposed():
    assert weaver.__version__


def test_error_hierarchy_has_one_root():
    assert issubclass(CommandError, WeaverError)
    assert issubclass(WeaverError, Exception)


def test_the_top_level_is_the_ordinary_notebook_surface_only():
    assert set(weaver.__all__) == {
        "__version__",
        "build",
        "BuildResult",
        "wipe",
        "WipeReport",
        "WipeResult",
        "load",
        "LoadRunReport",
        "LoadNodeReport",
        "LoadMessage",
        "LoadResult",
        "WeaverObject",
        "Folder",
        "Table",
        "View",
        "Lakehouse",
        "default_lakehouse",
        "lakehouse_for",
        "WeaverError",
        "CommandError",
        "ConfigError",
        "IdentityError",
    }


def test_internal_composition_seams_are_not_top_level_attributes():
    internal = {
        "Workspace",
        "FabricWorkspace",
        "LocalWorkspace",
        "Location",
        "Store",
        "FilesystemStore",
        "ItemRef",
        "FolderTarget",
        "DeltaTarget",
        "WarehouseTarget",
        "parse_item_repository",
        "ItemBindings",
        "InstallationEnvironment",
        "wipe_folder_target",
        "wipe_delta_target",
        "wipe_lakehouse",
        "wipe_sql_target",
        "generate_item_build_bundle",
        "build_uploaded_item_repository",
        "install_bundle",
        "prepare_weaver_lakehouse",
        "initialise_weaver_lakehouse",
        # Load planning, resolution, dispatch and logging stay in their owning
        # modules: what the namespace exposes is the operation and what it
        # returns, never how it decided.
        "LoadDag",
        "LoadNode",
        "InstalledEstate",
        "LoadEnvironment",
        "load_dag",
        "resolve_load_plan",
        "dispatch_load_node",
        "execute_load_plan",
        "open_task_log",
        "PhysicalTargetRef",
    }
    assert all(not hasattr(weaver, name) for name in internal)
