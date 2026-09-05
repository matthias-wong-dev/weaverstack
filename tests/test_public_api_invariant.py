"""The small notebook-facing product namespace."""

from __future__ import annotations

from support.weaver_test import weaver_test

import weaver
from weaver.errors import CommandError, WeaverError


@weaver_test()
def test_version_is_exposed():
    assert weaver.__version__


@weaver_test()
def test_error_hierarchy_has_one_root():
    assert issubclass(CommandError, WeaverError)
    assert issubclass(WeaverError, Exception)


@weaver_test()
def test_the_top_level_is_the_ordinary_notebook_surface_only():
    assert set(weaver.__all__) == {
        "__version__",
        # The reusable context every operation accepts. A callable rather than
        # the session package, which is why that package is `weaver.sessions`.
        "session",
        # Setting a project up: the operation, and what it reports doing.
        "initialise",
        "InitialiseReport",
        "FabricItemOutcome",
        "ExampleOutcome",
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
        "test",
        "ValidationRunReport",
        # Health: the operation, and the structured report it returns.
        "health",
        "HealthReport",
        "HealthSection",
        "HealthFinding",
        "LoadActivity",
        "ValidationNodeReport",
        "WeaverObject",
        # The authored shortcut declaration, imported by an item's shortcuts.py.
        "Shortcut",
        "Folder",
        "Table",
        "SparkSqlTable",
        "View",
        "Test",
        "Assumption",
        "SparkSqlTest",
        "SparkSqlAssumption",
        "Lakehouse",
        "default_lakehouse",
        "lakehouse_for",
        "current_workspace",
        "WeaverError",
        "CommandError",
        "ConfigError",
        "IdentityError",
        "ValidationError",
    }


@weaver_test()
def test_internal_composition_seams_are_not_top_level_attributes():
    internal = {
        "Workspace",
        "Workspace",
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
        "Builder",
        "Installer",
        "wipe_folder_target",
        "wipe_delta_target",
        "wipe_lakehouse",
        "wipe_sql_target",
        "generate_item_build_bundle",
        "install_bundle_archive",
        # Load planning, resolution, dispatch and logging stay in their owning
        # modules: what the namespace exposes is the operation and what it
        # returns, never how it decided.
        "LoadDag",
        "LoadNode",
        "InstalledDag",
        "LoadEnvironment",
        "load_dag",
        "resolve_load_plan",
        "dispatch_load_node",
        "execute_load_plan",
        "open_task_log",
        "PhysicalTargetRef",
        "check",
        "CheckResult",
        "install",
        # Connectivity is a desktop question. A notebook is already inside the
        # workspace it addresses, so it has nothing to ask.
        "doctor",
        "DoctorReport",
    }
    assert all(not hasattr(weaver, name) for name in internal)
