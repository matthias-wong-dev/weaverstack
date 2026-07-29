"""Weaver — the core framework distributed as ``weaverstack``.

The public surface grows one checkpoint at a time. Today it carries the
version, the error hierarchy, and the workspace and target vocabulary.

The core must remain importable without PySpark, without Fabric credentials
and without the optional CLI. It must never import :mod:`weaver_cli`.
"""

from __future__ import annotations

from .config import load_workspace, parse_workspace, resolve_workspace
from .errors import CommandError, ConfigError, IdentityError, WeaverError
from .workspaces import (
    ExecutionSettings,
    FabricWorkspace,
    LocalWorkspace,
    TargetDeclaration,
    Workspace,
)
from .locations import Location
from .objects import Folder, ObjectContext, Table, View, WeaverObject
from .resolution import LocalResolver
from .setup import (
    PreparedWeaverLakehouse,
    SetupResult,
    initialise_weaver_lakehouse,
    prepare_weaver_lakehouse,
)
from .store import Entry, LocalStore, Store, StoreError
from .push import PushResult, push_item_repository
from .unbind import UnbindResult, plan_unbind, unbind_targets
from .sql import (
    PooledSqlExecutor,
    SqlConnectionPool,
    SqlEndpoint,
    SqlError,
    SqlExecutor,
    SqlExecutionError,
    generate_warehouse_wipe_sql,
)
from .wipe import (
    WipeReport,
    wipe,
    wipe_delta_target,
    wipe_folder_target,
    wipe_lakehouse,
    wipe_selection,
    wipe_sql_target,
)
from .targets import (
    DeltaTarget,
    FolderTarget,
    ItemRef,
    WarehouseTarget,
)
from .declaration.model import (
    ItemDependency,
    RepositoryAlias,
    WeaverDocumentId,
    WeaverItem,
    WeaverItemId,
    WeaverRepository,
    WeaverSchemaId,
)
from .declaration.metadata import WeaverDocument
from .declaration.repository import parse_item_repository
from .build_bundle import (
    InstallationEnvironment,
    InstallationReport,
    ItemBinding,
    ItemBindings,
    LakehouseBinding,
    WarehouseBinding,
    generate_item_build_bundle,
    build_item_repository,
    build_uploaded_item_repository,
    install_bundle,
    install_bundle_archive,
    load_bundle,
    persist_bundle_archive,
    timestamped_archive_name,
    parse_item_binding,
    effective_item_bindings,
)

def _resolve_version() -> str:
    """The installed version, read from distribution metadata.

    The wheel's version is git-derived at build time (see ``hatch_build.py``),
    so an installed Weaver — in a notebook or a Fabric Environment — reports the
    exact checkout it was built from. A raw source tree that has never been
    installed falls back to a marker rather than crashing the import.
    """

    try:
        from importlib.metadata import version

        return version("weaverstack")
    except Exception:  # pragma: no cover - never worth crashing an import over
        return "0.0.0+unknown"


__version__ = _resolve_version()

__all__ = [
    "__version__",
    # errors
    "WeaverError",
    "CommandError",
    "ConfigError",
    "IdentityError",
    # Workspace — level four
    "Workspace",
    "FabricWorkspace",
    "LocalWorkspace",
    "ExecutionSettings",
    "TargetDeclaration",
    "load_workspace",
    "parse_workspace",
    "resolve_workspace",
    # identities — level three
    "ItemRef",
    "WarehouseTarget",
    # logical identities, independent of physical bindings
    "WeaverRepository",
    "WeaverItem",
    "WeaverItemId",
    "WeaverSchemaId",
    "WeaverDocumentId",
    "RepositoryAlias",
    "ItemDependency",
    "WeaverDocument",
    "parse_item_repository",
    # item-oriented build
    "ItemBinding",
    "ItemBindings",
    "LakehouseBinding",
    "WarehouseBinding",
    "parse_item_binding",
    "effective_item_bindings",
    "generate_item_build_bundle",
    "build_item_repository",
    "build_uploaded_item_repository",
    "load_bundle",
    "persist_bundle_archive",
    "install_bundle_archive",
    "timestamped_archive_name",
    "InstallationEnvironment",
    "install_bundle",
    "InstallationReport",
    "PreparedWeaverLakehouse",
    "prepare_weaver_lakehouse",
    # resolved locations and transport
    "Location",
    "LocalResolver",
    # authoring
    "WeaverObject",
    "Folder",
    "Table",
    "View",
    "ObjectContext",
    "Store",
    "LocalStore",
    "Entry",
    "StoreError",
    "PushResult",
    "push_item_repository",
    "UnbindResult",
    "plan_unbind",
    "unbind_targets",
    # SQL
    "SqlEndpoint",
    "SqlExecutor",
    "PooledSqlExecutor",
    "SqlConnectionPool",
    "SqlError",
    "SqlExecutionError",
    "generate_warehouse_wipe_sql",
    # wipe
    "wipe_sql_target",
    "wipe_lakehouse",
    "WipeReport",
]
