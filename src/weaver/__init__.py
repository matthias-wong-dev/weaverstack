"""Weaver — the core framework distributed as ``weaverstack``.

The public surface grows one checkpoint at a time. Today it carries the
version, the error hierarchy, and the host and target vocabulary.

The core must remain importable without PySpark, without Fabric credentials
and without the optional CLI. It must never import :mod:`weaver_cli`.
"""

from __future__ import annotations

from .config import load_hosts, parse_hosts
from .errors import CommandError, ConfigError, IdentityError, WeaverError
from .hosts import FabricHost, Host, LocalHost, WarehouseSettings
from .locations import Location
from .objects import Folder, ObjectContext, Table, View, WeaverObject
from .resolution import LocalResolver
from .store import Entry, LocalStore, Store, StoreError
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
    RepositoryRef,
    WarehouseTarget,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # errors
    "WeaverError",
    "CommandError",
    "ConfigError",
    "IdentityError",
    # hosts — level four
    "Host",
    "FabricHost",
    "LocalHost",
    "WarehouseSettings",
    "load_hosts",
    "parse_hosts",
    # identities — level three
    "ItemRef",
    "FolderTarget",
    "DeltaTarget",
    "WarehouseTarget",
    "RepositoryRef",
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
    # wipe
    "wipe",
    "wipe_folder_target",
    "wipe_delta_target",
    "wipe_sql_target",
    "wipe_lakehouse",
    "wipe_selection",
    "WipeReport",
]
