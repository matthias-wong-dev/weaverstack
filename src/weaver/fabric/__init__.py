"""The Fabric substrate: authentication, capacity, and workspace resources.

Everything here is optional. The core imports without it, and a local host
never reaches it. Install with the ``fabric`` extra.
"""

from __future__ import annotations

from .capacity import CapacityAction, CapacityError, capacity_command, run_capacity_action
from .client import FabricClient, FabricError
from .livy import LivyError, LivySession, StatementResult, emit_source
from .environment import (
    InstallResult,
    build_wheel,
    find_or_create_environment,
    install,
    missing_from_environment,
)
from .resolution import FabricResolver
from .session import FabricSessionResolver
from .store import FabricStore
from .onelake import (
    OneLakeDfsClient,
    abfss_root,
    lakehouse_artifact_segment,
    onelake_url,
    parse_onelake,
)
from .resources import (
    LAKEHOUSE,
    WAREHOUSE,
    Item,
    ItemNotFoundError,
    Workspace,
    create_lakehouse,
    create_warehouse,
    delete_item,
    find_item,
    find_workspace,
    list_items,
)
from .sql import (
    FABRIC_SQL_AUDIENCE,
    desktop_sql_executor,
    desktop_sql_pool,
    fabric_sql_executor,
    fabric_sql_pool,
)

__all__ = [
    "CapacityAction",
    "CapacityError",
    "capacity_command",
    "run_capacity_action",
    "FabricResolver",
    "FabricSessionResolver",
    "FabricStore",
    "install",
    "InstallResult",
    "build_wheel",
    "find_or_create_environment",
    "missing_from_environment",
    "LivySession",
    "LivyError",
    "StatementResult",
    "emit_source",
    "OneLakeDfsClient",
    "abfss_root",
    "onelake_url",
    "lakehouse_artifact_segment",
    "parse_onelake",
    "FabricClient",
    "FabricError",
    "Workspace",
    "Item",
    "ItemNotFoundError",
    "LAKEHOUSE",
    "WAREHOUSE",
    "find_workspace",
    "find_item",
    "list_items",
    "create_lakehouse",
    "create_warehouse",
    "delete_item",
    "desktop_sql_executor",
    "desktop_sql_pool",
    "fabric_sql_executor",
    "fabric_sql_pool",
    "FABRIC_SQL_AUDIENCE",
]
