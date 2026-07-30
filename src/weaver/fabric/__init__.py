"""The Fabric substrate: authentication, capacity, and workspace resources.

Everything here is optional. The core imports without it, and a local workspace
never reaches it. Install with the ``fabric`` extra.
"""

from __future__ import annotations

from .capacity import CapacityAction, CapacityError, capacity_command, run_capacity_action
from .client import FabricClient, FabricError
from .livy import (
    LivyError,
    LivySession,
    LivySessionInfo,
    StatementResult,
    WorkspaceLivySession,
    emit_source,
    list_livy_sessions,
    list_workspace_livy_sessions,
)
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
    SQL_ENDPOINT,
    Item,
    ItemNotFoundError,
    Workspace,
    create_lakehouse,
    create_warehouse,
    delete_item,
    find_item,
    find_workspace,
    list_items,
    refresh_sql_endpoint_metadata,
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
    "LivySessionInfo",
    "WorkspaceLivySession",
    "LivyError",
    "StatementResult",
    "emit_source",
    "list_livy_sessions",
    "list_workspace_livy_sessions",
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
    "SQL_ENDPOINT",
    "find_workspace",
    "find_item",
    "list_items",
    "refresh_sql_endpoint_metadata",
    "create_lakehouse",
    "create_warehouse",
    "delete_item",
    "desktop_sql_executor",
    "desktop_sql_pool",
    "fabric_sql_executor",
    "fabric_sql_pool",
    "FABRIC_SQL_AUDIENCE",
]
