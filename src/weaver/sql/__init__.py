"""Driver-neutral Warehouse SQL execution backed by ``mssql-python``."""

from __future__ import annotations

from .authentication import AccessTokenAuthentication, SqlAuthentication
from .connection import SqlEndpoint, build_connection_string, connect
from .errors import (
    SqlConnectionError,
    SqlError,
    SqlExecutionError,
    SqlPoolClosedError,
)
from .execution import PooledSqlExecutor, SqlExecutor, SqlRow
from .pool import (
    DEFAULT_MAX_CONNECTIONS,
    SqlConnectionLease,
    SqlConnectionPool,
    SqlPoolRegistry,
)
from .wipe import generate_warehouse_wipe_sql

__all__ = [
    "AccessTokenAuthentication",
    "SqlAuthentication",
    "SqlEndpoint",
    "build_connection_string",
    "connect",
    "SqlError",
    "SqlConnectionError",
    "SqlExecutionError",
    "SqlPoolClosedError",
    "SqlExecutor",
    "SqlRow",
    "PooledSqlExecutor",
    "SqlConnectionLease",
    "SqlConnectionPool",
    "SqlPoolRegistry",
    "DEFAULT_MAX_CONNECTIONS",
    "generate_warehouse_wipe_sql",
]
