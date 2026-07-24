"""Explicit desktop and Fabric-session SQL capability factories."""

from __future__ import annotations

from typing import Any

from ..errors import CommandError
from ..sql import (
    DEFAULT_MAX_CONNECTIONS,
    AccessTokenAuthentication,
    PooledSqlExecutor,
    SqlConnectionPool,
)
from .auth import SQL_SCOPE, get_token

# NotebookUtils accepts the SQL resource audience, not an OAuth ``.default``
# scope.  Keeping the two constants separate prevents a caller-boundary mix-up.
FABRIC_SQL_AUDIENCE = "https://database.windows.net/"


def desktop_sql_pool(
    target,
    host,
    *,
    credential=None,
    resolver=None,
    max_connections: int = DEFAULT_MAX_CONNECTIONS,
    connection_factory=None,
) -> SqlConnectionPool:
    """Cross from a desktop caller into a resolved Fabric Warehouse."""

    from ..resolution import resolver_for
    from ..sql.connection import connect

    resolver = resolver or resolver_for(host)
    endpoint = resolver.sql_endpoint(target)
    authentication = AccessTokenAuthentication(
        lambda: get_token(SQL_SCOPE, credential)
    )
    return SqlConnectionPool(
        endpoint,
        authentication,
        max_connections=max_connections,
        connection_factory=connection_factory or connect,
    )


def desktop_sql_executor(target, host, **kwargs) -> PooledSqlExecutor:
    """An explicitly cross-boundary desktop executor."""

    return PooledSqlExecutor(
        desktop_sql_pool(target, host, **kwargs),
        owns_pool=True,
    )


def fabric_sql_pool(
    target,
    host,
    *,
    resolver=None,
    runtime: Any | None = None,
    lakehouse: Any | None = None,
    credentials: Any | None = None,
    max_connections: int = DEFAULT_MAX_CONNECTIONS,
    connection_factory=None,
) -> SqlConnectionPool:
    """SQL access using the identity made available by a Fabric session."""

    from ..sql.connection import connect
    from .session import FabricSessionResolver

    if resolver is None:
        try:
            resolver = FabricSessionResolver(
                host,
                runtime=runtime,
                lakehouse=lakehouse,
                credentials=credentials,
            )
        except CommandError as exc:
            raise CommandError(
                "Fabric-native SQL is available only inside a supported Fabric "
                "notebook or Livy session"
            ) from exc
    if not isinstance(resolver, FabricSessionResolver):
        raise CommandError(
            "Fabric-native SQL needs a FabricSessionResolver; desktop callers "
            "must use desktop_sql_executor explicitly"
        )

    endpoint = resolver.sql_endpoint(target)
    notebook_credentials = credentials or getattr(resolver, "_credentials", None)
    if notebook_credentials is None:
        try:
            from notebookutils import credentials as notebook_credentials
        except ImportError as exc:
            raise CommandError(
                "Fabric-native SQL cannot acquire the Fabric session identity"
            ) from exc
    authentication = AccessTokenAuthentication(
        lambda: notebook_credentials.getToken(FABRIC_SQL_AUDIENCE)
    )
    return SqlConnectionPool(
        endpoint,
        authentication,
        max_connections=max_connections,
        connection_factory=connection_factory or connect,
    )


def fabric_sql_executor(target, host, **kwargs) -> PooledSqlExecutor:
    """An explicitly within-Fabric executor."""

    return PooledSqlExecutor(
        fabric_sql_pool(target, host, **kwargs),
        owns_pool=True,
    )
