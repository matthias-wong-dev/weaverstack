"""Resolved SQL endpoints and common ``mssql-python`` connection creation."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable

from .authentication import SqlAuthentication
from .errors import SqlConnectionError

DEFAULT_SQL_PORT = 1433
DEFAULT_CONNECT_TIMEOUT = 60

#: How many times opening a connection is attempted, and how long to wait between
#: attempts. A Fabric SQL endpoint refuses a connection while it is scaling, and
#: nothing has been sent yet when it does, so opening again is safe.
CONNECT_ATTEMPTS = 3
CONNECT_BACKOFF = 3.0


@dataclass(frozen=True)
class SqlEndpoint:
    """One resolved Warehouse SQL endpoint.

    The optional Fabric identities make the pool key stable without treating a
    display name or a credential-bearing connection string as identity.
    """

    server: str
    database: str
    port: int = DEFAULT_SQL_PORT
    workspace_id: str | None = None
    warehouse_id: str | None = None
    warehouse_name: str | None = None

    def __post_init__(self) -> None:
        if not self.server.strip():
            raise ValueError("SQL server must not be empty")
        if not self.database.strip():
            raise ValueError("SQL database must not be empty")

    @property
    def pool_key(self) -> tuple[str, str, str, str, int]:
        return (
            self.workspace_id or "",
            self.warehouse_id or "",
            self.server.lower(),
            self.database.lower(),
            self.port,
        )

    def __str__(self) -> str:
        return f"{self.server}:{self.port}/{self.database}"


def build_connection_string(endpoint: SqlEndpoint) -> str:
    """The validated Fabric Warehouse connection-string shape."""

    return (
        f"Server={endpoint.server},{endpoint.port};"
        f"Database={endpoint.database};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )


DriverConnector = Callable[..., Any]


def connect(
    endpoint: SqlEndpoint,
    authentication: SqlAuthentication,
    *,
    timeout: int = DEFAULT_CONNECT_TIMEOUT,
    connector: DriverConnector | None = None,
):
    """Open one physical connection using current authentication material."""

    if connector is None:
        try:
            from mssql_python import connect as connector
        except ImportError as exc:  # pragma: no cover - installation dependent
            raise SqlConnectionError(
                "mssql-python is required for Warehouse SQL execution"
            ) from exc

    for attempt in range(1, CONNECT_ATTEMPTS + 1):
        try:
            return connector(
                build_connection_string(endpoint),
                timeout=timeout,
                autocommit=True,
                **dict(authentication.connection_arguments()),
            )
        except SqlConnectionError:
            raise
        except Exception as exc:
            if attempt == CONNECT_ATTEMPTS:
                raise SqlConnectionError(
                    f"failed to connect to {endpoint}: {exc}"
                ) from exc
            time.sleep(CONNECT_BACKOFF * attempt)
