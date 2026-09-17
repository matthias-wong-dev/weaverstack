"""SQL errors that retain the endpoint and chain the driver exception."""

from __future__ import annotations

from ..errors import WeaverError


class SqlError(WeaverError):
    """Base class for SQL connection and execution failures."""

    executor = "TDS"


class SqlConnectionError(SqlError):
    """Raised when a physical SQL connection cannot be opened."""


class SqlExecutionError(SqlError):
    """Raised when a SQL statement, script, or query fails."""


class SqlPoolClosedError(SqlError):
    """Raised when a caller tries to lease from a closed pool."""
