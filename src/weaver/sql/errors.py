"""Weaver-facing SQL errors.

Driver exceptions do not escape this package.  The normalised errors retain the
selected endpoint and chain the original exception for diagnosis.
"""

from __future__ import annotations

from ..errors import WeaverError


class SqlError(WeaverError):
    """Base class for SQL connection and execution failures."""


class SqlConnectionError(SqlError):
    """Raised when a physical SQL connection cannot be opened."""


class SqlExecutionError(SqlError):
    """Raised when a SQL statement, script, or query fails."""


class SqlPoolClosedError(SqlError):
    """Raised when a caller tries to lease from a closed pool."""
