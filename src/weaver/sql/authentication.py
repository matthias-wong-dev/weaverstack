"""Authentication records consumed by the common SQL connector.

Identity policy stays at the caller boundary.  This module only converts a
fresh access token into the connection argument understood by
``mssql-python``.
"""

from __future__ import annotations

import struct
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Protocol

# SQL_COPT_SS_ACCESS_TOKEN from msodbcsql.h.  mssql-python accepts the same
# attrs_before shape as pyodbc and exposes this value as
# ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.
SQL_ACCESS_TOKEN_ATTRIBUTE = 1256


class SqlAuthentication(Protocol):
    """Authentication material for one new physical connection."""

    def connection_arguments(self) -> Mapping[str, object]:
        """Return current driver keyword arguments."""


@dataclass(frozen=True)
class AccessTokenAuthentication:
    """Acquire and encode a fresh Entra access token per connection."""

    token_provider: Callable[[], str]

    def connection_arguments(self) -> Mapping[str, object]:
        token = self.token_provider()
        token_bytes = token.encode("utf-16-le")
        packed = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        return {"attrs_before": {SQL_ACCESS_TOKEN_ATTRIBUTE: packed}}
