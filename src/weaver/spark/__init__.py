"""Named Spark destination addressing, token expansion, and catalogue operations.

The package remains importable without PySpark; callers supply a Spark session.
"""

from __future__ import annotations

from .catalogue import SparkCatalogue, drop_local_destination_catalogue
from .destination import (
    LOCAL_SEPARATOR,
    SparkDestination,
    fabric_destination,
    identifier,
    local_destination,
)
from .tokens import expand, object_token, schema_token
from .session import local_delta_session

__all__ = [
    "LOCAL_SEPARATOR",
    "SparkCatalogue",
    "SparkDestination",
    "drop_local_destination_catalogue",
    "expand",
    "fabric_destination",
    "identifier",
    "local_destination",
    "local_delta_session",
    "object_token",
    "schema_token",
]
