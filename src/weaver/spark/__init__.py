"""Addressing and operating a *named* Spark destination.

Spark execution can no longer assume one destination. Even the simplest install
involves two — the Weaver Lakehouse holding the catalogue, and the Lakehouse
being built — reached from one session, so "where" has to be said rather than
inherited from whatever the session is attached to.

Three pieces, in the order they are used:

:mod:`~weaver.spark.destination`
    what a Lakehouse is *called* here — Fabric's four-part name, or the local
    proxy's folded database name.
:mod:`~weaver.spark.tokens`
    how a frozen payload names an object without naming a destination, so a
    bundle stays comparable between environments.
:mod:`~weaver.spark.catalogue`
    the operations — create, execute, discover, exists — each against one named
    destination.

Nothing here imports PySpark. A session is passed in and used through ``sql``
and ``catalog``, so the core stays importable without a JVM.
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
