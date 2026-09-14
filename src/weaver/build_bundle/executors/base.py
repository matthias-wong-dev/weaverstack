"""Executor contracts for running frozen actions against resolved targets.

Executors perform no planning or repository reads. The installer owns timing,
status and reporting; the context supplies only runtime capabilities and resolved
targets.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol

from ...errors import InstallError
from ...locations import LakehouseSparkLocation
from ...spark import FabricSparkTarget
from ...store import Store
from ...targets import ItemRef
from ..models import InstallAction
from ..targets import BoundTarget


@dataclass(frozen=True)
class ResolvedTarget:
    """A manifest target resolved once for executor use.

    ``location`` addresses Lakehouse storage; ``destination`` is its four-part
    Spark catalogue name. Neither substitutes for the other. Both are ``None``
    for Warehouse targets, which use TDS.
    """

    bound: BoundTarget
    lakehouse: ItemRef
    location: LakehouseSparkLocation | None = None
    destination: FabricSparkTarget | None = None


@dataclass(frozen=True)
class InstallationContext:
    """Runtime services and resolved targets for the current batch.

    ``targets`` includes every plan target because a shortcut may span two.
    """

    resolver: Any
    store: Store
    target: ResolvedTarget
    sql: Any = None
    #: One Spark SQL statement with Weaver's identifier-case scope.
    spark_sql: Any = None
    #: Ordered Spark SQL statements in one submission and identifier-case scope.
    spark_sql_batch: Any = None
    targets: Mapping[str, ResolvedTarget] = field(default_factory=dict)
    #: One publication instant for every Registry row in this installation.
    build_datetime: str | None = None

    def resolved(self, target_id: str) -> ResolvedTarget:
        found = self.targets.get(target_id)
        if found is None:
            raise InstallError(
                f"action names target {target_id!r}, which this plan does not declare"
            )
        return found

    @property
    def destination(self) -> FabricSparkTarget:
        """Require an explicit Spark destination; never use the session default."""

        if self.target.destination is None:
            raise InstallError(
                f"target {self.target.bound.id!r} resolved to no Spark destination, "
                "so a statement naming an object has nowhere to run"
            )
        return self.target.destination


@dataclass(frozen=True)
class SkippedExecution:
    """An explicit, non-failing decision not to run on this host."""

    details: dict[str, Any] | None = None


class ActionExecutor(Protocol):
    name: str

    def execute(
        self,
        action: InstallAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | SkippedExecution | None: ...
