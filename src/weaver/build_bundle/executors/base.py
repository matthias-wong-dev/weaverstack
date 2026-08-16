"""The executor seam — dispatch only, no planning.

An executor runs one action's payload against one resolved target and returns
optional structured details, or raises. It never reads the repository, resolves
a dependency or selects a target: those decisions are all in the bundle already.
The installer owns timing, status and reporting; an executor owns the work.

The context carries runtime capabilities — the resolver, the store, Warehouse
SQL, Spark SQL — plus the one target the current batch is bound to. It carries no
planning input and no way back to the repository: everything an action needs is
its payload.
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
    """A manifest target resolved to what the executor addresses.

    ``lakehouse`` is the logical item the bundle named. The other two are that
    item resolved into the two things Spark needs to reach it, and they are two
    because Fabric answers them separately:

    ``location``
        the physical roots — where the bytes are, as an ``abfss://`` URL.
    ``destination``
        the catalogue name — what a statement calls it, as Fabric's four-part
        ``workspace.lakehouse.schema.object``.

    Neither substitutes for the other: a folder is created at a path and has no
    catalogue name, while a view exists only as a name.

    Resolved once per target rather than in each executor, so no executor
    re-decides where an action lands — and so one session can build several
    destinations without switching what it is attached to.

    Both are None for a Warehouse target, which is reached over TDS.
    """

    bound: BoundTarget
    lakehouse: ItemRef
    location: LakehouseSparkLocation | None = None
    destination: FabricSparkTarget | None = None


@dataclass(frozen=True)
class InstallationContext:
    """Runtime services and the one target the current batch is bound to.

    ``spark_sql`` runs Lakehouse work; ``sql`` runs Warehouse (T-SQL) work. A
    batch names one target, so only the capability its actions need has to be
    present.

    ``targets`` holds every target the plan declared, already resolved. It exists
    for the one action that spans two of them — an alias, which points a name in
    ``target`` at an object in another — and it carries resolved targets rather
    than ids so a second destination is addressed exactly as the batch's own is.
    """

    resolver: Any
    store: Store
    target: ResolvedTarget
    sql: Any = None
    #: One Spark SQL statement, wherever this host's Spark is, carrying Weaver's
    #: identifier-case scope with it.
    spark_sql: Any = None
    #: Several Spark SQL statements as one piece of work — ordered, one
    #: submission where they cross, one identifier-case scope over all of them.
    spark_sql_batch: Any = None
    targets: Mapping[str, ResolvedTarget] = field(default_factory=dict)
    #: This installation's publication instant, resolved into ``{{build_datetime}}``. One
    #: value for the whole run, so every Registry row a build writes carries the
    #: same one and two rows can be ordered against each other.
    build_datetime: str | None = None

    def resolved(self, target_id: str) -> ResolvedTarget:
        """Another target this plan declared, by the id an action names."""

        found = self.targets.get(target_id)
        if found is None:
            raise InstallError(
                f"action names target {target_id!r}, which this plan does not declare"
            )
        return found

    @property
    def destination(self) -> FabricSparkTarget:
        """How Fabric Spark addresses this batch's target.

        Failing rather than falling back to the session's own catalogue is what
        stops an action with nowhere to go from landing somewhere plausible
        (how-does-build-work §4).
        """

        if self.target.destination is None:
            raise InstallError(
                f"target {self.target.bound.id!r} resolved to no Spark destination, "
                "so a statement naming an object has nowhere to run"
            )
        return self.target.destination


@dataclass(frozen=True)
class SkippedExecution:
    """An executor's explicit, non-failing decision not to run on this host."""

    details: dict[str, Any] | None = None


class ActionExecutor(Protocol):
    name: str

    def execute(
        self,
        action: InstallAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | SkippedExecution | None: ...
