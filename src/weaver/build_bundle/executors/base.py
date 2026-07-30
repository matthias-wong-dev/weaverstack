"""The executor seam — dispatch only, no planning.

An executor runs one action's payload against one resolved target and returns
optional structured details, or raises. It never reads the repository, resolves
a dependency or selects a target: those decisions are all in the bundle already.
The installer owns timing, status and reporting; an executor owns the work.

The context carries runtime services — a Spark session, the resolver and store,
and the bundle's certified snapshot location — plus the one target the current
batch is bound to. It carries no planning input.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol

from ...errors import InstallError
from ...locations import LakehouseSparkLocation, Location
from ...spark import SparkCatalogue, SparkDestination
from ...store import Store
from ...targets import ItemRef
from ..models import BuildAction
from ..targets import BoundTarget


@dataclass(frozen=True)
class ResolvedTarget:
    """A manifest target resolved to what the executor addresses.

    ``lakehouse`` is the logical item the bundle named. The other two are that
    item resolved into the two things Spark needs to reach it, and they are two
    because Fabric answers them separately:

    ``location``
        the physical roots — where the bytes are. An ``abfss://`` URL on Fabric, a
        directory locally.
    ``destination``
        the catalogue name — what a statement calls it. Fabric's four-part
        ``workspace.lakehouse.schema.object``; locally the folded database name.

    Both are needed, and neither substitutes for the other: a folder is created at
    a path and has no catalogue name, while a view exists only as a name and has no
    path of its own.

    Resolution happens here, once per target, rather than in each executor. An
    executor that derived either for itself would be re-deciding where an action
    lands, which is a planning decision it is not allowed to make. It is also what
    lets one session build several destinations, and write the catalogue to a
    different one again, without ever switching what the session is attached to.

    Both are None for a Warehouse target, which is reached over TDS and has
    neither.
    """

    bound: BoundTarget
    lakehouse: ItemRef
    location: LakehouseSparkLocation | None = None
    destination: SparkDestination | None = None


@dataclass(frozen=True)
class InstallationContext:
    """Runtime services and the one target the current batch is bound to.

    ``spark`` runs Lakehouse work; ``sql`` runs Warehouse (T-SQL) work. A batch
    names one target, so only the capability its actions need has to be present.

    ``targets`` holds every target the plan declared, already resolved. It exists
    for the one action that legitimately spans two of them — an alias, which
    points a name in ``target`` at an object in another — and it carries resolved
    targets rather than ids so that a second destination is addressed exactly as
    the batch's own is, and never derived by an executor.
    """

    spark: Any
    resolver: Any
    store: Store
    snapshot: Location
    target: ResolvedTarget
    sql: Any = None
    snapshot_store: Store | None = None
    targets: Mapping[str, ResolvedTarget] = field(default_factory=dict)

    def resolved(self, target_id: str) -> ResolvedTarget:
        """Another target this plan declared, by the id an action names."""

        found = self.targets.get(target_id)
        if found is None:
            raise InstallError(
                f"action names target {target_id!r}, which this plan does not declare"
            )
        return found

    @property
    def catalogue(self) -> SparkCatalogue:
        """Catalogue operations against *this batch's* destination.

        Built per access rather than stored, so the context stays a frozen record
        of what was resolved. Failing here — rather than falling back to the
        session's own catalogue — is the point: an action with nowhere to go must
        stop, not land somewhere plausible (how-does-build-work §4).
        """

        if self.target.destination is None:
            raise InstallError(
                f"target {self.target.bound.id!r} resolved to no Spark destination, "
                "so a statement naming an object has nowhere to run"
            )
        return SparkCatalogue(self.spark, self.target.destination)


@dataclass(frozen=True)
class SkippedExecution:
    """An executor's explicit, non-failing decision not to run on this host."""

    details: dict[str, Any] | None = None


class ActionExecutor(Protocol):
    name: str

    def execute(
        self,
        action: BuildAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | SkippedExecution | None: ...
