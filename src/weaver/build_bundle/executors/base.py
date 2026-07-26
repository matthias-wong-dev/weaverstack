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

from dataclasses import dataclass
from typing import Any, Protocol

from ...locations import LakehouseSparkLocation, Location
from ...store import Store
from ...targets import ItemRef
from ..models import BuildAction
from ..targets import BoundTarget


@dataclass(frozen=True)
class ResolvedTarget:
    """A manifest target resolved to what the executor addresses.

    ``lakehouse`` is the logical item the bundle named. ``location`` is that item
    resolved to the physical roots Spark writes through, which is how a
    destination is reached: the session is attached to the *Weaver* Lakehouse — the
    fixed control plane — so a destination is never the current catalogue and must
    be addressed explicitly.

    Resolution happens here, once per target, rather than in each executor. An
    executor that derived its own paths would be re-deciding where an action
    lands, which is a planning decision it is not allowed to make. It is also what
    lets one session build several destinations without switching catalogues.

    ``location`` is None for a Warehouse target, which has no OneLake roots, and
    for a host that cannot resolve them.
    """

    bound: BoundTarget
    lakehouse: ItemRef
    location: LakehouseSparkLocation | None = None


@dataclass(frozen=True)
class InstallationContext:
    """Runtime services and the one target the current batch is bound to.

    ``spark`` runs Lakehouse work; ``sql`` runs Warehouse (T-SQL) work. A bundle
    is single-target, so only the one its actions need has to be present.
    """

    spark: Any
    resolver: Any
    store: Store
    snapshot: Location
    target: ResolvedTarget
    sql: Any = None


class ActionExecutor(Protocol):
    name: str

    def execute(
        self,
        action: BuildAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None: ...
