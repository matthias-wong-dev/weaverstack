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

from ...locations import Location
from ...store import Store
from ...targets import ItemRef
from ..models import BuildAction
from ..targets import BoundTarget


@dataclass(frozen=True)
class ResolvedTarget:
    """A manifest target resolved to what the executor addresses.

    Locally that is the destination Lakehouse ``ItemRef``; Fabric will add the
    concrete session handles alongside, through the same type.
    """

    bound: BoundTarget
    lakehouse: ItemRef


@dataclass(frozen=True)
class InstallationContext:
    """Runtime services and the one target the current batch is bound to."""

    spark: Any
    resolver: Any
    store: Store
    snapshot: Location
    target: ResolvedTarget


class ActionExecutor(Protocol):
    name: str

    def execute(
        self,
        action: BuildAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None: ...
