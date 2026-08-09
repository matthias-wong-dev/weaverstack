"""Locating what each planned node would dispatch, without dispatching it.

The seam between "what should run" and "run it", and it exists as its own layer
because the two questions fail for entirely different reasons. A planning fault
is a wrong graph; a resolution fault is a graph that is right about an estate
that is not there — a procedure that was never installed, a module deleted from
the runtime tree, a Warehouse that has been wiped. Keeping them apart means a
failure names which of the two it is.

The question this layer answers is exactly:

    Can the orchestrator locate what it would dispatch?

and deliberately not *does the primitive work*. Each primitive is independently
runnable and independently tested; re-proving that here would make orchestration
the owner of behaviour it does not implement.

**Physical state arrives as an inventory, not as a live connection.** The same
:class:`~weaver.build_bundle.prune.TargetInventory` a build reads before planning
answers every existence question here — is the target there, is the procedure
there, is the deployed file there, does the table it loads into exist. So this
module is pure: a graph and some observed state in, a resolved plan out. The
reading happens once, above, where the session already is.

Dry run is this layer plus nothing. §11's list — read the catalogue, reverse the
bindings, build the graph, check it is acyclic, order it, resolve every node,
verify targets, procedures, files, modules and refresh capability — is the whole
orchestration path with dispatch removed, which is why a dry run can be complete
and still touch nothing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .build_bundle.prune import TargetInventory
from .etl import load_procedure_name
from .load_plan import (
    ENDPOINT_REFRESH,
    PYTHON_FOLDER,
    PYTHON_TABLE,
    WAREHOUSE_PROCEDURE,
    LoadDag,
    LoadNode,
    PhysicalTargetRef,
)
from .load_report import (
    BLOCKED,
    DEPENDENCY_BLOCKED,
    DISPATCH_LOCATION_MISSING,
    INVALID,
    MODULE_IMPORT_FAILURE,
    TARGET_MISSING,
    VALIDATED,
    LoadMessage,
    LoadNodeReport,
    error,
    warning,
)
from .targets import ItemRef

def _new_runtime_scope():
    """A fresh runtime scope. Imported lazily so this module stays cheap."""

    from .runtime.python_context import RuntimeScope

    return RuntimeScope.new()


#: What a refresh resolves to when the host can perform one. Not a physical
#: object — a Lakehouse's SQL analytics endpoint is a capability of the item, so
#: the location names the item and the capability rather than a path.
ENDPOINT_SUFFIX = "sql_endpoint"

#: Why a refresh could not be resolved on this host. The emulator has no SQL
#: analytics endpoint at all, which is an honest absence rather than a fault —
#: the build's own executor skips for the same reason.
REFRESH_UNSUPPORTED = "SQL endpoint refresh is unsupported in this environment"


@dataclass(frozen=True)
class LoadEnvironment:
    """Runtime services and the observed physical state of one load run.

    ``inventories`` is keyed by the physical target's public spelling —
    ``Lakehouse/Raw_LH`` — because that is what a caller wrote and what a report
    prints, so a key that could not be read back would put a second vocabulary
    between the request and the answer. A target with no entry is a target that
    is not there.

    ``sql`` is per Warehouse rather than one connection, because a run may span
    two of them and a Warehouse is reached over its own TDS endpoint.
    """

    resolver: Any = None
    inventories: Mapping[str, TargetInventory] = field(default_factory=dict)
    store: Any = None
    spark: Any = None
    sql: Mapping[str, Any] = field(default_factory=dict)
    workspace: Any = None
    #: Where this run's deployed Python modules live, and how long they live.
    #: One scope per environment, and an environment is built once per run — so
    #: a rebuilt module is executed by the next load rather than shadowed by the
    #: one the session already imported. See
    #: :class:`weaver.runtime.python_context.RuntimeScope`.
    runtime_scope: Any = field(default_factory=lambda: _new_runtime_scope())

    def inventory(self, target: PhysicalTargetRef) -> TargetInventory | None:
        return self.inventories.get(str(target))

    def sql_for(self, target: PhysicalTargetRef) -> Any:
        return self.sql.get(target.name)

    def can_refresh(self) -> bool:
        return callable(getattr(self.resolver, "refresh_sql_endpoint", None))


def installed_file_location(node: LoadNode, environment: LoadEnvironment) -> str | None:
    """The deployed artefact's location, as this environment addresses it.

    Resolved rather than composed, so the desktop, the emulator and a Fabric
    session each get their own spelling of the same file from the one place that
    knows how a name becomes a location.
    """

    resolver = environment.resolver
    files_root = getattr(resolver, "files_root", None)
    if files_root is None or node.primitive_object is None:
        return None
    root = files_root(ItemRef(node.physical_target.name))
    relative = f"{node.primitive_object.schema}/{node.primitive_object.object}"
    return root.join(*relative.split("/")).value


def _module_class(filename: str) -> str | None:
    """``Sales__Order.py`` names class ``Sales__Order``.

    The same rule the authoring surface applies to a class name and the
    repository parser applies to a filename, so a deployed module's class is
    found by the rule that put it there rather than by importing and looking.
    """

    if not filename.endswith(".py"):
        return None
    stem = filename[: -len(".py")]
    return stem or None


def _holds(inventory: TargetInventory | None, where) -> bool:
    if inventory is None or where is None:
        return False
    return inventory.has_object(where.schema, where.object, where.object_type)


__all__ = ["ENDPOINT_SUFFIX", "LoadEnvironment", "REFRESH_UNSUPPORTED",
           "installed_file_location"]
