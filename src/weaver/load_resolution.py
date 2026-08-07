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


@dataclass(frozen=True)
class ResolvedLoadNode:
    """One node, and what the orchestrator would actually reach for.

    ``target_exists`` and ``primitive_exists`` are separate answers because they
    are separate failures: a Warehouse that has been wiped and a procedure that
    was never generated both stop this node, and telling a reader which one it
    was is the whole value of resolving ahead of dispatching.
    """

    node: LoadNode
    dispatch_location: str | None = None
    target_exists: bool = False
    primitive_exists: bool = False
    #: The class a deployed Python module must define, for the two Python kinds.
    expected_class: str | None = None
    validation_messages: tuple[LoadMessage, ...] = ()
    #: A capability this host does not have, so the node is omitted rather than
    #: failed. Only an endpoint refresh in an environment with no endpoint.
    unsupported: bool = False

    @property
    def node_id(self) -> str:
        return self.node.node_id

    @property
    def valid(self) -> bool:
        return not any(
            message.severity == "error" for message in self.validation_messages
        )


@dataclass(frozen=True)
class ResolvedLoadPlan:
    """A whole graph resolved against one environment."""

    dag: LoadDag
    nodes: tuple[ResolvedLoadNode, ...]

    @property
    def by_id(self) -> Mapping[str, ResolvedLoadNode]:
        return {node.node_id: node for node in self.nodes}

    @property
    def order(self) -> tuple[ResolvedLoadNode, ...]:
        resolved = self.by_id
        return tuple(resolved[node.node_id] for node in self.dag.order())

    @property
    def blocked(self) -> Mapping[str, frozenset[str]]:
        """Which invalid node blocks each node that may not run because of it."""

        blocked: dict[str, set[str]] = {}
        for resolved in self.nodes:
            if resolved.valid:
                continue
            for downstream in self.dag.descendants(resolved.node_id):
                blocked.setdefault(downstream, set()).add(resolved.node_id)
        return {node: frozenset(causes) for node, causes in blocked.items()}


def resolve_load_plan(
    dag: LoadDag, *, environment: LoadEnvironment
) -> ResolvedLoadPlan:
    """Resolve every node in ``dag`` to the installed primitive it would run."""

    return ResolvedLoadPlan(
        dag=dag,
        nodes=tuple(_resolve_node(node, environment) for node in dag.order()),
    )


def _resolve_node(node: LoadNode, environment: LoadEnvironment) -> ResolvedLoadNode:
    inventory = environment.inventory(node.physical_target)
    if node.primitive_kind == ENDPOINT_REFRESH:
        return _resolve_refresh(node, environment, inventory)
    messages: list[LoadMessage] = []
    if inventory is None:
        messages.append(
            error(
                TARGET_MISSING,
                f"{node.physical_target} is not present, so {node.node_id} has "
                "nowhere to run",
                source="load_resolution",
            )
        )
    location, expected_class = _dispatch_location(node, environment)
    if location is None:
        messages.append(
            error(
                DISPATCH_LOCATION_MISSING,
                f"{node.node_id} names primitive kind {node.primitive_kind!r}, "
                "which this environment cannot address",
                source="load_resolution",
            )
        )
    primitive_exists = _holds(inventory, node.primitive_object)
    if inventory is not None and not primitive_exists:
        messages.append(
            error(
                DISPATCH_LOCATION_MISSING,
                f"{node.node_id} would dispatch {location}, which is not installed "
                f"in {node.physical_target}",
                source="load_resolution",
            )
        )
    if inventory is not None and not _holds(inventory, node.physical_object):
        messages.append(
            error(
                TARGET_MISSING,
                f"{node.physical_target} does not hold {node.physical_object}, "
                "which this node loads into",
                source="load_resolution",
            )
        )
    if expected_class is None and node.primitive_kind in (PYTHON_TABLE, PYTHON_FOLDER):
        messages.append(
            error(
                MODULE_IMPORT_FAILURE,
                f"{node.node_id} names a deployed module whose expected class "
                "cannot be derived from its filename",
                source="load_resolution",
            )
        )
    return ResolvedLoadNode(
        node=node,
        dispatch_location=location,
        target_exists=inventory is not None,
        primitive_exists=primitive_exists,
        expected_class=expected_class,
        validation_messages=tuple(messages),
    )


def _resolve_refresh(
    node: LoadNode, environment: LoadEnvironment, inventory
) -> ResolvedLoadNode:
    """A barrier resolves to a capability, and its absence is not a failure."""

    messages: list[LoadMessage] = []
    if inventory is None:
        messages.append(
            error(
                TARGET_MISSING,
                f"{node.physical_target} is not present, so its SQL endpoint "
                "cannot be refreshed",
                source="load_resolution",
            )
        )
    supported = environment.can_refresh()
    if not supported:
        messages.append(
            warning(
                DISPATCH_LOCATION_MISSING,
                f"{REFRESH_UNSUPPORTED}; {node.node_id} will be skipped",
                source="load_resolution",
            )
        )
    return ResolvedLoadNode(
        node=node,
        dispatch_location=f"{node.physical_target}/{ENDPOINT_SUFFIX}",
        target_exists=inventory is not None,
        primitive_exists=supported,
        validation_messages=tuple(messages),
        unsupported=not supported,
    )


def _dispatch_location(
    node: LoadNode, environment: LoadEnvironment
) -> tuple[str | None, str | None]:
    """Where the primitive is, and the class a deployed module must define."""

    if node.primitive_kind == WAREHOUSE_PROCEDURE:
        procedure = load_procedure_name(node.logical_id.object_id)
        return f"{node.physical_target}/{procedure}", None
    if node.primitive_kind in (PYTHON_TABLE, PYTHON_FOLDER):
        return (
            installed_file_location(node, environment),
            _module_class(node.primitive_object.object),
        )
    return None, None


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


# --- the dry-run report -------------------------------------------------------


def dry_run_reports(plan: ResolvedLoadPlan) -> tuple[LoadNodeReport, ...]:
    """A resolved plan as node reports, with nothing executed.

    A validation status is never an execution status: a node that resolved is
    ``validated``, not ``succeeded``, and ``executed`` is false on every one of
    them. Reporting a dry run as a successful load would put a claim in a report
    that nothing performed — and the report shape is otherwise identical to a
    real run's, which is precisely what makes a dry run worth inspecting.
    """

    blocked = plan.blocked
    reports = []
    for resolved in plan.order:
        node = resolved.node
        causes = blocked.get(node.node_id, frozenset())
        messages = list(resolved.validation_messages)
        if resolved.valid and causes:
            status = BLOCKED
            messages.append(
                error(
                    DEPENDENCY_BLOCKED,
                    f"{node.node_id} cannot run: "
                    + ", ".join(sorted(causes))
                    + " did not validate",
                    source="load_resolution",
                )
            )
        else:
            status = VALIDATED if resolved.valid else INVALID
        reports.append(
            LoadNodeReport(
                node_id=node.node_id,
                logical_id=str(node.logical_id) if node.logical_id else None,
                physical_target=str(node.physical_target),
                primitive_kind=node.primitive_kind,
                dispatch_location=resolved.dispatch_location,
                status=status,
                executed=False,
                messages=tuple(messages),
            )
        )
    return tuple(reports)


__all__ = [
    "ENDPOINT_SUFFIX",
    "LoadEnvironment",
    "REFRESH_UNSUPPORTED",
    "ResolvedLoadNode",
    "ResolvedLoadPlan",
    "dry_run_reports",
    "installed_file_location",
    "resolve_load_plan",
]
