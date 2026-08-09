"""Can the Runner locate what it would dispatch — answered without asking Fabric.

The seam between "what should run" and "run it", kept apart because the two fail
for entirely different reasons. A planning fault is a wrong graph. A resolution
fault is a graph that is right about an estate that is not there: a procedure
that was never installed, a module deleted from the runtime tree, a Warehouse
that has been wiped. A run that could not tell them apart would send its reader
to check the wrong thing.

**Every answer here comes from the observed snapshot.** The reading happened once,
at a boundary, above the Runner — so this module is pure: a node and a RunState
in, a verdict out. That is what lets a dry run be complete and still touch
nothing, and what lets the whole of resolution be tested with no estate at all.

One thing deliberately does *not* live here: where the primitive physically is.
A path is a question for the resolver that owns the workspace, and it is needed
only at the moment of dispatch — so computing one during resolution would drag a
physical dependency into the one place that is supposed to be free of them.
"""

from __future__ import annotations

from dataclasses import dataclass

#: Primitive kinds this module knows how to reason about. A kind not named here
#: resolves as unaddressable rather than being assumed to work.
WAREHOUSE_PROCEDURE = "warehouse_procedure"
PYTHON_TABLE = "python_table"
PYTHON_FOLDER = "python_folder"
ENDPOINT_REFRESH = "endpoint_refresh"

PYTHON_KINDS = (PYTHON_TABLE, PYTHON_FOLDER)


@dataclass(frozen=True)
class Resolved:
    """One node, and whether what it needs is actually there.

    ``target_present`` and ``primitive_present`` are separate answers because
    they are separate failures: a Warehouse that has been wiped and a procedure
    that was never generated both stop this node, and telling a reader which one
    it was is the whole value of resolving ahead of dispatching.
    """

    node: object
    target_present: bool = False
    primitive_present: bool = False
    #: The class a deployed Python module must define, for the two Python kinds.
    expected_class: str | None = None
    messages: tuple[str, ...] = ()
    #: A capability this host does not have, so the node is omitted rather than
    #: failed. Only an endpoint refresh where there is no endpoint.
    unsupported: bool = False

    @property
    def valid(self) -> bool:
        return not self.messages


def resolve(node, state, *, can_refresh: bool = True) -> Resolved:
    """Whether this node's target and primitive are present in the snapshot.

    ``can_refresh`` is the one host capability resolution needs, and it is
    passed in rather than discovered: the emulator has no SQL analytics endpoint
    at all, which is an honest absence rather than a fault, and the caller is
    the only thing that knows which host it is on.
    """

    inventory = state.inventory(node.physical_target)
    if node.primitive_kind == ENDPOINT_REFRESH:
        return _refresh(node, inventory, can_refresh=can_refresh)

    messages: list[str] = []
    if inventory is None:
        messages.append(
            f"{node.physical_target} is not present, so {node.node_id} has "
            "nowhere to run"
        )

    expected_class = None
    if node.primitive_kind in PYTHON_KINDS:
        expected_class = _module_class(node)
        if expected_class is None:
            messages.append(
                f"{node.node_id} names a deployed module whose expected class "
                "cannot be derived from its filename"
            )
    elif node.primitive_kind not in (WAREHOUSE_PROCEDURE,):
        messages.append(
            f"{node.node_id} names primitive kind {node.primitive_kind!r}, "
            "which no runtime can address"
        )

    # A node that names a primitive must have it installed; a node that names
    # none has nothing here to check. The distinction matters because the kinds
    # differ: a load node always names the artefact it would run, and a node
    # whose primitive is addressed by identity rather than by a physical object
    # is not thereby unresolvable.
    primitive_present = (
        _holds(inventory, node.primitive_object)
        if node.primitive_object is not None
        else inventory is not None
    )
    if (
        inventory is not None
        and node.primitive_object is not None
        and not primitive_present
    ):
        messages.append(
            f"{node.node_id} would dispatch {node.primitive_object}, which is "
            f"not installed in {node.physical_target}"
        )
    if (
        inventory is not None
        and node.physical_object is not None
        and not _holds(inventory, node.physical_object)
    ):
        messages.append(
            f"{node.physical_target} does not hold {node.physical_object}, "
            "which this node loads into"
        )

    return Resolved(
        node=node,
        target_present=inventory is not None,
        primitive_present=primitive_present,
        expected_class=expected_class,
        messages=tuple(messages),
    )


def _refresh(node, inventory, *, can_refresh: bool) -> Resolved:
    """A barrier resolves to a capability, and its absence is not a failure."""

    messages: list[str] = []
    if inventory is None:
        messages.append(
            f"{node.physical_target} is not present, so its SQL endpoint "
            "cannot be refreshed"
        )
    return Resolved(
        node=node,
        target_present=inventory is not None,
        primitive_present=can_refresh,
        messages=tuple(messages),
        unsupported=not can_refresh,
    )


def _holds(inventory, reference) -> bool:
    """Whether the observed target holds this object.

    A node with no reference to check is not thereby satisfied: the reference is
    how a node says what it needs, and its absence is a node that cannot say.
    """

    if inventory is None or reference is None:
        return False
    return inventory.has_object(
        reference.schema, reference.object, reference.object_type
    )


def _module_class(node) -> str | None:
    """``Sales__Order.py`` names class ``Sales__Order``.

    The same rule the authoring surface applies to a class name and the
    repository parser applies to a filename, so a deployed module's class is
    found by the rule that put it there rather than by importing and looking.
    """

    reference = node.primitive_object
    filename = getattr(reference, "object", None)
    if not filename or not filename.endswith(".py"):
        return None
    return filename[: -len(".py")] or None


__all__ = [
    "ENDPOINT_REFRESH",
    "PYTHON_FOLDER",
    "PYTHON_KINDS",
    "PYTHON_TABLE",
    "WAREHOUSE_PROCEDURE",
    "Resolved",
    "resolve",
]
