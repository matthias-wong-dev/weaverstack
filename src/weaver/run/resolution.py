"""Resolve runnable nodes against an observed RunState.

Resolution uses the supplied snapshot and does not access Fabric or resolve
physical dispatch paths.
"""

from __future__ import annotations

from dataclasses import dataclass

from .result import (
    DISPATCH_LOCATION_MISSING,
    MODULE_IMPORT_FAILURE,
    TARGET_MISSING,
    error,
    warning,
)

#: Who noticed, for a reader following a node's messages across layers.
SOURCE = "run.resolution"

#: Primitive kinds this module knows how to reason about. A kind not named here
#: resolves as unaddressable rather than being assumed to work.
WAREHOUSE_PROCEDURE = "warehouse_procedure"
PYTHON_TABLE = "python_table"
PYTHON_FOLDER = "python_folder"
ENDPOINT_REFRESH = "endpoint_refresh"
#: How a validation is reached, from where it is installed.
PYTHON_VALIDATION = "python_validation"

PYTHON_KINDS = (PYTHON_TABLE, PYTHON_FOLDER)

#: What a refresh resolves to. Not a physical object — a Lakehouse's SQL
#: analytics endpoint is a capability of the item, so the address names the item
#: and the capability rather than a path.
ENDPOINT_SUFFIX = "sql_endpoint"


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
    #: What this node would reach for, named the way the estate names it. A
    #: *logical* address, not a path: the path is the resolver's business and is
    #: needed only at dispatch, but a reader of a dry run still wants to know
    #: which procedure or which module a node means.
    dispatch_location: str | None = None
    #: Typed findings, not sentences. A reader — and a task log — asks what
    #: *kind* of thing went wrong, and "target_missing" survives rewording in a
    #: way that a message does not.
    messages: tuple = ()
    #: A capability this host does not have, so the node is omitted rather than
    #: failed. Only an endpoint refresh where there is no endpoint.
    unsupported: bool = False

    @property
    def valid(self) -> bool:
        """No *error* stops this node. A warning is a finding, not a refusal."""

        from .result import SEVERITY_ERROR

        return not any(one.severity == SEVERITY_ERROR for one in self.messages)


def resolve(node, state, *, can_refresh: bool = True) -> Resolved:
    """Whether this node's target and primitive are present in the snapshot.

    ``can_refresh`` is the one host capability resolution needs, and it is
    passed in rather than discovered: only the caller knows whether the target
    it is resolving against has an endpoint to refresh.
    """

    inventory = state.inventory(node.physical_target)
    if node.primitive_kind == ENDPOINT_REFRESH:
        return _refresh(node, inventory, can_refresh=can_refresh)

    messages: list = []
    if inventory is None:
        messages.append(
            error(
                TARGET_MISSING,
                f"{node.physical_target} is not present, so {node.node_id} has "
                "nowhere to run",
                source=SOURCE,
            )
        )

    expected_class = None
    if node.primitive_kind in PYTHON_KINDS:
        expected_class = _module_class(node)
        if expected_class is None:
            messages.append(
                error(
                    MODULE_IMPORT_FAILURE,
                    f"{node.node_id} names a deployed module whose expected "
                    "class cannot be derived from its filename",
                    source=SOURCE,
                )
            )
    elif node.primitive_kind not in (WAREHOUSE_PROCEDURE, PYTHON_VALIDATION):
        messages.append(
            error(
                DISPATCH_LOCATION_MISSING,
                f"{node.node_id} names primitive kind {node.primitive_kind!r}, "
                "which no runtime can address",
                source=SOURCE,
            )
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
            error(
                DISPATCH_LOCATION_MISSING,
                f"{node.node_id} would dispatch {node.primitive_object}, which "
                f"is not installed in {node.physical_target}",
                source=SOURCE,
            )
        )
    if (
        inventory is not None
        and node.physical_object is not None
        and not _holds(inventory, node.physical_object)
    ):
        messages.append(
            error(
                TARGET_MISSING,
                f"{node.physical_target} does not hold {node.physical_object}, "
                "which this node loads into",
                source=SOURCE,
            )
        )

    return Resolved(
        node=node,
        target_present=inventory is not None,
        primitive_present=primitive_present,
        expected_class=expected_class,
        dispatch_location=_where(node),
        messages=tuple(messages),
    )


def _refresh(node, inventory, *, can_refresh: bool) -> Resolved:
    """A barrier resolves to a capability, and its absence is not a failure."""

    messages: list = []
    if inventory is None:
        messages.append(
            error(
                TARGET_MISSING,
                f"{node.physical_target} is not present, so its SQL endpoint "
                "cannot be refreshed",
                source=SOURCE,
            )
        )
    if not can_refresh:
        messages.append(
            warning(
                DISPATCH_LOCATION_MISSING,
                "SQL endpoint refresh is unsupported in this environment; "
                f"{node.node_id} will be skipped",
                source=SOURCE,
            )
        )
    return Resolved(
        node=node,
        target_present=inventory is not None,
        primitive_present=can_refresh,
        dispatch_location=f"{node.physical_target}/{ENDPOINT_SUFFIX}",
        messages=tuple(messages),
        unsupported=not can_refresh,
    )


def _where(node) -> str | None:
    """The installed thing this node would reach for, named logically.

    A Warehouse node means a procedure; a Python node means a deployed module.
    Both are addressable from the node alone — the absolute path is the
    resolver's business, and a dry run that had to resolve one would have to
    reach a workspace to say what it intends.
    """

    if node.primitive_kind == WAREHOUSE_PROCEDURE:
        from ..etl import load_procedure_name

        if node.logical_id is None:
            return None
        return (
            f"{node.physical_target}/{load_procedure_name(node.logical_id.object_id)}"
        )
    if node.primitive_kind in PYTHON_KINDS and node.primitive_object is not None:
        return (
            f"{node.physical_target}/{node.primitive_object.schema}/"
            f"{node.primitive_object.object}"
        )
    return None


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
    "PYTHON_VALIDATION",
    "WAREHOUSE_PROCEDURE",
    "Resolved",
    "resolve",
]
