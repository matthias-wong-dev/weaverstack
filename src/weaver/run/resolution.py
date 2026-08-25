"""Derive a runnable node's dispatch address from the catalogue graph."""

from __future__ import annotations

from dataclasses import dataclass

from .result import (
    DISPATCH_LOCATION_MISSING,
    MODULE_IMPORT_FAILURE,
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
ONELAKE_PUBLICATION = "onelake_publication"
#: How a validation is reached, from where it is installed.
PYTHON_VALIDATION = "python_validation"

PYTHON_KINDS = (PYTHON_TABLE, PYTHON_FOLDER)

#: What a refresh resolves to. Not a physical object — a Lakehouse's SQL
#: analytics endpoint is a capability of the item, so the address names the item
#: and the capability rather than a path.
ENDPOINT_SUFFIX = "sql_endpoint"


@dataclass(frozen=True)
class Resolved:
    """One node with the address and metadata needed for dispatch."""

    node: object
    expected_class: str | None = None
    dispatch_location: str | None = None
    messages: tuple = ()
    unsupported: bool = False

    @property
    def valid(self) -> bool:
        """No *error* stops this node. A warning is a finding, not a refusal."""

        from .result import SEVERITY_ERROR

        return not any(one.severity == SEVERITY_ERROR for one in self.messages)


def resolve(node, *, can_refresh: bool = True) -> Resolved:
    """Derive dispatch metadata without reading the physical target."""

    if node.primitive_kind == ENDPOINT_REFRESH:
        return _refresh(node, can_refresh=can_refresh)

    messages: list = []
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
    elif node.primitive_kind not in (
        WAREHOUSE_PROCEDURE,
        PYTHON_VALIDATION,
        ONELAKE_PUBLICATION,
    ):
        messages.append(
            error(
                DISPATCH_LOCATION_MISSING,
                f"{node.node_id} names primitive kind {node.primitive_kind!r}, "
                "which no runtime can address",
                source=SOURCE,
            )
        )

    return Resolved(
        node=node,
        expected_class=expected_class,
        dispatch_location=_where(node),
        messages=tuple(messages),
    )


def _refresh(node, *, can_refresh: bool) -> Resolved:
    """A barrier resolves to a capability, and its absence is not a failure."""

    messages: list = []
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
    "ONELAKE_PUBLICATION",
    "PYTHON_FOLDER",
    "PYTHON_KINDS",
    "PYTHON_TABLE",
    "PYTHON_VALIDATION",
    "WAREHOUSE_PROCEDURE",
    "Resolved",
    "resolve",
]
