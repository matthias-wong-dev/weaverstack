"""Derive a runnable node's dispatch address from the catalogue graph."""

from __future__ import annotations

from dataclasses import dataclass

from .result import (
    DISPATCH_LOCATION_MISSING,
    MODULE_IMPORT_FAILURE,
    error,
    warning,
)

SOURCE = "run.resolution"

#: Primitive kinds this module reasons about. A kind not named here
#: resolves as unaddressable rather than being assumed to work.
WAREHOUSE_PROCEDURE = "warehouse_procedure"
PYTHON_TABLE = "python_table"
PYTHON_FOLDER = "python_folder"
ENDPOINT_REFRESH = "endpoint_refresh"
ONELAKE_PUBLICATION = "onelake_publication"
#: How a validation is reached, from where it is installed.
PYTHON_VALIDATION = "python_validation"

PYTHON_KINDS = (PYTHON_TABLE, PYTHON_FOLDER)

#: What a refresh resolves to. Not a physical object, a Lakehouse's SQL
#: analytics endpoint is a capability of the item, so the address names the item
#: and the capability rather than a path.
ENDPOINT_SUFFIX = "sql_endpoint"


@dataclass(frozen=True)
class Resolved:
    """A node with the address and metadata needed for dispatch."""

    node: object
    expected_class: str | None = None
    dispatch_location: str | None = None
    messages: tuple = ()
    unsupported: bool = False

    @property
    def valid(self) -> bool:
        from .result import SEVERITY_ERROR

        return not any(one.severity == SEVERITY_ERROR for one in self.messages)


def resolve(node, *, can_refresh: bool = True) -> Resolved:

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
                    f"Cannot run {node.node_id}: its deployed Python filename does "
                    "not identify the expected class. Rebuild and reinstall the project.",
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
                f"Cannot run {node.node_id}: primitive kind "
                f"{node.primitive_kind!r} is unsupported",
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

    messages: list = []
    if not can_refresh:
        messages.append(
            warning(
                DISPATCH_LOCATION_MISSING,
                f"Skipped {node.node_id}: this Session cannot refresh SQL endpoints",
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
    """Name the installed procedure or module without resolving a physical path."""

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
    """Derive the deployed class without importing the module."""

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
