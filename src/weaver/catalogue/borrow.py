"""The statements that make one Warehouse read another's rows.

Three-part is how a Fabric Warehouse reaches another item in its workspace, the
same reach a built Warehouse's ``_`` surface views use.

See ``design/catalogue.md``.
"""

from __future__ import annotations

from typing import Iterable, Mapping, Sequence

from ..declaration.metadata import AUDIT_LIVE_DELETE_DATETIME
from ..declaration.model import WeaverDocumentId
from .claims import catalogue_columns
from .fork import create_statement, local_relation
from .tables import CATALOGUE_SCHEMA, MIRROR, ROLE_DATA, STANDARD_SURFACE_TABLES
from .tsql import identifier, literal

#: What stands at a borrowed relation's address, whatever the source is: a
#: table and a view both read the same way through a View.
BORROWED_TYPE = "view"

PROCEDURE_TYPE = "stored_procedure"


def borrowable(registered: Mapping[WeaverDocumentId, object]) -> tuple:
    """The data relations of one item, which are what a mirror points at."""

    return tuple(
        sorted(
            (
                identity
                for identity, document in registered.items()
                if document.object_role == ROLE_DATA
                and getattr(identity, "object_id", None) is not None
            ),
            key=str,
        )
    )


def executable(registered: Mapping[WeaverDocumentId, object]) -> tuple:
    """Everything the Registry certifies as a procedure, in every schema."""

    return tuple(
        sorted(
            (
                identity
                for identity, document in registered.items()
                if document.object_type == PROCEDURE_TYPE
            ),
            key=str,
        )
    )


def missing_programmables(required: Iterable[WeaverDocumentId], copied) -> tuple:
    """Certified procedures the source Warehouse did not supply.

    ``copied`` names every procedure read from the source, ``schema.name``.
    """

    held = {value.casefold() for value in copied}
    return tuple(
        identity
        for identity in required
        if identity.object_id.qualified.casefold() not in held
    )


def schema_statements(schemas: Iterable[str]) -> tuple[str, ...]:
    """One ``create schema`` per named schema. ``_`` is the surface's to make."""

    return tuple(
        f"if schema_id(N'{schema}') is null exec('create schema {identifier(schema)}');"
        for schema in sorted(set(schemas))
        if schema != CATALOGUE_SCHEMA
    )


def view_statement(identity: WeaverDocumentId, *, source_target: str) -> str:
    """A View at this identity's address, reading the source's relation."""

    schema = identity.object_id.schema
    name = identity.object_id.object
    source = ".".join(identifier(part) for part in (source_target, schema, name))
    return (
        f"create or alter view {identifier(schema)}.{identifier(name)} "
        f"as select * from {source};"
    )


def surface_statements(catalogue_name: str) -> tuple[str, ...]:
    """The ``_`` schema and the views a Warehouse reads Weaver state through."""

    return (
        f"if schema_id(N'{CATALOGUE_SCHEMA}') is null "
        f"exec('create schema {identifier(CATALOGUE_SCHEMA)}');",
    ) + tuple(
        f"create or alter view "
        f"{identifier(CATALOGUE_SCHEMA)}.{identifier(table.name)} as select * from "
        + ".".join(
            identifier(part) for part in (catalogue_name, CATALOGUE_SCHEMA, table.name)
        )
        + ";"
        for table in STANDARD_SURFACE_TABLES
    )


def borrow_statements(
    identities: Sequence[WeaverDocumentId],
    *,
    source_target: str,
    catalogue_name: str,
) -> tuple[str, ...]:
    """The schemas, Views and ``_`` surface one Warehouse mirror stands on."""

    return (
        surface_statements(catalogue_name)
        + schema_statements(identity.object_id.schema for identity in identities)
        + tuple(
            view_statement(identity, source_target=source_target)
            for identity in identities
        )
    )


def programmable_statements(definitions: Iterable[str]) -> tuple[str, ...]:
    """Each programmable, as a create-or-alter of the source's own definition."""

    return tuple(
        _as_create_or_alter(definition)
        for definition in definitions
        if definition and definition.strip()
    )


def _as_create_or_alter(definition: str) -> str:
    """The source's own text, made re-runnable.

    Fabric returns the module as it was written, so a plain ``create`` would
    fail on a second mirror.
    """

    body = definition.strip()
    lowered = body.casefold()
    for prefix in ("create or alter", "create"):
        if lowered.startswith(prefix):
            return "create or alter" + body[len(prefix) :]
    return body


def record_statements(
    identities: Sequence[WeaverDocumentId],
    *,
    source_workspace: str,
    source_target: str,
) -> tuple[str, ...]:
    """Create ``_.Mirror`` if this catalogue has none, then record the rows.

    For the catalogue Warehouse. Each row is deleted first, so a rerun replaces
    rather than duplicates.
    """

    if not identities:
        return ()
    rows = [
        _row(
            identity,
            source_workspace=source_workspace,
            source_target=source_target,
        )
        for identity in identities
    ]
    return (create_statement(MIRROR), _delete(rows), _insert(rows))


def _row(
    identity: WeaverDocumentId, *, source_workspace: str, source_target: str
) -> dict[str, str]:
    schema, name = catalogue_columns(identity)
    return {
        "Item type": identity.item.item_type,
        "Item name": identity.item.item_name,
        "Schema name": schema,
        "Object name": name,
        "Source workspace name": source_workspace,
        "Source target name": source_target,
        "Source schema name": identity.object_id.schema,
        "Source object name": identity.object_id.object,
        "Physical type": MIRROR.column("physical_type").to_public(BORROWED_TYPE),
    }


def _key_predicate(row: Mapping[str, str]) -> str:
    return " and ".join(
        f"{identifier(column)} = {literal(row[column])}"
        for column in ("Item type", "Item name", "Schema name", "Object name")
    )


def _delete(rows: Sequence[Mapping[str, str]]) -> str:
    predicates = " or ".join(f"({_key_predicate(row)})" for row in rows)
    return f"delete from {local_relation(MIRROR)} where {predicates};"


def _insert(rows: Sequence[Mapping[str, str]]) -> str:
    columns = [column for column in MIRROR.public_columns if column not in _AUDIT]
    named = ", ".join(identifier(column) for column in columns)
    audit = ", ".join(identifier(column) for column in _AUDIT)
    values = ",\n       ".join(
        "("
        + ", ".join(literal(row[column]) for column in columns)
        + ", sysdatetime(), sysdatetime(), "
        + f"convert(datetime2(6), '{AUDIT_LIVE_DELETE_DATETIME}'))"
        for row in rows
    )
    return f"insert into {local_relation(MIRROR)} ({named}, {audit})\nvalues {values};"


#: The audit trio every catalogue row carries.
_AUDIT = ("Row insert datetime", "Row update datetime", "Row delete datetime")


__all__ = [
    "BORROWED_TYPE",
    "PROCEDURE_TYPE",
    "borrow_statements",
    "borrowable",
    "executable",
    "missing_programmables",
    "programmable_statements",
    "record_statements",
    "schema_statements",
    "surface_statements",
    "view_statement",
]
