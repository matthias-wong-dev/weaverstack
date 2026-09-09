"""The statements that make one Warehouse read another's rows.

Three-part is how a Fabric Warehouse reaches another item in its workspace, the
same reach a built Warehouse's ``_`` surface views use.

See ``design/catalogue.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from ..declaration.metadata import AUDIT_LIVE_DELETE_DATETIME
from ..declaration.model import LAKEHOUSE, WeaverDocumentId
from .claims import catalogue_columns, stored_area
from .fork import create_statement, local_relation
from .tables import CATALOGUE_SCHEMA, MIRROR, ROLE_DATA, STANDARD_SURFACE_TABLES
from .tsql import identifier, literal

#: What stands at a borrowed Warehouse relation's address, whatever the source
#: is: a table and a view both read the same way through a View.
BORROWED_TYPE = "view"

PROCEDURE_TYPE = "stored_procedure"

#: What a Lakehouse borrows through a OneLake shortcut. A View is not among
#: them: a shortcut addresses storage, and a view is a definition.
POINTER_TYPES = ("table", "folder")


@dataclass(frozen=True)
class Borrowed:
    """One relation a mirror points at, and what stands at its address."""

    identity: WeaverDocumentId
    #: What Registry certifies the object as.
    declared: str
    #: What physically stands at the address once the mirror is built.
    physical: str

    @property
    def is_pointer(self) -> bool:
        """Whether a shortcut stands there rather than a view Weaver wrote."""

        return self.physical in POINTER_TYPES

    @property
    def area(self) -> str | None:
        """The Lakehouse area this sits in, or ``None`` for a Warehouse."""

        return stored_area(catalogue_columns(self.identity)[0])[0]

    @property
    def schema(self) -> str:
        """The relational schema, with any Lakehouse area taken off."""

        return stored_area(catalogue_columns(self.identity)[0])[1]

    @property
    def name(self) -> str:
        return self.identity.object_id.object


def borrowable(
    registered: Mapping[WeaverDocumentId, object], *, kind: str
) -> tuple[Borrowed, ...]:
    """The data relations of one item, and what a mirror puts at each address.

    A Warehouse reads everything through a View over the source's three-part
    name. A Lakehouse points a shortcut at a table or a folder, because those
    are storage, and wraps a source view in a view of its own.
    """

    relations = sorted(
        (
            (identity, document.object_type)
            for identity, document in registered.items()
            if document.object_role == ROLE_DATA
            and getattr(identity, "object_id", None) is not None
        ),
        key=lambda pair: str(pair[0]),
    )
    return tuple(
        Borrowed(
            identity=identity,
            declared=declared,
            physical=declared if kind == LAKEHOUSE else BORROWED_TYPE,
        )
        for identity, declared in relations
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


def wrapper_view_statement(borrowed: Borrowed, *, destination, source) -> str:
    """A view in the destination Lakehouse selecting from the source's view.

    Both sides are spelled four-part, so the statement names no ambient
    catalogue and the source's definition is never read: a change at the source
    stays visible through the wrapper until an ordinary build replaces it.
    """

    return (
        f"CREATE OR REPLACE VIEW "
        f"{destination.qualify(borrowed.schema, borrowed.name)} "
        f"AS SELECT * FROM {source.qualify(borrowed.schema, borrowed.name)}"
    )


def pointer_shortcuts(borrowed: Sequence[Borrowed], *, source, path_of) -> tuple:
    """Each borrowed table and folder as the shortcut that will stand for it.

    ``source`` is the resolved source item and ``path_of`` gives one object's
    path inside it, which storage may spell differently from the declaration.
    The destination path is the estate's own address, which a mirror keeps.

    One shape serves the transport and the readiness wait: ``path``, ``name``,
    ``source`` and ``source_path`` are what a shortcut request carries, and
    ``shortcut`` and ``type`` are what waiting on one needs.
    """

    return tuple(
        {
            "shortcut": str(each.identity),
            "type": each.physical,
            "path": f"{each.area}/{each.schema}",
            "name": each.name,
            "source": source,
            "source_path": path_of(each),
        }
        for each in borrowed
        if each.is_pointer
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
    borrowed: Sequence[Borrowed],
    *,
    source_target: str,
    catalogue_name: str,
) -> tuple[str, ...]:
    """The schemas, Views and ``_`` surface one Warehouse mirror stands on."""

    return (
        surface_statements(catalogue_name)
        + schema_statements(each.schema for each in borrowed)
        + tuple(
            view_statement(each.identity, source_target=source_target)
            for each in borrowed
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
    borrowed: Sequence[Borrowed],
    *,
    source_workspace: str,
    source_target: str,
) -> tuple[str, ...]:
    """Create ``_.Mirror`` if this catalogue has none, then record the rows.

    For the catalogue Warehouse. Each row is deleted first, so a rerun replaces
    rather than duplicates.
    """

    if not borrowed:
        return ()
    rows = [
        _row(each, source_workspace=source_workspace, source_target=source_target)
        for each in borrowed
    ]
    return (create_statement(MIRROR), _delete(rows), _insert(rows))


def _row(
    borrowed: Borrowed, *, source_workspace: str, source_target: str
) -> dict[str, str]:
    identity = borrowed.identity
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
        "Physical type": MIRROR.column("physical_type").to_public(borrowed.physical),
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
    "POINTER_TYPES",
    "PROCEDURE_TYPE",
    "Borrowed",
    "borrow_statements",
    "borrowable",
    "executable",
    "missing_programmables",
    "programmable_statements",
    "record_statements",
    "schema_statements",
    "pointer_shortcuts",
    "surface_statements",
    "view_statement",
    "wrapper_view_statement",
]
