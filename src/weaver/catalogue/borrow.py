"""Build the relations through which one item reads another.

A Fabric Warehouse reaches another Warehouse in its workspace by three-part
name, as its ``_`` surface views do.

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

#: A Warehouse borrows both tables and views through a view.
BORROWED_TYPE = "view"

PROCEDURE_TYPE = "stored_procedure"

#: Lakehouses borrow stored objects through OneLake shortcuts, not views.
POINTER_TYPES = ("table", "folder")


@dataclass(frozen=True)
class Borrowed:
    identity: WeaverDocumentId
    #: What Registry certifies the object as.
    declared: str
    #: What physically stands at the address once the mirror is built.
    physical: str

    @property
    def is_pointer(self) -> bool:
        return self.physical in POINTER_TYPES

    @property
    def area(self) -> str | None:
        return stored_area(catalogue_columns(self.identity)[0])[0]

    @property
    def schema(self) -> str:
        return stored_area(catalogue_columns(self.identity)[0])[1]

    @property
    def name(self) -> str:
        return self.identity.object_id.object


def borrowable(
    registered: Mapping[WeaverDocumentId, object], *, kind: str
) -> tuple[Borrowed, ...]:
    """Return the data relations a mirror can borrow from one item.

    A Warehouse reads each relation through a view over its three-part source
    name. A Lakehouse uses shortcuts for stored objects and wrapper views for
    source views.

    The Weaver-owned ``_`` schema is excluded. Its Lakehouse runtime tree at
    ``Files/_.Load`` is copied into the destination instead.
    """

    relations = sorted(
        (
            (identity, document.object_type)
            for identity, document in registered.items()
            if document.object_role == ROLE_DATA
            and getattr(identity, "object_id", None) is not None
            and identity.object_id.schema != CATALOGUE_SCHEMA
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
    """Return certified procedures absent from ``copied`` ``schema.name`` values."""

    held = {value.casefold() for value in copied}
    return tuple(
        identity
        for identity in required
        if identity.object_id.qualified.casefold() not in held
    )


def wrapper_view_statement(borrowed: Borrowed, *, destination, source) -> str:
    """Create a destination Lakehouse view over a source view.

    Both sides use four-part names. Source changes remain visible through the
    wrapper until an ordinary build replaces it.
    """

    return (
        f"CREATE OR REPLACE VIEW "
        f"{destination.qualify(borrowed.schema, borrowed.name)} "
        f"AS SELECT * FROM {source.qualify(borrowed.schema, borrowed.name)}"
    )


def pointer_shortcuts(borrowed: Sequence[Borrowed], *, source, path_of) -> tuple:
    """Return shortcut requests for borrowed tables and folders.

    ``path_of`` supplies the source's physical spelling. The destination retains
    the recorded logical address. The mapping serves both shortcut
    creation and the readiness wait.
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


def surface_shortcuts(item, *, catalogue) -> tuple:
    """Return the standard ``_`` surface shortcuts for a mirrored Lakehouse.

    Mirrored and built Lakehouses share the declaration returned by
    :func:`weaver.catalogue.builtin.standard_surface_references`. The shortcuts
    read the destination catalogue Warehouse.
    """

    from ..declaration.model import TABLES
    from .builtin import standard_surface_references

    declarations, pairs = standard_surface_references(item)
    return tuple(
        {
            "shortcut": str(pair.destination),
            "type": declaration.shortcut_type,
            "path": f"{pair.destination.area}/{pair.destination.object_id.schema}",
            "name": pair.destination.object_id.object,
            "source": catalogue,
            "source_path": (
                f"{TABLES}/{pair.source.object_id.schema}"
                f"/{pair.source.object_id.object}"
            ),
        }
        for declaration, pair in zip(declarations, pairs)
    )


def schema_statements(schemas: Iterable[str]) -> tuple[str, ...]:
    # The surface creates Weaver's ``_`` schema.
    return tuple(
        f"if schema_id(N'{schema}') is null exec('create schema {identifier(schema)}');"
        for schema in sorted(set(schemas))
        if schema != CATALOGUE_SCHEMA
    )


def view_statement(identity: WeaverDocumentId, *, source_target: str) -> str:
    schema = identity.object_id.schema
    name = identity.object_id.object
    source = ".".join(identifier(part) for part in (source_target, schema, name))
    return (
        f"create or alter view {identifier(schema)}.{identifier(name)} "
        f"as select * from {source};"
    )


def surface_statements(catalogue_name: str) -> tuple[str, ...]:
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
    return (
        surface_statements(catalogue_name)
        + schema_statements(each.schema for each in borrowed)
        + tuple(
            view_statement(each.identity, source_target=source_target)
            for each in borrowed
        )
    )


def programmable_statements(definitions: Iterable[str]) -> tuple[str, ...]:
    return tuple(
        _as_create_or_alter(definition)
        for definition in definitions
        if definition and definition.strip()
    )


def _as_create_or_alter(definition: str) -> str:
    # Fabric returns the original declaration, which may use non-repeatable CREATE.
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
    """Replace these objects' ``_.Mirror`` rows in the catalogue Warehouse."""

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
    "surface_shortcuts",
    "surface_statements",
    "view_statement",
    "wrapper_view_statement",
]
