"""Read and reconcile catalogue claims before bundle generation."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from ..declaration.model import WeaverDocumentId, WeaverItemId
from ..errors import BuildError
from ..spark.tokens import object_token
from .reader import _is_absent, read_installation
from .render import InstallationScope, identifier, literal
from .tables import CATALOGUE_TABLES, DICTIONARY_TABLES, REGISTRY, CatalogueTable


@dataclass(frozen=True)
class CatalogueState:
    status: str
    rows: Mapping[WeaverItemId, Mapping[str, tuple[Mapping[str, object], ...]]]
    present_tables: frozenset[str]
    missing_tables: frozenset[str]


@dataclass(frozen=True)
class ReconciledCatalogue:
    rows: Mapping[WeaverItemId, Mapping[str, tuple[Mapping[str, object], ...]]]
    delete_dml: tuple[str, ...] = ()
    stale_objects: tuple[str, ...] = ()


@dataclass(frozen=True)
class CatalogueClaimDelete:
    """One frozen removal of one document's rows from one catalogue table."""

    identity: WeaverDocumentId
    table: CatalogueTable
    statement: str


@dataclass(frozen=True, order=True)
class _StaleObject:
    """Typed physical evidence attached to one unchanged four-part catalogue key."""

    object_type: str
    schema_name: str
    object_name: str

    @property
    def catalogue_key(self) -> tuple[str, str]:
        return (self.schema_name, self.object_name)


def read_catalogue_state(catalogue: Any, items) -> CatalogueState:
    """Read selected scopes and distinguish absent, partial and invalid shape."""

    present: set[str] = set()
    missing: set[str] = set()
    incompatible: list[str] = []
    for table in CATALOGUE_TABLES:
        name = catalogue.expand(object_token("_", table.name))
        try:
            columns = catalogue.spark.table(name).columns
        except Exception as exc:
            if _is_absent(exc):
                missing.add(table.name)
                continue
            raise
        present.add(table.name)
        folded = {column.casefold() for column in columns}
        required = {column.name.casefold() for column in table.columns}
        absent_columns = sorted(required - folded)
        if absent_columns:
            incompatible.append(f"{table.name}.{absent_columns[0]}")
    if incompatible:
        raise BuildError(
            "catalogue schema is incompatible; missing required column(s): "
            + ", ".join(incompatible)
        )
    status = "valid"
    if not present:
        status = "absent"
    elif missing:
        status = "partial"
    rows = {
        item: MappingProxyType(
            read_installation(
                catalogue,
                scope=InstallationScope(item.item_type, item.item_name),
            )
        )
        for item in items
    }
    return CatalogueState(
        status=status,
        rows=MappingProxyType(rows),
        present_tables=frozenset(present),
        missing_tables=frozenset(missing),
    )


def reconcile_catalogue_state(
    state: CatalogueState, *, inventories: Mapping[WeaverItemId, Any]
) -> ReconciledCatalogue:
    """Discard catalogue roots disproved by prepared target inventories."""

    reconciled = {}
    deletes: list[str] = []
    stale_labels: list[str] = []
    for item, tables in state.rows.items():
        inventory = inventories.get(item)
        stale: set[_StaleObject] = set()
        if inventory is not None:
            for row in tables.get(REGISTRY.name, ()):
                schema = str(row.get("schema_name") or "")
                name = str(row.get("object_name") or "")
                object_type = str(row.get("object_type") or "")
                if not inventory.has_object(schema, name, object_type):
                    stale.add(_StaleObject(object_type, schema, name))
        stale_keys = {obj.catalogue_key for obj in stale}
        filtered = {}
        scope = InstallationScope(item.item_type, item.item_name)
        for table in CATALOGUE_TABLES:
            rows = tables.get(table.name, ())
            if not stale or not _object_columns(table):
                filtered[table.name] = tuple(rows)
                continue
            filtered[table.name] = tuple(
                row
                for row in rows
                if (str(row.get("schema_name")), str(row.get("object_name")))
                not in stale_keys
            )
            if table.name in state.present_tables:
                for obj in sorted(stale):
                    deletes.append(
                        _delete_object(
                            table, scope, obj.schema_name, obj.object_name
                        )
                    )
        reconciled[item] = MappingProxyType(filtered)
        stale_labels.extend(
            f"{item}/{obj.schema_name}.{obj.object_name}" for obj in stale
        )
    return ReconciledCatalogue(
        rows=MappingProxyType(reconciled),
        delete_dml=tuple(dict.fromkeys(deletes)),
        stale_objects=tuple(sorted(stale_labels)),
    )


def _object_columns(table: CatalogueTable) -> bool:
    names = set(table.column_names)
    return {"schema_name", "object_name"} <= names


def _delete_object(
    table: CatalogueTable,
    scope: InstallationScope,
    schema: str,
    name: str,
) -> str:
    return (
        f"DELETE FROM {object_token('_', table.name)}\n"
        f"WHERE {scope.predicate}\n"
        f"  AND {identifier('schema_name')} = {literal(schema)}\n"
        f"  AND {identifier('object_name')} = {literal(name)}"
    )


def registered_documents(
    catalogue: ReconciledCatalogue, *, items: set[WeaverItemId] | None = None
) -> tuple[WeaverDocumentId, ...]:
    """Canonical document identities certified by the reconciled Registry."""

    identities = []
    for item, tables in catalogue.rows.items():
        if items is not None and item not in items:
            continue
        for row in tables.get(REGISTRY.name, ()):
            identities.append(_row_identity(item, row))
    return tuple(sorted(set(identities), key=str))


def registered_document_types(
    catalogue: ReconciledCatalogue, *, items: set[WeaverItemId] | None = None
) -> Mapping[WeaverDocumentId, str]:
    """Installed physical type for every document certified by Registry."""

    types = {}
    for item, tables in catalogue.rows.items():
        if items is not None and item not in items:
            continue
        for row in tables.get(REGISTRY.name, ()):
            types[_row_identity(item, row)] = str(row.get("object_type") or "")
    return MappingProxyType(types)


def render_catalogue_claim_deletes(
    catalogue: ReconciledCatalogue,
    identities,
) -> tuple[CatalogueClaimDelete, ...]:
    """Uncertify selected documents, then remove their owned dictionary rows.

    Only tables that currently contain matching rows receive DML.  This keeps a
    partially present catalogue safe: a missing table is never named merely
    because it belongs to the conceptual catalogue schema.
    """

    selected = tuple(sorted(set(identities), key=str))
    deletes = []
    for identity in selected:
        tables = catalogue.rows.get(identity.item, {})
        scope = InstallationScope(identity.item.item_type, identity.item.item_name)
        schema = (
            f"Files/{identity.object_id.schema}"
            if identity.is_files
            else identity.object_id.schema
        )
        for table in (REGISTRY, *reversed(DICTIONARY_TABLES)):
            if not _object_columns(table):
                continue
            matching = any(
                str(row.get("schema_name")) == schema
                and str(row.get("object_name")) == identity.object_id.object
                for row in tables.get(table.name, ())
            )
            if not matching:
                continue
            deletes.append(
                CatalogueClaimDelete(
                    identity=identity,
                    table=table,
                    statement=_delete_object(
                        table, scope, schema, identity.object_id.object
                    ),
                )
            )
    return tuple(deletes)


def _row_identity(item: WeaverItemId, row: Mapping[str, object]) -> WeaverDocumentId:
    schema = str(row.get("schema_name") or "")
    is_files = schema.startswith("Files/")
    logical_schema = schema[len("Files/") :] if is_files else schema
    prefix = "Files/" if is_files else ""
    return WeaverDocumentId.parse(
        f"{item}/{prefix}{logical_schema}.{row.get('object_name')}"
    )
