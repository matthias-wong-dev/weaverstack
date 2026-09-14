"""Project bound items' catalogue rows from prepared repository intent.

Projection performs no source reads, module imports or physical inspection. All
rows carry the supplied installation scope. Projection includes only bound
items; unbound items are not deletion candidates. Shortcuts remain distinct from
dependencies: dependencies preserve authored references, while shortcut rows
record destinations.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from ..declaration.metadata import (
    ASSUMPTION,
    FOLDER,
    TABLE,
    TEST,
    VIEW,
    Reference,
)
from ..declaration.model import (
    LOGICAL_TARGET,
    TABLE_SHORTCUT,
    VIEW_SHORTCUT,
    WAREHOUSE,
    WeaverDocumentId,
    WeaverItemId,
    WeaverRepository,
    WeaverSchemaId,
)
from ..declaration.references import declared_column_notes, resolve_text
from ..etl import PROCEDURE_TYPE, artefacts_by_identity, item_runtime_artefacts
from .claims import catalogue_schema
from .render import InstallationScope, Row, column_set
from .tables import (
    COLUMN_DICTIONARY,
    DEPENDENCY,
    FOLDER_DICTIONARY,
    FOREIGN_KEY_DICTIONARY,
    KEY_DICTIONARY,
    KEY_PRIMARY,
    KEY_UNIQUE,
    PROJECTED_TABLES,
    REGISTRY,
    ROLE_DATA,
    ROLE_SHORTCUT,
    SCHEMA_DICTIONARY,
    SHORTCUT,
    TABLE_DICTIONARY,
    TEST_DICTIONARY,
    CatalogueTable,
)

# Keep projection independent of build-package binding classes.
LAKEHOUSE_TARGET = "lakehouse"
WAREHOUSE_TARGET = "warehouse"

OBJECT_TYPE_FOR_KIND = {FOLDER: "folder", TABLE: "table", VIEW: "view"}

SCHEMA_TYPE = "schema"

# TestDictionary describes the validation; Registry certifies its compiled artefact.
TEST_TYPE_FOR_KIND = {TEST: "test", ASSUMPTION: "assumption"}


@dataclass(frozen=True)
class CatalogueProjection:
    scope: InstallationScope
    rows: Mapping[str, tuple[Row, ...]]

    def for_table(self, table: CatalogueTable) -> tuple[Row, ...]:
        return self.rows.get(table.name, ())

    @property
    def total(self) -> int:
        return sum(len(rows) for rows in self.rows.values())


def project_item_catalogue(
    repository: WeaverRepository,
    *,
    item: WeaverItemId,
    retained: Iterable[WeaverDocumentId],
) -> CatalogueProjection:
    """Project one item's authored and package-owned catalogue relations.

    Shortcut Registry rows are excluded because their physical type requires a
    target binding. :func:`project_shortcut_registry` adds them.
    """

    scope = InstallationScope(item.item_type, item.item_name)
    retained = tuple(sorted(set(retained), key=str))
    if any(identity.item != item for identity in retained):
        raise ValueError(
            f"item projection for {item} includes a document owned by another item"
        )

    # Shortcut destinations have no table, column or key declarations to project.
    shortcut_by_destination = {
        declaration.destination: declaration
        for declaration in repository.shortcuts
        if declaration.owner == item
    }
    logical_shortcut_by_destination = {
        shortcut.destination: shortcut
        for shortcut in repository.logical_shortcuts
        if shortcut.destination.item == item
    }
    shortcut_destinations = set(shortcut_by_destination) | set(
        logical_shortcut_by_destination
    )
    retained_shortcuts = tuple(
        shortcut_by_destination[identity]
        for identity in retained
        if identity in shortcut_by_destination
    )
    installed = artefacts_by_identity(item_runtime_artefacts(repository, item=item))
    retained_artefacts = tuple(
        installed[identity] for identity in retained if identity in installed
    )
    # Validations have logical identities but materialise no data objects.
    retained_validations = tuple(
        identity
        for identity in retained
        if identity not in shortcut_destinations
        and identity not in installed
        and repository.source_documents[identity].is_validation
    )
    validation_set = set(retained_validations)
    retained = tuple(
        identity
        for identity in retained
        if identity not in shortcut_destinations
        and identity not in installed
        and identity not in validation_set
    )
    documents = [repository.source_documents[identity] for identity in retained]
    all_documents = tuple(repository.source_documents.values())
    rows: dict[str, list[dict]] = {table.name: [] for table in PROJECTED_TABLES}

    for identity, source in zip(retained, documents):
        common = _identity(scope, identity)
        signature = source.physical_signature
        rows[REGISTRY.name].append(
            {
                **common,
                "object_type": OBJECT_TYPE_FOR_KIND[source.kind],
                "object_role": ROLE_DATA,
                "signature": signature,
            }
        )
        described = _described(
            source,
            all_documents,
            repository,
        )
        if source.kind == FOLDER:
            rows[FOLDER_DICTIONARY.name].append(
                {
                    **common,
                    **described,
                    "file_key": column_set(source.document.file_keys),
                    **_behaviour(source),
                    "signature": signature,
                }
            )
        else:
            rows[TABLE_DICTIONARY.name].append(
                {
                    **common,
                    "object_type": OBJECT_TYPE_FOR_KIND[source.kind],
                    **described,
                    "primary_key": column_set(source.document.primary_key),
                    "not_null_columns": column_set(source.document.declared_not_null),
                    "identity_column": source.document.identity,
                    "comparison_columns": column_set(
                        source.document.comparison_columns
                    ),
                    **_behaviour(source),
                    "signature": signature,
                }
            )
        for column_name, note in declared_column_notes(source):
            resolved = resolve_text(
                note,
                owner=source,
                documents=all_documents,
                shortcuts=repository.logical_shortcuts,
            )
            rows[COLUMN_DICTIONARY.name].append(
                {
                    **common,
                    "column_name": column_name,
                    "description": resolved.literal,
                    "description_reference": resolved.reference,
                    "is_identity": column_name == source.document.identity,
                    "signature": signature,
                }
            )
        if source.document.primary_key:
            rows[KEY_DICTIONARY.name].append(
                {
                    **common,
                    "key_type": KEY_PRIMARY,
                    "column_set": column_set(source.document.primary_key),
                    "signature": signature,
                }
            )
        for unique in source.document.unique_keys:
            rows[KEY_DICTIONARY.name].append(
                {
                    **common,
                    "key_type": KEY_UNIQUE,
                    "column_set": column_set(unique),
                    "signature": signature,
                }
            )
        rows[FOREIGN_KEY_DICTIONARY.name].extend(
            _foreign_keys(source, identity, scope, signature)
        )

    # Registry certifies the validation's compiled artefact, not its logical ID.
    for identity in retained_validations:
        source = repository.source_documents[identity]
        document = source.document
        rows[TEST_DICTIONARY.name].append(
            {
                **_identity(scope, identity),
                "test_type": TEST_TYPE_FOR_KIND[document.kind],
                **_described(source, all_documents, repository),
                # Assumptions have one side and therefore no correlation key.
                "primary_key": column_set(document.primary_key) or None,
                "signature": source.effective_signature,
            }
        )

    # Runtime artefacts claim only Registry. Their role distinguishes otherwise
    # identical load and Test modules for downstream dispatch.
    for artefact in retained_artefacts:
        rows[REGISTRY.name].append(
            {
                **_identity(scope, artefact.identity),
                "object_type": artefact.object_type,
                "object_role": artefact.role,
                "signature": artefact.signature,
            }
        )

    # Validation dependencies use the logical identity, not the compiled artefact.
    consumers = set(retained) | validation_set
    for edge in repository.dependency_edges:
        if edge.consumer not in consumers:
            continue
        source = repository.source_documents[edge.consumer]
        producer = edge.producer
        rows[DEPENDENCY.name].append(
            {
                **_identity_as(scope, edge.consumer, role="referencing"),
                "dependency_reference": edge.reference,
                # Unresolved physical and shortcut references have no producer ID.
                "referenced_item_type": (
                    producer.item.item_type if producer is not None else None
                ),
                "referenced_item_name": (
                    producer.item.item_name if producer is not None else None
                ),
                "referenced_schema_name": (
                    _catalogue_schema(producer) if producer is not None else None
                ),
                "referenced_object_name": (
                    producer.object_id.object if producer is not None else None
                ),
                "signature": source.effective_signature,
            }
        )

    for declaration in repository.shortcuts:
        if declaration.owner != item:
            continue
        target_object = declaration.target_object
        rows[SHORTCUT.name].append(
            {
                **_scope(scope),
                "shortcut_id": declaration.shortcut_id,
                # Folder destinations keep their area prefix; schema shortcuts do not.
                "schema_name": (
                    declaration.schema
                    if declaration.is_schema
                    else _catalogue_schema(declaration.destination)
                ),
                "object_name": (
                    None
                    if declaration.is_schema
                    else declaration.destination.object_id.object
                ),
                "shortcut_type": declaration.shortcut_type,
                "target_type": declaration.target_type,
                "target_item_type": declaration.target_item.item_type,
                "target_item_name": declaration.target_item.item_name,
                # Logical targets use Registry identity, including Folder area.
                "target_schema_name": (
                    _catalogue_schema(declaration.logical_source)
                    if declaration.is_logical
                    else declaration.target_schema
                ),
                "target_object_name": (
                    declaration.logical_source.object_id.object
                    if declaration.is_logical
                    else (target_object.object if target_object is not None else None)
                ),
                "target_workspace_name": declaration.workspace,
                "signature": declaration.signature,
            }
        )

    # Persist package-owned logical shortcuts for operation without a repository.
    for shortcut in sorted(
        (
            shortcut
            for destination, shortcut in logical_shortcut_by_destination.items()
            if destination not in shortcut_by_destination
        ),
        key=lambda shortcut: str(shortcut.destination),
    ):
        destination = shortcut.destination
        source = shortcut.source
        rows[SHORTCUT.name].append(
            {
                **_scope(scope),
                "shortcut_id": destination.object_id.qualified,
                "schema_name": destination.object_id.schema,
                "object_name": destination.object_id.object,
                "shortcut_type": (
                    VIEW_SHORTCUT
                    if destination.item.item_type == WAREHOUSE
                    else TABLE_SHORTCUT
                ),
                "target_type": LOGICAL_TARGET,
                "target_item_type": source.item.item_type,
                "target_item_name": source.item.item_name,
                "target_schema_name": source.object_id.schema,
                "target_object_name": source.object_id.object,
                "target_workspace_name": None,
                "signature": shortcut.signature,
            }
        )

    used_schemas = sorted(
        {
            (_catalogue_schema(identity), identity.object_id.schema)
            for identity in retained + retained_validations
        }
        | {
            (
                _catalogue_schema(declaration.destination),
                declaration.destination.object_id.schema,
            )
            for declaration in retained_shortcuts
            # A schema shortcut presents a namespace owned by its source item.
            if not declaration.is_schema
        }
        # Procedures can use schemas with no declared object. Deployed files do
        # not: their schema is a path described by the owning folder document.
        | {
            (artefact.identity.object_id.schema, artefact.identity.object_id.schema)
            for artefact in retained_artefacts
            if artefact.object_type == PROCEDURE_TYPE
        }
    )
    item_model = next(model for model in repository.items if model.identity == item)
    schemas = {identity.schema: identity for identity in item_model.schemas}
    for catalogue_name, declared_name in used_schemas:
        schema = repository.schema_documents[schemas[declared_name]]
        rows[SCHEMA_DICTIONARY.name].append(
            {
                **_scope(scope),
                "schema_name": catalogue_name,
                "description": schema.description,
                "description_reference": None,
                "signature": schema.source_hash,
            }
        )

    return CatalogueProjection(
        scope=scope,
        rows={name: tuple(values) for name, values in rows.items()},
    )


def project_shortcut_registry(
    repository: WeaverRepository,
    *,
    item: WeaverItemId,
    retained: Iterable[WeaverDocumentId],
    target_kind: str,
) -> tuple[Row, ...]:
    """Project Registry certification for bound shortcut destinations.

    Target kind is required because Warehouse shortcuts are views. A schema
    shortcut certifies only its presented namespace; its contents belong to the
    source item and may change without a build.
    """

    scope = InstallationScope(item.item_type, item.item_name)
    wanted = set(retained)
    return tuple(
        {
            **_identity(scope, declaration.destination),
            "object_type": _shortcut_object_type(declaration, target_kind),
            "object_role": ROLE_SHORTCUT,
            "signature": declaration.signature,
        }
        for declaration in sorted(
            (
                declaration
                for declaration in repository.shortcuts
                if declaration.owner == item and declaration.destination in wanted
            ),
            key=lambda declaration: str(declaration.destination),
        )
    )


def _shortcut_object_type(declaration, target_kind: str) -> str:
    """Return the destination's physical type in catalogue vocabulary."""

    if declaration.is_schema:
        return SCHEMA_TYPE
    if declaration.destination.is_files:
        return OBJECT_TYPE_FOR_KIND[FOLDER]
    if target_kind == WAREHOUSE_TARGET:
        return OBJECT_TYPE_FOR_KIND[VIEW]
    return OBJECT_TYPE_FOR_KIND[TABLE]


def _scope(scope: InstallationScope) -> dict[str, str]:
    return dict(scope.values)


def _catalogue_schema(identity: WeaverDocumentId) -> str:
    return catalogue_schema(identity)


def _identity(scope: InstallationScope, identity) -> dict:
    """Return catalogue identity columns.

    Schema shortcuts repeat the schema as the object because Registry keys both
    columns.
    """

    if isinstance(identity, WeaverSchemaId):
        return {
            **_scope(scope),
            "schema_name": identity.schema,
            "object_name": identity.schema,
        }
    return {
        **_scope(scope),
        "schema_name": _catalogue_schema(identity),
        "object_name": identity.object_id.object,
    }


def _identity_as(
    scope: InstallationScope, identity: WeaverDocumentId, *, role: str
) -> dict:
    """Return an object's identity under one side of a relationship."""

    return {
        **_scope(scope),
        f"{role}_schema_name": _catalogue_schema(identity),
        f"{role}_object_name": identity.object_id.object,
    }


def _described(source, all_documents, repository) -> dict:
    description = resolve_text(
        source.document.description,
        owner=source,
        documents=all_documents,
        shortcuts=repository.logical_shortcuts,
    )
    lineage = resolve_text(
        source.document.lineage,
        owner=source,
        documents=all_documents,
        shortcuts=repository.logical_shortcuts,
    )
    return {
        "description": description.literal,
        "description_reference": description.reference,
        "lineage": lineage.literal,
        "lineage_reference": lineage.reference,
    }


def _behaviour(source) -> dict:
    return {
        "is_incremental": source.document.is_incremental,
        "is_static": source.document.static,
        "prohibit_rebuild": source.document.prohibit_rebuild,
    }


def _foreign_keys(source, identity, scope, signature) -> list[dict]:
    rows = []
    for key in source.document.foreign_keys:
        reference = key.logical_reference or Reference(
            schema=key.reference.schema,
            object=key.reference.object,
        )
        primary_item = (
            WeaverItemId(reference.item_type, reference.item_name)
            if reference.is_item_qualified
            else identity.item
        )
        rows.append(
            {
                **_identity_as(scope, identity, role="foreign"),
                "foreign_column_set": column_set(key.columns),
                "primary_item_type": primary_item.item_type,
                "primary_item_name": primary_item.item_name,
                # Relationship identities use Registry naming, including area.
                "primary_schema_name": _catalogue_schema(
                    WeaverDocumentId(
                        primary_item,
                        reference.object_id,
                        is_files=reference.is_files,
                    )
                ),
                "primary_object_name": reference.object,
                "primary_column_set": column_set(key.reference_columns),
                "signature": signature,
            }
        )
    return rows
