"""Project one logical Weaver item into the item-scoped catalogue."""

from __future__ import annotations

import hashlib
from typing import Iterable

from ..ses.metadata import FOLDER, TABLE, VIEW, Reference
from ..ses.model import WeaverDocumentId, WeaverItemId, WeaverRepository
from ..ses.references import declared_column_notes, resolve_text
from .item_tables import (
    ALIAS,
    CATALOGUE_TABLES,
    COLUMN_DICTIONARY,
    DEPENDENCY,
    FOLDER_DICTIONARY,
    FOREIGN_KEY_DICTIONARY,
    INDEX_DICTIONARY,
    INSTALLATION,
    ItemInstallationScope,
    REGISTRY,
    SCHEMA_DICTIONARY,
    TABLE_DICTIONARY,
)
from .projection import CatalogueProjection, OBJECT_TYPE_FOR_KIND
from .render import column_set
from .tables import KEY_PRIMARY, KEY_UNIQUE, ROLE_DATA


def project_item_installation(
    repository: WeaverRepository,
    *,
    item: WeaverItemId,
    retained: Iterable[WeaverDocumentId],
    target_name: str,
    weaver_version: str,
) -> CatalogueProjection:
    scope = ItemInstallationScope(item.item_type, item.item_name)
    retained = tuple(sorted(set(retained), key=str))
    if any(identity.item != item for identity in retained):
        raise ValueError(f"item projection {item} received a document owned elsewhere")
    documents = [repository.source_documents[identity] for identity in retained]
    all_documents = tuple(repository.source_documents.values())
    rows: dict[str, list[dict]] = {table.name: [] for table in CATALOGUE_TABLES}

    for identity, source in zip(retained, documents):
        common = _identity(scope, identity)
        signature = source.source_hash
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
                    "comparison_columns": column_set(source.document.comparison_columns),
                    **_behaviour(source),
                    "signature": signature,
                }
            )
        for column_name, note in declared_column_notes(source):
            resolved = resolve_text(
                note,
                owner=source,
                documents=all_documents,
                aliases=repository.aliases,
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
            rows[INDEX_DICTIONARY.name].append(
                {
                    **common,
                    "index_type": KEY_PRIMARY,
                    "column_set": column_set(source.document.primary_key),
                    "signature": signature,
                }
            )
        for unique in source.document.unique_keys:
            rows[INDEX_DICTIONARY.name].append(
                {
                    **common,
                    "index_type": KEY_UNIQUE,
                    "column_set": column_set(unique),
                    "signature": signature,
                }
            )
        rows[FOREIGN_KEY_DICTIONARY.name].extend(
            _foreign_keys(source, identity, common, signature)
        )

    retained_set = set(retained)
    for edge in repository.dependency_edges:
        if edge.consumer not in retained_set:
            continue
        source = repository.source_documents[edge.consumer]
        rows[DEPENDENCY.name].append(
            {
                **_identity(scope, edge.consumer),
                "dependency_name": edge.reference,
                "is_within_item": edge.is_within_item,
                "signature": source.source_hash,
            }
        )

    for alias in repository.aliases:
        if alias.destination.item != item:
            continue
        rows[ALIAS.name].append(
            {
                **_scope(scope),
                "destination_schema_name": _catalogue_schema(alias.destination),
                "destination_object_name": alias.destination.object_id.object,
                "source_item_type": alias.source.item.item_type,
                "source_item_name": alias.source.item.item_name,
                "source_schema_name": _catalogue_schema(alias.source),
                "source_object_name": alias.source.object_id.object,
                "signature": _alias_signature(alias),
            }
        )

    used_schemas = sorted(
        {
            (_catalogue_schema(identity), identity.object_id.schema)
            for identity in retained
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

    rows[INSTALLATION.name].append(
        {
            **_scope(scope),
            "target_name": target_name,
            "weaver_version": weaver_version,
            "signature": item_model.signature,
        }
    )
    return CatalogueProjection(
        scope=scope,
        rows={name: tuple(values) for name, values in rows.items()},
    )


def _alias_signature(alias) -> str:
    declaration = f"{alias.destination}\0{alias.source}".encode("utf-8")
    return hashlib.sha256(declaration).hexdigest()


def _scope(scope: ItemInstallationScope) -> dict[str, str]:
    return dict(scope.values)


def _catalogue_schema(identity: WeaverDocumentId) -> str:
    prefix = "Files/" if identity.is_files else ""
    return f"{prefix}{identity.object_id.schema}"


def _identity(scope: ItemInstallationScope, identity: WeaverDocumentId) -> dict:
    return {
        **_scope(scope),
        "schema_name": _catalogue_schema(identity),
        "object_name": identity.object_id.object,
    }


def _described(source, all_documents, repository) -> dict:
    description = resolve_text(
        source.document.description,
        owner=source,
        documents=all_documents,
        aliases=repository.aliases,
    )
    lineage = resolve_text(
        source.document.lineage,
        owner=source,
        documents=all_documents,
        aliases=repository.aliases,
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


def _foreign_keys(source, identity, common, signature) -> list[dict]:
    rows = []
    for key in source.document.foreign_keys:
        reference = key.logical_reference or Reference(
            schema=key.reference.schema,
            object=key.reference.object,
        )
        parent_item = (
            WeaverItemId(reference.item_type, reference.item_name)
            if reference.is_item_qualified
            else identity.item
        )
        rows.append(
            {
                **common,
                "column_set": column_set(key.columns),
                "reference_item_type": parent_item.item_type,
                "reference_item_name": parent_item.item_name,
                "reference_schema_name": (
                    f"Files/{reference.schema}" if reference.is_files else reference.schema
                ),
                "reference_object_name": reference.object,
                "reference_column_set": column_set(key.reference_columns),
                "signature": signature,
            }
        )
    return rows
