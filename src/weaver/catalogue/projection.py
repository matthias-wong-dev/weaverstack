"""Projecting one logical item's catalogue rows from a validated declaration.

This is the boundary the whole design turns on. On one side is a declaration
that has already been read, validated and closed, and a build that has already
decided which items it is installing. On the other side are rows. Nothing here
re-reads a source file, imports an object module, or asks a physical table what
shape it is — every value comes from the validated declaration or from the
declaration's own resolved graph.

**Only bound items are projected.** Objects owned by unbound items are *out of
scope*, not deleted: a build has no opinion about an item it was not asked to
install, and projecting them would invite a comparison that removed them.

**Every row is stamped with the same item scope.** The scope is passed in once
and applied to every row, rather than each projector deriving it — a projector
that derived it differently would silently write into the wrong installation,
which the renderer then refuses.

**An alias is not a dependency.** A dependency row records the reference exactly
as the author wrote it, and :data:`~weaver.catalogue.tables.ALIAS` records what
the consuming item's alias points at. Joining Dependency, Alias and Registry is
what yields the estate's whole graph; keeping them apart is what stops one item
appearing to depend directly on another's physical object.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from ..build_bundle.targets import LAKEHOUSE_TARGET, WAREHOUSE_TARGET
from ..declaration.metadata import FOLDER, TABLE, VIEW, ObjectId, Reference
from ..declaration.model import WeaverDocumentId, WeaverItemId, WeaverRepository
from ..declaration.references import declared_column_notes, resolve_text
from .render import InstallationScope, Row, column_set
from .tables import (
    ALIAS,
    CATALOGUE_TABLES,
    COLUMN_DICTIONARY,
    DEPENDENCY,
    FOLDER_DICTIONARY,
    FOREIGN_KEY_DICTIONARY,
    INDEX_DICTIONARY,
    INSTALLATION,
    KEY_PRIMARY,
    KEY_UNIQUE,
    REGISTRY,
    ROLE_DATA,
    SCHEMA_DICTIONARY,
    TABLE_DICTIONARY,
    CatalogueTable,
)

#: How an Weaver document kind names itself in the catalogue. Deliberately a translation
#: rather than a reuse: Weaver document kinds are title case and the catalogue's vocabulary
#: is lower case, and pinning the mapping here means a new Weaver document kind must be given
#: a catalogue meaning rather than leaking one.
OBJECT_TYPE_FOR_KIND = {FOLDER: "folder", TABLE: "table", VIEW: "view"}


@dataclass(frozen=True)
class CatalogueProjection:
    """Every catalogue row one build invocation wants, for one installation."""

    scope: InstallationScope
    rows: Mapping[str, tuple[Row, ...]]

    def for_table(self, table: CatalogueTable) -> tuple[Row, ...]:
        return self.rows.get(table.name, ())

    @property
    def total(self) -> int:
        return sum(len(rows) for rows in self.rows.values())


def project_item_installation(
    repository: WeaverRepository,
    *,
    item: WeaverItemId,
    retained: Iterable[WeaverDocumentId],
    target_name: str,
    weaver_version: str,
    target_kind: str = LAKEHOUSE_TARGET,
) -> CatalogueProjection:
    scope = InstallationScope(item.item_type, item.item_name)
    retained = tuple(sorted(set(retained), key=str))
    if any(identity.item != item for identity in retained):
        raise ValueError(f"item projection {item} received a document owned elsewhere")

    # ``retained`` carries both kinds of registered object. Splitting them here
    # rather than at the call site keeps the caller from having to know which is
    # which — the repository already does.
    alias_by_destination = {
        alias.destination: alias
        for alias in repository.aliases
        if alias.destination.item == item
    }
    retained_aliases = tuple(
        alias_by_destination[identity]
        for identity in retained
        if identity in alias_by_destination
    )
    retained = tuple(
        identity for identity in retained if identity not in alias_by_destination
    )
    documents = [repository.source_documents[identity] for identity in retained]
    all_documents = tuple(repository.source_documents.values())
    rows: dict[str, list[dict]] = {table.name: [] for table in CATALOGUE_TABLES}

    for identity, source in zip(retained, documents):
        common = _identity(scope, identity)
        signature = source.effective_signature
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
                "signature": source.effective_signature,
            }
        )

    for alias in retained_aliases:
        rows[REGISTRY.name].append(
            {
                **_identity(scope, alias.destination),
                "object_type": _alias_object_type(alias.destination, target_kind),
                "object_role": ROLE_DATA,
                "signature": alias.signature,
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
                "signature": alias.signature,
            }
        )

    used_schemas = sorted(
        {
            (_catalogue_schema(identity), identity.object_id.schema)
            for identity in retained
        }
        | {
            (_catalogue_schema(alias.destination), alias.destination.object_id.schema)
            for alias in retained_aliases
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


def _alias_object_type(destination: WeaverDocumentId, target_kind: str) -> str:
    """What an alias destination physically *is*, in the catalogue's vocabulary.

    Not a type of its own. An alias is registered as the thing it actually is —
    a folder under ``Files``, a view in a Warehouse, a table in a Lakehouse —
    because to every reader of the catalogue that is what it is, and because the
    operations that matter (does it exist, how is it addressed, how is it
    dropped) are the ordinary ones for that type. That a Lakehouse table alias
    happens to be implemented as a OneLake shortcut is execution detail, the way
    a managed table's storage layout is.

    Its *alias-ness* is not lost: :data:`~weaver.catalogue.tables.ALIAS` records
    it, and that is the only place that does.
    """

    if destination.is_files:
        return OBJECT_TYPE_FOR_KIND[FOLDER]
    if target_kind == WAREHOUSE_TARGET:
        return OBJECT_TYPE_FOR_KIND[VIEW]
    return OBJECT_TYPE_FOR_KIND[TABLE]


def _scope(scope: InstallationScope) -> dict[str, str]:
    return dict(scope.values)


def _catalogue_schema(identity: WeaverDocumentId) -> str:
    prefix = "Files/" if identity.is_files else ""
    return f"{prefix}{identity.object_id.schema}"


def _identity(scope: InstallationScope, identity: WeaverDocumentId) -> dict:
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
