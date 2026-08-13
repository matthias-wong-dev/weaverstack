"""Projecting one logical item's catalogue rows from a validated declaration.

Nothing here re-reads a source file, imports an object module, or asks a
physical table what shape it is: every value comes from the validated
declaration or its resolved graph.

Only bound items are projected. Objects owned by unbound items are out of scope
rather than deleted — projecting them would invite a comparison that removed
them.

Every row is stamped with the same item scope, passed in once, so no projector
can derive a different one and write into the wrong installation.

An alias is not a dependency. A dependency row records the reference as the
author wrote it; :data:`~weaver.catalogue.tables.ALIAS` records what the
consuming item's alias points at. Keeping them apart is what stops one item
appearing to depend directly on another's physical object.
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
    ObjectId,
    Reference,
)
from ..declaration.model import WeaverDocumentId, WeaverItemId, WeaverRepository
from ..declaration.references import declared_column_notes, resolve_text
from ..etl import PROCEDURE_TYPE, artefacts_by_identity, item_runtime_artefacts
from .claims import catalogue_schema
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
    TEST_DICTIONARY,
    CatalogueTable,
)

# Catalogue projection consumes the stable persisted target-kind vocabulary; it
# does not depend on build-package binding classes. Keeping these values here
# also prevents importing the build package while catalogue reconciliation is
# still initialising.
LAKEHOUSE_TARGET = "lakehouse"
WAREHOUSE_TARGET = "warehouse"

#: Map Weaver document kinds to the catalogue's lower-case vocabulary.
OBJECT_TYPE_FOR_KIND = {FOLDER: "folder", TABLE: "table", VIEW: "view"}

#: How a validation kind names itself in ``TestDictionary.test_type``. A
#: separate vocabulary from the Registry's ``object_role``, because the logical
#: declaration and the physical primitive are different things — the dictionary
#: describes the Test, the Registry certifies the procedure or module it
#: compiled to.
TEST_TYPE_FOR_KIND = {TEST: "test", ASSUMPTION: "assumption"}


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


def project_item_catalogue(
    repository: WeaverRepository,
    *,
    item: WeaverItemId,
    retained: Iterable[WeaverDocumentId],
) -> CatalogueProjection:
    """One item's catalogue rows, from the declaration and nothing else.

    Every value here is a function of *source*: what the item declares, what it
    aliases, what its documents describe. Nothing about a build, a binding or a
    target reaches it. That is what makes the projection something a developer
    keeps correct by adding a declaration, rather than a fixture someone has to
    remember to update alongside one.

    Not an *installation* projection, despite what this was once called. It says
    what the repository declares; whether any of it has been installed, where,
    and by which Weaver are separate facts composed at publication.

    **No Registry row is written for an alias destination.** The Alias row —
    this name points at that object — is a declaration and belongs here. The
    Registry row is a certification that a physical object exists at that name
    *and what it is*, and an alias is a view in a Warehouse and a table in a
    Lakehouse. That cannot be answered without a binding, so it is not answered
    here; see :func:`project_alias_registry`.
    """

    scope = InstallationScope(item.item_type, item.item_name)
    retained = tuple(sorted(set(retained), key=str))
    if any(identity.item != item for identity in retained):
        raise ValueError(f"item projection {item} received a document owned elsewhere")

    # ``retained`` carries every kind of registered object. Splitting them here
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
    installed = artefacts_by_identity(item_runtime_artefacts(repository, item=item))
    retained_artefacts = tuple(
        installed[identity] for identity in retained if identity in installed
    )
    # A validation carries the item's ordinary logical identity and is not a data
    # object, so it is separated here for the same reason an alias is: what
    # follows projects tables, columns and keys, and a Test has none of them.
    retained_validations = tuple(
        identity
        for identity in retained
        if identity not in alias_by_destination
        and identity not in installed
        and repository.source_documents[identity].is_validation
    )
    validation_set = set(retained_validations)
    retained = tuple(
        identity
        for identity in retained
        if identity not in alias_by_destination
        and identity not in installed
        and identity not in validation_set
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

    # A validation claims TestDictionary and its dependencies, and nothing else.
    # In particular it claims no Registry row: Registry certifies a physical
    # object that exists, and nothing is materialised under the logical Test ID.
    # What *is* certified is the procedure or module the validation compiles to,
    # and that artefact has an identity of its own.
    for identity in retained_validations:
        source = repository.source_documents[identity]
        document = source.document
        rows[TEST_DICTIONARY.name].append(
            {
                **_identity(scope, identity),
                "test_type": TEST_TYPE_FOR_KIND[document.kind],
                **_described(source, all_documents, repository),
                # Correlation information about a Test, and structurally absent
                # from an Assumption, which has one side to correlate.
                "primary_key": column_set(document.primary_key) or None,
                "signature": source.effective_signature,
            }
        )

    # A runtime artefact claims the Registry and nothing else. It has no columns
    # to describe, no keys to record and no dependencies to keep — it is a
    # deployed module or a generated statement, and what the catalogue knows
    # about it is that Weaver installed it, what it is for, and at what
    # signature.
    #
    # The role is the artefact's own. A Test module and a load module are the
    # same shape, so this row is the only place the difference survives, and
    # everything downstream that must not run a Test as a load reads it here.
    for artefact in retained_artefacts:
        rows[REGISTRY.name].append(
            {
                **_identity(scope, artefact.identity),
                "object_type": artefact.object_type,
                "object_role": artefact.role,
                "signature": artefact.signature,
            }
        )

    # A validation's dependencies belong to its logical identity, exactly as an
    # object's do. That is what lets the graph say Sales.Orders precedes
    # Sales.OrdersUpToDate without a Registry row standing in for the Test.
    consumers = set(retained) | validation_set
    for edge in repository.dependency_edges:
        if edge.consumer not in consumers:
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
            # A validation names a schema the item declares, and putting one to
            # use is what makes it a schema the installation uses.
            for identity in retained + retained_validations
        }
        | {
            (_catalogue_schema(alias.destination), alias.destination.object_id.schema)
            for alias in retained_aliases
        }
        # A generated load procedure puts a schema into use that no document
        # declares an object in, so it would otherwise be a schema the
        # installation uses and does not describe. A deployed file contributes
        # nothing here: its schema half is a path, and the namespace it sits in
        # is described by the folder document that owns the tree.
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


def project_alias_registry(
    repository: WeaverRepository,
    *,
    item: WeaverItemId,
    retained: Iterable[WeaverDocumentId],
    target_kind: str,
) -> tuple[Row, ...]:
    """Registry rows certifying this item's alias destinations, given a binding.

    Separate from :func:`project_item_catalogue` because it is the one part of an
    item's catalogue that cannot be derived from source: an alias is registered
    as the thing it physically *is*, and that depends on what it was bound to.

    Requiring the kind rather than defaulting it is deliberate. A default would
    write a *wrong* Registry row quietly — a Warehouse alias recorded as a table —
    and a wrong certification is the one failure the catalogue must never produce
    on its own.
    """

    scope = InstallationScope(item.item_type, item.item_name)
    wanted = set(retained)
    return tuple(
        {
            **_identity(scope, alias.destination),
            "object_type": _alias_object_type(alias.destination, target_kind),
            "object_role": ROLE_DATA,
            "signature": alias.signature,
        }
        for alias in sorted(
            (
                alias
                for alias in repository.aliases
                if alias.destination.item == item and alias.destination in wanted
            ),
            key=lambda alias: str(alias.destination),
        )
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
    return catalogue_schema(identity)


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
