"""Projecting one logical item's catalogue rows from a prepared repository.

Nothing here re-reads a source file, imports an object module, or asks a
physical table what shape it is: every value comes from the validated
declaration or its resolved graph.

Only bound items are projected. Objects owned by unbound items are out of scope
rather than deleted. Projecting them would invite a comparison that removed them.

Every row is stamped with the same item scope, passed in once, so no projector
can derive a different one and write into the wrong installation.

A shortcut is not a dependency. A dependency row records the reference as the
author wrote it; :data:`~weaver.catalogue.tables.SHORTCUT` records what the
consuming item's shortcut points at. Keeping them apart is what stops one item
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

# Catalogue projection consumes the stable persisted target-kind vocabulary; it
# does not depend on build-package binding classes. Keeping these values here
# also prevents importing the build package while catalogue reconciliation is
# still initialising.
LAKEHOUSE_TARGET = "lakehouse"
WAREHOUSE_TARGET = "warehouse"

#: Map Weaver document kinds to the catalogue's lower-case vocabulary.
OBJECT_TYPE_FOR_KIND = {FOLDER: "folder", TABLE: "table", VIEW: "view"}

#: What a schema shortcut is registered as: the namespace it presents.
SCHEMA_TYPE = "schema"

#: How a validation kind names itself in ``TestDictionary.test_type``. Separate
#: from the Registry's ``object_role``: the dictionary describes the Test, the
#: Registry certifies the procedure or module it compiled to.
TEST_TYPE_FOR_KIND = {TEST: "test", ASSUMPTION: "assumption"}


@dataclass(frozen=True)
class CatalogueProjection:
    """Every catalogue row one build invocation needs, for one installation."""

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
    """One item's catalogue rows, from prepared logical intent and nothing else.

    Every value is a function of repository intent: authored declarations plus
    package-owned relations introduced during preparation. Nothing about a
    build, a binding or a target reaches it.

    It says what the repository declares; whether any of it is installed, where,
    and by which Weaver are composed at publication.

    No Registry row is written for a shortcut destination here. The logical
    relation belongs in this projection whether it is authored or package-owned;
    the Registry row certifies that a physical object exists at that name and
    what it is, which needs a binding. See :func:`project_shortcut_registry`.
    """

    scope = InstallationScope(item.item_type, item.item_name)
    retained = tuple(sorted(set(retained), key=str))
    if any(identity.item != item for identity in retained):
        raise ValueError(f"item projection {item} received a document owned elsewhere")

    # ``retained`` carries every kind of registered object. Splitting them here
    # rather than at the call site keeps the caller from having to know which is
    # which, because the repository already does.
    # Every declaration, not only the logical ones: a shortcut destination is
    # not a source document whichever kind of target it names, and what follows
    # projects tables, columns and keys that it has none of.
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
    # A validation carries the item's ordinary logical identity and is not a data
    # object, so it is separated here for the same reason a shortcut is: what
    # follows projects tables, columns and keys, and a Test has none of them.
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

    # A validation claims TestDictionary and its dependencies, and nothing else.
    # In particular it claims no Registry row: Registry certifies a physical
    # object that exists, and nothing is materialised under the logical Test ID.
    # What is certified is the procedure or module the validation compiles to,
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
    # to describe, no keys to record and no dependencies to keep. It is a deployed
    # module or a generated statement, and what the catalogue records
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
        producer = edge.producer
        rows[DEPENDENCY.name].append(
            {
                **_identity_as(scope, edge.consumer, role="referencing"),
                "dependency_reference": edge.reference,
                # Null where the edge did not resolve, being an authored physical
                # name or a reference that leaves the item through a shortcut.
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
                # The declaration as written, which is also the key, and the
                # same schema/object pair Registry names an object by.
                "shortcut_id": declaration.shortcut_id,
                # The destination identity as Registry stores it, so a Folder
                # keeps its ``Files/`` prefix and stays apart from a table of
                # the same Schema.Object. A schema shortcut names a namespace,
                # which has no area to prefix.
                "schema_name": (
                    declaration.schema
                    if declaration.is_schema
                    else _catalogue_schema(declaration.destination)
                ),
                # A schema shortcut presents a namespace, so it names no object.
                "object_name": (
                    None
                    if declaration.is_schema
                    else declaration.destination.object_id.object
                ),
                "shortcut_type": declaration.shortcut_type,
                "target_type": declaration.target_type,
                "target_item_type": declaration.target_item.item_type,
                "target_item_name": declaration.target_item.item_name,
                # A logical target is a Weaver document, so its identity is
                # stored whole and the same way Registry stores one. A Folder
                # source keeps its ``Files/`` prefix, which is what separates it
                # from a table of the same Schema.Object.
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

    # Package-owned logical shortcuts have no authored declaration, but an
    # installed operation has no repository to recover them from. Persist the
    # same producer pair an authored logical shortcut persists.
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
            # A validation names a schema the item declares, and putting one to
            # use is what makes it a schema the installation uses.
            for identity in retained + retained_validations
        }
        | {
            (
                _catalogue_schema(declaration.destination),
                declaration.destination.object_id.schema,
            )
            for declaration in retained_shortcuts
            # A schema shortcut presents the source item's namespace, so the
            # item does not own that schema and never declares it.
            if not declaration.is_schema
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


def project_shortcut_registry(
    repository: WeaverRepository,
    *,
    item: WeaverItemId,
    retained: Iterable[WeaverDocumentId],
    target_kind: str,
) -> tuple[Row, ...]:
    """Registry rows certifying this item's shortcut destinations, given a binding.

    Separate from :func:`project_item_catalogue` because it is the one part of
    an item's catalogue that source cannot derive: a shortcut is registered as
    what it physically is, which depends on its binding.

    The kind is required rather than defaulted, because a default would record a
    Warehouse view as a table.

    A schema shortcut is registered as the schema it presents, and what is inside
    one is not: those objects belong to the item the shortcut points at and can
    change without a build.
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
    """What a shortcut destination physically is, in the catalogue's vocabulary.

    Not a type of its own: a shortcut is registered as what it is, so existence,
    addressing and dropping are the ordinary operations for that type. That a
    Lakehouse table shortcut is a OneLake shortcut is execution detail.

    What it is for is the object role, which is ``shortcut``, and where it
    points is :data:`~weaver.catalogue.tables.SHORTCUT`.
    """

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
    """The two columns every catalogue table names an object by.

    A schema shortcut presents a namespace, so it names the schema and the
    schema is also what it installs: the object half repeats it rather than
    being null, because the Registry keys on both.
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
    """The owning object's identity under a relationship's own column names.

    A relationship table names both sides, so neither can be the unqualified
    ``schema_name``/``object_name`` pair every other table uses.
    """

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
                # Stored as Registry stores the object it names, area and all,
                # so the two sides of a relationship are looked up one way.
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
