"""Projecting one installation's catalogue rows from validated SES.

This is the boundary the whole design turns on. On one side is a repository that
has already been read, validated and closed, and a build projection that has
already decided which objects this target actually gets. On the other side are
rows. Nothing here re-reads a source file, imports an object module, or asks a
physical table what shape it is — every value comes from the validated
declaration or from the repository's own resolved graph.

**Only the retained subgraph is projected.** A build binds one physical side, and
the planner has already dropped everything whose target is unbound plus anything
stranded above it. Those omitted objects are *out of scope*, not deleted: a
Lakehouse build has no opinion at all about the Warehouse installation, and
projecting them would invite a comparison that removed them.

**Every row is stamped with the same installation scope.** The scope is passed in
once and applied to every row, rather than each projector deriving it — a
projector that derived it differently would silently write into the wrong
installation, which the renderer then refuses. Belt and braces, and the braces are
here.

**A cross-engine alias is not a dependency.** Where an object's reference resolves
through a ``Warehouse alias`` or ``Lakehouse alias``, the dependency row records
the *alias* name, which is the name that binds in the owner's own namespace, and
:data:`~weaver.catalogue.tables.ALIAS` records what that alias points at. Joining
Dependency, Alias and Registry is what yields the estate's whole graph; keeping
them apart is what stops a Warehouse object appearing to depend directly on a
Delta table.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from ..ses.metadata import FOLDER, FOLDER_TARGET, TABLE, VIEW, ObjectId
from ..ses.references import ResolvedText, declared_column_notes, resolve_text
from ..ses.repository import SesRepository
from ..ses.source import SourceDocument
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
    target_type_for_ses_target,
)

#: How an SES kind names itself in the catalogue. Deliberately a translation
#: rather than a reuse: SES kinds are title case and the catalogue's vocabulary is
#: lower case, and pinning the mapping here means a new SES kind must be given a
#: catalogue meaning rather than leaking one.
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


def project_installation(
    repository: SesRepository,
    *,
    retained: Iterable[str],
    scope: InstallationScope,
    target_name: str,
    weaver_version: str,
) -> CatalogueProjection:
    """Project one repository, as installed against one physical target.

    ``retained`` is the planner's retained node ids — the coherent subgraph this
    build actually materialises. ``target_name`` is the bound item's resolved
    name, which is an attribute of the installation and never part of its
    identity.
    """

    retained = list(retained)
    documents = [repository.by_id[node] for node in retained]
    _check_target_types(documents, scope)

    rows: dict[str, list[Row]] = {table.name: [] for table in CATALOGUE_TABLES}
    documents_for_references = repository.documents

    for document in sorted(documents, key=lambda each: each.node_id):
        signature = document.source_hash
        rows[REGISTRY.name].append(_registry_row(document, scope, signature))
        if document.target_kind == FOLDER_TARGET:
            rows[FOLDER_DICTIONARY.name].append(
                _folder_row(document, scope, signature, documents_for_references)
            )
        else:
            rows[TABLE_DICTIONARY.name].append(
                _table_row(document, scope, signature, documents_for_references)
            )
        rows[COLUMN_DICTIONARY.name].extend(
            _column_rows(document, scope, signature, documents_for_references)
        )
        rows[INDEX_DICTIONARY.name].extend(_index_rows(document, scope, signature))
        rows[FOREIGN_KEY_DICTIONARY.name].extend(
            _foreign_key_rows(document, scope, signature, repository.name)
        )
        rows[ALIAS.name].extend(_alias_rows(document, scope, signature))
        rows[DEPENDENCY.name].extend(
            _external_dependency_rows(document, scope, signature)
        )

    rows[DEPENDENCY.name].extend(
        _managed_dependency_rows(repository, retained, scope)
    )
    rows[SCHEMA_DICTIONARY.name].extend(
        _schema_rows(repository, documents, scope)
    )
    rows[INSTALLATION.name].append(
        {
            **_scope_of(scope),
            "target_name": target_name,
            "weaver_version": weaver_version,
            "signature": repository.signature,
        }
    )

    return CatalogueProjection(
        scope=scope,
        rows={name: tuple(table_rows) for name, table_rows in rows.items()},
    )


def _check_target_types(
    documents: Iterable[SourceDocument], scope: InstallationScope
) -> None:
    """Every retained object must install into the scope being projected.

    The planner's single-binding rule already guarantees it, so a violation here
    means projection is being handed a subgraph from a different build. Cheap to
    assert, and the consequence of not asserting is a row written into the wrong
    installation.
    """

    wrong = [
        document.node_id
        for document in documents
        if target_type_for_ses_target(document.target_kind) != scope.target_type
    ]
    if wrong:
        raise ValueError(
            f"projecting installation {scope} but {len(wrong)} retained object(s) "
            f"install elsewhere: {', '.join(sorted(wrong)[:3])}"
        )


def _scope_of(scope: InstallationScope) -> dict[str, object]:
    return {"repository": scope.repository, "target_type": scope.target_type}


def _identity(document: SourceDocument, scope: InstallationScope) -> dict[str, object]:
    return {
        **_scope_of(scope),
        "schema_name": document.object_id.schema,
        "object_name": document.object_id.object,
    }


def _described(
    text, document: SourceDocument, documents: Iterable[SourceDocument], *, prefix: str
) -> dict[str, object]:
    """One piece of metadata as its prose and the pointer it was copied from."""

    resolved: ResolvedText = resolve_text(text, owner=document, documents=documents)
    return {prefix: resolved.literal, f"{prefix}_reference": resolved.reference}


# --- per-object rows ---------------------------------------------------------


def _registry_row(
    document: SourceDocument, scope: InstallationScope, signature: str
) -> Row:
    return {
        **_identity(document, scope),
        "object_type": OBJECT_TYPE_FOR_KIND[document.kind],
        # Everything Weaver builds today holds or shapes data. `load` arrives with
        # stored procedures, which do work rather than hold rows.
        "object_role": ROLE_DATA,
        "signature": signature,
    }


def _table_row(
    document: SourceDocument,
    scope: InstallationScope,
    signature: str,
    documents: Iterable[SourceDocument],
) -> Row:
    ses = document.document
    return {
        **_identity(document, scope),
        "object_type": OBJECT_TYPE_FOR_KIND[document.kind],
        **_described(ses.description, document, documents, prefix="description"),
        **_described(ses.lineage, document, documents, prefix="lineage"),
        "primary_key": column_set(ses.primary_key),
        "not_null_columns": column_set(ses.declared_not_null),
        "identity_column": ses.identity,
        "comparison_columns": column_set(ses.comparison_columns),
        "is_incremental": ses.is_incremental,
        "is_static": ses.static,
        "prohibit_rebuild": ses.prohibit_rebuild,
        "signature": signature,
    }


def _folder_row(
    document: SourceDocument,
    scope: InstallationScope,
    signature: str,
    documents: Iterable[SourceDocument],
) -> Row:
    ses = document.document
    return {
        **_identity(document, scope),
        **_described(ses.description, document, documents, prefix="description"),
        **_described(ses.lineage, document, documents, prefix="lineage"),
        "file_key": column_set(ses.file_keys),
        "is_incremental": ses.is_incremental,
        "is_static": ses.static,
        "prohibit_rebuild": ses.prohibit_rebuild,
        "signature": signature,
    }


def _column_rows(
    document: SourceDocument,
    scope: InstallationScope,
    signature: str,
    documents: Iterable[SourceDocument],
) -> list[Row]:
    """Only the columns an author described, plus Weaver's own surrogate.

    Not every column of every object: the dictionary is descriptive, and a
    query-shaped object's full column list is not knowable when the bundle is
    generated. Whether every column *should* carry a note is a quality question to
    ask of this table, not a precondition for filling it.
    """

    identity_name = document.document.identity
    rows: list[Row] = []
    for name, note in declared_column_notes(document):
        resolved = resolve_text(note, owner=document, documents=documents)
        rows.append(
            {
                **_identity(document, scope),
                "column_name": name,
                "description": resolved.literal,
                "description_reference": resolved.reference,
                "is_identity": name == identity_name,
                "signature": signature,
            }
        )
    return rows


def _index_rows(
    document: SourceDocument, scope: InstallationScope, signature: str
) -> list[Row]:
    ses = document.document
    rows: list[Row] = []
    if ses.primary_key:
        rows.append(
            {
                **_identity(document, scope),
                "index_type": KEY_PRIMARY,
                "column_set": column_set(ses.primary_key),
                "signature": signature,
            }
        )
    for unique_key in ses.unique_keys:
        rows.append(
            {
                **_identity(document, scope),
                "index_type": KEY_UNIQUE,
                "column_set": column_set(unique_key),
                "signature": signature,
            }
        )
    return rows


def _foreign_key_rows(
    document: SourceDocument,
    scope: InstallationScope,
    signature: str,
    repository_name: str,
) -> list[Row]:
    """Declared relationships, with the parent as a logical two-part name.

    The parent's repository is the owner's own: SES declares a parent in two
    parts, so a relationship cannot yet cross repositories. The column is carried
    anyway so the shape does not change when it can.
    """

    return [
        {
            **_identity(document, scope),
            "column_set": column_set(key.columns),
            "reference_repository": repository_name,
            "reference_schema_name": key.reference.schema,
            "reference_object_name": key.reference.object,
            "reference_column_set": column_set(key.reference_columns),
            "signature": signature,
        }
        for key in document.document.foreign_keys
    ]


def _alias_rows(
    document: SourceDocument, scope: InstallationScope, signature: str
) -> list[Row]:
    """The cross-engine name this object publishes, if it publishes one.

    The row belongs to the *owner's* installation, because the alias is a property
    of the owner's declaration. So a Lakehouse build records the Warehouse alias
    of its Delta tables even though it builds no Warehouse object — which is
    right: the fact was declared, and the Warehouse installation may not exist
    yet.
    """

    rows: list[Row] = []
    for alias, alias_target_type in (
        (document.warehouse_alias, "warehouse"),
        (document.lakehouse_alias, "lakehouse"),
    ):
        if alias is None:
            continue
        rows.append(
            {
                **_identity(document, scope),
                "alias_target_type": alias_target_type,
                "alias_schema_name": alias.schema,
                "alias_object_name": alias.object,
                "signature": signature,
            }
        )
    return rows


def _external_dependency_rows(
    document: SourceDocument, scope: InstallationScope, signature: str
) -> list[Row]:
    """Three-part references — a physical target the author named deliberately.

    These are real dependencies and are recorded as such, with
    ``is_within_repository`` false: the first part is a physical item, not a
    repository, so nothing in this repository resolves them and nothing should try.

    Four-part names are skipped. The catalogue's reference has three parts and a
    workspace has nowhere to go, so recording one would mean discarding the part
    that made it unambiguous.
    """

    rows: list[Row] = []
    seen: set[tuple[str, str, str]] = set()
    for reference in document.qualified_references:
        if len(reference.parts) != 3:
            continue
        item, schema, name = reference.parts
        if (item, schema, name) in seen:
            continue
        seen.add((item, schema, name))
        rows.append(
            {
                **_identity(document, scope),
                "dependency_repository": item,
                "dependency_schema_name": schema,
                "dependency_object_name": name,
                "is_within_repository": False,
                "signature": signature,
            }
        )
    return rows


def _managed_dependency_rows(
    repository: SesRepository, retained: Iterable[str], scope: InstallationScope
) -> list[Row]:
    """Resolved edges of the repository graph, for retained consumers.

    The reference recorded is the name that binds in the consumer's own
    namespace — the producer's own name for a native edge, and the *alias* name
    where resolution crossed engines. That keeps every dependency row
    same-namespace by construction, and leaves crossing to Alias.
    """

    retained = set(retained)
    documents = repository.by_id
    aliases = _alias_names(repository)
    rows: list[Row] = []
    for edge in repository.dependency_edges:
        if edge.consumer not in retained:
            continue
        consumer = documents[edge.consumer]
        reference = _reference_of(edge, documents, aliases)
        if reference is None:  # pragma: no cover - every edge resolves by construction
            continue
        rows.append(
            {
                **_identity(consumer, scope),
                "dependency_repository": repository.name,
                "dependency_schema_name": reference.schema,
                "dependency_object_name": reference.object,
                "is_within_repository": True,
                "signature": consumer.source_hash,
            }
        )
    return rows


def _alias_names(repository: SesRepository) -> dict[tuple[str, str], ObjectId]:
    """Published alias names by (publisher node id, resolution kind).

    An alias edge records the alias, not the native producer, and the edge itself
    only says which kind of alias resolved it — so the name is looked up from the
    repository's own alias registers rather than reconstructed.
    """

    found: dict[tuple[str, str], ObjectId] = {}
    for alias, document in repository.lakehouse_aliases.items():
        found[(document.node_id, "lakehouse_alias")] = alias
    for alias, document in repository.warehouse_aliases.items():
        found[(document.node_id, "warehouse_alias")] = alias
    return found


def _reference_of(edge, documents, aliases) -> ObjectId | None:
    if edge.resolution_kind == "native":
        return documents[edge.producer].object_id
    return aliases.get((edge.producer, edge.resolution_kind))


def _schema_rows(
    repository: SesRepository,
    documents: Iterable[SourceDocument],
    scope: InstallationScope,
) -> list[Row]:
    """One row per schema the retained objects actually use.

    A schema the repository declares but this installation does not use is not
    projected — the row would claim the schema is part of an installation that
    never created it.
    """

    used = sorted({document.object_id.schema for document in documents})
    rows: list[Row] = []
    for schema_id in used:
        schema = repository.schemas[schema_id]
        rows.append(
            {
                **_scope_of(scope),
                "schema_name": schema_id,
                "description": schema.description,
                # A schema's Description is plain text — it has no reference form —
                # so there is never a pointer to record. The column is kept so the
                # shape matches every other described row.
                "description_reference": None,
                "signature": schema.source_hash,
            }
        )
    return rows
