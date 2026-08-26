"""Read, merge and validate a Weaver repository through a Store.

The first two directory levels identify the item type and logical item. The
owning item determines each SQL document's dialect.

Composition has one path. The authored tree, Weaver-owned content and generated
content are each read into a :class:`RepositoryPart`, combined through
:data:`merge_repository`, and only then validated, signed and resolved into a
:class:`~weaver.declaration.model.WeaverRepository`. Nothing is injected into a
partly-built repository afterwards.
"""

from __future__ import annotations

import ast
import hashlib
from dataclasses import dataclass, field, replace
from typing import Iterable, Mapping

from ..errors import DiscoveryError
from ..locations import Location
from ..store import FilesystemStore, Store
from .dependencies import PythonImport
from .graph import Graph
from .item_dependencies import resolve_item_dependencies
from .metadata import (
    ALIAS_KEYS,
    ASSUMPTION,
    DELTA_TARGET,
    FOLDER_TARGET,
    PYTHON,
    SQL_TARGET,
    TEST,
    ObjectId,
)
from .model import (
    FILES,
    ITEM_TYPES,
    LAKEHOUSE,
    WAREHOUSE,
    RepositoryShortcut,
    ShortcutDeclaration,
    WeaverDocumentId,
    WeaverItem,
    WeaverItemId,
    WeaverRepository,
    WeaverSchemaId,
)
from .references import validate_repository_metadata
from .programmable import PROGRAMMABLES_DIRECTORY, read_programmable
from .schemas import SchemaSes, read_schema_document
from .shortcuts import (
    LAKEHOUSE_FILE,
    SHORTCUT_FILES,
    WAREHOUSE_FILE,
    read_lakehouse_shortcuts,
    read_warehouse_shortcuts,
    validate_destinations,
)
from .source import (
    SourceDocument,
    language_for_filename,
    read_source_document,
)

#: Never read, never installed.
IGNORED_DIRECTORIES = frozenset(
    {
        "__pycache__",
        ".git",
        ".venv",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".idea",
    }
)
IGNORED_FILENAMES = frozenset({".DS_Store", "Thumbs.db"})
IGNORED_SUFFIXES = (".pyc", ".pyo", ".swp", ".orig", ".rej")

#: Validation source directories and their declared kinds.
VALIDATION_DIRECTORIES = {"tests": TEST, "assumptions": ASSUMPTION}

#: The subdirectories an item may author, for the message that lists them.
_AUTHORED_SUBDIRECTORIES = (
    "only schemas/, lib/, tests/, assumptions/, Warehouse programmables/ and "
    "Lakehouse Files/ are authored subdirectories of an item"
)


def _read_validation(
    relative: str,
    within: list[str],
    *,
    item: WeaverItemId,
    root: Location,
    store: Store,
    source_documents: dict[WeaverDocumentId, SourceDocument],
    validations_by_item: dict[WeaverItemId, list[WeaverDocumentId]],
) -> None:
    """Read one validation file into the repository."""

    directory = within[0]
    kind = VALIDATION_DIRECTORIES[directory]
    if len(within) != 2:
        raise DiscoveryError(
            f"{relative}: a {kind} lives directly under {directory}/, with no "
            "further subdirectories"
        )

    filename = within[1]
    language = language_for_filename(filename, item.item_type)
    if language is None:
        raise DiscoveryError(f"{relative}: not a Weaver validation file")
    if language == PYTHON and item.item_type != LAKEHOUSE:
        raise DiscoveryError(
            f"{relative}: Python validation runs through Spark, so it belongs to a "
            f"Lakehouse item. Write a {kind} for a {item.item_type} in SQL"
        )

    source = read_source_document(
        relative, store.read(root.join(*relative.split("/"))), item.item_type
    )
    if source.document.kind != kind:
        raise DiscoveryError(
            f"{relative}: {directory}/ declares a {kind}, and this file declares a "
            f"{source.document.kind}. Move it to "
            f"{_directory_for(source.document.kind)}/"
        )

    identity = WeaverDocumentId(item, source.object_id)
    source = replace(source, logical_id=identity)
    _insert_exact_case(source_documents, identity, source, relative, what="declaration")
    validations_by_item[item].append(identity)


def _directory_for(kind: str) -> str:
    for directory, declared in VALIDATION_DIRECTORIES.items():
        if declared == kind:
            return directory
    return "the item root"  # pragma: no cover - every validation kind has one


@dataclass(frozen=True)
class RepositoryPart:
    """The declarations one source contributes, awaiting merge.

    The pre-resolution container repository composition merges. It holds
    per-item declaration lists and the document maps behind them, plus the
    shortcut declarations the source contributes and, for generated content,
    the bytes of the declaration files it brought. An item may be assembled
    from several parts: authored declarations and the generated runtime tree
    that serves them meet here first.

    Parts are provenance-labelled so a merge collision can say what collided.
    """

    label: str
    items: tuple[WeaverItemId, ...] = ()
    documents: Mapping[WeaverItemId, tuple[WeaverDocumentId, ...]] = field(
        default_factory=dict
    )
    validations: Mapping[WeaverItemId, tuple[WeaverDocumentId, ...]] = field(
        default_factory=dict
    )
    schemas: Mapping[WeaverItemId, tuple[WeaverSchemaId, ...]] = field(
        default_factory=dict
    )
    source_documents: Mapping[WeaverDocumentId, SourceDocument] = field(
        default_factory=dict
    )
    schema_documents: Mapping[WeaverSchemaId, SchemaSes] = field(default_factory=dict)
    #: The stored procedures this part contributes, by owning item.
    programmables: Mapping[WeaverItemId, tuple] = field(default_factory=dict)
    shortcuts: tuple[ShortcutDeclaration, ...] = ()
    logical_shortcuts: tuple[RepositoryShortcut, ...] = ()
    #: Files whose bytes live in the store rather than in this part, which is
    #: the authored tree. The repository signature reads them back once.
    store_files: tuple[str, ...] = ()
    support_files: tuple[str, ...] = ()
    support_file_contents: Mapping[str, bytes] = field(default_factory=dict)
    #: Declaration bytes this part contributed by path, being everything not
    #: read from the store. The repository signature hashes them as declared.
    declared_files: Mapping[str, bytes] = field(default_factory=dict)


def merge_repository(*parts: RepositoryPart) -> RepositoryPart:
    """Combine repository parts into one, before resolution.

    The rules are deliberately simple. A unique identity is added; a duplicate
    identity is refused; identities differing only by case are refused. There
    is no precedence: no part overrides another, so a collision is always a
    fault to repair at the source. Item membership itself is not an identity a
    merge refuses, because composition is exactly how one item comes to hold
    both authored declarations and the generated ones that serve them.
    """

    documents: dict[WeaverDocumentId, SourceDocument] = {}
    schemas: dict[WeaverSchemaId, SchemaSes] = {}
    programmables: dict = {}
    shortcuts: dict[str, ShortcutDeclaration] = {}
    pairs: dict[str, RepositoryShortcut] = {}
    support: dict[str, bytes] = {}
    declared: dict[str, bytes] = {}
    store_files: set[str] = set()
    item_ids: set[WeaverItemId] = set()
    for part in parts:
        item_ids.update(part.items)
        for identity, value in part.source_documents.items():
            _merge_insert(documents, identity, value, part.label, what="declaration")
        for identity, value in part.schema_documents.items():
            _merge_insert(schemas, identity, value, part.label, what="schema")
        for item, contributed in part.programmables.items():
            for programmable in contributed:
                _merge_insert(
                    programmables,
                    programmable.identity,
                    (item, programmable),
                    part.label,
                    what="programmable",
                )
        for pair in part.logical_shortcuts:
            _merge_insert(
                pairs, str(pair.destination), pair, part.label, what="logical shortcut"
            )
        for declaration in part.shortcuts:
            _merge_insert(
                shortcuts,
                str(declaration.destination),
                declaration,
                part.label,
                what="shortcut",
            )
        for relative, content in part.support_file_contents.items():
            _merge_insert(support, relative, content, part.label, what="support file")
        for relative, content in part.declared_files.items():
            _merge_insert(
                declared, relative, content, part.label, what="declaration file"
            )
        store_files.update(part.store_files)
    return RepositoryPart(
        label="merged",
        items=tuple(sorted(item_ids)),
        documents=_by_item(parts, "documents", sorted(item_ids)),
        validations=_by_item(parts, "validations", sorted(item_ids)),
        schemas=_by_item(parts, "schemas", sorted(item_ids)),
        programmables=_programmables_by_item(programmables),
        source_documents=documents,
        schema_documents=schemas,
        shortcuts=tuple(
            sorted(
                shortcuts.values(),
                key=lambda declaration: str(declaration.destination),
            )
        ),
        logical_shortcuts=tuple(
            sorted(pairs.values(), key=lambda pair: str(pair.destination))
        ),
        store_files=tuple(sorted(store_files)),
        support_files=tuple(sorted(support)),
        support_file_contents=support,
        declared_files=declared,
    )


def _by_item(
    parts: tuple[RepositoryPart, ...], name: str, items: Iterable[WeaverItemId]
) -> dict[WeaverItemId, tuple]:
    """One per-item declaration list, accumulated across every part."""

    merged: dict[WeaverItemId, list] = {item: [] for item in items}
    for part in parts:
        for item, declarations in getattr(part, name).items():
            merged.setdefault(item, []).extend(declarations)
    return {item: tuple(found) for item, found in merged.items()}


def _programmables_by_item(programmables: dict) -> dict[WeaverItemId, tuple]:
    """The merged programmables, regrouped by owning item."""

    merged: dict[WeaverItemId, list] = {}
    for _identity, (item, programmable) in programmables.items():
        merged.setdefault(item, []).append(programmable)
    return {item: tuple(sorted(found, key=lambda each: str(each.identity))) for item, found in merged.items()}


def _merge_insert(destination: dict, identity, value, label: str, *, what: str) -> None:
    rendered = str(identity)
    for existing in destination:
        if str(existing) == rendered:
            raise DiscoveryError(
                f"{rendered} ({what}) is contributed twice: once by {label} content"
            )
        if str(existing).casefold() == rendered.casefold():
            raise DiscoveryError(
                f"{rendered} ({what}, {label}) and {existing} differ only by case "
                "and cannot coexist"
            )
    destination[identity] = value


def parse_item_repository(
    root: Location,
    *,
    store: Store | None = None,
) -> WeaverRepository:
    """Read the workspace declaration without executing authored code."""

    store = store or FilesystemStore()
    if not store.exists(root):
        raise DiscoveryError(f"repository root does not exist: {root}")
    if not store.is_directory(root):
        raise DiscoveryError(f"repository root is not a directory: {root}")

    owned = weaver_owned_content()
    authored = _read_authored_repository(root, store)
    generated = _generated_content(authored)
    merged = merge_repository(owned, authored, generated)
    return compose_repository(merged, root=root, store=store)


def weaver_owned_content() -> RepositoryPart:
    """The Weaver-owned declarations every repository is composed with.

    Today that is the built-in catalogue item. Read through the ordinary
    readers, so it enters the merge as repository content like any other and
    carries no injection path of its own.
    """

    from ..catalogue.builtin import BUILTIN_ITEM, item_repository_files

    files = item_repository_files()
    documents: dict[WeaverDocumentId, SourceDocument] = {}
    schemas: dict[WeaverSchemaId, SchemaSes] = {}
    documents_by_item: dict[WeaverItemId, list[WeaverDocumentId]] = {
        BUILTIN_ITEM: []
    }
    schemas_by_item: dict[WeaverItemId, list[WeaverSchemaId]] = {BUILTIN_ITEM: []}
    for relative, data in sorted(files.items()):
        if "/schemas/" in relative:
            schema = read_schema_document(relative, data)
            identity = WeaverSchemaId(BUILTIN_ITEM, schema.schema_id)
            schemas[identity] = schema
            schemas_by_item[BUILTIN_ITEM].append(identity)
            continue
        source = read_source_document(relative, data, BUILTIN_ITEM.item_type)
        # Folder declarations are stored under Files/.
        is_files = f"/{FILES}/" in relative
        identity = WeaverDocumentId(BUILTIN_ITEM, source.object_id, is_files=is_files)
        documents[identity] = replace(source, logical_id=identity)
        documents_by_item[BUILTIN_ITEM].append(identity)
    return RepositoryPart(
        label="package-owned",
        items=(BUILTIN_ITEM,),
        documents={item: tuple(found) for item, found in documents_by_item.items()},
        schemas={item: tuple(found) for item, found in schemas_by_item.items()},
        source_documents=documents,
        schema_documents=schemas,
        declared_files=files,
    )

def _read_authored_repository(root: Location, store: Store) -> RepositoryPart:
    """Read one repository tree as its authored declarations.

    Structure first, then documents, schemas, validations and shortcuts. What
    comes back is a part, not a repository: composition with Weaver-owned and
    generated content happens above this.
    """

    prefix = root.value.rstrip("/") + "/"
    entries: list[tuple[str, bool]] = []
    for entry in store.list(root, recursive=True):
        relative = entry.location.value[len(prefix) :]
        parts = relative.split("/")
        if (
            "_ignore" in parts
            or any(part in IGNORED_DIRECTORIES for part in parts)
            or parts[-1] in IGNORED_FILENAMES
            or parts[-1].endswith(IGNORED_SUFFIXES)
        ):
            continue
        entries.append((relative, entry.is_directory))

    builtin_prefix = str(_builtin_item())
    authored_builtin = sorted(
        relative
        for relative, _is_directory in entries
        if relative == builtin_prefix or relative.startswith(builtin_prefix + "/")
    )
    if authored_builtin:
        raise DiscoveryError(
            f"{authored_builtin[0]}: {builtin_prefix} is package-owned and must "
            "not be authored"
        )

    for relative, is_directory in entries:
        if not is_directory and relative.rsplit("/", 1)[-1] == "__init__.py":
            raise DiscoveryError(
                f"{relative}: user-authored __init__.py is not allowed; "
                "Weaver supplies package loading"
            )

    for surface in SHORTCUT_FILES:
        if any(relative == surface for relative, _ in entries):
            raise DiscoveryError(
                f"{surface} belongs to the item that declares it. Put it in "
                f"<ItemType>/<ItemName>/{surface}."
            )
    # Named by their exact retired spelling, so a repository still carrying one
    # is told what it has rather than what it should have had.
    for retired in ("alias.yml", "external.yml"):
        if any(relative.rsplit("/", 1)[-1] == retired for relative, _ in entries):
            raise DiscoveryError(
                f"{retired} has been replaced by shortcuts. A Lakehouse declares "
                f"them in {LAKEHOUSE_FILE}, and a Warehouse in {WAREHOUSE_FILE}."
            )

    invalid_roots = sorted(
        {
            relative.split("/", 1)[0]
            for relative, _ in entries
            if relative.split("/", 1)[0] not in ITEM_TYPES
        }
    )
    if invalid_roots:
        raise DiscoveryError(
            f"{invalid_roots[0]}: first directory must be exactly one of "
            + ", ".join(sorted(ITEM_TYPES))
        )

    for relative, is_directory in entries:
        if not is_directory:
            continue
        parts = relative.split("/")
        if len(parts) <= 2:
            continue
        item = WeaverItemId(parts[0], parts[1])
        within = parts[2:]
        if within == ["schemas"]:
            continue
        if within == [FILES] and item.item_type == LAKEHOUSE:
            continue
        if within[0] == "lib" and item.item_type == LAKEHOUSE:
            continue
        if within == [PROGRAMMABLES_DIRECTORY] and item.item_type == WAREHOUSE:
            continue
        if within[0] in VALIDATION_DIRECTORIES and len(within) == 1:
            continue
        raise DiscoveryError(f"{relative}: {_AUTHORED_SUBDIRECTORIES}")

    item_ids: set[WeaverItemId] = set()
    files: list[str] = []
    for relative, is_directory in entries:
        parts = relative.split("/")
        if len(parts) == 1:
            if is_directory and parts[0] in ITEM_TYPES:
                continue
            raise DiscoveryError(
                f"{relative}: the declaration root may contain only item type "
                "directories and _ignore/"
            )
        if parts[0] not in ITEM_TYPES:
            raise DiscoveryError(
                f"{relative}: first directory must be exactly one of "
                + ", ".join(sorted(ITEM_TYPES))
            )
        item = WeaverItemId(parts[0], parts[1])
        item_ids.add(item)
        if len(parts) == 2:
            if not is_directory:
                raise DiscoveryError(f"{relative}: an item must be a directory")
            continue
        if not is_directory:
            files.append(relative)

    source_documents: dict[WeaverDocumentId, SourceDocument] = {}
    schema_documents: dict[WeaverSchemaId, SchemaSes] = {}
    support_files: list[str] = []
    documents_by_item: dict[WeaverItemId, list[WeaverDocumentId]] = {
        item: [] for item in item_ids
    }
    schemas_by_item: dict[WeaverItemId, list[WeaverSchemaId]] = {
        item: [] for item in item_ids
    }
    #: Validation declarations, separate from materialised objects.
    validations_by_item: dict[WeaverItemId, list[WeaverDocumentId]] = {
        item: [] for item in item_ids
    }
    programmables_by_item: dict[WeaverItemId, list] = {
        item: [] for item in item_ids
    }

    shortcut_files: dict[WeaverItemId, str] = {}
    warehouse_shortcut_files: dict[WeaverItemId, str] = {}
    for relative in sorted(files):
        parts = relative.split("/")
        item = WeaverItemId(parts[0], parts[1])
        within = parts[2:]

        if within == [LAKEHOUSE_FILE]:
            # Declarations, not a Weaver document: read for what they say and
            # never executed here.
            if item.item_type != LAKEHOUSE:
                raise DiscoveryError(
                    f"{relative}: {LAKEHOUSE_FILE} declares OneLake shortcuts, "
                    "which belong to a Lakehouse item. A Warehouse declares "
                    f"its shortcuts in {WAREHOUSE_FILE}."
                )
            shortcut_files[item] = relative
            support_files.append(relative)
            continue

        if within == [WAREHOUSE_FILE]:
            if item.item_type != WAREHOUSE:
                raise DiscoveryError(
                    f"{relative}: {WAREHOUSE_FILE} declares Warehouse views, "
                    "which belong to a Warehouse item. A Lakehouse declares "
                    f"its shortcuts in {LAKEHOUSE_FILE}."
                )
            warehouse_shortcut_files[item] = relative
            support_files.append(relative)
            continue

        if within[0] == "lib":
            if item.item_type != LAKEHOUSE:
                raise DiscoveryError(f"{relative}: lib/ belongs to a Lakehouse item")
            if len(within) == 1:
                raise DiscoveryError(f"{relative}: lib must be a directory")
            support_files.append(relative)
            continue

        if within[0] == "schemas":
            if len(within) != 2 or not within[1].endswith(".yml"):
                raise DiscoveryError(
                    f"{relative}: schema declarations are schemas/<Schema>.yml"
                )
            schema = read_schema_document(
                relative, store.read(root.join(*relative.split("/")))
            )
            identity = WeaverSchemaId(item, schema.schema_id)
            _insert_exact_case(
                schema_documents, identity, schema, relative, what="schema"
            )
            schemas_by_item[item].append(identity)
            continue

        if within[0] == PROGRAMMABLES_DIRECTORY:
            if len(within) != 2:
                raise DiscoveryError(
                    f"{relative}: a programmable lives directly under "
                    f"{PROGRAMMABLES_DIRECTORY}/, with no further subdirectories"
                )
            programmable = read_programmable(
                relative,
                store.read(root.join(*relative.split("/"))),
                owner=item,
            )
            programmables_by_item[item].append(programmable)
            continue

        if within[0] in VALIDATION_DIRECTORIES:
            _read_validation(
                relative,
                within,
                item=item,
                root=root,
                store=store,
                source_documents=source_documents,
                validations_by_item=validations_by_item,
            )
            continue

        is_files = within[0] == FILES
        if is_files:
            if item.item_type != LAKEHOUSE:
                raise DiscoveryError(f"{relative}: Files/ belongs to a Lakehouse item")
            if len(within) != 2:
                raise DiscoveryError(
                    f"{relative}: Folder documents live directly under Files/"
                )
        elif len(within) != 1:
            raise DiscoveryError(f"{relative}: {_AUTHORED_SUBDIRECTORIES}")

        filename = within[-1]
        if language_for_filename(filename, item.item_type) is None:
            raise DiscoveryError(f"{relative}: not a Weaver object file")
        source = read_source_document(
            relative,
            store.read(root.join(*relative.split("/"))),
            item.item_type,
        )
        if source.warehouse_alias is not None or source.lakehouse_alias is not None:
            retired = " and ".join(sorted(ALIAS_KEYS))
            raise DiscoveryError(
                f"{relative}: the document-local {retired} headers have been "
                f"replaced by shortcuts. Declare them in {LAKEHOUSE_FILE} for a "
                f"Lakehouse item, or {WAREHOUSE_FILE} for a Warehouse one."
            )
        if item.item_type == LAKEHOUSE:
            expected = FOLDER_TARGET if is_files else DELTA_TARGET
        else:
            expected = SQL_TARGET
        if source.target_kind != expected:
            location = "Files/" if is_files else f"{item.item_type} item root"
            raise DiscoveryError(
                f"{relative}: {source.document.kind} in {source.language} does not "
                f"belong at the {location}"
            )
        identity = WeaverDocumentId(item, source.object_id, is_files=is_files)
        source = replace(source, logical_id=identity)
        _insert_exact_case(
            source_documents, identity, source, relative, what="document"
        )
        documents_by_item[item].append(identity)

    # Generate runtime declarations for items with load code.
    from ..etl import ETL_SCHEMA

    for item in sorted(item_ids):
        authored_into_schema = [
            str(schema)
            for schema in schemas_by_item[item]
            if schema.schema == ETL_SCHEMA
        ] + [
            source_documents[identity].relative_path
            for identity in documents_by_item[item]
            if identity.object_id.schema == ETL_SCHEMA
        ]
        if authored_into_schema:
            raise DiscoveryError(
                f"{sorted(authored_into_schema)[0]}: schema {ETL_SCHEMA!r} is "
                "generated Weaver infrastructure. It holds the runtime tree a "
                "load is deployed into and the schema generated load procedures "
                "live in, so an item may not author into it"
            )

    shortcuts = _read_item_declarations(
        root, store, shortcut_files, read=read_lakehouse_shortcuts
    ) + _read_item_declarations(
        root, store, warehouse_shortcut_files, read=read_warehouse_shortcuts
    )
    validate_destinations(
        shortcuts,
        documents=source_documents,
        schemas_by_item={
            item: [schema.schema for schema in declared]
            for item, declared in schemas_by_item.items()
        },
    )
    logical_shortcuts = _logical_pairs(
        shortcuts,
        source_documents=source_documents,
        schemas_by_item=schemas_by_item,
    )
    validate_repository_metadata(source_documents.values(), shortcuts=logical_shortcuts)

    # Held rather than re-read: a ``lib/`` file is deployed by the load layer, so
    # its bytes have to reach both the signature it is selected by and the
    # payload the bundle carries, and neither may reopen the repository.
    #
    # Every support file, not only the Python ones. A `.py` filter here was
    # reading across from the top level, where a Weaver document is python,
    # sql or yml. But `lib/` is an ordinary directory the runtime tree
    # reproduces verbatim, and a module that reads a data file beside it needs
    # that file to have travelled with it.
    support_file_contents = {
        relative: store.read(root.join(*relative.split("/")))
        for relative in sorted(support_files)
    }

    return RepositoryPart(
        label="authored",
        items=tuple(sorted(item_ids)),
        documents={
            item: tuple(sorted(found, key=str)) for item, found in documents_by_item.items()
        },
        validations={
            item: tuple(sorted(found, key=str))
            for item, found in validations_by_item.items()
        },
        schemas={
            item: tuple(sorted(found, key=str)) for item, found in schemas_by_item.items()
        },
        source_documents=source_documents,
        schema_documents=schema_documents,
        programmables={
            item: tuple(sorted(found, key=lambda each: str(each.identity)))
            for item, found in programmables_by_item.items()
            if found
        },
        shortcuts=shortcuts,
        logical_shortcuts=logical_shortcuts,
        store_files=tuple(files),
        support_files=tuple(support_files),
        support_file_contents=support_file_contents,
    )


def _generated_content(authored: RepositoryPart) -> RepositoryPart:
    """The declarations Weaver generates from one repository's authored content.

    Two contributions. The per-item runtime tree: a schema declaration and, for
    a Lakehouse, the folder document that owns its deployed files. And the
    standard Weaver catalogue surface: every normal item presents
    ``_.Installation`` and the operational tables as ordinary logical shortcut
    declarations, merged before resolution like any other content.
    """

    from ..catalogue.builtin import BUILTIN_ITEM, standard_surface_references
    from ..etl import generated_item_files

    generated_files: dict[str, bytes] = {}
    documents_by_item: dict[WeaverItemId, list[WeaverDocumentId]] = {}
    schemas_by_item: dict[WeaverItemId, list[WeaverSchemaId]] = {}
    source_documents: dict[WeaverDocumentId, SourceDocument] = {}
    schema_documents: dict[WeaverSchemaId, SchemaSes] = {}
    programmables_by_item: dict[WeaverItemId, list] = {}
    for item in sorted(authored.items):
        if item == BUILTIN_ITEM:
            continue
        documents = [
            authored.source_documents[identity]
            for identity in authored.documents.get(item, ())
            + authored.validations.get(item, ())
        ]
        # Validations also require generated runtime declarations.
        item_files = generated_item_files(
            item,
            documents=documents,
            support_paths=list(authored.support_files),
        )
        if item_files:
            for relative, data in sorted(item_files.items()):
                if "/schemas/" in relative:
                    schema = read_schema_document(relative, data)
                    identity = WeaverSchemaId(item, schema.schema_id)
                    schema_documents[identity] = schema
                    schemas_by_item.setdefault(item, []).append(identity)
                    continue
                source = read_source_document(relative, data, item.item_type)
                identity = WeaverDocumentId(item, source.object_id, is_files=True)
                source_documents[identity] = replace(source, logical_id=identity)
                documents_by_item.setdefault(item, []).append(identity)
            generated_files.update(item_files)

        from ..etl import item_generated_programmables

        generated_programmables = item_generated_programmables(
            item=item,
            documents=[
                document
                for document in documents
                if document.relative_path not in authored.declared_files
            ],
        )
        if generated_programmables:
            programmables_by_item[item] = list(generated_programmables)

    shortcuts: list = []
    pairs: list = []
    for item in sorted(authored.items):
        if item == BUILTIN_ITEM:
            continue
        declarations, logical_pairs = standard_surface_references(item)
        shortcuts.extend(declarations)
        pairs.extend(logical_pairs)

    return RepositoryPart(
        label="generated",
        items=tuple(
            sorted(
                set(documents_by_item) | set(schemas_by_item) | {item for item in authored.items if item != BUILTIN_ITEM}
            )
        ),
        documents={
            item: tuple(sorted(found, key=str)) for item, found in documents_by_item.items()
        },
        schemas={
            item: tuple(sorted(found, key=str)) for item, found in schemas_by_item.items()
        },
        source_documents=source_documents,
        schema_documents=schema_documents,
        programmables={
            item: tuple(sorted(found, key=lambda each: str(each.identity)))
            for item, found in programmables_by_item.items()
            if found
        },
        shortcuts=tuple(shortcuts),
        logical_shortcuts=tuple(pairs),
        declared_files=generated_files,
    )


def compose_repository(
    merged: RepositoryPart,
    *,
    root: Location,
    store: Store,
) -> WeaverRepository:
    """Sign and resolve a merged part into the final repository."""

    items: list[WeaverItem] = []
    for item_id in merged.items:
        schemas = tuple(sorted(merged.schemas.get(item_id, ()), key=str))
        documents = tuple(sorted(merged.documents.get(item_id, ()), key=str))
        validations = tuple(sorted(merged.validations.get(item_id, ()), key=str))
        programmables = merged.programmables.get(item_id, ())
        declared = {schema.schema for schema in schemas}
        # Validations must use an item-declared schema.
        for document_id in documents + validations:
            if document_id.object_id.schema not in declared:
                source = merged.source_documents[document_id]
                raise DiscoveryError(
                    f"{source.relative_path}: schema {document_id.object_id.schema!r} "
                    f"is not declared by item {item_id}"
                )
        items.append(
            WeaverItem(
                item_id,
                schemas=schemas,
                documents=documents,
                validations=validations,
                programmables=programmables,
            )
        )

    source_documents = _with_build_signatures(
        dict(merged.source_documents),
        support_files=merged.support_files,
        store=store,
        root=root,
    )

    items = [
        replace(
            model,
            signature=_item_signature(
                model,
                source_documents=source_documents,
                schema_documents=merged.schema_documents,
                support_files=merged.support_files,
                store=store,
                root=root,
            ),
        )
        for model in items
    ]

    repository = WeaverRepository(
        name=root.name,
        root=root,
        items=tuple(items),
        source_documents=source_documents,
        schema_documents=merged.schema_documents,
        programmables={
            programmable.identity: programmable
            for model in items
            for programmable in model.programmables
        },
        support_files=merged.support_files,
        support_file_contents=merged.support_file_contents,
        signature=_repository_signature(merged, store, root),
        logical_shortcuts=merged.logical_shortcuts,
        shortcuts=merged.shortcuts,
        generated_files=merged.declared_files,
    )
    return resolve_item_dependencies(repository)


def _repository_signature(part: RepositoryPart, store: Store, root: Location) -> str:
    """Hash every declaration file the repository is composed from."""

    from .source import content_hash

    digest = hashlib.sha256()
    declared = part.declared_files
    for relative in sorted(set(part.store_files) | set(declared)):
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        content = (
            declared[relative]
            if relative in declared
            else store.read(root.join(*relative.split("/")))
        )
        digest.update(content_hash(content).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def _with_build_signatures(
    documents: Mapping[WeaverDocumentId, SourceDocument],
    *,
    support_files: Iterable[str],
    store: Store,
    root: Location,
) -> dict[WeaverDocumentId, SourceDocument]:
    """Attach each document's own, statically reachable implementation hash.

    ``lib/`` is item-owned source, but hashing the whole directory into every
    object would let an unrelated helper rebuild the item. Each Python document
    carries the transitive closure of the helpers it can import instead, found
    statically: helper modules are parsed, never imported.
    """

    from .source import PYTHON, content_hash

    helper_paths: dict[WeaverItemId, dict[tuple[str, ...], str]] = {}
    helper_hashes: dict[str, str] = {}
    for relative in support_files:
        parts = relative.split("/")
        if len(parts) < 4 or parts[2] != "lib" or not relative.endswith(".py"):
            continue
        item = WeaverItemId(parts[0], parts[1])
        module = tuple(parts[2:-1] + [parts[-1][:-3]])
        helper_paths.setdefault(item, {})[module] = relative
        helper_hashes[relative] = content_hash(
            store.read(root.join(*relative.split("/")))
        )

    parsed_imports: dict[str, tuple[PythonImport, ...]] = {}

    def imports_for(relative: str) -> tuple[PythonImport, ...]:
        if relative in parsed_imports:
            return parsed_imports[relative]
        data = store.read(root.join(*relative.split("/")))
        try:
            text = data.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise DiscoveryError(f"{relative}: must be UTF-8 text ({exc})") from exc
        try:
            module = ast.parse(text)
        except SyntaxError as exc:
            raise DiscoveryError(
                f"{relative}: invalid imported helper Python: {exc}"
            ) from exc
        parsed_imports[relative] = _python_imports(module)
        return parsed_imports[relative]

    resolved: dict[WeaverDocumentId, SourceDocument] = {}
    for identity, source in documents.items():
        if source.language != PYTHON:
            resolved[identity] = replace(source, build_signature=source.source_hash)
            continue

        available = helper_paths.get(identity.item, {})
        current = _module_within_item(source.relative_path)
        pending = list(_helper_targets(source.python_imports, current, available))
        reached: set[tuple[str, ...]] = set()
        while pending:
            helper = pending.pop()
            if helper in reached:
                continue
            reached.add(helper)
            relative = available[helper]
            pending.extend(
                target
                for target in _helper_targets(imports_for(relative), helper, available)
                if target not in reached
            )

        if not reached:
            resolved[identity] = replace(source, build_signature=source.source_hash)
            continue
        digest = hashlib.sha256()
        entries = [(source.relative_path, source.source_hash)] + [
            (available[module], helper_hashes[available[module]])
            for module in sorted(reached)
        ]
        for relative, signature in entries:
            digest.update(relative.encode("utf-8"))
            digest.update(b"\0")
            digest.update(signature.encode("ascii"))
            digest.update(b"\n")
        resolved[identity] = replace(source, build_signature=digest.hexdigest())
    return resolved


def _module_within_item(relative: str) -> tuple[str, ...]:
    parts = relative.split("/")[2:]
    return tuple(parts[:-1] + [parts[-1][:-3]])


def _helper_targets(
    imports: Iterable[PythonImport],
    current: tuple[str, ...],
    available: Mapping[tuple[str, ...], str],
) -> tuple[tuple[str, ...], ...]:
    found: set[tuple[str, ...]] = set()
    package = current[:-1]
    for imported in imports:
        module = tuple(imported.module.split(".")) if imported.module else ()
        if imported.level:
            parents = imported.level - 1
            if parents > len(package):
                continue
            base = package[: len(package) - parents] + module
        else:
            base = module
        if base in available:
            found.add(base)
        for name in imported.names:
            candidate = base + tuple(name.split("."))
            if candidate in available:
                found.add(candidate)
    return tuple(sorted(found))


def _python_imports(module: ast.Module) -> tuple[PythonImport, ...]:
    imports: list[PythonImport] = []
    for node in ast.walk(module):
        if isinstance(node, ast.ImportFrom):
            imports.append(
                PythonImport(
                    module=node.module,
                    level=node.level,
                    names=tuple(imported.name for imported in node.names),
                )
            )
        elif isinstance(node, ast.Import):
            imports.extend(
                PythonImport(module=imported.name, names=(imported.name,))
                for imported in node.names
            )
    return tuple(imports)


def _item_signature(
    item: WeaverItem,
    *,
    source_documents: Mapping[WeaverDocumentId, SourceDocument],
    schema_documents: Mapping[WeaverSchemaId, SchemaSes],
    support_files: Iterable[str],
    store: Store,
    root: Location,
) -> str:
    """Certify exactly one logical item's authored and generated inputs.

    An item's ``shortcuts.py`` or ``shortcuts.yml`` sits under its own prefix and
    is certified with its other support files. The producer's content does not
    participate: a logical dependency does not make an independently installed
    producer part of the consumer's source item.
    """

    from .source import content_hash

    entries: list[tuple[str, str]] = []
    # Validation too: a changed Test is a changed item, and an item signature
    # that ignored it would leave an edited Test installed as the old one.
    for identity in item.declarations:
        source = source_documents[identity]
        entries.append((source.relative_path, source.source_hash))
    for identity in item.schemas:
        schema = schema_documents[identity]
        entries.append((schema.relative_path, schema.source_hash))
    # An authored programmable is item source: its content belongs to what the
    # signature certifies. A generated one signs itself, as every artefact does.
    for programmable in item.programmables:
        if programmable.relative_path is not None:
            entries.append(
                (programmable.relative_path, programmable.signature)
            )

    prefix = f"{item.identity.item_type}/{item.identity.item_name}/"
    for relative in support_files:
        if relative.startswith(prefix):
            entries.append(
                (
                    relative,
                    content_hash(store.read(root.join(*relative.split("/")))),
                )
            )
    digest = hashlib.sha256()
    digest.update(str(item.identity).encode("utf-8"))
    digest.update(b"\n")
    for relative, source_hash in sorted(entries):
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(source_hash.encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def _insert_exact_case(
    destination: dict,
    identity,
    value,
    relative: str,
    *,
    what: str,
) -> None:
    rendered = str(identity)
    for existing, existing_value in destination.items():
        if str(existing) == rendered:
            prior = getattr(existing_value, "relative_path", str(existing))
            raise DiscoveryError(
                f"{rendered} is declared twice: {prior} and {relative}"
            )
        if str(existing).casefold() == rendered.casefold():
            raise DiscoveryError(
                f"{rendered} and {existing} differ only by case and cannot coexist"
            )
    destination[identity] = value


def _builtin_item() -> WeaverItemId:
    """The reserved catalogue item identity, imported late to avoid a cycle."""

    from ..catalogue.builtin import BUILTIN_ITEM

    return BUILTIN_ITEM


def _read_item_declarations(root, store, files, *, read):
    """Read one declaration surface for every item that has it."""

    declarations = []
    for item in sorted(files):
        relative = files[item]
        try:
            text = store.read(root.join(*relative.split("/"))).decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise DiscoveryError(f"{relative}: must be UTF-8 text ({exc})") from exc
        declarations.extend(read(text, owner=item, relative=relative))
    return tuple(declarations)


def _logical_pairs(
    shortcuts,
    *,
    source_documents: Mapping[WeaverDocumentId, SourceDocument],
    schemas_by_item: Mapping[WeaverItemId, list[WeaverSchemaId]],
) -> tuple[RepositoryShortcut, ...]:
    """The logical pairs the ``logical`` shortcuts stand for.

    A logical shortcut names a Weaver document, so it resolves, orders and
    reports exactly as a logical reference always has. A physical one names a
    Fabric item and has no logical source, so it contributes nothing here and is
    planned from its declaration instead.
    """

    native_folded = {
        str(identity).casefold(): identity for identity in source_documents
    }
    pairs: list[RepositoryShortcut] = []
    for declaration in shortcuts:
        if not declaration.is_logical:
            continue
        destination = declaration.destination
        item = declaration.owner
        source = declaration.logical_source
        if source not in source_documents:
            case_match = native_folded.get(str(source).casefold())
            detail = f"; declared spelling is {case_match}" if case_match else ""
            raise DiscoveryError(
                f"{item}: logical target {source} is not a document in this "
                f"repository{detail}"
            )
        declared_schemas = {schema.schema for schema in schemas_by_item[item]}
        if destination.object_id.schema not in declared_schemas:
            raise DiscoveryError(
                f"{item}: {destination} sits in schema "
                f"{destination.object_id.schema!r}, which the item does not "
                "declare"
            )
        pairs.append(RepositoryShortcut(destination=destination, source=source))
    return tuple(pairs)


def _repository_files(store: Store, root: Location) -> list[str]:
    prefix = root.value.rstrip("/") + "/"
    relatives: list[str] = []
    for entry in store.list(root, recursive=True):
        if entry.is_directory:
            continue
        relative = entry.location.value[len(prefix) :]
        if _ignored(relative):
            continue
        relatives.append(relative)
    return sorted(relatives)


def _ignored(relative: str) -> bool:
    parts = relative.split("/")
    if any(part in IGNORED_DIRECTORIES for part in parts[:-1]):
        return True
    filename = parts[-1]
    return filename in IGNORED_FILENAMES or filename.endswith(IGNORED_SUFFIXES)


def importable_module_name(relative_path: str) -> str | None:
    """The full dotted module a repository-relative path is importable as.

    ``_helpers/dates.py`` is ``_helpers.dates``, not ``dates``. A nested module
    lives in its package's namespace and cannot shadow a top-level one.
    ``_helpers/__init__.py`` is the package itself, ``_helpers``.
    """

    if not relative_path.endswith(".py"):
        return None
    stem = relative_path[: -len(".py")]
    if stem.endswith("/__init__"):
        stem = stem[: -len("/__init__")]
    return stem.replace("/", ".")


# --- schema, namespace and shortcut resolution ---------------------------------


# --- the internal dependency graph -------------------------------------------


def _canonical(qualified: str) -> str:
    """Object identities are compared without regard to case.

    A developer may write `sales__order` where the house style is
    `Sales__Order`, and SQL is case-insensitive by nature. Two objects whose
    IDs differ only by case are refused, so the folding is unambiguous.
    """

    return qualified.lower()


def effective_dependencies(document: SourceDocument) -> tuple[ObjectId, ...]:
    """What this document depends on: declared if declared, else discovered.

    A declaration replaces discovery rather than adding to it, so an author can
    remove an edge as well as add one, because the phantom dependency an unused
    import creates has no other cure. ``Dependencies: []`` is such a declaration,
    so an
    explicit none suppresses discovery rather than falling back to it.

    One rule for every kind, validation included. What differs is only whether a
    kind is required to declare: a Spark SQL object is, because its query may
    read by path and a load ordered by a half-known graph builds things in the
    wrong order. A validation is not, because it reads objects that its own
    installation has already put in place. Validation runs after the load
    artefacts, and a validation never depends on another validation, so an edge
    inference missed costs an ordering nicety rather than a wrong estate.
    """

    if document.document.declares_dependencies:
        return document.declared_dependencies
    return document.referenced_object_ids


def _resolve(
    dependency: ObjectId,
    by_id: Mapping[str, list[SourceDocument]],
    referrer: SourceDocument,
) -> SourceDocument | None:
    """The object a two-part reference names, when that is unambiguous.

    A two-part name resolves in the namespace of whoever wrote it: T-SQL
    resolves inside the Warehouse, Spark SQL inside the Lakehouse. So the
    referrer's own target wins when it has a candidate, `join Sales.Customer`
    in a Warehouse query means the Warehouse's Sales.Customer, because that is
    what the SQL would actually bind to.

    Failing that, a single candidate anywhere is the answer, and it may cross a
    boundary: a Warehouse query reading a Delta table is the ordinary case, and
    the one the SQL endpoint and the shortcuts exist to bridge.

    Two candidates in neither of those positions is ambiguous and is
    left for the build, which has the targets and the shortcut bindings.
    """

    candidates = by_id.get(_canonical(dependency.qualified), [])
    if not candidates:
        return None
    own_target = [
        candidate
        for candidate in candidates
        if candidate.target_kind == referrer.target_kind
        and candidate.node_id != referrer.node_id
    ]
    if len(own_target) == 1:
        return own_target[0]
    elsewhere = [
        candidate for candidate in candidates if candidate.node_id != referrer.node_id
    ]
    return elsewhere[0] if len(elsewhere) == 1 else None


def _by_id(documents: Iterable[SourceDocument]) -> Mapping[str, list[SourceDocument]]:
    grouped: dict[str, list[SourceDocument]] = {}
    for document in documents:
        grouped.setdefault(_canonical(document.qualified), []).append(document)
    return grouped


def build_internal_graph(
    documents: Iterable[SourceDocument], *, external_names: Iterable[str] = ()
) -> Graph:
    """The graph over references that resolve within this repository.

    Nodes are ``target:Schema.Object``, because an ID alone is not unique.
    References resolving to nothing here, or to more than one thing, are left
    out entirely. They may be shortcuts, objects of another repository, or
    mistakes, and telling those apart needs the external-dependency
    configuration supplied at build.
    """

    documents = list(documents)
    by_id = _by_id(documents)
    known_external = {_canonical(name) for name in external_names}

    edges: list[tuple[str, str]] = []
    for document in documents:
        for dependency in effective_dependencies(document):
            if _canonical(dependency.qualified) in known_external:
                # Provided from outside, a boundary, not an edge within this graph.
                continue
            upstream = _resolve(dependency, by_id, document)
            if upstream is not None and upstream.node_id != document.node_id:
                edges.append((upstream.node_id, document.node_id))

    return Graph((document.node_id for document in documents), edges)


def unresolved_references(
    documents: Iterable[SourceDocument], *, external_names: Iterable[str] = ()
) -> dict[str, tuple[str, ...]]:
    """Per object, the references naming nothing in this repository.

    Recorded rather than refused: resolution needs the external-dependency
    configuration, and that is a build concern.
    """

    documents = list(documents)
    by_id = _by_id(documents)
    known_external = {_canonical(name) for name in external_names}
    unresolved: dict[str, tuple[str, ...]] = {}
    for document in documents:
        outside = tuple(
            dependency.qualified
            for dependency in effective_dependencies(document)
            if _canonical(dependency.qualified) not in known_external
            and _resolve(dependency, by_id, document) is None
        )
        physical = tuple(str(reference) for reference in document.qualified_references)
        if outside or physical:
            unresolved[document.node_id] = outside + physical
    return unresolved
