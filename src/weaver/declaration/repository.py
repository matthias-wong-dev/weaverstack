"""Read and validate a Weaver repository through a Store.

The first two directory levels identify the item type and logical item. The
owning item determines each SQL document's dialect.
"""

from __future__ import annotations

import ast
import hashlib
from dataclasses import replace
from typing import Iterable, Mapping

from ..errors import DiscoveryError
from ..locations import Location
from ..store import FilesystemStore, Store
from .dependencies import PythonImport
from .graph import Graph
from .item_dependencies import resolve_item_dependencies
from .metadata import (
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
    WeaverDocumentId,
    WeaverItem,
    WeaverItemId,
    WeaverRepository,
    WeaverSchemaId,
)
from .references import validate_repository_metadata
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
    "only schemas/, lib/, tests/, assumptions/ and Lakehouse Files/ are authored "
    "subdirectories of an item"
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

    from ..catalogue.builtin import BUILTIN_ITEM, item_repository_files

    generated_files = item_repository_files()
    builtin_prefix = "Warehouse/_weaver"
    authored_builtin = sorted(
        relative
        for relative, _is_directory in entries
        if relative == builtin_prefix or relative.startswith(builtin_prefix + "/")
    )
    if authored_builtin:
        raise DiscoveryError(
            f"{authored_builtin[0]}: Warehouse/_weaver is package-owned and must "
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
    retired = ("shortcut.yml", "external.yml")
    for name in retired:
        if any(relative.rsplit("/", 1)[-1] == name for relative, _ in entries):
            raise DiscoveryError(
                f"{name} has been replaced. A Lakehouse declares its shortcuts "
                f"in {LAKEHOUSE_FILE}, and a Warehouse in {WAREHOUSE_FILE}."
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
        if (
            source.warehouse_shortcut is not None
            or source.lakehouse_shortcut is not None
        ):
            raise DiscoveryError(
                f"{relative}: document-local Warehouse shortcut/Lakehouse shortcut "
                f"headers have been replaced. Declare shortcuts in "
                f"{LAKEHOUSE_FILE} or {WAREHOUSE_FILE}."
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

    builtin_item = BUILTIN_ITEM
    item_ids.add(builtin_item)
    documents_by_item[builtin_item] = []
    schemas_by_item[builtin_item] = []
    for relative, data in sorted(generated_files.items()):
        if "/schemas/" in relative:
            schema = read_schema_document(relative, data)
            identity = WeaverSchemaId(builtin_item, schema.schema_id)
            schema_documents[identity] = schema
            schemas_by_item[builtin_item].append(identity)
            continue
        source = read_source_document(relative, data, builtin_item.item_type)
        # Folder declarations are stored under Files/.
        is_files = f"/{FILES}/" in relative
        identity = WeaverDocumentId(builtin_item, source.object_id, is_files=is_files)
        source_documents[identity] = replace(source, logical_id=identity)
        documents_by_item[builtin_item].append(identity)

    # Generate runtime declarations for items with load code.
    from ..etl import ETL_SCHEMA, generated_item_files

    for item in sorted(item_ids):
        if item == builtin_item:
            continue
        authored = [
            str(schema)
            for schema in schemas_by_item[item]
            if schema.schema == ETL_SCHEMA
        ] + [
            source_documents[identity].relative_path
            for identity in documents_by_item[item]
            if identity.object_id.schema == ETL_SCHEMA
        ]
        if authored:
            raise DiscoveryError(
                f"{sorted(authored)[0]}: schema {ETL_SCHEMA!r} is generated Weaver "
                "infrastructure — it holds the runtime tree a load is deployed "
                "into and the schema generated load procedures live in, so an "
                "item may not author into it"
            )

    for item in sorted(item_ids):
        item_files = generated_item_files(
            item,
            # Validations also require generated runtime declarations.
            documents=[
                source_documents[identity]
                for identity in documents_by_item[item]
                + validations_by_item.get(item, [])
            ],
            support_paths=support_files,
        )
        if not item_files:
            continue
        generated_files = {**generated_files, **item_files}
        for relative, data in sorted(item_files.items()):
            if "/schemas/" in relative:
                schema = read_schema_document(relative, data)
                identity = WeaverSchemaId(item, schema.schema_id)
                schema_documents[identity] = schema
                schemas_by_item[item].append(identity)
                continue
            source = read_source_document(relative, data, item.item_type)
            identity = WeaverDocumentId(item, source.object_id, is_files=True)
            source_documents[identity] = replace(source, logical_id=identity)
            documents_by_item[item].append(identity)

    items: list[WeaverItem] = []
    for item_id in sorted(item_ids):
        schemas = tuple(sorted(schemas_by_item[item_id], key=str))
        documents = tuple(sorted(documents_by_item[item_id], key=str))
        validations = tuple(sorted(validations_by_item.get(item_id, ()), key=str))
        declared = {schema.schema for schema in schemas}
        # Validations must use an item-declared schema.
        for document_id in documents + validations:
            if document_id.object_id.schema not in declared:
                source = source_documents[document_id]
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
            )
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

    source_documents = _with_build_signatures(
        source_documents,
        support_files=support_files,
        store=store,
        root=root,
    )

    items = [
        replace(
            model,
            signature=_item_signature(
                model,
                source_documents=source_documents,
                schema_documents=schema_documents,
                support_files=support_files,
                store=store,
                root=root,
            ),
        )
        for model in items
    ]

    # Held rather than re-read: a ``lib/`` file is deployed by the load layer, so
    # its bytes have to reach both the signature it is selected by and the
    # payload the bundle carries, and neither may reopen the repository.
    #
    # Every support file, not only the Python ones. A `.py` filter here was
    # reading across from the *top level*, where a Weaver document is python,
    # sql or yml — but `lib/` is an ordinary directory the runtime tree
    # reproduces verbatim, and a module that reads a data file beside it needs
    # that file to have travelled with it.
    support_file_contents = {
        relative: store.read(root.join(*relative.split("/")))
        for relative in sorted(support_files)
    }

    repository = WeaverRepository(
        name=root.name,
        root=root,
        items=tuple(items),
        source_documents=source_documents,
        schema_documents=schema_documents,
        support_files=tuple(sorted(support_files)),
        support_file_contents=support_file_contents,
        signature=_item_repository_signature(
            files, store, root, generated=generated_files
        ),
        logical_shortcuts=logical_shortcuts,
        shortcuts=shortcuts,
        generated_files=generated_files,
    )
    return resolve_item_dependencies(repository)


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
                    names=tuple(shortcut.name for shortcut in node.names),
                )
            )
        elif isinstance(node, ast.Import):
            imports.extend(
                PythonImport(module=shortcut.name, names=(shortcut.name,))
                for shortcut in node.names
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

    An item's ``shortcuts.py`` or ``external.yml`` sits under its own prefix and
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


def _item_repository_signature(
    paths: Iterable[str],
    store: Store,
    root: Location,
    *,
    generated: Mapping[str, bytes] | None = None,
) -> str:
    """Hash included item-oriented files; `_ignore/` never reaches this list."""

    from .source import content_hash

    digest = hashlib.sha256()
    generated = generated or {}
    for relative in sorted(set(paths) | set(generated)):
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(
            content_hash(
                generated.get(relative)
                if relative in generated
                else store.read(root.join(*relative.split("/")))
            ).encode("ascii")
        )
        digest.update(b"\n")
    return digest.hexdigest()


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

    ``_helpers/dates.py`` is ``_helpers.dates``, not ``dates`` — a nested module
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
    remove an edge as well as add one — the phantom dependency an unused import
    creates has no other cure. ``Dependencies: []`` is such a declaration, so an
    explicit none suppresses discovery rather than falling back to it.

    One rule for every kind, validation included. What differs is only whether a
    kind is *required* to declare: a Spark SQL object is, because its query may
    read by path and a load ordered by a half-known graph builds things in the
    wrong order. A validation is not, because it reads objects that its own
    installation has already put in place — validation runs after the load
    artefacts, and a validation never depends on another validation — so an
    edge inference missed costs an ordering nicety rather than a wrong estate.
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
    referrer's own target wins when it has a candidate — `join Sales.Customer`
    in a Warehouse query means the Warehouse's Sales.Customer, because that is
    what the SQL would actually bind to.

    Failing that, a single candidate anywhere is the answer, and it may cross a
    boundary: a Warehouse query reading a Delta table is the ordinary case, and
    the one the SQL endpoint and the shortcuts exist to bridge.

    Two candidates in neither of those positions is genuinely ambiguous and is
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
    References resolving to nothing here — or to more than one thing — are left
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
                # Provided from outside — a boundary, not an edge within this graph.
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
