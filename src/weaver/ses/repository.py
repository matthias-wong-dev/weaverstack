"""An SES repository — a folder of object files, read and checked as a whole.

Object documents live at the repository root. Subdirectories hold helpers,
templates and anything else the objects need; they travel with the repository
but are not themselves objects.

::

    sales-etl/
    ├── Sales__OrderExport.py          Folder
    ├── Sales__Order.py                Delta table
    ├── Sales.OrderSummary.spark.sql   Delta table, Spark SQL
    ├── Reporting.OrderReport.sql      Warehouse table
    └── _helpers/
        └── dates.py

Reading goes through a :class:`~weaver.store.Store`, so the same reader serves
a local checkout and — once the Fabric store exists — a repository installed in
the Weaver Lakehouse.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable, Mapping

from ..errors import DiscoveryError
from ..locations import Location
from ..store import LocalStore, Store
from .graph import Graph
from .metadata import (
    DELTA_TARGET,
    FOLDER_TARGET,
    LAKEHOUSE_NAMESPACE,
    PYTHON,
    SQL_TARGET,
    WAREHOUSE_NAMESPACE,
    ObjectId,
)
from .schemas import SchemaSes, is_schema_file, read_schema_document
from .source import (
    PYTHON_ID_SEPARATOR,
    SourceDocument,
    language_for_filename,
    read_source_document,
)

#: Never read, never installed.
IGNORED_DIRECTORIES = frozenset(
    {"__pycache__", ".git", ".venv", ".pytest_cache", ".mypy_cache", ".ruff_cache", ".idea"}
)
IGNORED_FILENAMES = frozenset({".DS_Store", "Thumbs.db"})
IGNORED_SUFFIXES = (".pyc", ".pyo", ".swp", ".orig", ".rej")


@dataclass(frozen=True)
class DependencyEdge:
    """One resolved dependency, and how it was resolved.

    ``consumer`` and ``producer`` are node ids. ``reference`` is the two-part
    name the consumer wrote. ``resolution_kind`` records whether it bound to a
    native object or crossed engines through an alias, which is what a later
    build planner needs to know where an alias must be materialised.
    """

    consumer: str
    producer: str
    reference: str
    resolution_kind: str  # "native" | "lakehouse_alias" | "warehouse_alias"

    def __str__(self) -> str:
        return f"{self.producer} -> {self.consumer} ({self.reference}, {self.resolution_kind})"


@dataclass(frozen=True)
class SesRepository:
    """Every object in one repository, plus the support files that travel with it.

    Beyond the object documents, the reader has resolved the whole repository:
    the declared schemas, the four namespace symbol tables, the dependency edges
    with their provenance, the complete acyclic graph, and which schemas each
    namespace uses. Everything needed to plan a build without rediscovering what
    the repository means.
    """

    name: str
    root: Location
    documents: tuple[SourceDocument, ...]
    support_files: tuple[str, ...]
    signature: str
    schemas: Mapping[str, SchemaSes]
    lakehouse_native: Mapping[ObjectId, SourceDocument]
    warehouse_native: Mapping[ObjectId, SourceDocument]
    folder_native: Mapping[ObjectId, SourceDocument]
    lakehouse_aliases: Mapping[ObjectId, SourceDocument]
    warehouse_aliases: Mapping[ObjectId, SourceDocument]
    dependency_edges: tuple[DependencyEdge, ...]
    dependency_graph: Graph
    schemas_by_namespace: Mapping[str, frozenset[str]]
    external_references: Mapping[str, tuple[str, ...]]

    @property
    def graph(self) -> Graph:
        """The complete, alias-closed, acyclic dependency graph."""

        return self.dependency_graph

    @property
    def unresolved(self) -> dict[str, tuple[str, ...]]:
        """Per object, the references that leave the repository.

        A valid repository resolves every ordinary two-part reference, so what
        remains here is deliberately outside it: physically-qualified names and
        table-valued functions, recorded for the build rather than refused.
        """

        return dict(self.external_references)

    @property
    def by_id(self) -> Mapping[str, SourceDocument]:
        """By ``target:Schema.Object``, which is the unique identity."""

        return {document.node_id: document for document in self.documents}

    @property
    def by_qualified(self) -> Mapping[str, tuple[SourceDocument, ...]]:
        """By ``Schema.Object`` — several, when one ID has several targets."""

        grouped: dict[str, list[SourceDocument]] = {}
        for document in self.documents:
            grouped.setdefault(document.qualified, []).append(document)
        return {key: tuple(value) for key, value in grouped.items()}

    @property
    def object_ids(self) -> tuple[ObjectId, ...]:
        return tuple(document.object_id for document in self.documents)

    @property
    def module_names(self) -> Mapping[str, SourceDocument]:
        """Python object modules, by importable name.

        This is what makes an import a dependency: a module name in here names
        an object, anything else is an ordinary import.
        """

        return {
            document.module_name: document
            for document in self.documents
            if document.module_name is not None
        }

    def __getitem__(self, key: str) -> SourceDocument:
        """By node id, or by ID alone when that names exactly one object."""

        by_id = self.by_id
        if key in by_id:
            return by_id[key]
        candidates = self.by_qualified.get(key, ())
        if len(candidates) == 1:
            return candidates[0]
        if candidates:
            targets = ", ".join(sorted(document.node_id for document in candidates))
            raise DiscoveryError(
                f"{key!r} names more than one object in repository {self.name!r} — "
                f"say which: {targets}"
            )
        raise DiscoveryError(
            f"{key!r} is not an object in repository {self.name!r}"
        )

    def __len__(self) -> int:
        return len(self.documents)

    def __iter__(self):
        return iter(self.documents)


def read_repository(
    root: Location,
    *,
    store: Store | None = None,
    name: str | None = None,
) -> SesRepository:
    """Read and validate every object in a repository."""

    store = store or LocalStore()
    if not store.exists(root):
        raise DiscoveryError(f"repository root does not exist: {root}")
    if not store.is_directory(root):
        raise DiscoveryError(f"repository root is not a directory: {root}")

    paths = _repository_files(store, root)
    documents: list[SourceDocument] = []
    schema_files: list[str] = []
    support: list[str] = []

    for relative in paths:
        if is_schema_file(relative):
            schema_files.append(relative)
        elif "/" in relative or not _is_object_filename(relative):
            support.append(relative)
        else:
            documents.append(
                read_source_document(relative, store.read(root.join(*relative.split("/"))))
            )

    documents.sort(key=lambda document: document.qualified)
    _reject_duplicate_ids(documents)
    _reject_case_only_duplicates(documents)
    _reject_module_collisions(documents, support)

    schemas = _read_schemas(schema_files, store, root)
    lakehouse_native, warehouse_native, folder_native = _classify_native(documents)
    _reject_folder_delta_collision(folder_native, lakehouse_native)
    lakehouse_aliases, warehouse_aliases = _register_aliases(
        documents, lakehouse_native, warehouse_native
    )
    _validate_schemas_declared(
        schemas,
        lakehouse_native,
        warehouse_native,
        folder_native,
        lakehouse_aliases,
        warehouse_aliases,
    )
    edges, external = _resolve_dependencies(
        documents,
        lakehouse_native,
        warehouse_native,
        folder_native,
        lakehouse_aliases,
        warehouse_aliases,
    )
    graph = Graph(
        (document.node_id for document in documents),
        [(edge.producer, edge.consumer) for edge in edges],
    )
    schemas_by_namespace = _schemas_by_namespace(
        lakehouse_native,
        warehouse_native,
        folder_native,
        lakehouse_aliases,
        warehouse_aliases,
    )

    return SesRepository(
        name=name or root.name,
        root=root,
        documents=tuple(documents),
        support_files=tuple(sorted(support)),
        signature=_signature(paths, documents, support, store, root),
        schemas=schemas,
        lakehouse_native=lakehouse_native,
        warehouse_native=warehouse_native,
        folder_native=folder_native,
        lakehouse_aliases=lakehouse_aliases,
        warehouse_aliases=warehouse_aliases,
        dependency_edges=tuple(edges),
        dependency_graph=graph,
        schemas_by_namespace=schemas_by_namespace,
        external_references=external,
    )


def _is_object_filename(filename: str) -> bool:
    if filename.startswith("_"):
        return False
    return language_for_filename(filename) is not None


def _repository_files(store: Store, root: Location) -> list[str]:
    prefix = root.value.rstrip("/") + "/"
    relatives: list[str] = []
    for entry in store.list(root, recursive=True):
        if entry.is_directory:
            continue
        relative = entry.location.value[len(prefix):]
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


def _reject_duplicate_ids(documents: Iterable[SourceDocument]) -> None:
    """Uniqueness is per physical target, not per ID.

    ``Sales.Order`` may be a folder, a Delta table and a Warehouse table at
    once — three different places. What cannot happen is two objects
    materialising into the same place: a Python table and a Spark SQL table
    with one ID would both claim ``Tables/Sales/Order``.
    """

    seen: dict[str, str] = {}
    for document in documents:
        existing = seen.get(_canonical(document.node_id))
        if existing is not None:
            raise DiscoveryError(
                f"{document.qualified} is declared twice for the "
                f"{document.target_kind} target: {existing} and "
                f"{document.relative_path}"
            )
        seen[_canonical(document.node_id)] = document.relative_path


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


def _reject_module_collisions(
    documents: Iterable[SourceDocument], support: Iterable[str]
) -> None:
    """A helper must not be importable under an object's module name.

    Dependencies are imports, so a helper reachable by an object's module name
    would silently be read as a dependency on that object. Comparison is on the
    *complete* dotted name: ``parsers/Sales__Order.py`` is
    ``parsers.Sales__Order`` and collides with nothing.
    """

    object_modules = {
        document.module_name: document.relative_path
        for document in documents
        if document.module_name is not None
    }
    for relative in support:
        module = importable_module_name(relative)
        if module is not None and module in object_modules:
            raise DiscoveryError(
                f"{relative} is importable as {module!r}, the same module as the "
                f"object {object_modules[module]} — an import of it would be read "
                "as a dependency on that object"
            )


def _signature(
    paths: list[str],
    documents: list[SourceDocument],
    support: list[str],
    store: Store,
    root: Location,
) -> str:
    """One hash for the whole tree, over sorted (path, content hash) pairs."""

    from .source import content_hash

    hashes = {document.relative_path: document.source_hash for document in documents}
    digest = hashlib.sha256()
    for relative in sorted(paths):
        if relative not in hashes:
            hashes[relative] = content_hash(store.read(root.join(*relative.split("/"))))
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashes[relative].encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


# --- schema, namespace and alias resolution ---------------------------------


def _read_schemas(
    schema_files: Iterable[str], store: Store, root: Location
) -> dict[str, SchemaSes]:
    """Parse every ``_schemas/*.yml`` file, keyed by its Schema ID."""

    schemas: dict[str, SchemaSes] = {}
    seen: dict[str, str] = {}  # canonical id -> declared id
    for relative in sorted(schema_files):
        schema = read_schema_document(relative, store.read(root.join(*relative.split("/"))))
        canonical = schema.schema_id.lower()
        existing = seen.get(canonical)
        if existing == schema.schema_id:
            # Same declared id twice — only reachable on a case-sensitive file
            # system, since filenames must match the id and are unique per folder.
            raise DiscoveryError(
                f"schema {schema.schema_id!r} is declared twice: "
                f"{schemas[existing].relative_path} and {relative}"
            )
        if existing is not None:
            raise DiscoveryError(
                f"schemas {existing!r} and {schema.schema_id!r} differ only by case — "
                "schema identity is case-insensitive, so one must change "
                f"({schemas[existing].relative_path} and {relative})"
            )
        seen[canonical] = schema.schema_id
        schemas[schema.schema_id] = schema
    return schemas


def _classify_native(
    documents: Iterable[SourceDocument],
) -> tuple[
    dict[ObjectId, SourceDocument],
    dict[ObjectId, SourceDocument],
    dict[ObjectId, SourceDocument],
]:
    """Sort each object into its native partition.

    Uniqueness within a partition is already guaranteed by node-id
    de-duplication, so one name may appear once as a Folder, once as a Delta
    table and once as a Warehouse table — three different places.
    """

    lakehouse: dict[ObjectId, SourceDocument] = {}
    warehouse: dict[ObjectId, SourceDocument] = {}
    folder: dict[ObjectId, SourceDocument] = {}
    partition = {FOLDER_TARGET: folder, DELTA_TARGET: lakehouse, SQL_TARGET: warehouse}
    for document in documents:
        partition[document.target_kind][document.object_id] = document
    return lakehouse, warehouse, folder


def _reject_folder_delta_collision(
    folder_native: Mapping[ObjectId, SourceDocument],
    lakehouse_native: Mapping[ObjectId, SourceDocument],
) -> None:
    """A Lakehouse name is a Folder or a Delta table, never both.

    Both live in the Lakehouse, and a Python Folder and a Python Delta table of
    one ID would be the very same file on disk — so the pairing cannot even be
    written. It is refused explicitly rather than left as an impossible case a
    reader has to reason out, or as a silent resolution ambiguity.
    """

    folder_by_canonical = {_canonical(object_id.qualified): object_id for object_id in folder_native}
    for object_id in lakehouse_native:
        collision = folder_by_canonical.get(_canonical(object_id.qualified))
        if collision is not None:
            folder = folder_native[collision]
            delta = lakehouse_native[object_id]
            raise DiscoveryError(
                f"{object_id.qualified} is declared as both a Folder ({folder.relative_path}) "
                f"and a Delta table ({delta.relative_path}) — a Lakehouse name is one or the "
                "other, never both"
            )


def _canonical_index(objects: Mapping[ObjectId, SourceDocument]) -> dict[str, SourceDocument]:
    """A case-folded lookup, because SQL binding is case-insensitive."""

    return {_canonical(object_id.qualified): document for object_id, document in objects.items()}


def _register_aliases(
    documents: Iterable[SourceDocument],
    lakehouse_native: Mapping[ObjectId, SourceDocument],
    warehouse_native: Mapping[ObjectId, SourceDocument],
) -> tuple[dict[ObjectId, SourceDocument], dict[ObjectId, SourceDocument]]:
    """Register cross-engine aliases and prove each name has one owner.

    A ``Warehouse alias`` publishes a Lakehouse object into the Warehouse; a
    ``Lakehouse alias`` publishes a Warehouse object into the Lakehouse. Each
    published name must be unique in its destination and must not collide with a
    native object already living there, so every name resolves to exactly one
    owner before dependency resolution begins.
    """

    warehouse_aliases: dict[ObjectId, SourceDocument] = {}
    lakehouse_aliases: dict[ObjectId, SourceDocument] = {}
    warehouse_seen: dict[str, SourceDocument] = {}
    lakehouse_seen: dict[str, SourceDocument] = {}
    errors: list[str] = []

    for document in documents:
        if document.warehouse_alias is not None:
            alias = document.warehouse_alias
            key = _canonical(alias.qualified)
            if key in warehouse_seen:
                errors.append(
                    f"Warehouse alias {alias.qualified} is published by both "
                    f"{warehouse_seen[key].relative_path} and {document.relative_path}"
                )
            else:
                warehouse_seen[key] = document
                warehouse_aliases[alias] = document
        if document.lakehouse_alias is not None:
            alias = document.lakehouse_alias
            key = _canonical(alias.qualified)
            if key in lakehouse_seen:
                errors.append(
                    f"Lakehouse alias {alias.qualified} is published by both "
                    f"{lakehouse_seen[key].relative_path} and {document.relative_path}"
                )
            else:
                lakehouse_seen[key] = document
                lakehouse_aliases[alias] = document

    warehouse_native_canon = {_canonical(object_id.qualified) for object_id in warehouse_native}
    lakehouse_native_canon = {_canonical(object_id.qualified) for object_id in lakehouse_native}
    for alias, document in warehouse_aliases.items():
        if _canonical(alias.qualified) in warehouse_native_canon:
            errors.append(
                f"Warehouse alias {alias.qualified} ({document.relative_path}) collides with a "
                "Warehouse-native object of the same name — the name would have two owners"
            )
    for alias, document in lakehouse_aliases.items():
        if _canonical(alias.qualified) in lakehouse_native_canon:
            errors.append(
                f"Lakehouse alias {alias.qualified} ({document.relative_path}) collides with a "
                "Lakehouse-native object of the same name — the name would have two owners"
            )

    if errors:
        raise DiscoveryError(
            "alias namespace ownership is ambiguous:\n  - " + "\n  - ".join(errors)
        )
    return lakehouse_aliases, warehouse_aliases


def _validate_schemas_declared(
    schemas: Mapping[str, SchemaSes],
    lakehouse_native: Mapping[ObjectId, SourceDocument],
    warehouse_native: Mapping[ObjectId, SourceDocument],
    folder_native: Mapping[ObjectId, SourceDocument],
    lakehouse_aliases: Mapping[ObjectId, SourceDocument],
    warehouse_aliases: Mapping[ObjectId, SourceDocument],
) -> None:
    """Every schema an object or alias implies must be declared under ``_schemas``."""

    declared = set(schemas)
    errors: list[str] = []

    def check(schema_id: str, using: str, source_path: str) -> None:
        if schema_id not in declared:
            errors.append(
                f"schema {schema_id!r} is not declared but is used by {using} "
                f"({source_path}) — add _schemas/{schema_id}.yml"
            )

    for partition in (folder_native, lakehouse_native, warehouse_native):
        for object_id, document in partition.items():
            check(object_id.schema, f"object {object_id.qualified}", document.relative_path)
    for aliases, label in ((lakehouse_aliases, "Lakehouse alias"), (warehouse_aliases, "Warehouse alias")):
        for alias, document in aliases.items():
            check(alias.schema, f"{label} {alias.qualified}", document.relative_path)

    if errors:
        raise DiscoveryError("undeclared schema(s):\n  - " + "\n  - ".join(sorted(set(errors))))


def _resolve_reference(
    consumer: SourceDocument,
    dependency: ObjectId,
    from_python_import: bool,
    lakehouse_index: Mapping[str, SourceDocument],
    warehouse_index: Mapping[str, SourceDocument],
    folder_index: Mapping[str, SourceDocument],
    lakehouse_alias_index: Mapping[str, SourceDocument],
    warehouse_alias_index: Mapping[str, SourceDocument],
    module_index: Mapping[str, SourceDocument],
) -> tuple[SourceDocument, str] | None:
    """Resolve one two-part reference in its consumer's namespace, via aliases.

    A Python import names a specific module, so it resolves to that exact object
    whatever else shares the name. Everything else resolves in the consumer's
    namespace: native first, then an alias published into that namespace, whose
    edge closes back to the native object that declared it.
    """

    if from_python_import:
        module = f"{dependency.schema}{PYTHON_ID_SEPARATOR}{dependency.object}"
        target = module_index.get(module)
        if target is not None:
            return target, "native"

    key = _canonical(dependency.qualified)
    if consumer.namespace == LAKEHOUSE_NAMESPACE:
        native = lakehouse_index.get(key) or folder_index.get(key)
        if native is not None:
            return native, "native"
        alias = lakehouse_alias_index.get(key)
        if alias is not None:
            return alias, "lakehouse_alias"
    else:
        native = warehouse_index.get(key)
        if native is not None:
            return native, "native"
        alias = warehouse_alias_index.get(key)
        if alias is not None:
            return alias, "warehouse_alias"
    return None


def _resolve_dependencies(
    documents: Iterable[SourceDocument],
    lakehouse_native: Mapping[ObjectId, SourceDocument],
    warehouse_native: Mapping[ObjectId, SourceDocument],
    folder_native: Mapping[ObjectId, SourceDocument],
    lakehouse_aliases: Mapping[ObjectId, SourceDocument],
    warehouse_aliases: Mapping[ObjectId, SourceDocument],
) -> tuple[list[DependencyEdge], dict[str, tuple[str, ...]]]:
    """Close every managed dependency, rejecting any two-part name that cannot."""

    documents = list(documents)
    lakehouse_index = _canonical_index(lakehouse_native)
    warehouse_index = _canonical_index(warehouse_native)
    folder_index = _canonical_index(folder_native)
    lakehouse_alias_index = _canonical_index(lakehouse_aliases)
    warehouse_alias_index = _canonical_index(warehouse_aliases)
    module_index = {
        document.module_name: document
        for document in documents
        if document.module_name is not None
    }

    edges: list[DependencyEdge] = []
    external: dict[str, tuple[str, ...]] = {}
    seen: set[tuple[str, str]] = set()
    unresolved: list[str] = []

    for document in documents:
        from_python_import = document.language == PYTHON and not document.declared_dependencies
        for dependency in effective_dependencies(document):
            resolved = _resolve_reference(
                document,
                dependency,
                from_python_import,
                lakehouse_index,
                warehouse_index,
                folder_index,
                lakehouse_alias_index,
                warehouse_alias_index,
                module_index,
            )
            if resolved is None:
                unresolved.append(_unresolved_message(document, dependency))
                continue
            producer, kind = resolved
            if producer.node_id == document.node_id:
                continue
            key = (producer.node_id, document.node_id)
            if key in seen:
                continue
            seen.add(key)
            edges.append(
                DependencyEdge(
                    consumer=document.node_id,
                    producer=producer.node_id,
                    reference=dependency.qualified,
                    resolution_kind=kind,
                )
            )
        references = document.external_references
        if references:
            external[document.node_id] = references

    if unresolved:
        raise DiscoveryError(
            "unresolved two-part reference(s) — every managed name must resolve "
            "within the repository:\n  - " + "\n  - ".join(sorted(unresolved))
        )

    edges.sort(key=lambda edge: (edge.producer, edge.consumer))
    return edges, external


def _unresolved_message(consumer: SourceDocument, dependency: ObjectId) -> str:
    if consumer.namespace == WAREHOUSE_NAMESPACE:
        remedy = "declare a Lakehouse alias on the target, add the SES object, or add its schema"
    else:
        remedy = "declare a Warehouse alias on the target, add the SES object, or add its schema"
    return (
        f"{consumer.node_id} references {dependency.qualified}, which resolves to nothing "
        f"in the {consumer.namespace} namespace ({consumer.relative_path}) — {remedy}"
    )


def _schemas_by_namespace(
    lakehouse_native: Mapping[ObjectId, SourceDocument],
    warehouse_native: Mapping[ObjectId, SourceDocument],
    folder_native: Mapping[ObjectId, SourceDocument],
    lakehouse_aliases: Mapping[ObjectId, SourceDocument],
    warehouse_aliases: Mapping[ObjectId, SourceDocument],
) -> dict[str, frozenset[str]]:
    """Which schemas each namespace uses — native IDs plus aliases published in."""

    lakehouse = {object_id.schema for object_id in lakehouse_native}
    lakehouse |= {object_id.schema for object_id in folder_native}
    lakehouse |= {alias.schema for alias in lakehouse_aliases}
    warehouse = {object_id.schema for object_id in warehouse_native}
    warehouse |= {alias.schema for alias in warehouse_aliases}
    return {
        LAKEHOUSE_NAMESPACE: frozenset(lakehouse),
        WAREHOUSE_NAMESPACE: frozenset(warehouse),
    }


# --- the internal dependency graph -------------------------------------------


def _canonical(qualified: str) -> str:
    """Object identities are compared without regard to case.

    A developer may write `sales__order` where the house style is
    `Sales__Order`, and SQL is case-insensitive by nature. Two objects whose
    IDs differ only by case are refused, so the folding is unambiguous.
    """

    return qualified.lower()


def effective_dependencies(document: SourceDocument) -> tuple[ObjectId, ...]:
    """What this object depends on: declared if declared, else discovered.

    A declaration replaces discovery rather than adding to it, so an author can
    remove an edge as well as add one — the phantom dependency an unused import
    creates has no other cure.
    """

    if document.declared_dependencies:
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
        candidate for candidate in candidates
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


def _reject_case_only_duplicates(documents: Iterable[SourceDocument]) -> None:
    """One spelling per name across the whole repository.

    The same Schema.Object may live in more than one target — a Delta table and
    a Warehouse table of one name is the ordinary cross-engine case — but it must
    be written identically in each. Two spellings that differ only by case are
    two names for one thing, which the case-insensitive identity model cannot
    tell apart, so one must change. (A genuine duplicate within one target is
    already refused as declared-twice.)
    """

    seen: dict[str, str] = {}
    for document in sorted(documents, key=lambda document: (document.qualified, document.relative_path)):
        canonical = _canonical(document.qualified)
        existing = seen.get(canonical)
        if existing is not None and existing != document.qualified:
            raise DiscoveryError(
                f"{document.qualified} and {existing} differ only by case — a name is "
                "spelled one way across the repository, so one must change"
            )
        seen.setdefault(canonical, document.qualified)
