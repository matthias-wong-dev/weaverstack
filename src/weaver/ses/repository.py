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
from .metadata import PYTHON, ObjectId
from .source import SourceDocument, language_for_filename, read_source_document

#: Never read, never installed.
IGNORED_DIRECTORIES = frozenset(
    {"__pycache__", ".git", ".venv", ".pytest_cache", ".mypy_cache", ".ruff_cache", ".idea"}
)
IGNORED_FILENAMES = frozenset({".DS_Store", "Thumbs.db"})
IGNORED_SUFFIXES = (".pyc", ".pyo", ".swp", ".orig", ".rej")


@dataclass(frozen=True)
class SesRepository:
    """Every object in one repository, plus the support files that travel with it."""

    name: str
    root: Location
    documents: tuple[SourceDocument, ...]
    support_files: tuple[str, ...]
    signature: str

    @property
    def graph(self) -> Graph:
        """Dependencies that resolve within this repository."""

        return build_internal_graph(self.documents)

    @property
    def unresolved(self) -> dict[str, tuple[str, ...]]:
        """References naming nothing here — settled at build."""

        return unresolved_references(self.documents)

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
    support: list[str] = []

    for relative in paths:
        if "/" in relative or not _is_object_filename(relative):
            support.append(relative)
            continue
        documents.append(read_source_document(relative, store.read(root.join(*relative.split("/")))))

    documents.sort(key=lambda document: document.qualified)
    _reject_duplicate_ids(documents)
    _reject_case_only_duplicates(documents)
    _reject_module_collisions(documents, support)
    build_internal_graph(documents)  # a repository whose graph cannot be ordered is invalid

    return SesRepository(
        name=name or root.name,
        root=root,
        documents=tuple(documents),
        support_files=tuple(sorted(support)),
        signature=_signature(paths, documents, support, store, root),
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
    seen: dict[str, str] = {}
    for document in documents:
        key = _canonical(document.node_id)
        existing = seen.get(key)
        if existing is not None and existing != document.node_id:
            raise DiscoveryError(
                f"{document.qualified} and {existing.split(':', 1)[1]} differ only by "
                "case in the same target — identities are compared "
                "case-insensitively, so one must change"
            )
        seen[key] = document.node_id
