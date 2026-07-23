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
    def by_id(self) -> Mapping[str, SourceDocument]:
        return {document.qualified: document for document in self.documents}

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

    def __getitem__(self, qualified: str) -> SourceDocument:
        try:
            return self.by_id[qualified]
        except KeyError:
            raise DiscoveryError(
                f"{qualified!r} is not an object in repository {self.name!r}"
            ) from None

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
    _reject_module_collisions(documents, support)

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
    seen: dict[str, str] = {}
    for document in documents:
        existing = seen.get(document.qualified)
        if existing is not None:
            raise DiscoveryError(
                f"{document.qualified} is declared twice: {existing} and "
                f"{document.relative_path}"
            )
        seen[document.qualified] = document.relative_path


def _reject_module_collisions(
    documents: Iterable[SourceDocument], support: Iterable[str]
) -> None:
    """A helper must not share a module name with an object.

    Dependencies are imports, so a helper importable under an object's module
    name would silently become a dependency on that object.
    """

    object_modules = {
        document.module_name: document.relative_path
        for document in documents
        if document.module_name is not None
    }
    for relative in support:
        parts = relative.split("/")
        if not parts[-1].endswith(".py"):
            continue
        module = parts[-1][: -len(".py")]
        if module in object_modules:
            raise DiscoveryError(
                f"{relative} shares the module name {module!r} with the object "
                f"{object_modules[module]} — an import of it would be read as a "
                "dependency on that object"
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
