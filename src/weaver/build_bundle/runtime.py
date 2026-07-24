"""The minimal load runtime a Python payload calls to materialise its object.

A generated Python payload is a small wrapper::

    from DWG__Customer import DWG__Customer
    from weaver.build_bundle.runtime import materialise
    materialise(DWG__Customer)

Everything physical — the Spark session, the resolved target, the store — is the
installer's *ambient* context, bound with :func:`installing` before the payload
runs and read here. So the payload embeds no path and does not know where it is
installed.

This runtime keeps the authoring contract intact: an object's ``read()`` only
*proposes* — a Folder returns its staged directory, a Table returns rows. The
runtime owns the mutation: it promotes the staging directory, or writes the Delta
table and registers it under its two-part name so a later Spark reader binds it.

Scope is the first vertical slice: a non-incremental, full-replace build. Upsert
merge, incremental accumulation, audit-column accounting and delete application
are deliberately out — they are load policy, added when load is built out. The
runtime is local-only for now; a Fabric materialiser satisfies the same shape.
"""

from __future__ import annotations

import shutil
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Type

from ..errors import LoadError
from ..objects import Folder, Table, WeaverObject, active_resolver
from ..ses.source import SourceDocument, read_source_document
from ..targets import DeltaTarget, FolderTarget, ItemRef


@dataclass(frozen=True)
class Installation:
    """The runtime services a payload needs, bound for its execution.

    ``resolver`` is a level-three resolver (``LocalResolver`` today); ``lakehouse``
    is the bound Lakehouse the object materialises into. No planning input is
    here — the runtime executes, it does not decide.
    """

    spark: Any
    resolver: Any
    lakehouse: ItemRef


_active_installation: ContextVar[Installation | None] = ContextVar(
    "weaver_active_installation", default=None
)


@contextmanager
def installing(installation: Installation) -> Iterator[None]:
    """Bind the ambient installation for the payloads run inside the block."""

    token = _active_installation.set(installation)
    try:
        yield
    finally:
        _active_installation.reset(token)


def _current() -> Installation:
    installation = _active_installation.get()
    if installation is None:
        raise LoadError(
            "materialise() ran without an installation bound — the build installer "
            "sets one around each payload; it cannot be called from ordinary code"
        )
    return installation


# --- entry point -------------------------------------------------------------


def materialise(cls: Type[WeaverObject]) -> None:
    """Build the object ``cls`` into the ambient target."""

    installation = _current()
    document = _document_for_class(cls)

    context = _LocalObjectContext(document, installation)
    with active_resolver(_dependency_resolver(installation)):
        instance = cls(context)
        if issubclass(cls, Folder):
            staging, _deletes = instance.read()
            _materialise_folder(document, installation, staging)
        elif issubclass(cls, Table):
            rows, _deletes = instance.read()
            _materialise_table(document, installation, rows)
        else:  # pragma: no cover - only Folder and Table are executable
            raise LoadError(f"{cls.__name__} is not a materialisable object")


# --- materialisation ---------------------------------------------------------


def _materialise_folder(document: SourceDocument, installation: Installation, staging: Any) -> None:
    """Promote the object's staged directory to its final Folder location."""

    final = _folder_path(installation, document)
    staging_path = Path(str(staging))
    final.parent.mkdir(parents=True, exist_ok=True)
    if final.exists():
        shutil.rmtree(final)
    if staging_path.exists():
        staging_path.replace(final)
    else:
        final.mkdir(parents=True, exist_ok=True)


def _materialise_table(document: SourceDocument, installation: Installation, rows: Any) -> None:
    """Write the proposed rows as Delta (full replace) and register the table.

    Registration mirrors Fabric: once declared, ``Schema.Object`` is queryable by
    name, which is what lets a later Spark SQL view read it.
    """

    spark = installation.spark
    path = _delta_path(installation, document)
    dataframe = _as_dataframe(spark, rows, document)

    (
        dataframe.write.format("delta")
        .option("delta.columnMapping.mode", "name")
        .option("overwriteSchema", "true")
        .mode("overwrite")
        .save(path)
    )

    schema = document.object_id.schema
    name = document.object_id.object
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {_ident(schema)}")
    spark.sql(f"DROP TABLE IF EXISTS {_ident(schema)}.{_ident(name)}")
    spark.sql(
        f"CREATE TABLE {_ident(schema)}.{_ident(name)} USING delta LOCATION '{path}'"
    )


def _as_dataframe(spark: Any, rows: Any, document: SourceDocument) -> Any:
    """A Spark DataFrame from whatever ``read()`` proposed.

    A DataFrame passes straight through; a list of rows is created against the
    declared schema so column types are the ones the author declared, not
    Spark's guess.
    """

    if hasattr(rows, "write"):
        return rows
    schema_ddl = _schema_ddl(document)
    if schema_ddl is not None:
        return spark.createDataFrame(list(rows), schema=schema_ddl)
    return spark.createDataFrame(list(rows))


# --- object context ----------------------------------------------------------


class _LocalObjectContext:
    """The context Weaver injects into one executing object, backed by the host.

    Duck-typed against :class:`~weaver.objects.ObjectContext`; only what the
    first vertical slice needs is live, and the rest raises clearly rather than
    pretending to work.
    """

    def __init__(self, document: SourceDocument, installation: Installation) -> None:
        self._document = document
        self._installation = installation

    @property
    def spark(self) -> Any:
        return self._installation.spark

    @property
    def object_path(self) -> Any:
        if self._document.target_kind == "folder":
            return _folder_path(self._installation, self._document)
        return Path(_delta_path(self._installation, self._document))

    @property
    def schema(self) -> tuple[Any, ...]:
        return self._document.document.schema

    @property
    def primary_key(self) -> tuple[str, ...]:
        return self._document.document.primary_key

    @property
    def is_incremental(self) -> bool:
        return self._document.document.is_incremental

    def current_dataframe(self) -> Any:
        path = _delta_path(self._installation, self._document)
        if not Path(path).exists():
            return None
        return self._installation.spark.read.format("delta").load(path)

    def empty_frame(self) -> Any:
        ddl = _schema_ddl(self._document)
        return self._installation.spark.createDataFrame([], schema=ddl or "")

    def staging_folder(self) -> Any:
        staging = Path(
            self._installation.resolver.folder_staging(
                _folder_target(self._installation),
                self._document.object_id.schema,
                self._document.object_id.object,
            ).value
        )
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir(parents=True, exist_ok=True)
        return staging


# --- dependency resolution ---------------------------------------------------


def _dependency_resolver(installation: Installation):
    """Resolve one dependency accessor to a materialised handle.

    ``folder_path()`` yields the depended-on Folder's real directory;
    ``dataframe()`` reads the depended-on table's Delta by path. The producer has
    already been built in an earlier sequence, so both are on disk.
    """

    def resolve(cls: Type[WeaverObject], accessor: str) -> Any:
        document = _document_for_class(cls)
        if accessor == "path":
            return _folder_path(installation, document)
        if accessor == "dataframe":
            return installation.spark.read.format("delta").load(
                _delta_path(installation, document)
            )
        raise LoadError(f"unknown dependency accessor {accessor!r} on {cls.__name__}")

    return resolve


# --- helpers -----------------------------------------------------------------


def _document_for_class(cls: Type[WeaverObject]) -> SourceDocument:
    """Read one object's own source file for its declared metadata.

    This reads a single object file — the one being executed — for its declared
    schema and identity; it resolves no dependency, no alias and no target, so it
    is execution metadata, not a second planning pass.
    """

    module = sys.modules.get(cls.__module__)
    file = getattr(module, "__file__", None)
    if file is None:
        raise LoadError(f"cannot locate the source file for {cls.__name__}")
    path = Path(file)
    return read_source_document(path.name, path.read_bytes())


def _folder_target(installation: Installation) -> FolderTarget:
    return FolderTarget(lakehouse=installation.lakehouse)


def _folder_path(installation: Installation, document: SourceDocument) -> Path:
    location = installation.resolver.folder_object(
        _folder_target(installation),
        document.object_id.schema,
        document.object_id.object,
    )
    return Path(location.value)


def _delta_path(installation: Installation, document: SourceDocument) -> str:
    return installation.resolver.delta_table(
        DeltaTarget(lakehouse=installation.lakehouse),
        document.object_id.schema,
        document.object_id.object,
    ).value


def _schema_ddl(document: SourceDocument) -> str | None:
    """A Spark DDL schema string from the declared columns, or None if undeclared."""

    columns = document.document.schema
    if not columns:
        return None
    return ", ".join(f"`{column.name}` {column.type}" for column in columns)


def _ident(name: str) -> str:
    """Back-tick quote a catalog identifier so spaces and keywords are safe."""

    return "`" + name.replace("`", "``") + "`"
