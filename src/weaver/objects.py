"""Base classes for Python-authored Weaver objects.

Objects receive a Spark session, resolved Lakehouse destination, and identity.
"""

from __future__ import annotations

from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .errors import LoadError, WeaverError
from .lakehouse import Lakehouse, default_lakehouse

if TYPE_CHECKING:  # pragma: no cover - for type readers only
    from .catalogue.state import Catalogue
    from .runtime.folder_load import StagingFolder
    from .runtime.load_result import LoadResult

#: What Weaver names a folder's staging sibling. Repeated from
#: :mod:`weaver.runtime.folder_load` rather than imported: the authoring surface
#: stays importable without the runtime beneath it. ``tests/test_objects.py``
#: asserts the two are identical.
STAGING_SUFFIX = "_Staging"

#: What separates schema from object in a class name: a module name cannot carry
#: a dot, so ``Sales.Order`` is spelled ``Sales__Order``. Repeated from
#: :func:`weaver.declaration.source.object_id_for_filename` because the authoring
#: surface must not import the parser.
CLASS_ID_SEPARATOR = "__"

#: The two kinds of validation, as their declarations spell them. Repeated from
#: :mod:`weaver.declaration.metadata` for the same reason: this module is
#: imported by authored code and pulls in no parser.
#: ``tests/test_objects_declaration.py`` asserts the two are identical.
TEST = "Test"
ASSUMPTION = "Assumption"


class WeaverObject:
    """Base for every authored object.

    ``spark`` is mandatory. Another Weaver object may be passed in its place, and
    the new object inherits that one's session, Lakehouse and catalogue::

        My__Table(spark)                               # freestanding
        My__Table(spark, catalogue="Warehouse/Weaver")  # anchored

    ``catalogue`` names the Warehouse holding the catalogue, or is a
    :class:`~weaver.catalogue.state.Catalogue` already read. Named, it opens and
    owns the Session it reads and writes through; handed one, it reuses that
    one's. Without it the object is freestanding: ``read()`` works and ``load()``
    raises. With it, construction resolves which installed object this is and
    raises :class:`~weaver.errors.ConfigError` if the catalogue records it zero
    times or twice.

    An orchestrated run supplies its catalogue through :meth:`with_catalogue`, so
    a deployed primitive's constructor stays ``cls(spark, lakehouse=...)``.
    """

    def __init__(
        self,
        spark: Any,
        *,
        lakehouse: Lakehouse | None = None,
        catalogue: "str | Catalogue | None" = None,
    ) -> None:
        inherited = None
        if isinstance(spark, WeaverObject):
            owner = spark
            spark = owner.spark
            if lakehouse is None:
                lakehouse = owner.lakehouse
            # The catalogue, never a value read from it: a child resolves its
            # own identity and its own bookmark against the same catalogue.
            inherited = owner._catalogue
        if spark is None:
            raise LoadError(
                f"{type(self).__name__} needs the Spark session it runs through. "
                f"Construct it as {type(self).__name__}(spark), or as "
                f"{type(self).__name__}(self) from another object"
            )
        if isinstance(lakehouse, str):
            raise LoadError(
                f"{type(self).__name__} takes a resolved Lakehouse, not the name "
                f"{lakehouse!r}. Resolve it first with "
                f"weaver.lakehouse_for(resolver, {lakehouse!r})"
            )
        if lakehouse is not None and not isinstance(lakehouse, Lakehouse):
            raise LoadError(
                f"{type(self).__name__} takes a resolved Lakehouse, got "
                f"{type(lakehouse).__name__}"
            )

        #: The session this object reads and writes through.
        self.spark = spark
        #: The destination this object materialises into, resolved once.
        self.lakehouse: Lakehouse = (
            lakehouse if lakehouse is not None else default_lakehouse(spark)
        )
        #: The destination's root, which is what Spark and Hadoop address. Tables and
        #: folders both hang off it, so nothing an object reaches needs a mount.
        self.spark_root = self.lakehouse.spark_root

        #: The catalogue this object is anchored to, or None if freestanding.
        #: Private: authored code asks the object about itself, not about how the
        #: answer was obtained.
        self._catalogue = None
        #: This object's installed identity, resolved once with the anchor.
        self._installed = None
        from .catalogue.state import Catalogue as _Catalogue

        if isinstance(catalogue, _Catalogue):
            # One already read, and its Session with it: a run and an authored
            # notebook that has one both hand it over rather than pay again.
            self.with_catalogue(catalogue)
        elif catalogue is not None:
            from .runtime.anchor import anchored

            self._catalogue, self._installed = anchored(self, catalogue)
        elif inherited is not None:
            self.with_catalogue(inherited)

    # --- identity ---------------------------------------------------------

    @property
    def identity(self) -> tuple[str, str]:
        """The schema and object name this class declares."""

        return _identity(type(self).__name__)

    @property
    def object_id(self) -> str:
        """This object's ``Schema.Object`` ID, from its class name."""

        return "{}.{}".format(*self.identity)

    # --- the catalogue this object is anchored to --------------------------

    def with_catalogue(self, catalogue: Any, identity: Any = None) -> "WeaverObject":
        """Anchor this object to an already-populated catalogue, and return it.

        How an orchestrated run supplies what a standalone load names with
        ``catalogue=``. Set after construction, so a deployed primitive's
        constructor stays ``cls(spark, lakehouse=...)``.

        ``identity`` is this object's installed identity where the caller has it,
        as a run does. Resolved from the catalogue otherwise.
        """

        from .runtime.anchor import resolved_identity

        self._catalogue = catalogue
        self._installed = (
            identity if identity is not None else resolved_identity(self, catalogue)
        )
        return self

    @property
    def installed(self):
        """This object's identity in the catalogue, or None if freestanding."""

        return self._installed

    #: Whether this object's catalogue identity carries the ``Files/`` prefix. A
    #: Folder and a Table of the same name are two objects, and one bookmark
    #: cannot stand for both.
    _is_files = False

    def bookmark(self):
        """The UTC instant immediately before this object's last clean load began.

        An aware datetime, always. An object with no bookmark row for its current
        physical incarnation reads as the sentinel, so an incremental read asks
        for everything::

            def read(self):
                return Source__Export(self).files_since(self.bookmark())

        Answerable only by a catalogue-anchored object. A freestanding one raises,
        and can still be constructed and read: it is :meth:`load` that needs the
        catalogue this answers from.
        """

        return self._anchor().bookmark(self._installed)

    def _anchor(self):
        """The catalogue this object is anchored to, or a failure saying it is not.

        Asked by every load, because a load records how far it read. Required
        here rather than where the row is written.
        """

        if self._catalogue is not None:
            return self._catalogue
        raise LoadError(
            f"{self.object_id} is not anchored to the Weaver catalogue, so it "
            "cannot read its bookmark or record one, and a load needs both. "
            "Construct it as "
            f'{type(self).__name__}(spark, catalogue="Warehouse/<name>").'
        )

    def _bookmarked(self, result, began):
        """One load's result, carrying the instant a clean run of it began.

        Reported and never written: whoever records the load advances the
        bookmark, and this is where the instant comes from. Taken by the engine
        that ran the load. Only a clean success, because a load that rejected a
        row has not read its window.
        """

        from dataclasses import replace as _replace

        if not result.succeeded or result.rows_rejected:
            return result
        return _replace(result, bookmark_datetime=began)

    def _physical_target(self) -> str:
        """The physical target this object materialises into, as a log names it.

        A Python object materialises into a Lakehouse: rows as Delta files, files
        under the Files area, both beneath the destination it resolved.
        """

        return f"Lakehouse/{self.lakehouse.name}"

    def _record(self, settled) -> None:
        """Record one settled unit of this object's own work, and wait for it.

        Synchronous. An orchestrated run records every node through one queue and
        flushes at the end; a caller who ran this object by hand is told it
        finished once the record has landed.
        """

        from .run.record import RunRecord, new_workflow_id

        record = RunRecord(
            workflow_id=new_workflow_id(),
            task_type=self._task_type,
            catalogue=self._anchor(),
        )
        record.settled(settled)
        record.flush()

    #: What this object's work is recorded as. A load unless a subclass says
    #: otherwise; a validation says otherwise.
    _task_type = "load"

    def read(self):
        raise NotImplementedError(f"{type(self).__name__} must implement read()")

    def _read_result(self):
        """Run ``read()`` and return its normalised load result."""

        from .runtime.load_contract import normalise_read_result

        return normalise_read_result(self.read())

    # --- the load contract, read from this module's own docstring ----------

    def _document(self):
        """This object's parsed declaration, from the module it was defined in.

        Read on every call rather than cached, so an edit takes effect on the
        next reload.
        """

        import sys

        from .runtime.load_contract import document_for_module

        module = sys.modules.get(type(self).__module__)
        if module is None:  # pragma: no cover - a class with no importable module
            raise LoadError(
                f"{type(self).__name__} was defined outside an importable module, "
                "so its Weaver metadata cannot be read"
            )
        return document_for_module(module)

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.object_id} in {self.lakehouse.name}>"


def _sentinel():
    """What an object no clean load has run for reads as. See `Catalogue.bookmark`."""

    from .catalogue.tables import BOOKMARK_SENTINEL

    return BOOKMARK_SENTINEL


def _recorded_load(object, **policy) -> "LoadResult":
    """One object's load, with its operational record written before it returns.

    Anything the load raises is recorded and then re-raised unchanged. A refusal
    Weaver itself named is Failed and anything else is Error, which is the line
    ``_.Load`` draws from ``error_number()``.

    ``reload`` in ``policy`` also names what this interface does before the load:
    the object's load state is ended and made durable, and only then is the load
    called to empty the target and run. So a reload that fails half way leaves no
    bookmark and no settled status over rows that are gone.
    """

    from .run.record import RunRecord, new_workflow_id

    # Before anything else, so the refusal to start is never recorded: an
    # unanchored object has no catalogue to record into and no identity to
    # record against.
    catalogue = object._anchor()
    reload = bool(policy.get("reload", False))
    record = RunRecord(
        workflow_id=new_workflow_id(),
        task_type=object._task_type,
        catalogue=catalogue,
    )
    if reload:
        record.reset(object._installed)
    started = datetime.now(timezone.utc)
    try:
        result = _stamped(object._load(**policy), reload)
    except Exception as raised:
        _settle(
            record,
            _settled(
                object,
                _stamped(_carried(raised), reload),
                started=started,
                raised=True,
                refused=isinstance(raised, WeaverError),
            ),
        )
        raise
    _settle(record, _settled(object, result, started=started))
    return result


def _settle(record, settled) -> None:
    """Record one settled unit of work, and wait for it."""

    record.settled(settled)
    record.flush()


def _stamped(result, reload: bool):
    """One result, carrying whether the caller asked for a reload.

    Reload is what this interface was asked for, and an engine told to reload
    repeats it back. So it is stamped here, and :data:`RESULT_COLUMNS` carries
    no column for it.
    """

    return result.reloaded() if reload else result


def _settled(object, result, *, started, raised: bool = False, refused: bool = False):
    """One standalone load, in the terms every runtime table records."""

    from .run.record import settled_load

    return settled_load(
        object._installed,
        result,
        physical_target=object._physical_target(),
        started=started,
        completed=datetime.now(timezone.utc),
        raised=raised,
        refused=refused,
    )


def _carried(raised: BaseException):
    """Whatever counts the failure was carrying, or none it can report."""

    from .runtime.load_result import LoadResult

    carried = getattr(raised, "result", None)
    return (
        carried
        if carried is not None
        else LoadResult.failure(f"{type(raised).__name__}: {raised}")
    )


def _refuse_no_staging(contract, what: str, instead: str) -> None:
    """``None`` from a non-incremental ``read()``, which cannot mean "no work".

    For a non-incremental source, staging is the whole truth: an explicitly
    empty relation or folder retires everything the target holds, which is a
    load. So there is nothing ``None`` could be read as, and an author is told
    what to write instead.
    """

    if contract.incremental:
        return
    raise LoadError(
        f"{contract.qualified}: a non-incremental {what}'s read() cannot return "
        "None. The source is the whole truth, so an empty one retires everything "
        f"the target holds. Return {instead}, or declare Incremental: true, where "
        "None means there is no work."
    )


class Folder(WeaverObject):
    """Files materialised into a Lakehouse Files directory.

    ``read()`` writes into this object's staging directory and returns it.
    When an incremental folder needs explicit deletes, it returns
    ``(staging_folder, files_to_delete)`` instead.

    Its location has two spellings, and neither converts to the other by string
    surgery: :meth:`path` is a :class:`pathlib.Path` for ordinary Python,
    :meth:`spark_path` the ``abfss://`` string an engine needs.
    """

    #: A Folder's catalogue identity carries the ``Files/`` prefix, so a Folder
    #: and a Table of the same name keep separate bookmarks.
    _is_files = True

    def __init__(self, spark: Any, **kwargs: Any) -> None:
        super().__init__(spark, **kwargs)
        self._issued_staging = None
        self._read_staging = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        authored_read = cls.__dict__.get("read")
        if authored_read is None:
            return

        # Authors override read(), so this is the boundary that also covers a
        # direct obj.read() call without changing that public API.
        @wraps(authored_read)
        def read_with_staging(self, *args, **read_kwargs):
            self._clear_read_staging()
            return authored_read(self, *args, **read_kwargs)

        cls.read = read_with_staging

    def path(self) -> Path:
        """This folder's materialised location, as Python addresses it::

            for file in Sales__Export(self).path().glob("*.json"):
                ...

        In OneLake this is Weaver's mount of the root the Lakehouse resolved to,
        never ``/lakehouse/default``. That names whatever a notebook attached,
        and a load runs detached against Lakehouses it resolved by name.
        """

        return self.lakehouse.folder_path(*self.identity)

    def spark_path(self) -> str:
        """This folder's location, as Spark addresses it::

            rows = self.spark.read.json(Sales__Export(self).spark_path())

        The ``abfss://`` form, which is what Spark reads.
        """

        return self.lakehouse.folder_spark_path(*self.identity)

    def files_since(self, bookmark: datetime) -> dict[Path, datetime]:
        """Current files changed strictly after an aware ``bookmark``, and when::

            for path in Sales__Landing(self).files_since(bookmark):
                ...

        Keys are full paths ordinary Python can open; values are UTC.
        """

        from .runtime.folder_load import files_since

        return files_since(self.path(), bookmark)

    def latest_files(self) -> dict[Path, datetime]:
        """The current files from the newest change that left files in place."""

        from .runtime.folder_load import latest_files

        return latest_files(self.path())

    def deleted_since(self, bookmark: datetime) -> dict[Path, datetime]:
        """Files deleted strictly after an aware ``bookmark``, and when.

        A returned path is the file the deletion retired, so it normally does
        not exist.
        """

        from .runtime.folder_load import deleted_since

        return deleted_since(self.path(), bookmark)

    def staging_folder(self) -> "StagingFolder":
        """The staging directory available to this ``read()``.

        A load receives its fixed sibling staging directory. A standalone
        ``read()`` receives a temporary directory, reused until the next
        read on this object.
        """

        issued = getattr(self, "_issued_staging", None)
        if issued is not None:
            return issued
        staging = getattr(self, "_read_staging", None)
        if staging is None:
            import tempfile

            from .runtime.folder_load import StagingFolder

            staging = StagingFolder(path=Path(tempfile.mkdtemp(prefix="weaver-")))
            self._read_staging = staging
        return staging

    def _clear_read_staging(self) -> None:
        """Remove the temporary staging directory from a previous read."""

        staging = getattr(self, "_read_staging", None)
        if staging is None:
            return
        try:
            if staging.path.exists():
                import shutil

                shutil.rmtree(staging.path)
        finally:
            self._read_staging = None

    def __del__(self) -> None:
        """Best-effort cleanup for staging a caller did not consume."""

        try:
            self._clear_read_staging()
        except Exception:
            pass

    def _staging_path(self) -> Path:
        """Where staging goes: the destination's own path, with a suffix.

        The same sibling ``folder_staging`` the resolver exposes
        issues.
        """

        destination = self.path()
        return destination.with_name(f"{destination.name}{STAGING_SUFFIX}")

    def load(self, fault_tolerant: bool = False, reload: bool = False) -> "LoadResult":
        """Run this folder's load and record what it did.

        Independently runnable, needing no repository and no bundle::

            Sales__Export(spark, catalogue="Warehouse/Weaver").load()

        The standalone interface: it needs the catalogue, records this folder's
        operational state, and flushes before returning. An orchestrated run
        calls :meth:`_load` and records what settled itself, so one row has one
        writer.

        ``reload`` is refused. Reload clears the target before ``read()`` runs,
        and for a folder that is a file reconciliation this branch does not have.
        """

        if reload:
            raise LoadError(
                f"{self.object_id}: reload covers tables. A folder's contents are "
                "files, and clearing them is a reconciliation Weaver does not do "
                "here. Load it without reload."
            )
        return _recorded_load(self, fault_tolerant=fault_tolerant)

    def _load(self, fault_tolerant: bool = False) -> "LoadResult":
        """Run this folder's ``read()`` and publish what it staged.

        The load itself and nothing else: it writes no operational state, so
        whoever called it owns the record.

        Staging is reset, issued to ``read()``, published, and removed on
        success. It is retained on failure, as the one directory worth opening.
        """

        self._anchor()

        from .runtime.folder_load import (
            adopt_existing_files,
            load_folder,
            new_staging_folder,
            remove_staging,
        )
        from .runtime.load_contract import FolderLoadContract
        from .runtime.load_result import LoadResult

        # Before the gate and before read(), so the instant a clean load is
        # bookmarked at precedes everything it read.
        began = datetime.now(timezone.utc)
        contract = FolderLoadContract.from_document(self._document())
        # A static folder bypasses staging, source reads and file reconciliation.
        # The bookmark decides it, not the folder's contents: Static means "load
        # this once", and a bookmark is the record of whether that has happened.
        if contract.static and self.bookmark() > _sentinel():
            return LoadResult(succeeded=True, is_static_skip=True)

        # Before read(), so what the authored code sees through files_since() and
        # its bookmark is a history that accounts for the files already there. A
        # Static folder is left alone: loading it once is the whole contract, and
        # its files are the thing that was loaded.
        if not contract.static:
            adopt_existing_files(self.path())

        issued = new_staging_folder(self.path(), self._staging_path())
        self._issued_staging = issued
        try:
            staged, deletes = self._read_result()
            if staged is None:
                _refuse_no_staging(contract, "folder", "self.staging_folder()")
            if staged is None and deletes is None:
                # An incremental source that has already found nothing to
                # do: no file is staged and none is claimed, so nothing is
                # scanned and nothing is published.
                result = LoadResult(succeeded=True)
            elif staged is None:
                # Deletion only. The issued staging is empty, which for an
                # incremental folder already means "nothing new", so the
                # reconciliation retires exactly the files claimed.
                result = load_folder(
                    contract=contract,
                    destination=self.path(),
                    staging=issued.path,
                    deletes=deletes,
                    fault_tolerant=fault_tolerant,
                )
            elif staged is not issued:
                raise LoadError(
                    f"{type(self).__name__}.read() returned "
                    f"{type(staged).__name__} {staged!r} rather than the folder "
                    "self.staging_folder() issued. Return self.staging_folder()."
                )
            else:
                result = load_folder(
                    contract=contract,
                    destination=self.path(),
                    staging=issued.path,
                    deletes=deletes,
                    fault_tolerant=fault_tolerant,
                )
        finally:
            # Cleared whatever happened, so a second load cannot be handed the
            # first one's directory.
            self._issued_staging = None
        remove_staging(issued.path)
        return self._bookmarked(result, began)


class Table(WeaverObject):
    """Rows materialised into a Delta table or a Warehouse table.

    ``read()`` returns staging::

        return rows

    An incremental table may return an explicit delete claim beside it, because
    a window on the truth cannot retire a row by not carrying it::

        return rows, retired

    A non-incremental source is the whole truth, so a row's absence from it is
    what retires the row and there is nothing a second value could say.
    """

    def dataframe(self) -> Any:
        """This table as it currently stands, read from its Delta files.

        By path rather than catalogue name: a path needs nothing attached, so
        the same call serves any resolved Lakehouse.
        """

        return self.spark.read.format("delta").load(
            self.lakehouse.table_path(*self.identity)
        )

    def _staged(self, contract) -> tuple[Any, Any]:
        """What ``read()`` staged, and the delete claim it was allowed to make.

        A non-incremental table returns staging on its own, and is refused on the
        shape of what it returned rather than on what the second value holds: no
        Spark job runs to establish that a frame was empty, and an author reading
        the message is told what to write instead.

        :func:`weaver.runtime.table_load._delete_driver` holds the same rule at
        the other end, for rows that reach a load without passing through here.
        """

        from .runtime.load_contract import normalise_read_result

        returned = self.read()
        if not contract.incremental and isinstance(returned, tuple):
            raise LoadError(
                f"{contract.qualified}: a non-incremental table returns staging "
                "on its own. The source is the whole truth, so a row's absence "
                "from it is what retires the row. Return the staging frame, or "
                "declare Incremental: true."
            )
        staged, deletes = normalise_read_result(returned)
        if staged is None:
            _refuse_no_staging(contract, "table", "the staging frame")
        return staged, deletes

    def empty_dataframe(self) -> Any:
        """This table's shape with no rows, which is an incremental load's no-op.

        Taken from the table itself, so the columns are the ones the load has to
        match. The physical table must therefore already exist.
        """

        return self.dataframe().limit(0)

    def load(
        self,
        fault_tolerant: bool = False,
        ignore_stability_threshold: bool = False,
        reload: bool = False,
    ) -> "LoadResult":
        """Run this table's load and record what it did.

        Independently runnable, needing no repository and no bundle::

            Sales__Customer(spark, catalogue="Warehouse/Weaver").load()

        The standalone interface: it needs the catalogue, records this table's
        operational state, and flushes before returning. An orchestrated run
        calls :meth:`_load` and records what settled itself, so one row has one
        writer.

        ``reload`` reconstructs the table from zero: the bookmark goes back to
        the sentinel, ``_.LoadStatus`` goes to Pending, the target is emptied,
        and the authored load then runs against both.
        """

        return _recorded_load(
            self,
            fault_tolerant=fault_tolerant,
            ignore_stability_threshold=ignore_stability_threshold,
            reload=reload,
        )

    def _load(
        self,
        fault_tolerant: bool = False,
        ignore_stability_threshold: bool = False,
        reload: bool = False,
    ) -> "LoadResult":
        """Run this table's ``read()`` and write what it staged.

        The load itself and nothing else: it writes no operational state, so
        whoever called it owns the record.

        ``ignore_stability_threshold`` waives the declared delete and update
        limits for one run, for when a very large change is the correct answer.

        ``reload`` empties the target before ``read()`` is called. Two kinds of
        incremental source depend on that ordering: one reads its window from the
        bookmark, which the caller has already put back to the sentinel, and one
        joins against the target itself. Both then start from zero.
        """

        self._anchor()

        from .runtime.load_contract import LoadContract
        from .runtime.load_result import LoadResult
        from .runtime.table_load import clear_table, load_table

        # Before the gate and before read(), so the instant a clean load is
        # bookmarked at precedes everything it read.
        began = datetime.now(timezone.utc)
        contract = LoadContract.from_document(self._document())
        # Before read(), so a seeded static object costs no source query. The
        # bookmark decides it, not the table's contents. Static means "load this
        # once", and the bookmark records whether that has happened, so a table
        # populated by hand is still loaded and a table a clean load emptied is
        # still skipped. A reload is the caller saying to load it again.
        if not reload and contract.static and self.bookmark() > _sentinel():
            return LoadResult(succeeded=True, is_static_skip=True)

        if reload:
            # Before read(), because an incremental source may join against the
            # target to find what it has still to produce.
            clear_table(self.spark, contract=contract, lakehouse=self.lakehouse)

        # Staging: unvalidated, unreconciled, nothing yet classified as new or
        # changed.
        staged, deletes = self._staged(contract)
        if staged is None and deletes is None:
            # An incremental source that has already found nothing to do.
            # Nothing is staged and nothing is claimed, so no Spark job runs to
            # establish that a frame the author never built would have been empty.
            return self._bookmarked(LoadResult(succeeded=True), began)
        if staged is None:
            # Deletion only. The target's own shape stands in for staging, so the
            # reconciliation retires exactly the rows claimed and inserts none.
            staged = self.empty_dataframe()
        return self._bookmarked(
            load_table(
                self.spark,
                contract=contract,
                lakehouse=self.lakehouse,
                staging_frame=staged,
                deletes=deletes,
                fault_tolerant=fault_tolerant,
                ignore_stability_threshold=ignore_stability_threshold,
            ),
            began,
        )


class SparkSqlTable(Table):
    """A table whose ``read()`` is a Spark SQL program rather than Python.

    **Generated, not authored.** A developer writes ``Sales.OrderSummary.sql``
    and Weaver installs ``Sales__OrderSummary.py``, which is this class with the
    authored SQL attached::

        class Sales__OrderSummary(SparkSqlTable):
            sql = SQL

    Public because the deployed module imports it, not as a second way to author
    an object: a repository ``.py`` subclassing this is refused.

    The program's shape is its contract: one query stages, a second names the
    keys to delete. See :mod:`weaver.declaration.spark_sql_program`.
    """

    #: The authored program, addressed and embedded when the module was built.
    sql: str = ""

    def _document(self):
        """This module's contract, read as the Spark SQL document it came from.

        The docstring is the authored ``.sql`` header verbatim, so it is parsed
        under SQL rules: Python's would refuse a table that leaves its schema to
        be inferred, which only a SQL table does.
        """

        import sys

        from .declaration.metadata import SPARK_SQL, parse_document
        from .runtime.load_contract import module_metadata_text

        module = sys.modules.get(type(self).__module__)
        if module is None:  # pragma: no cover - a class with no importable module
            raise LoadError(
                f"{type(self).__name__} was defined outside an importable module, "
                "so its Weaver metadata cannot be read"
            )
        return parse_document(module_metadata_text(module), language=SPARK_SQL)

    def read(self):
        """Run the embedded program and return what it staged.

        The same shapes an authored ``read()`` returns: staging on its own, and
        for an incremental table naming keys to delete, the two together.
        """

        from .runtime.load_contract import LoadContract
        from .runtime.spark_sql_table import read_spark_sql

        return read_spark_sql(
            self.spark,
            sql=self.sql,
            contract=LoadContract.from_document(self._document()),
        )


class View(WeaverObject):
    """A view over other objects, declared in SQL.

    A view has no ``read()``: its definition is its query.
    """

    def dataframe(self) -> Any:
        """This view's contents.

        By name, not by path: a view exists only in the catalogue.
        """

        return self.spark.table(self.lakehouse.qualify(*self.identity))


class _Validation(WeaverObject):
    """What a Test and an Assumption share: they judge rather than materialise.

    A validation has two interfaces, and they divide the same way a loadable
    object's do.

    ``read()`` is the primitive: it evaluates the validation and returns the rows
    that are the evidence: the discrepancies for a Test, the contradicting rows
    for an Assumption. It records nothing and needs no catalogue, so an author
    can call it and look at what came back. An orchestrated run calls it and
    records centrally.

    ``run()`` is the standalone interface: it needs the catalogue, calls
    ``read()``, records what the validation found, and flushes before returning::

        Sales__OrdersReconcile(spark, catalogue="Warehouse/Weaver").run()
    """

    #: A validation's work is recorded as a test, whichever kind it is: the two
    #: are told apart by the row's own ``Test type``.
    _task_type = "test"

    #: Which kind of validation this is, as its declaration spells it.
    _validation_kind = ""

    def run(self):
        """Evaluate this validation and record what it found.

        Returns the validation's own result, discrepancy counts for a Test and
        violation counts for an Assumption, rather than the rows. The rows are
        what ``read()`` gives; a durable record of them would put whatever the
        validation selected into the estate's own evidence.

        A validation that could not be evaluated is recorded as an Error and then
        raised. It found nothing, and reporting zero discrepancies for it is the
        one answer a validation must never give. The generated ``_.Test`` does
        the same inside its own TRY/CATCH.
        """

        self._anchor()
        started = datetime.now(timezone.utc)
        try:
            result = self._evaluated()
        except Exception as unevaluated:
            self._record(
                self._settled_validation(
                    self._failed_to_run(f"{type(unevaluated).__name__}: {unevaluated}"),
                    started=started,
                    raised=True,
                )
            )
            raise
        self._record(self._settled_validation(result, started=started))
        return result

    def _evaluated(self):
        """What this validation found, from the rows its ``read()`` returned."""

        from .runtime.validation_result import result_from_rows

        result, _rows = result_from_rows(self.read(), kind=self._validation_kind)
        return result

    def _failed_to_run(self, message: str):
        """A result in this validation's own vocabulary, carrying no counts."""

        from .runtime.validation_result import AssumptionResult, TestResult

        kind = TestResult if self._validation_kind == TEST else AssumptionResult
        return kind.failed_to_run(message)

    def _settled_validation(self, result, *, started, raised: bool = False):
        from .run.record import settled_validation

        return settled_validation(
            self._installed,
            result,
            physical_target=self._physical_target(),
            kind=self._validation_kind,
            started=started,
            completed=datetime.now(timezone.utc),
            raised=raised,
        )


class Assumption(_Validation):
    """A statement about the estate that returns the rows contradicting it.

    An Assumption succeeds when it returns nothing::

        \"\"\"
        Assumption ID: Sales.OrdersUpToDate

        Description: Orders contain data up to the expected business date.
        \"\"\"

        from Sales__Orders import Sales__Orders

        from weaver import Assumption


        class Sales__OrdersUpToDate(Assumption):
            def read(self):
                orders = Sales__Orders(self).dataframe()
                return orders.where(...)   # empty when the assumption holds

    What ``read()`` returns is the evidence itself, so there is nothing to
    correlate and an Assumption may not declare a primary key. In every other
    way it is an ordinary Weaver object.
    """

    _validation_kind = ASSUMPTION

    def read(self):
        raise NotImplementedError(
            f"{type(self).__name__} must implement read(), returning the rows "
            "that contradict the assumption. No rows means it holds."
        )


class Test(_Validation):
    """A comparison of an expected relation with an actual one.

    A Test succeeds when the two are the same set::

        \"\"\"
        Test ID: Sales.OrdersReconcile

        Description: Orders reconcile to the independently derived expected relation.

        Primary key: Order id
        \"\"\"

        from Sales__Orders import Sales__Orders
        from Sales__OrderSource import Sales__OrderSource

        from weaver import Test


        class Sales__OrdersReconcile(Test):
            def expected(self):
                return Sales__OrderSource(self).dataframe()

            def actual(self):
                return Sales__Orders(self).dataframe()

    The author writes the two sides; ``read()`` is Weaver's symmetric difference
    and may not be overridden, so passing means the same for every Test. The
    parser refuses an override too.

    The declared primary key correlates diagnostic rows across the two sides and
    changes nothing about what is compared. See
    :mod:`weaver.runtime.test_compare`.
    """

    #: Not a pytest test class. Weaver's Test is a data validation and pytest's
    #: collector recognises only the name, so it would warn about every module
    #: that imports this one into a test.
    __test__ = False

    _validation_kind = TEST

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "read" in cls.__dict__:
            raise LoadError(
                f"{cls.__name__} defines read(), which a Test may not: Weaver "
                "compares the two sides. Write expected() and actual(), or "
                "declare an Assumption to return the rows directly."
            )

    def expected(self):
        raise NotImplementedError(
            f"{type(self).__name__} must implement expected(), returning the "
            "relation the actual data is required to match"
        )

    def actual(self):
        raise NotImplementedError(
            f"{type(self).__name__} must implement actual(), returning the "
            "relation under test"
        )

    def _sides(self):
        """The two relations to compare, as a pair.

        The hook a compiled Test overrides. A Spark SQL Test's sides come from
        one program: running it twice would compare two snapshots of whatever
        its setup materialised and report the difference as failure.
        """

        return self.expected(), self.actual()

    def read(self):
        """The rows on which expected and actual disagree.

        Empty when the Test passes. Each row carries ``_weaver_side``, the side
        it came from, and ``_weaver_sk``, which pairs the two sides of one
        changed entity when a primary key is declared.
        """

        from .runtime.test_compare import compare

        expected, actual = self._sides()
        return compare(
            expected,
            actual,
            primary_key=self._document().primary_key,
            what=type(self).__name__,
        )


class _SparkSqlValidation:
    """What the two generated validation bases share.

    Generated, not authored: a developer writes ``Sales.OrdersReconcile.sql``
    and Weaver installs ``Sales__OrdersReconcile.py``, one of these classes with
    the authored SQL attached. A repository ``.py`` subclassing one is refused.
    """

    #: The authored program, addressed and embedded when the module was built.
    sql: str = ""

    def _document(self):
        """This module's contract, read as the SQL document it came from.

        The docstring is the authored ``.sql`` header verbatim, so it is parsed
        under SQL rules rather than Python's.
        """

        import sys

        from .declaration.metadata import SPARK_SQL, parse_document
        from .runtime.load_contract import module_metadata_text

        module = sys.modules.get(type(self).__module__)
        if module is None:  # pragma: no cover - a class with no importable module
            raise LoadError(
                f"{type(self).__name__} was defined outside an importable module, "
                "so its Weaver metadata cannot be read"
            )
        return parse_document(module_metadata_text(module), language=SPARK_SQL)


class SparkSqlTest(_SparkSqlValidation, Test):
    """A Test whose two sides are a Spark SQL program rather than Python.

    The program's shape is its contract: after any setup, the first query is
    expected and the second is actual. See
    :mod:`weaver.declaration.validation_program`.
    """

    __test__ = False

    def _sides(self):
        """Both relations, from one execution of the program."""

        from .runtime.spark_sql_validation import read_spark_sql_test

        return read_spark_sql_test(self.spark, sql=self.sql, what=type(self).__name__)

    def expected(self):
        return self._sides()[0]

    def actual(self):
        return self._sides()[1]


class SparkSqlAssumption(_SparkSqlValidation, Assumption):
    """An Assumption whose violating rows are a Spark SQL program.

    After any setup, one query returns the rows that contradict it.
    """

    def read(self):
        from .runtime.spark_sql_validation import read_spark_sql_assumption

        return read_spark_sql_assumption(
            self.spark, sql=self.sql, what=type(self).__name__
        )


def _identity(class_name: str) -> tuple[str, str]:
    """``Sales__Order`` → ``("Sales", "Order")``; ``___Load`` → ``("_", "Load")``.

    A run of leading underscores is read as a schema plus the separator: ``_``
    is a real schema, so ``_.Load`` spells as three. The rule is the parser's
    (:func:`weaver.declaration.source.python_id_parts`), repeated here rather
    than imported.
    """

    leading = len(class_name) - len(class_name.lstrip("_"))
    if leading >= len(CLASS_ID_SEPARATOR) + 1:
        split = [
            class_name[: leading - len(CLASS_ID_SEPARATOR)],
            class_name[leading:],
        ]
    else:
        split = class_name.split(CLASS_ID_SEPARATOR)
    parts = [part.strip() for part in split]
    if len(parts) != 2 or not all(parts):
        raise LoadError(
            f"{class_name!r} does not name an object. A Weaver class separates "
            f"schema and object with {CLASS_ID_SEPARATOR!r}, as in Sales__Order."
        )
    return parts[0], parts[1]


#: The authoring base classes, by the metadata kind that selects them.
#:
#: :class:`SparkSqlTable` is absent: it is the generated form of a ``.sql``
#: table, and admitting it would make one object authorable two ways, with two
#: parsers that could disagree about what it declared.
BASE_CLASSES = {
    "Folder": Folder,
    "Table": Table,
    "View": View,
    "Test": Test,
    "Assumption": Assumption,
}
BASE_CLASS_NAMES = frozenset(cls.__name__ for cls in BASE_CLASSES.values())
