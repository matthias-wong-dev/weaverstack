"""Base classes for Python-authored Weaver objects."""

from __future__ import annotations

from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .errors import LoadError, WeaverError
from .lakehouse import Lakehouse, default_lakehouse
from .spark import identifier

if TYPE_CHECKING:  # pragma: no cover - for type readers only
    from .catalogue.state import Catalogue
    from .runtime.folder_load import StagingFolder
    from .runtime.load_result import LoadResult

#: Repeated here so the authoring surface does not import the runtime.
STAGING_SUFFIX = "_Staging"

#: ``Sales.Order`` is spelled ``Sales__Order`` because module names cannot contain dots.
CLASS_ID_SEPARATOR = "__"

#: Repeated here so authored code does not import the declaration parser.
TEST = "Test"
ASSUMPTION = "Assumption"


class WeaverObject:
    """Base class for authored objects.

    Pass a Spark session, or another object whose session, Lakehouse and
    catalogue this object should inherit::

        My__Table(spark)                               # freestanding
        My__Table(spark, catalogue="Warehouse/Weaver")  # anchored

    ``catalogue`` may name its Warehouse or supply an already-read
    :class:`~weaver.catalogue.state.Catalogue`. Without it, ``read()`` works but
    ``load()`` and ``run()`` do not.
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
            # A child resolves its own identity and bookmark against the same catalogue.
            inherited = owner._catalogue
        if spark is None:
            raise LoadError(
                f"{type(self).__name__} needs a Spark session. "
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
                f"{type(lakehouse).__name__}. Resolve the Lakehouse first with "
                "weaver.lakehouse_for()"
            )

        self.spark = spark
        self.lakehouse: Lakehouse = (
            lakehouse if lakehouse is not None else default_lakehouse(spark)
        )
        self.spark_root = self.lakehouse.spark_root

        self._catalogue = None
        self._installed = None
        from .catalogue.state import Catalogue as _Catalogue

        if isinstance(catalogue, _Catalogue):
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
        """Anchor this object to an already-read catalogue and return it.

        ``identity`` supplies the installed identity when it is already known;
        otherwise the catalogue resolves it.
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

    #: The ``Files/`` prefix keeps Folder and Table bookmarks distinct.
    _is_files = False

    def bookmark(self):
        """Return the UTC instant before this object's last clean load began.

        The result is always timezone-aware. An object with no bookmark returns
        the sentinel, so an incremental read asks for everything::

            def read(self):
                return Source__Export(self).files_since(self.bookmark())

        Only a catalogue-anchored object has a bookmark.
        """

        return self._anchor().bookmark(self._installed)

    def _anchor(self):
        if self._catalogue is not None:
            return self._catalogue
        raise LoadError(
            f"{self.object_id} is not anchored to the Weaver catalogue. Construct it as "
            f'{type(self).__name__}(spark, catalogue="Warehouse/<name>").'
        )

    def _bookmarked(self, result, began):
        from dataclasses import replace as _replace

        # Only a clean load has consumed its complete source window.
        if not result.succeeded or result.rows_rejected:
            return result
        return _replace(result, bookmark_datetime=began)

    def _physical_target(self) -> str:
        return f"Lakehouse/{self.lakehouse.name}"

    def _record(self, settled) -> None:
        """Record standalone work synchronously before returning."""

        from .run.record import RunRecord, new_workflow_id

        record = RunRecord(
            workflow_id=new_workflow_id(),
            task_type=self._task_type,
            catalogue=self._anchor(),
        )
        record.settled(settled)
        record.flush()

    _task_type = "load"

    def read(self):
        raise NotImplementedError(f"{type(self).__name__} must implement read()")

    def _read_result(self):
        from .runtime.load_contract import normalise_read_result

        return normalise_read_result(self.read())

    # --- the load contract, read from this module's own docstring ----------

    def _document(self):
        """Parse the declaration on every call so a reloaded module takes effect."""

        import sys

        from .runtime.load_contract import document_for_module

        module = sys.modules.get(type(self).__module__)
        if module is None:  # pragma: no cover - a class with no importable module
            raise LoadError(
                f"{type(self).__name__} was defined outside an importable module, "
                "so its Weaver metadata cannot be read. Define it in an importable "
                "module"
            )
        return document_for_module(module)

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.object_id} in {self.lakehouse.name}>"


def _sentinel():
    from .catalogue.tables import BOOKMARK_SENTINEL

    return BOOKMARK_SENTINEL


def _audit_names() -> tuple[str, ...]:
    """The three row audit columns, in their physical Delta spelling."""

    from .declaration.metadata import AUDIT_COLUMNS, PYTHON, audit_column_name

    return tuple(audit_column_name(logical, PYTHON) for logical in AUDIT_COLUMNS)


def _business_names(physical) -> tuple[str, ...]:
    """Physical columns less the ones Weaver owns, in physical order."""

    from .declaration.metadata import PYTHON, signature_column_name

    managed = {*_audit_names(), signature_column_name(PYTHON)}
    return tuple(name for name in physical if name not in managed)


def _recorded_load(object, **policy) -> "LoadResult":
    """Run one object and write its operational record before returning.

    Exceptions are recorded and re-raised unchanged. Weaver errors are Failed;
    other exceptions are Error. ``reload`` resets load state before execution.
    """

    from .run.record import RunRecord, new_workflow_id

    # An unanchored refusal cannot be recorded without a catalogue or identity.
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
        result = object._load(**policy)
    except Exception as raised:
        _settle(
            record,
            _settled(
                object,
                _carried(raised),
                started=started,
                raised=True,
                refused=isinstance(raised, WeaverError),
            ),
        )
        raise
    _settle(record, _settled(object, result, started=started))
    return result


def _settle(record, settled) -> None:
    record.settled(settled)
    record.flush()


def _settled(object, result, *, started, raised: bool = False, refused: bool = False):
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
    from .runtime.load_result import LoadResult

    carried = getattr(raised, "result", None)
    return (
        carried
        if carried is not None
        else LoadResult.failure(f"{type(raised).__name__}: {raised}")
    )


def _refuse_no_staging(contract, what: str, instead: str) -> None:
    if contract.incremental:
        return
    raise LoadError(
        f"{contract.qualified}.read() returned None for a non-incremental {what}. "
        f"Return {instead}, or declare Incremental: true when there is no work."
    )


class Folder(WeaverObject):
    """Files materialised into a Lakehouse Files directory.

    ``read()`` writes into this object's staging directory and returns it.
    When an incremental folder needs explicit deletes, it returns
    ``(staging_folder, files_to_delete)`` instead.

    :meth:`path` returns a :class:`pathlib.Path`; :meth:`spark_path` returns its
    ``abfss://`` address.
    """

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

        # Wrap the authored override so direct read() calls also reset staging.
        @wraps(authored_read)
        def read_with_staging(self, *args, **read_kwargs):
            self._clear_read_staging()
            return authored_read(self, *args, **read_kwargs)

        cls.read = read_with_staging

    def path(self) -> Path:
        """Return this folder's mounted path for Python file access::

            for file in Sales__Export(self).path().glob("*.json"):
                ...

        The path uses the resolved Lakehouse rather than the notebook's default.
        """

        return self.lakehouse.folder_path(*self.identity)

    def spark_path(self) -> str:
        """Return this folder's ``abfss://`` address for Spark::

        rows = self.spark.read.json(Sales__Export(self).spark_path())
        """

        return self.lakehouse.folder_spark_path(*self.identity)

    def files_since(self, bookmark: datetime) -> dict[Path, datetime]:
        """Return files changed strictly after an aware ``bookmark`` and their UTC times::

        for path in Sales__Landing(self).files_since(bookmark):
            ...
        """

        from .runtime.folder_load import files_since

        return files_since(self.path(), bookmark)

    def latest_files(self) -> dict[Path, datetime]:
        """Return current files from the latest change that left files in place."""

        from .runtime.folder_load import latest_files

        return latest_files(self.path())

    def deleted_since(self, bookmark: datetime) -> dict[Path, datetime]:
        """Return files deleted strictly after an aware ``bookmark`` and their UTC times.

        A returned path is the file the deletion retired, so it normally does
        not exist.
        """

        from .runtime.folder_load import deleted_since

        return deleted_since(self.path(), bookmark)

    def staging_folder(self) -> "StagingFolder":
        """Return the staging directory available to this ``read()``.

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
        try:
            self._clear_read_staging()
        except Exception:
            pass

    def _staging_path(self) -> Path:
        destination = self.path()
        return destination.with_name(f"{destination.name}{STAGING_SUFFIX}")

    def load(self, fault_tolerant: bool = False, reload: bool = False) -> "LoadResult":
        """Run and record this folder's load.

        Independently runnable, needing no repository and no bundle::

            Sales__Export(spark, catalogue="Warehouse/Weaver").load()

        This standalone interface requires a catalogue and flushes its record
        before returning. Folders do not support ``reload``.
        """

        if reload:
            raise LoadError(
                f"{self.object_id} is a Folder and cannot be reloaded. Load it "
                "without reload."
            )
        return _recorded_load(self, fault_tolerant=fault_tolerant)

    def _load(self, fault_tolerant: bool = False) -> "LoadResult":
        """Run ``read()`` and publish its staging without recording the load.

        Staging is reset, issued to ``read()``, published, and removed on
        success. It is retained on failure for inspection.
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

        # A clean load's bookmark must precede everything read in its window.
        began = datetime.now(timezone.utc)
        contract = FolderLoadContract.from_document(self._document())
        # The bookmark, not current contents, records whether Static loaded once.
        if contract.static and self.bookmark() > _sentinel():
            return LoadResult(succeeded=True, is_static_skip=True)

        # Adopt before read() so file history includes existing destination files.
        # Static folders retain their original load-once contents.
        if not contract.static:
            adopt_existing_files(self.path())

        issued = new_staging_folder(self.path(), self._staging_path())
        self._issued_staging = issued
        try:
            staged, deletes = self._read_result()
            if staged is None:
                _refuse_no_staging(contract, "folder", "self.staging_folder()")
            if staged is None and deletes is None:
                result = LoadResult(succeeded=True)
            elif staged is None:
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
            # Never hand a later load this load's staging directory.
            self._issued_staging = None
        remove_staging(issued.path)
        return self._bookmarked(result, began)


class Table(WeaverObject):
    """Rows materialised into a Delta table or a Warehouse table.

    ``read()`` returns staging::

        return rows

    An incremental table with no primary key appends every returned row. Existing
    rows are left in place, and a declared identity is generated for each insert.

    A keyed incremental table may return an explicit delete claim beside staging,
    because a window on the truth cannot retire a row by not carrying it::

        return rows, retired

    A non-incremental source is the whole truth. A keyed table retires rows absent
    from it, while an unkeyed table is replaced wholesale.
    """

    def columns(self) -> tuple[str, ...]:
        """This table's business column names, in declared order.

        The projection that settles a staging frame::

            return frame.select(*self.columns())

        A column the frame does not carry fails there, so an absence a source
        genuinely has must be written as an explicit expression. Weaver's own
        audit and signature columns are never reported.
        """

        return self._business_columns()

    def primary_key_columns(self) -> tuple[str, ...]:
        """The declared primary key, in declaration order, or ``()`` when unkeyed.

        A delete claim is that projection of the rows the load retires::

            return frame.where(...).select(*self.primary_key_columns())
        """

        return tuple(self._document().primary_key)

    def dataframe(self, row_audit_columns: bool = False) -> Any:
        """Read this table from its resolved Lakehouse's Delta path.

        Returns the business columns. ``row_audit_columns`` appends the three
        row audit datetimes after them. The row signature is Weaver's load
        bookkeeping and is never returned.
        """

        frame = self._physical_dataframe()
        # Quoted here rather than in columns(), which reports the names the
        # author wrote. Spark splits an unquoted dotted identifier, so a
        # declared `A.B` would resolve as field B of a column A.
        projected = (
            identifier(name) for name in self._projection(frame, row_audit_columns)
        )
        return frame.select(*projected)

    def _physical_dataframe(self) -> Any:
        """The stored table as it is, Weaver's own columns included."""

        return self.spark.read.format("delta").load(
            self.lakehouse.table_path(*self.identity)
        )

    def _business_columns(self, frame=None) -> tuple[str, ...]:
        """The declared columns, or what an inferred table's own frame holds.

        ``frame`` is the physical frame when the caller already has one, so a
        projection reads the table once.
        """

        document = self._document()
        declared = document.schema
        if declared:
            return tuple(column.name for column in declared)
        physical = self._physical_dataframe() if frame is None else frame
        business = _business_names(physical.columns)
        if document.identity is None:
            return business
        return tuple(
            name for name in business if name.lower() != document.identity.lower()
        )

    def _projection(self, frame, row_audit_columns: bool) -> tuple[str, ...]:
        """The author-facing column list for a frame already read.

        Opting in asks for all three audit columns, so a table missing one
        fails the same way a missing business column does, rather than
        quietly returning a narrower frame than the caller asked for.
        """

        business = self._business_columns(frame)
        if not row_audit_columns:
            return business
        return business + _audit_names()

    def _staged(self, contract) -> tuple[Any, Any]:
        """Return staging and any permitted delete claim from ``read()``.

        A non-incremental table returns staging on its own, and is refused on the
        returned shape without running Spark to inspect the second value.
        """

        from .runtime.load_contract import normalise_read_result

        returned = self.read()
        if not contract.incremental and isinstance(returned, tuple):
            raise LoadError(
                f"{contract.qualified}.read() returned a pair for a non-incremental "
                "Table. Return the staging frame alone, or "
                "declare Incremental: true."
            )
        staged, deletes = normalise_read_result(returned)
        if staged is None:
            _refuse_no_staging(contract, "table", "the staging frame")
        return staged, deletes

    def empty_dataframe(self, row_audit_columns: bool = False) -> Any:
        """Return this table's existing shape with no rows.

        The same shape as :meth:`dataframe` with the same argument, so a
        deletion-only load stages a business-shaped frame. The physical table
        must already exist.
        """

        return self.dataframe(row_audit_columns=row_audit_columns).limit(0)

    def load(
        self,
        fault_tolerant: bool = False,
        ignore_stability_threshold: bool = False,
        reload: bool = False,
    ) -> "LoadResult":
        """Run and record this table's load.

        Independently runnable, needing no repository and no bundle::

            Sales__Customer(spark, catalogue="Warehouse/Weaver").load()

        This standalone interface requires a catalogue and flushes its record
        before returning.

        ``reload`` reconstructs the table from zero: the bookmark row is
        removed, ``_.LoadStatus`` goes to Pending, the target is emptied, and the
        authored load then runs.
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
        """Run ``read()`` and write its staging without recording the load.

        ``ignore_stability_threshold`` waives the declared delete and update
        limits for one run, for when a very large change is the correct answer.

        ``reload`` empties the target before ``read()`` is called. The caller has
        already removed the bookmark, so an incremental source starts from zero
        either way it reads.
        """

        self._anchor()

        from .runtime.load_contract import LoadContract
        from .runtime.load_result import LoadResult
        from .runtime.table_load import clear_table, load_table

        # A clean load's bookmark must precede everything read in its window.
        began = datetime.now(timezone.utc)
        contract = LoadContract.from_document(self._document())
        # The bookmark, not current contents, records whether Static loaded once.
        # Reload resets that state before reaching this gate.
        if not reload and contract.static and self.bookmark() > _sentinel():
            return LoadResult(succeeded=True, is_static_skip=True)

        if reload:
            # Clear before read(): incremental source logic may inspect the target.
            clear_table(self.spark, contract=contract, lakehouse=self.lakehouse)

        staged, deletes = self._staged(contract)
        if staged is None and deletes is None:
            return self._bookmarked(LoadResult(succeeded=True), began)
        if staged is None:
            # A deletion-only load uses the target's shape without inserting rows.
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

    Weaver generates this class from an authored ``Sales.OrderSummary.sql``::

        class Sales__OrderSummary(SparkSqlTable):
            sql = SQL

    Repository Python may not subclass this class directly.

    The program's shape is its contract: one query stages, a second names the
    keys to delete. See :mod:`weaver.declaration.spark_sql_program`.
    """

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
                "so its Weaver metadata cannot be read. Define it in an importable "
                "module"
            )
        return parse_document(module_metadata_text(module), language=SPARK_SQL)

    def read(self):
        """Run the embedded program and return staging with any delete keys."""

        from .runtime.load_contract import LoadContract
        from .runtime.spark_sql_table import read_spark_sql

        return read_spark_sql(
            self.spark,
            sql=self.sql,
            contract=LoadContract.from_document(self._document()),
        )


class View(WeaverObject):
    """A SQL view, whose query is its definition and has no ``read()``."""

    def dataframe(self) -> Any:
        """Read this view by its catalogue name."""

        return self.spark.table(self.lakehouse.qualify(*self.identity))


class _Validation(WeaverObject):
    """Shared execution contract for Tests and Assumptions.

    ``read()`` returns evidence without recording it. ``run()`` requires a
    catalogue, records the result, and flushes before returning::

        Sales__OrdersReconcile(spark, catalogue="Warehouse/Weaver").run()
    """

    #: The recorded row's ``Test type`` distinguishes the validation kind.
    _task_type = "test"

    _validation_kind = ""

    def run(self):
        """Evaluate and record this validation.

        Returns discrepancy counts for a Test or violation counts for an
        Assumption. Use ``read()`` for the evidence rows.

        An evaluation error is recorded as Error and re-raised; it never reports
        zero failures.
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
        from .runtime.validation_result import result_from_rows

        result, _rows = result_from_rows(self.read(), kind=self._validation_kind)
        return result

    def _failed_to_run(self, message: str):
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

    An Assumption may not declare a primary key because its returned rows are the
    evidence rather than two relations to correlate.
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

    Implement ``expected()`` and ``actual()``. ``read()`` is their symmetric
    difference and may not be overridden.

    The declared primary key correlates diagnostic rows across the two sides and
    changes nothing about what is compared. See
    :mod:`weaver.runtime.test_compare`.
    """

    #: Prevent pytest from collecting Weaver validation classes.
    __test__ = False

    _validation_kind = TEST

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "read" in cls.__dict__:
            raise LoadError(
                f"{cls.__name__} defines read(), but a Test compares two sides. "
                "Write expected() and actual(), or "
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
        """Return both relations from one execution when a compiled Test overrides it."""

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
    """Shared base for generated, not directly authored, SQL validations."""

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
                "so its Weaver metadata cannot be read. Define it in an importable "
                "module"
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

    Leading underscores include the schema and separator, so ``_.Load`` uses
    three underscores. This repeats the parser's rule without importing it.
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


#: ``SparkSqlTable`` is generated and cannot be selected as an authoring base.
BASE_CLASSES = {
    "Folder": Folder,
    "Table": Table,
    "View": View,
    "Test": Test,
    "Assumption": Assumption,
}
BASE_CLASS_NAMES = frozenset(cls.__name__ for cls in BASE_CLASSES.values())
