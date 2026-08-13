"""The authoring surface — what a developer writes.

An object is a class in a file named for its ID::

    # Sales__Order.py
    \"\"\"
    Table ID: Sales.Order

    Description: One row per confirmed customer order.

    Lineage: The sales system order export.

    Primary key: Order id

    Schema:
      Order id: string
      Amount: decimal(18,2)
    \"\"\"

    from Sales__Customer import Sales__Customer

    from weaver import Table


    class Sales__Order(Table):
        def read(self):
            customers = Sales__Customer(self).dataframe()
            existing = self.dataframe()
            ...
            return upserts, deletes

**An object is an ordinary Python object bound to a Spark session.** The session
is the one mandatory argument, because authored code executes through it::

    Sales__Order(spark).read()

That is the whole runtime model. There is no hidden active object, no ambient
resolver, and no context injected around a call — an object holds a session, a
destination Lakehouse and its own identity, and every physical access is an
ordinary instance method on it.

**The destination is a resolved Lakehouse, never a name.** In a notebook it is
the one attached to the session, and Weaver reads it
(:func:`weaver.lakehouse.default_lakehouse`); with no unique attachment,
construction fails rather than guessing which Lakehouse a load should land in. An
orchestrator, or anyone addressing more than one Lakehouse, resolves it *outside*
the object and passes it in::

    Sales__Order(spark, lakehouse=resolved_lakehouse)

A string is refused: looking a name up needs a workspace resolver, and that is
not a decision authored code makes.

**Dependencies are imports, and a dependency is constructed from its dependent**::

    Another__Table(self).dataframe()
    My__Folder(self).path()

Importing another object's module declares the dependency; Weaver reads that from
the source without executing it. Passing ``self`` hands over the session and the
resolved Lakehouse together, so a dependency always resolves against the same
target environment as the object reading it. A Python import only ever names an
object in the *same* item, which is why nothing more is needed here — a
cross-item dependency is an alias, and aliases are resolved during build.

**Identity comes from the class name.** ``Sales__Order`` is ``Sales.Order``: the
same rule the repository parser applies to the filename, and the parser has
already refused any file where the two disagree. Nothing is re-parsed, and
nothing is looked up in the catalogue, to know what an object is.

**Objects never mutate the target.** ``read()`` proposes; Weaver owns writing,
CRUD accounting, staging and logging. A Folder writes into its staging directory
and returns it; a Table returns rows.

Nothing here imports PySpark. The session is used through its ordinary API, so
this module stays importable anywhere.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from .errors import LoadError
from .lakehouse import Lakehouse, default_lakehouse

if TYPE_CHECKING:  # pragma: no cover - for type readers only
    from .runtime.folder_load import StagingFolder

#: What Weaver names a folder's staging sibling. Repeated from
#: :mod:`weaver.runtime.folder_load` rather than imported, because the authoring
#: surface must stay importable without the runtime beneath it — the same reason
#: :data:`CLASS_ID_SEPARATOR` is repeated from the parser. The two are asserted
#: identical by ``tests/test_objects.py``.
STAGING_SUFFIX = "_Staging"

#: What separates schema from object in a class name. A module name cannot carry
#: a dot, so a Python object spells ``Sales.Order`` as ``Sales__Order`` — the rule
#: :func:`weaver.declaration.source.object_id_for_filename` applies to the
#: filename, repeated here because the authoring surface must not import the
#: parser (the parser imports it, for the base classes).
CLASS_ID_SEPARATOR = "__"


class WeaverObject:
    """Base for every authored object.

    ``spark`` is the session authored code runs through, and it is mandatory.
    Another Weaver object may be passed in its place — ``Another__Table(self)`` —
    inheriting that object's session and Lakehouse, which is how one object
    reaches another.
    """

    def __init__(self, spark: Any, *, lakehouse: Lakehouse | None = None) -> None:
        if isinstance(spark, WeaverObject):
            owner = spark
            spark = owner.spark
            if lakehouse is None:
                lakehouse = owner.lakehouse
        if spark is None:
            raise LoadError(
                f"{type(self).__name__} needs the Spark session it runs through — "
                f"construct it as {type(self).__name__}(spark), or as "
                f"{type(self).__name__}(self) from another object"
            )
        if isinstance(lakehouse, str):
            raise LoadError(
                f"{type(self).__name__} takes a resolved Lakehouse, not the name "
                f"{lakehouse!r} — resolve it first with "
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
        #: The destination's root — what Spark and Hadoop address. Tables and
        #: folders both hang off it, so nothing an object reaches needs a mount.
        self.spark_root = self.lakehouse.spark_root

    # --- identity ---------------------------------------------------------

    @property
    def identity(self) -> tuple[str, str]:
        """The schema and object name this class declares."""

        return _identity(type(self).__name__)

    @property
    def object_id(self) -> str:
        """This object's ``Schema.Object`` ID, from its class name."""

        return "{}.{}".format(*self.identity)

    def read(self):
        raise NotImplementedError(f"{type(self).__name__} must implement read()")

    # --- the load contract, read from this module's own docstring ----------

    def _document(self):
        """This object's parsed declaration, from the module it was defined in.

        The module *is* the contract. A deployed object in a session has no
        repository to reopen and no catalogue to query, so if its docstring were
        not sufficient then ``load()`` would be the tail end of an orchestration
        rather than something runnable on its own.

        It is read on every call rather than cached, which is what makes an edit
        visible on the next reload — the notebook loop this is meant to support.
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


class Folder(WeaverObject):
    """Files materialised into a Lakehouse Files directory.

    ``read()`` writes into this object's staging directory and returns
    ``(staging_folder, files_to_delete)``.

    **Two spellings of one location, and which you want depends on who reads
    it.** Authored folder code is ordinary Python — it globs, opens and writes —
    so :meth:`path` is a :class:`pathlib.Path`. An engine cannot use that:
    ``spark.read`` wants the ``abfss://`` form, so :meth:`spark_path` is a
    string. Neither is convertible into the other by string surgery, which is
    why there are two methods rather than one and a rule.
    """

    def path(self) -> Path:
        """This folder's materialised location, as *Python* addresses it::

            for file in Sales__Export(self).path().glob("*.json"):
                ...

        A real ``Path``, so ``/``, ``.glob()``, ``.open()``, ``.read_text()`` and
        ``.write_text()`` all work. In OneLake it is Weaver's mount of the root
        the Lakehouse resolved to — never ``/lakehouse/default``, which names
        only whatever a notebook attached, and a load runs detached against
        Lakehouses it resolved by name.
        """

        return self.lakehouse.folder_path(*self.identity)

    def spark_path(self) -> str:
        """This folder's location, as *Spark* addresses it::

            rows = self.spark.read.json(Sales__Export(self).spark_path())

        The ``abfss://`` form on Fabric, the same directory locally. What one
        object hands another when the reader is an engine.
        """

        return self.lakehouse.folder_spark_path(*self.identity)

    def staging_folder(self) -> "StagingFolder":
        """The staging directory Weaver issued for this load.

        Called from ``read()``, and it hands back the *same* object every time
        within one load — the one Weaver reset before ``read()`` began and will
        publish from afterwards. There is no shared staging area and no run
        identifier: staging belongs to the object, at a fixed sibling path named
        ``<destination>_Staging``, so a failed load leaves exactly one directory
        to look at and the next run knows where it is.

        Outside a load there is nothing to issue, and asking says so rather than
        inventing a directory nobody reset.
        """

        issued = getattr(self, "_issued_staging", None)
        if issued is None:
            raise LoadError(
                f"{type(self).__name__}.staging_folder() is issued by load(), and "
                "nothing has issued one — call it from read(), or call "
                f"{type(self).__name__}(spark).load() to run the load that issues it"
            )
        return issued

    def _staging_path(self) -> Path:
        """Where staging goes: the destination's own path, with a suffix.

        The same sibling :meth:`weaver.resolution.LocalResolver.folder_staging`
        issues, and fixed rather than per-run — see :meth:`staging_folder`.
        """

        destination = self.path()
        return destination.with_name(f"{destination.name}{STAGING_SUFFIX}")

    def load(self, fault_tolerant: bool = False) -> "LoadResult":
        """Run this folder's ``read()`` and publish what it staged.

        Independently runnable, which is the point::

            Sales__Export(spark).load(fault_tolerant=False)

        No repository, no catalogue, no bundle and no orchestrator — the module
        carries its own contract and this object carries its own destination.

        The order below is the whole lifecycle, and each step is there because
        the alternative loses something:

        .. code-block:: text

            reset the fixed staging directory   a run begins from nothing it
                                                did not itself produce
            issue one StagingFolder             read() fills what load() will
                                                publish, and they are the same
            run read()                          the author's work
            check identity, not equality        a copy would mean publishing a
                                                directory nobody reset
            publish                             from the issued path
            remove staging on success           nothing to mistake for evidence
            retain staging on failure           the one directory worth looking
                                                at when a load fails
        """

        from .runtime.folder_load import (
            folder_is_populated,
            load_folder,
            new_staging_folder,
            remove_staging,
        )
        from .runtime.load_contract import FolderLoadContract
        from .runtime.load_result import LoadResult

        contract = FolderLoadContract.from_document(self._document())
        # Static folders bypass staging, source reads, and file reconciliation.
        #
        # `static` first, and the order is not style: Python evaluates arguments
        # eagerly, so asking whether the folder is populated *inside* the call
        # would walk the managed tree on every ordinary load to answer a question
        # only a static one can act on.
        if contract.static and folder_is_populated(self.path(), contract.file_keys):
            return LoadResult(succeeded=True)

        issued = new_staging_folder(self.path(), self._staging_path())
        self._issued_staging = issued
        try:
            staged, deletes = _load_pair(self, self.read())
            if staged is not issued:
                raise LoadError(
                    f"{type(self).__name__}.read() must return the StagingFolder "
                    f"self.staging_folder() issued, and returned "
                    f"{type(staged).__name__} {staged!r} instead — return "
                    "self.staging_folder()"
                )
            result = load_folder(
                contract=contract,
                destination=self.path(),
                staging=issued.path,
                deletes=deletes,
                fault_tolerant=fault_tolerant,
            )
        finally:
            # Cleared whatever happened, so a second load cannot be handed the
            # first one's directory — and so asking outside a load still fails.
            self._issued_staging = None
        remove_staging(issued.path)
        return result


class Table(WeaverObject):
    """Rows materialised into a Delta table or a Warehouse table.

    ``read()`` returns ``(upserts, deletes)``.
    """

    def dataframe(self) -> Any:
        """This table as it currently stands, read from its Delta files.

        Addressed by path rather than by catalogue name, which is how Weaver
        reaches Delta everywhere else: a path needs nothing attached, so the same
        call serves any resolved Lakehouse.
        """

        return self.spark.read.format("delta").load(
            self.lakehouse.table_path(*self.identity)
        )

    def empty_dataframe(self) -> Any:
        """This table's shape with no rows — an incremental load's no-op result.

        Taken from the table itself, so the columns are exactly the ones the load
        has to match. That means the physical table must already exist, which is no
        constraint at all: a load returning *this* table's empty shape is by
        definition running against a target that has been built.
        """

        return self.dataframe().limit(0)

    def load(
        self,
        fault_tolerant: bool = False,
        ignore_stability_threshold: bool = False,
    ) -> "LoadResult":
        """Run this table's ``read()`` and write what it staged.

        Independently runnable, which is the point::

            Sales__Customer(spark).load(fault_tolerant=True)

        No repository, no catalogue, no bundle and no orchestrator — the module
        carries its own contract and this object carries its own destination.

        ``ignore_stability_threshold`` waives the declared delete and update
        limits for one run. It exists for the case where a very large change is
        the correct answer — a genuine bulk retirement — and is a deliberate act
        each time rather than a setting that stays on.
        """

        from .runtime.load_contract import LoadContract
        from .runtime.load_result import LoadResult
        from .runtime.table_load import load_table, table_is_populated

        contract = LoadContract.from_document(self._document())
        # Before read(), so a static object that is already seeded costs nothing
        # — no query against the source, no staging table, no comparison. The
        # primitive ran and found the work done; that is not an orchestration
        # skip, and the successful no-op result says as much.
        #
        # `static` first, and the order is not style: Python evaluates arguments
        # eagerly, so asking whether the target is populated *inside* the call
        # would put a Spark action on every ordinary load to answer a question
        # only a static one can act on.
        if contract.static and table_is_populated(
            self.spark, contract=contract, lakehouse=self.lakehouse
        ):
            return LoadResult(succeeded=True)

        # The first value is *staging* — unvalidated, unreconciled, nothing yet
        # classified as new or changed. Naming it so is the point.
        staged, deletes = _load_pair(self, self.read())
        return load_table(
            self.spark,
            contract=contract,
            lakehouse=self.lakehouse,
            staging_frame=staged,
            deletes=deletes,
            fault_tolerant=fault_tolerant,
            ignore_stability_threshold=ignore_stability_threshold,
        )


class SparkSqlTable(Table):
    """A table whose ``read()`` is a Spark SQL program rather than Python.

    **Generated, not authored.** A developer writes ``Sales.OrderSummary.sql``
    and Weaver installs ``Sales__OrderSummary.py``, which is this class with the
    authored SQL attached::

        class Sales__OrderSummary(SparkSqlTable):
            sql = SQL

    That module is an ordinary deployed primitive: it imports, constructs and
    loads exactly as a hand-written one does, and orchestration cannot tell the
    two apart. It is public because the deployed module imports it and because
    someone reaching for an installed primitive in a notebook meets it — not as
    a second way to author an object. A repository ``.py`` subclassing this is
    refused, because ``.sql`` is where a SQL table is written.

    The program's shape is its contract: one query stages, a second names the
    keys to delete. See :mod:`weaver.declaration.spark_sql_program`.
    """

    #: The authored program, addressed and embedded when the module was built.
    sql: str = ""

    def _document(self):
        """This module's contract, read as the Spark SQL document it came from.

        The docstring *is* the authored ``.sql`` header, carried over verbatim,
        so it has to be parsed as what it was written as. Reading it as Python
        metadata would apply Python's rules to a SQL declaration — and refuse
        every table that leaves its schema to be inferred, which is a shape only
        a SQL table has.
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
        """Run the embedded program and return ``(staging, deletes)``."""

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

        By name, not by path: a view exists only in the catalogue, so unlike a
        table there is nothing on disk to address.
        """

        return self.spark.table(self.lakehouse.qualify(*self.identity))


class Assumption(WeaverObject):
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

    ``read()`` is authored, and what it returns *is* the evidence — there is no
    expected relation to compare against and so nothing to correlate. That is
    why an Assumption may not declare a primary key.

    It is an ordinary Weaver object in every other way: constructed from a
    session, or from another object with ``Sales__Orders(self)``, reaching its
    dependencies through the same imports as a Table does.
    """

    def read(self):
        raise NotImplementedError(
            f"{type(self).__name__} must implement read(), returning the rows that "
            "contradict the assumption — no rows means it holds"
        )


class Test(WeaverObject):
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

    **The author writes the two sides; Weaver writes the comparison.** ``read()``
    computes the symmetric difference and is deliberately not authorable — a Test
    that could redefine it would still be called a Test while meaning something
    else, and the one thing a reader must be able to assume about every Test in
    an estate is what passing means. An override is refused here and by the
    repository parser, so it fails whether the class is written in a notebook or
    committed to a repository.

    The declared primary key correlates diagnostic rows across the two sides. It
    changes nothing about what is compared or counted — see
    :mod:`weaver.runtime.test_compare`.
    """

    #: Not a pytest test class. Weaver's Test is a data validation and pytest's
    #: collector recognises only the name, so it would warn about every module
    #: that imports this one into a test.
    __test__ = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "read" in cls.__dict__:
            raise LoadError(
                f"{cls.__name__} must not define read() — a Test's comparison is "
                "Weaver's, so that passing means the same thing for every Test. "
                "Write expected() and actual(); to author the returned rows "
                "directly, declare an Assumption instead"
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

        The hook a compiled Test overrides. A Spark SQL Test's two sides come
        out of one program, and running that program twice — once for each side
        — would compare two different snapshots of anything its setup
        materialised, then report the difference between them as failure. So the
        pair is produced in one call, and an ordinary authored Test simply asks
        its own two methods.
        """

        return self.expected(), self.actual()

    def read(self):
        """The rows on which expected and actual disagree.

        Empty when the Test passes. Each row carries ``_weaver_side`` — which
        side it came from — and ``_weaver_sk``, which pairs the two sides of one
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

    **Generated, not authored.** A developer writes
    ``Sales.OrdersReconcile.sql`` and Weaver installs
    ``Sales__OrdersReconcile.py``, which is one of these classes with the
    authored SQL attached. The installed module is an ordinary Weaver primitive:
    it imports, constructs and runs exactly as a hand-written one does, and
    orchestration cannot tell the two apart.

    Public because the deployed module imports it and because someone reaching
    for an installed primitive in a notebook meets it — not as a second way to
    author a validation. A repository ``.py`` subclassing one is refused,
    because ``.sql`` is where a SQL validation is written.
    """

    #: The authored program, addressed and embedded when the module was built.
    sql: str = ""

    def _document(self):
        """This module's contract, read as the SQL document it came from.

        The docstring *is* the authored ``.sql`` header, carried over verbatim,
        so it has to be parsed as what it was written as. Reading it as Python
        metadata would apply Python's rules to a SQL declaration.
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

        return read_spark_sql_test(
            self.spark, sql=self.sql, what=type(self).__name__
        )

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


def _load_pair(obj, returned):
    """Unpack what ``read()`` returned, refusing anything else by name.

    Both kinds of object return a pair, and the error has to name the object
    rather than surface as a tuple-unpacking failure three frames deeper — an
    author who returned a single frame should be told that, not shown a
    ValueError about lengths.
    """

    if not isinstance(returned, tuple) or len(returned) != 2:
        raise LoadError(
            f"{type(obj).__name__}.read() must return a pair — "
            f"(staging, deletes) for a Table, (staging_folder, files_to_delete) "
            f"for a Folder — and returned {type(returned).__name__}"
        )
    return returned


def _identity(class_name: str) -> tuple[str, str]:
    """``Sales__Order`` → ``("Sales", "Order")``; ``___Load`` → ``("_", "Load")``.

    A run of leading underscores is read as a schema plus the separator, because
    ``_`` is a real schema and spelling ``_.Load`` as a class name produces three
    of them. The rule is the parser's — see
    :func:`weaver.declaration.source.python_id_parts` — repeated rather than
    imported, for the same reason the separator itself is.
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
            f"{class_name!r} does not name an object: a Weaver class separates "
            f"schema and object with {CLASS_ID_SEPARATOR!r}, as in Sales__Order"
        )
    return parts[0], parts[1]


#: The authoring base classes, by the metadata kind that selects them.
#:
#: :class:`SparkSqlTable` is deliberately absent. It is a *generated* base — the
#: installed form of a ``.sql`` table — and admitting it here would make the same
#: object authorable two ways, with two parsers, two dependency readings and two
#: chances to disagree about what it declared.
BASE_CLASSES = {
    "Folder": Folder,
    "Table": Table,
    "View": View,
    "Test": Test,
    "Assumption": Assumption,
}
BASE_CLASS_NAMES = frozenset(cls.__name__ for cls in BASE_CLASSES.values())
