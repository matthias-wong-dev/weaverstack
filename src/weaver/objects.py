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

from typing import Any

from .errors import LoadError
from .lakehouse import Lakehouse, default_lakehouse

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
    """

    def path(self) -> str:
        """This folder's materialised location, as Spark addresses it.

        What one object hands another: a table reading these files does it with
        ``spark.read``, which wants the ``abfss://`` form. Addressed by the root
        the Lakehouse was resolved to, never by ``/lakehouse/default`` — that
        names only whatever a notebook attached, and a load runs detached against
        Lakehouses it resolved by name.
        """

        return self.lakehouse.folder_path(*self.identity)

    def local_path(self) -> str:
        """This folder's location for code that opens files rather than reading
        them through Spark.

        The same bytes as :meth:`path`, spelled as a filesystem path. In OneLake
        that is a mount Weaver makes of the resolved root; locally the two are
        the same directory.
        """

        return self.lakehouse.folder_local_path(*self.identity)

    def staging_folder(self) -> str:
        """The object-local staging directory to write into.

        The destination's own path with ``_Staging`` appended — the same sibling
        :meth:`weaver.resolution.LocalResolver.folder_staging` issues. There is no
        shared staging area and no run identifier: staging belongs to the object,
        so a failed load leaves exactly one directory to look at.
        """

        return f"{self.local_path()}_Staging"

    def load(self, fault_tolerant: bool = False) -> "LoadResult":
        """Run this folder's ``read()`` and publish what it staged.

        Independently runnable, which is the point::

            Sales__Export(spark).load(fault_tolerant=False)

        No repository, no catalogue, no bundle and no orchestrator — the module
        carries its own contract and this object carries its own destination.
        """

        from .runtime.folder_load import load_folder, new_staging_folder
        from .runtime.load_contract import FolderLoadContract

        contract = FolderLoadContract.from_document(self._document())
        # Weaver issues staging, before read() rather than after the load. A run
        # must begin from nothing it did not itself produce, or the previous
        # run's files are published again and a replacement concludes that
        # nothing was retired. Clearing afterwards instead would destroy the one
        # directory worth looking at when a load fails.
        issued = new_staging_folder(self.local_path(), self.staging_folder())
        staged, deletes = _load_pair(self, self.read())
        if str(staged) != issued:
            raise LoadError(
                f"{type(self).__name__}.read() returned {staged!r}, which is not "
                f"the staging folder Weaver issued ({issued!r}) — return "
                "self.staging_folder()"
            )
        return load_folder(
            contract=contract,
            destination=self.local_path(),
            staging=issued,
            deletes=deletes,
            fault_tolerant=fault_tolerant,
        )


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
        from .runtime.table_load import load_table

        contract = LoadContract.from_document(self._document())
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
BASE_CLASSES = {"Folder": Folder, "Table": Table, "View": View}
BASE_CLASS_NAMES = frozenset(cls.__name__ for cls in BASE_CLASSES.values())
