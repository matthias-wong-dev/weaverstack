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
            customers = Sales__Customer.dataframe()
            ...
            return upserts, deletes

An object reaches its *own* destination through ``self.path``; it reaches a
*dependency's* through that object's classmethod — ``dataframe()`` for a table
or view, ``folder_path()`` for a folder. The two are deliberately different
names: a classmethod named ``path`` would replace the inherited ``self.path``
property on every Folder.

**Dependencies are imports.** Importing another object's module declares a
dependency on it; Weaver reads that from the source without executing it. There
are no string keys to mistype, and an IDE can autocomplete and navigate to the
object being depended on — which a string never could.

**Accessors are classmethods, not properties.** A class-level property would
need a metaclass, since Python no longer chains ``classmethod`` and
``property``. ``Customer.dataframe()`` is the plainer construction, and it is
inherited, so tooling can see it.

**Objects never mutate the target.** ``read()`` proposes; Weaver owns writing,
CRUD accounting, staging and logging. A Folder stages into a Weaver-issued
directory and returns it; a Table returns rows.

Nothing here imports PySpark. Every accessor delegates to a context Weaver
injects for the step, so this module is importable anywhere.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any, Protocol, runtime_checkable

from .errors import LoadError

#: The resolver for the object currently executing. Weaver sets this for the
#: duration of a step; a ContextVar rather than a global so concurrent steps
#: cannot see each other's dependencies.
_active_resolver: ContextVar[Any] = ContextVar("weaver_dependency_resolver", default=None)


@runtime_checkable
class ObjectContext(Protocol):
    """What Weaver injects for one executing object.

    Declared as a protocol so the authoring surface does not depend on the
    runtime that satisfies it.
    """

    @property
    def spark(self) -> Any: ...

    @property
    def object_path(self) -> Any: ...

    @property
    def schema(self) -> tuple[Any, ...]: ...

    @property
    def primary_key(self) -> tuple[str, ...]: ...

    @property
    def is_incremental(self) -> bool: ...

    def current_dataframe(self) -> Any: ...

    def empty_frame(self) -> Any: ...

    def staging_folder(self) -> Any: ...


class WeaverObject:
    """Base for every authored object."""

    def __init__(self, context: ObjectContext | None = None) -> None:
        self._context = context

    # --- depending on this object ---------------------------------------

    @classmethod
    def _resolve(cls, accessor: str) -> Any:
        resolver = _active_resolver.get()
        if resolver is None:
            raise LoadError(
                f"{cls.__name__}.{accessor}() is only available while Weaver is "
                "executing an object. It resolves a dependency from the running "
                "workflow, so it cannot be called from ordinary code."
            )
        return resolver(cls, accessor)

    # --- this object's own context ---------------------------------------

    @property
    def context(self) -> ObjectContext:
        if self._context is None:
            raise LoadError(
                f"{type(self).__name__} has no Weaver context — objects are "
                "constructed by Weaver, not directly."
            )
        return self._context

    @property
    def path(self) -> Any:
        """This object's destination. Read-only: do not write here."""

        return self.context.object_path

    @property
    def spark(self) -> Any:
        return self.context.spark

    def read(self):
        raise NotImplementedError(
            f"{type(self).__name__} must implement read()"
        )


class Folder(WeaverObject):
    """Files materialised into a Lakehouse Files directory.

    ``read()`` writes into the staging directory Weaver issues and returns
    ``(staging_folder, files_to_delete)``.
    """

    @classmethod
    def folder_path(cls) -> Any:
        """This folder's materialised location, for a dependent object.

        Not ``path()``: that name belongs to the instance property every object
        uses for its *own* destination, and a classmethod of the same name
        would replace it — silently, since a bound method is truthy.
        """

        return cls._resolve("path")

    def staging_folder(self) -> Any:
        """A fresh, empty, object-local staging directory to write into."""

        return self.context.staging_folder()


class Table(WeaverObject):
    """Rows materialised into a Delta table or a Warehouse table.

    ``read()`` returns ``(upserts, deletes)``.
    """

    @classmethod
    def dataframe(cls) -> Any:
        """This table's contents, for a dependent object."""

        return cls._resolve("dataframe")

    @property
    def schema(self) -> tuple[Any, ...]:
        return self.context.schema

    @property
    def primary_key(self) -> tuple[str, ...]:
        return self.context.primary_key

    @property
    def is_incremental(self) -> bool:
        return self.context.is_incremental

    @property
    def current_dataframe(self) -> Any:
        """The persisted table, or None if it has never been written."""

        return self.context.current_dataframe()

    def empty_frame(self) -> Any:
        """An empty DataFrame in this object's declared schema."""

        return self.context.empty_frame()


class View(WeaverObject):
    """A view over other objects, declared in SQL.

    A view has no ``read()``: its definition is its query.
    """

    @classmethod
    def dataframe(cls) -> Any:
        """This view's contents, for a dependent object."""

        return cls._resolve("dataframe")


#: The authoring base classes, by the metadata kind that selects them.
BASE_CLASSES = {"Folder": Folder, "Table": Table, "View": View}
BASE_CLASS_NAMES = frozenset(cls.__name__ for cls in BASE_CLASSES.values())
