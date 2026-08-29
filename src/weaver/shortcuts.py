"""Shortcuts, as an author declares them and as a program uses them.

An item declares its shortcuts once, in ``shortcuts.py`` at the item root::

    from weaver import Shortcut

    Sales__Customer = Shortcut(
        shortcut_type="table",
        target_type="logical",
        target="Lakehouse/Sales/Sales.Customer",
    )

A build reads that file rather than running it, and deploys a generated module of
the same name beside the item's programs, so the same names are importable::

    from shortcuts import Sales__Customer

    Sales__Customer(self).dataframe()

What a program reads is the destination item's own table or folder. A shortcut is
materialised in the item that declares it, so nothing here opens the source item:
which workspace and which Fabric item it was in was settled when the bundle was
generated.

A logical shortcut also carries the Weaver document its target named. Data comes
through the local shortcut; Weaver semantics come from that identity, so a
logical shortcut answers ``bookmark()`` and a logical Folder shortcut answers the
three Folder change-history methods. A physical shortcut names a Fabric location,
and those four are unavailable on one.

OneLake makes a shortcut a read-write window into the item it points at, so a
write beneath one lands in that item. These objects read and do not write.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .errors import LoadError

#: The declaration kinds, repeated here so an authored file needs one import.
TABLE = "table"
SCHEMA = "schema"
FOLDER = "folder"


@dataclass(frozen=True)
class Shortcut:
    """One declared shortcut, as it is written in ``shortcuts.py``.

    Authored rather than executed: a build parses the file, so this carries the
    declaration and answers nothing about the estate. The deployed runtime module
    a program imports is generated from the same declaration.
    """

    shortcut_type: str
    target_type: str
    target: str
    workspace: str | None = None

    def __call__(self, owner: Any):
        raise LoadError(
            "an authored Shortcut declares what to create, not how to read it. "
            "Inside a load, import the name from the deployed 'shortcuts' module."
        )


class _Bound:
    """One shortcut, addressed in the item that declares it.

    ``source`` is the Weaver document a logical shortcut named, and None for a
    physical one. Physical access never uses it: it answers what the local
    shortcut cannot, which is what Weaver records about the object on the far
    side.
    """

    def __init__(self, owner: Any, source: str | None = None) -> None:
        self._owner = owner
        self._source = source

    @property
    def spark(self):
        return self._owner.spark

    @property
    def lakehouse(self):
        return self._owner.lakehouse

    def _logical_source(self):
        """The Weaver document this shortcut names.

        Fails on a physical shortcut, which names a Fabric location. Weaver
        records nothing about what is there.
        """

        from .declaration.model import WeaverDocumentId

        if self._source is None:
            raise LoadError(
                "this shortcut has a physical target, so it names a Fabric "
                "location and Weaver records nothing about what is there. "
                "Declare target_type='logical' to read the source object's "
                "Weaver metadata through it."
            )
        return WeaverDocumentId.parse(self._source)

    def bookmark(self):
        """The source object's bookmark, read through the owner's catalogue.

        The instant immediately before the source's last clean load began. It is
        the source's own, not this item's: a consumer catching up measures from
        its own :meth:`weaver.objects.WeaverObject.bookmark`.
        """

        identity = self._logical_source()
        return self._owner._anchor().bookmark(identity)


class _TableReader(_Bound):
    """One table this item presents, addressed as any other Weaver table is."""

    def __init__(
        self, owner: Any, schema: str, name: str, source: str | None = None
    ) -> None:
        super().__init__(owner, source)
        self._schema = schema
        self._name = name

    def dataframe(self):
        """The rows, read from Delta by path.

        The same :class:`~weaver.lakehouse.Lakehouse` addressing
        :meth:`weaver.objects.Table.dataframe` uses, and for the same reason: a
        path needs nothing attached, so the call serves any resolved Lakehouse.
        """

        return self.spark.read.format("delta").load(
            self.lakehouse.table_path(self._schema, self._name)
        )

    def empty_dataframe(self):
        """This table's shape with no rows, which is an incremental load's no-op.

        Physical, as :meth:`dataframe` is, so a physical shortcut answers it too.
        """

        return self.dataframe().limit(0)


class _FolderReader(_Bound):
    """One folder this item presents, addressed as any other Weaver folder is.

    The change history is read through the local shortcut path, because OneLake
    presents the source's ``_changes`` directory beneath it. What the logical
    source supplies is the claim that this folder is a Weaver-managed Folder.
    """

    def __init__(
        self, owner: Any, schema: str, name: str, source: str | None = None
    ) -> None:
        super().__init__(owner, source)
        self._schema = schema
        self._name = name

    def path(self):
        """The folder's location, as Python addresses it."""

        return self.lakehouse.folder_path(self._schema, self._name)

    def spark_path(self) -> str:
        """The folder's location, as Spark addresses it."""

        return self.lakehouse.folder_spark_path(self._schema, self._name)

    def files_since(self, bookmark):
        """Current files changed strictly after an aware ``bookmark``, and when.

        Keys are paths beneath this item's shortcut, so what comes back is
        readable here::

            for path in Sales__Landing(self).files_since(self.bookmark()):
                ...
        """

        from .runtime.folder_load import files_since

        self._logical_source()
        return files_since(self.path(), bookmark)

    def latest_files(self):
        """The current files from the newest change that left files in place."""

        from .runtime.folder_load import latest_files

        self._logical_source()
        return latest_files(self.path())

    def deleted_since(self, bookmark):
        """Files deleted strictly after an aware ``bookmark``, and when.

        A returned path is the file the deletion retired, so it normally does
        not exist.
        """

        from .runtime.folder_load import deleted_since

        self._logical_source()
        return deleted_since(self.path(), bookmark)


class _SchemaReader(_Bound):
    """A schema shortcut, which presents the source item's namespace.

    Its contents belong to the item it points at and can change without a build,
    so a table is named when it is read rather than generated as a symbol::

        Reference(self).Customer.dataframe()
        Reference(self).table("Customer Detail").dataframe()

    Attribute access is the ordinary form and delegates to :meth:`table`, which
    stays available for a name that is not a Python identifier.
    """

    def __init__(self, owner: Any, schema: str) -> None:
        super().__init__(owner)
        self._schema = schema

    def table(self, name: str) -> _TableReader:
        if not isinstance(name, str) or not name.strip():
            raise LoadError("a schema shortcut reads a table by name")
        return _TableReader(self._owner, self._schema, name)

    def __getattr__(self, name: str) -> _TableReader:
        # Only for names this class does not define, so nothing here shadows a
        # table. Private names are excluded so a copy or a pickle does not read
        # as a table lookup.
        if name.startswith("_"):
            raise AttributeError(name)
        return self.table(name)


@dataclass(frozen=True)
class TableShortcut:
    """A deployed table shortcut. Constructed by the generated module.

    ``source`` is the Weaver document a logical declaration named, and is absent
    from a physical one.
    """

    schema: str
    object: str
    source: str | None = None

    def __call__(self, owner: Any) -> _TableReader:
        return _TableReader(owner, self.schema, self.object, self.source)


@dataclass(frozen=True)
class FolderShortcut:
    """A deployed folder shortcut. Constructed by the generated module."""

    schema: str
    object: str
    source: str | None = None

    def __call__(self, owner: Any) -> _FolderReader:
        return _FolderReader(owner, self.schema, self.object, self.source)


@dataclass(frozen=True)
class SchemaShortcut:
    """A deployed schema shortcut. Constructed by the generated module."""

    schema: str

    def __call__(self, owner: Any) -> _SchemaReader:
        return _SchemaReader(owner, self.schema)


#: How each declared kind is spelled in the generated module.
_RUNTIME_CLASS = {
    TABLE: "TableShortcut",
    FOLDER: "FolderShortcut",
    SCHEMA: "SchemaShortcut",
}


def render_runtime_module(declarations) -> str:
    """The deployed ``shortcuts.py`` for one item's declarations.

    Generated rather than copied, because the authored file describes what to
    create and a program needs what to read. The destination is where the data
    is. A logical declaration also carries the Weaver document it named, which
    is what ``bookmark()`` and the Folder change history are answered from.
    """

    lines = [
        '"""Deployed shortcut declarations. Generated by Weaver; do not edit."""',
        "",
        "from weaver.shortcuts import FolderShortcut, SchemaShortcut, TableShortcut",
        "",
    ]
    for declaration in sorted(declarations, key=lambda each: each.name):
        constructor = _RUNTIME_CLASS[declaration.shortcut_type]
        if declaration.shortcut_type == SCHEMA:
            arguments = f"schema={declaration.name!r}"
        else:
            identity = declaration.destination.object_id
            arguments = f"schema={identity.schema!r}, object={identity.object!r}"
            if declaration.is_logical:
                arguments += f", source={str(declaration.logical_source)!r}"
        lines.append(f"{declaration.name} = {constructor}({arguments})")
    return "\n".join(lines) + "\n"
