"""Authored shortcut declarations and their deployed readers.

Builds parse an item's authored ``shortcuts.py`` and deploy a module with the
same symbols. Logical shortcuts retain Weaver metadata; physical shortcuts do
not. These readers never write through OneLake shortcuts.
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
    """A shortcut declaration in an item's authored ``shortcuts.py``."""

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
    """A shortcut addressed in the item that declares it.

    ``source`` identifies the Weaver document behind a logical shortcut and is
    ``None`` for a physical shortcut. Data is always read from the local
    destination.
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
        """Return the Weaver document named by a logical shortcut."""

        from .declaration.model import WeaverDocumentId

        if self._source is None:
            raise LoadError(
                "this shortcut has a physical target and no Weaver metadata. "
                "Declare target_type='logical' to read the source object's "
                "metadata."
            )
        return WeaverDocumentId.parse(self._source)

    def bookmark(self):
        """The source object's bookmark, read through the owner's catalogue.

        The instant immediately before the source's last clean load began. A
        consumer catching up measures from its own
        :meth:`weaver.objects.WeaverObject.bookmark` instead, because the
        boundary there is its own last clean load.
        """

        identity = self._logical_source()
        return self._owner._anchor().bookmark(identity)


class _TableReader(_Bound):
    def __init__(
        self, owner: Any, schema: str, name: str, source: str | None = None
    ) -> None:
        super().__init__(owner, source)
        self._schema = schema
        self._name = name

    def dataframe(self):
        """Read rows by Delta path without relying on an attached Lakehouse."""

        return self.spark.read.format("delta").load(
            self.lakehouse.table_path(self._schema, self._name)
        )

    def empty_dataframe(self):
        """Return this table's shape with no rows."""

        return self.dataframe().limit(0)


class _FolderReader(_Bound):
    """A folder presented through a shortcut.

    Change history is read through the local shortcut path and requires a
    logical source.
    """

    def __init__(
        self, owner: Any, schema: str, name: str, source: str | None = None
    ) -> None:
        super().__init__(owner, source)
        self._schema = schema
        self._name = name

    def path(self):
        return self.lakehouse.folder_path(self._schema, self._name)

    def spark_path(self) -> str:
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
            raise LoadError("a schema shortcut table name must be a non-empty string")
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
    """A deployed table shortcut.

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
    """A deployed folder shortcut."""

    schema: str
    object: str
    source: str | None = None

    def __call__(self, owner: Any) -> _FolderReader:
        return _FolderReader(owner, self.schema, self.object, self.source)


@dataclass(frozen=True)
class SchemaShortcut:
    """A deployed schema shortcut."""

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

    Logical declarations retain the Weaver document used by ``bookmark()`` and
    Folder change history.
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
