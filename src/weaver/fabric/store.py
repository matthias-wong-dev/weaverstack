"""Session-native storage for Weaver running inside Microsoft Fabric.

This is the within-host counterpart to :class:`OneLakeDfsClient`.  It uses the
``notebookutils.fs`` object already present in a Fabric Spark session and never
authenticates back across the workspace boundary.

Only directory operations are implemented at this checkpoint because wipe is
the first core operation to need the in-session store. Byte reads and writes
remain explicit errors until a session-native binary contract is proven.
"""

from __future__ import annotations

from typing import Any

from ..errors import CommandError
from ..locations import Location
from ..store import Entry, StoreError


class FabricStore:
    """Within-host Fabric storage, backed by ``notebookutils.fs``."""

    def __init__(self, fs: Any | None = None) -> None:
        if fs is None:
            try:
                from notebookutils import fs as notebook_fs
            except ImportError as exc:
                raise CommandError(
                    "FabricStore is available only inside a Fabric session; "
                    "desktop access uses OneLakeDfsClient explicitly"
                ) from exc
            fs = notebook_fs
        self.fs = fs

    @staticmethod
    def _path(location: Location) -> str:
        if not isinstance(location, Location):
            raise CommandError(
                f"store operations take a Location, got {type(location).__name__}"
            )
        if not location.value.startswith("abfss://"):
            raise CommandError(
                f"FabricStore needs an abfss location, got {location.value!r}"
            )
        return location.value

    def exists(self, location: Location) -> bool:
        return bool(self.fs.exists(self._path(location)))

    def is_directory(self, location: Location) -> bool:
        path = self._path(location)
        if not self.fs.exists(path):
            return False
        parent, _, name = path.rstrip("/").rpartition("/")
        if not parent:
            return True
        for info in self.fs.ls(parent):
            info_path = str(getattr(info, "path", "")).rstrip("/")
            info_name = str(getattr(info, "name", "")).rstrip("/")
            if info_path == path.rstrip("/") or info_name == name:
                return bool(getattr(info, "isDir", False))
        return False

    def list(self, location: Location, *, recursive: bool = False) -> list[Entry]:
        path = self._path(location)
        if not self.fs.exists(path):
            raise StoreError(f"cannot list a location that does not exist: {path}")

        entries = self._list_once(location)
        if not recursive:
            return entries

        found = list(entries)
        pending = [entry.location for entry in entries if entry.is_directory]
        while pending:
            directory = pending.pop()
            children = self._list_once(directory)
            found.extend(children)
            pending.extend(
                child.location for child in children if child.is_directory
            )
        return found

    def _list_once(self, location: Location) -> list[Entry]:
        entries = []
        for info in self.fs.ls(self._path(location)):
            name = str(getattr(info, "name", "")).rstrip("/")
            raw_path = str(getattr(info, "path", "")).rstrip("/")
            child = (
                Location(raw_path)
                if raw_path.startswith("abfss://")
                else location / name
            )
            is_directory = bool(getattr(info, "isDir", False))
            entries.append(
                Entry(
                    location=child,
                    is_directory=is_directory,
                    size=None if is_directory else int(getattr(info, "size", 0)),
                )
            )
        return entries

    def read(self, location: Location) -> bytes:
        raise StoreError("FabricStore byte reads are not implemented")

    def write(self, location: Location, data: bytes) -> None:
        raise StoreError("FabricStore byte writes are not implemented")

    def delete(self, location: Location, *, recursive: bool = False) -> None:
        if not self.fs.rm(self._path(location), recurse=recursive):
            raise StoreError(f"could not delete {location.value}")

    def make_directory(self, location: Location) -> None:
        if not self.fs.mkdirs(self._path(location)):
            raise StoreError(f"could not create directory {location.value}")
