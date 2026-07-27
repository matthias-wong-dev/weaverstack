"""Session-native storage for Weaver running inside Microsoft Fabric.

This is the within-host counterpart to :class:`OneLakeDfsClient`.  It uses the
``notebookutils.fs`` object already present in a Fabric Spark session and never
authenticates back across the workspace boundary.

Directory operations came first, for wipe. Byte reads and writes came next, for
installing a build bundle in-session — the installer reads ``plan.yml`` and the
generated payloads from OneLake and writes an install report back. They go
through ``notebookutils.fs.head``/``put``, which exchange UTF-8 text: a build
bundle's manifest and payloads are UTF-8, so this is exact for them. Arbitrary
binary artefacts do not pass through those methods: recursive repository
materialisation and bundle-archive persistence use ``notebookutils.fs.cp``
between OneLake and the driver's ``file:/tmp`` filesystem.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..errors import CommandError
from ..locations import Location
from ..store import Entry, StoreError

#: notebookutils.fs.head reads up to this many bytes. Bundle files are tiny; this
#: ceiling is only a guard against an unexpectedly large one.
_MAX_READ_BYTES = 256 * 1024 * 1024


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
        """The file's bytes, decoded as the UTF-8 text a bundle is made of."""

        path = self._path(location)
        if not self.fs.exists(path):
            raise StoreError(f"cannot read a location that does not exist: {path}")
        try:
            text = self.fs.head(path, _MAX_READ_BYTES)
        except Exception as exc:  # notebookutils raises a bare Py4J error
            raise StoreError(f"cannot read {location.value}: {exc}") from exc
        return text.encode("utf-8")

    def write(self, location: Location, data: bytes) -> None:
        """Write UTF-8 text (a bundle manifest, a payload, an install report)."""

        path = self._path(location)
        try:
            self.fs.put(path, data.decode("utf-8"), True)
        except Exception as exc:
            raise StoreError(f"cannot write {location.value}: {exc}") from exc

    def delete(self, location: Location, *, recursive: bool = False) -> None:
        if not self.fs.rm(self._path(location), recurse=recursive):
            raise StoreError(f"could not delete {location.value}")

    def make_directory(self, location: Location) -> None:
        if not self.fs.mkdirs(self._path(location)):
            raise StoreError(f"could not create directory {location.value}")

    def copy_to_local(self, source: Location, destination: Path) -> None:
        """Recursively copy OneLake content to the Fabric driver's filesystem."""

        destination.parent.mkdir(parents=True, exist_ok=True)
        target = f"file:{destination.as_posix()}"
        try:
            copied = self.fs.cp(self._path(source), target, True)
        except Exception as exc:
            raise StoreError(
                f"cannot materialise {source.value} at {destination}: {exc}"
            ) from exc
        if copied is False:
            raise StoreError(f"could not materialise {source.value} at {destination}")

    def copy_from_local(self, source: Path, destination: Location) -> None:
        """Copy one driver-local file or tree into OneLake without text decoding."""

        origin = f"file:{source.as_posix()}"
        try:
            copied = self.fs.cp(origin, self._path(destination), source.is_dir())
        except Exception as exc:
            raise StoreError(
                f"cannot persist {source} at {destination.value}: {exc}"
            ) from exc
        if copied is False:
            raise StoreError(f"could not persist {source} at {destination.value}")
