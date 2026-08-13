"""Location-based file transport primitives.

Store handles listing, reading, writing, and deletion. Higher-level operations
own synchronization and deletion policy; listings include metadata for their
incremental decisions.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol, runtime_checkable

from .errors import CommandError, WeaverError
from .locations import Location


class StoreError(WeaverError):
    """Raised when a store operation fails."""


@dataclass(frozen=True)
class Entry:
    """One listed item, with enough metadata to diff without reading."""

    location: Location
    is_directory: bool
    size: int | None = None
    modified: datetime | None = None
    etag: str | None = None

    @property
    def name(self) -> str:
        return self.location.name


@runtime_checkable
class Store(Protocol):
    """File transport within one workspace.

    A within-workspace store operates beneath a local root or through Fabric's
    session-native utilities. A cross-boundary caller may also implement this
    protocol (the desktop's OneLake DFS client) and inject it explicitly, but
    moving files from a laptop into Fabric remains CLI orchestration rather than
    a workspace default.
    """

    def exists(self, location: Location) -> bool: ...

    def is_directory(self, location: Location) -> bool: ...

    def list(self, location: Location, *, recursive: bool = False) -> list[Entry]: ...

    def read(self, location: Location) -> bytes: ...

    def write(self, location: Location, data: bytes) -> None: ...

    def delete(self, location: Location, *, recursive: bool = False) -> None: ...

    def make_directory(self, location: Location) -> None: ...


class FilesystemStore:
    """Filesystem implementation.

    Not sandboxed to a workspace root, because push reads from arbitrary source
    directories. Containment comes from name validation in
    :mod:`weaver.targets`, which rejects separators and traversal.
    """

    def _local(self, location: Location) -> Path:
        if not isinstance(location, Location):
            raise CommandError(
                f"store operations take a Location, got {type(location).__name__}"
            )
        if location.is_url:
            raise CommandError(
                f"FilesystemStore cannot address the URL location {location.value!r}"
            )
        return location.path

    def exists(self, location: Location) -> bool:
        return self._local(location).exists()

    def is_directory(self, location: Location) -> bool:
        return self._local(location).is_dir()

    def list(self, location: Location, *, recursive: bool = False) -> list[Entry]:
        root = self._local(location)
        if not root.exists():
            raise StoreError(f"cannot list a location that does not exist: {location.value}")
        if not root.is_dir():
            raise StoreError(f"cannot list a file: {location.value}")
        paths = sorted(root.rglob("*") if recursive else root.glob("*"))
        return [self._entry(path, location, root) for path in paths]

    def _entry(self, path: Path, root_location: Location, root: Path) -> Entry:
        relative = path.relative_to(root).as_posix()
        info = path.stat()
        is_directory = path.is_dir()
        return Entry(
            location=root_location.join(*relative.split("/")),
            is_directory=is_directory,
            size=None if is_directory else info.st_size,
            modified=datetime.fromtimestamp(info.st_mtime, tz=timezone.utc),
        )

    def read(self, location: Location) -> bytes:
        path = self._local(location)
        try:
            return path.read_bytes()
        except OSError as exc:
            raise StoreError(f"cannot read {location.value}: {exc}") from exc

    def write(self, location: Location, data: bytes) -> None:
        path = self._local(location)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            path.write_bytes(data)
        except OSError as exc:
            raise StoreError(f"cannot write {location.value}: {exc}") from exc

    def delete(self, location: Location, *, recursive: bool = False) -> None:
        path = self._local(location)
        if _is_link(path):
            # A link is removed, never followed: the alias it stands for is
            # disposable, the object it points at belongs to another item.
            if _is_junction(path):
                path.rmdir()
            else:
                path.unlink()
            return
        if not path.exists():
            return
        if path.is_dir():
            if not recursive:
                raise StoreError(
                    f"{location.value} is a directory — pass recursive=True to delete it"
                )
            shutil.rmtree(path)
        else:
            path.unlink()

    def make_directory(self, location: Location) -> None:
        self._local(location).mkdir(parents=True, exist_ok=True)

    def copy_to_local(self, source: Location, destination: Path) -> None:
        """Copy one local file or tree to an exact driver-local path."""

        source_path = self._local(source)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if source_path.is_dir():
            shutil.copytree(source_path, destination)
        else:
            shutil.copy2(source_path, destination)

    def copy_from_local(self, source: Path, destination: Location) -> None:
        """Copy one driver-local file or tree to an exact local location."""

        destination_path = self._local(destination)
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            shutil.copytree(source, destination_path)
        else:
            shutil.copy2(source, destination_path)

    def link(self, source: Location, destination: Location) -> None:
        """Make ``destination`` refer to ``source`` without copying it.

        The emulator's counterpart of a OneLake shortcut, and the reason it is a
        link rather than a copy: a shortcut has no bytes of its own, so a copy
        would drift the moment the source was rebuilt and would make the emulator
        stop reproducing what Fabric does.

        Not part of the :class:`Store` protocol. A shortcut in Fabric is made
        through the workspace API, not through file transport, so an environment
        either offers this or offers that — see
        :mod:`weaver.build_bundle.executors.alias`.
        """

        source_path = self._local(source)
        if not source_path.exists():
            raise StoreError(f"cannot link to something that does not exist: {source.value}")
        destination_path = self._local(destination)
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        if _is_link(destination_path):
            if _is_junction(destination_path):
                destination_path.rmdir()
            else:
                destination_path.unlink()
        try:
            destination_path.symlink_to(source_path, target_is_directory=source_path.is_dir())
        except OSError as exc:
            # Creating a Windows symlink normally needs Developer Mode or an
            # elevated process. A directory junction has the shortcut semantics
            # the emulator needs without either privilege: it follows the live
            # source, resolves to it, and removing it leaves the source alone.
            if _create_junction(source_path, destination_path, exc):
                return
            raise StoreError(
                f"cannot link {destination.value} to {source.value}: {exc}"
            ) from exc


def _is_junction(path: Path) -> bool:
    check = getattr(path, "is_junction", None)
    return bool(check is not None and check())


def _is_link(path: Path) -> bool:
    return path.is_symlink() or _is_junction(path)


def _create_junction(source: Path, destination: Path, symlink_error: OSError) -> bool:
    """Use a privilege-free Windows directory link when symlinks are denied."""

    import os

    if os.name != "nt" or not source.is_dir():
        return False
    try:
        import _winapi

        create = getattr(_winapi, "CreateJunction", None)
        if create is None:
            return False
        create(str(source.resolve()), str(destination))
        return True
    except OSError as junction_error:
        raise StoreError(
            f"cannot link {destination} to {source}: symlink failed with "
            f"{symlink_error}; junction failed with {junction_error}"
        ) from junction_error
