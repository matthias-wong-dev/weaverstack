"""File transport — the operations Weaver performs on locations.

This is transport, never policy. It knows how to list, read, write, delete and
move; it has no opinion about what *should* be copied or deleted. Push,
deployment and Folder reconciliation each carry their own rules and sit on top
of these primitives:

- push owns its destination subtree, so a file missing from source is deleted;
- Folder reconciliation deletes only within its ``File key`` scope, and under
  ``Incremental`` deletes nothing — governed by load policy, not by transport.

Collapsing those into one ``sync(delete_missing=...)`` would put a data-correctness
decision behind a transport flag, so they stay separate.

**Listing carries metadata.** :class:`Entry` reports size, modification time and
etag, because every incremental strategy needs them. A listing of bare names
would foreclose all of them.

There is deliberately no move operation. Staging promotion, destination
replacement and atomic publication are all real needs, but their contract comes
from the load algorithm, not from a guess made here — and the mechanisms differ
enough (a local rename, a OneLake copy, an in-session ``notebookutils.fs.mv``)
that a single reassuring name would hide materially different cost. Load
introduces whatever it actually needs, named for what it does.
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
    """File transport within one host.

    A within-host store operates beneath a local root or through Fabric's
    session-native utilities. A cross-boundary caller may also implement this
    protocol (the desktop's OneLake DFS client) and inject it explicitly, but
    moving files from a laptop into Fabric remains CLI orchestration rather than
    a host default.
    """

    def exists(self, location: Location) -> bool: ...

    def is_directory(self, location: Location) -> bool: ...

    def list(self, location: Location, *, recursive: bool = False) -> list[Entry]: ...

    def read(self, location: Location) -> bytes: ...

    def write(self, location: Location, data: bytes) -> None: ...

    def delete(self, location: Location, *, recursive: bool = False) -> None: ...

    def make_directory(self, location: Location) -> None: ...


class LocalStore:
    """Filesystem implementation.

    Not sandboxed to a host root, because push reads from arbitrary source
    directories. Containment comes from name validation in
    :mod:`weaver.targets`, which rejects separators and traversal.
    """

    def _local(self, location: Location) -> Path:
        if not isinstance(location, Location):
            raise CommandError(
                f"store operations take a Location, got {type(location).__name__}"
            )
        if location.is_url:
            raise CommandError(f"LocalStore cannot address the URL location {location.value!r}")
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
