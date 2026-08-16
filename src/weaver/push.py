"""Validate and replace an authored repository at a named location."""

from __future__ import annotations

from dataclasses import dataclass

from .declaration.repository import (
    IGNORED_DIRECTORIES,
    IGNORED_FILENAMES,
    IGNORED_SUFFIXES,
    parse_item_repository,
)
from .errors import CommandError
from .locations import Location
from .store import FilesystemStore, Store


@dataclass(frozen=True)
class PushResult:
    source: str
    destination: str
    repository_signature: str
    files: tuple[str, ...]

    def to_mapping(self) -> dict[str, object]:
        return {
            "source": self.source,
            "destination": self.destination,
            "repository_signature": self.repository_signature,
            "files": list(self.files),
        }


def push_item_repository(
    source: Location,
    destination: Location,
    *,
    destination_store: Store,
    source_store: Store | None = None,
) -> PushResult:
    """Validate ``source`` completely, then replace ``destination`` with it."""

    source_store = source_store or FilesystemStore()
    repository = parse_item_repository(source, store=source_store)
    if source_store is destination_store and source == destination:
        raise CommandError("push source and destination are the same repository")

    prefix = source.value.rstrip("/") + "/"
    files: list[tuple[str, bytes]] = []
    for entry in source_store.list(source, recursive=True):
        if entry.is_directory:
            continue
        relative = entry.location.value[len(prefix) :]
        parts = relative.split("/")
        if (
            "_ignore" in parts
            or any(part in IGNORED_DIRECTORIES for part in parts)
            or parts[-1] in IGNORED_FILENAMES
            or parts[-1].endswith(IGNORED_SUFFIXES)
        ):
            continue
        files.append((relative, source_store.read(entry.location)))

    if destination_store.exists(destination):
        destination_store.delete(destination, recursive=True)
    destination_store.make_directory(destination)
    for relative, content in sorted(files):
        destination_store.write(destination.join(*relative.split("/")), content)
    return PushResult(
        source=source.value,
        destination=destination.value,
        repository_signature=repository.signature,
        files=tuple(relative for relative, _content in sorted(files)),
    )
