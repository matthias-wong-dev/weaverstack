"""Reconcile a Python-defined folder into its destination.

Object code fills a Weaver-issued staging folder and returns files to delete.
Validation confirms staged and deleted paths are within the declared file key
before publishing changes.

Weaver lists, compares, copies and deletes through the Lakehouse's store, never
through the mount: a mount's listing can still show a file deleted through
OneLake. Mounted paths are only handed to authored code.
"""

from __future__ import annotations

import contextlib
import filecmp
import fnmatch
import json
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ..errors import LoadError
from ..locations import Location
from ..store import StoreError, StoreNotFoundError
from .load_contract import FolderLoadContract
from .load_result import LoadResult

#: Weaver-owned metadata inside a managed folder. Object code may never stage
#: or delete this tree, and ordinary reconciliation never inventories it.
CHANGES_DIRECTORY = "_changes"

CHANGE_DATETIME_FORMAT = "%Y-%m-%dT%H-%M-%S.%fZ"

#: Characters that make a delete entry a pattern rather than a path. A delete is
#: an exact statement about one file; a glob would let an object remove files it
#: never named and could not have known were there.
_GLOB_CHARS = set("*?[]")

_TMP_PREFIX = "._weaver_tmp_"

STAGING_SUFFIX = "_Staging"
REJECT_SUFFIX = "_Reject"

INTOLERANT_MESSAGE = (
    "staged files were rejected and fault_tolerant = 0, so the folder was not modified"
)
TOLERATED_MESSAGE = "staged files were rejected and excluded from the load"


@dataclass(frozen=True)
class FolderFiles:
    """One Files folder, as its store reaches it and as authored code opens it.

    ``store`` must also copy and move a file within itself and copy one to the
    driver. ``path`` is the mounted address of ``location``.
    """

    store: Any
    location: Location
    path: Path

    def sibling(self, suffix: str) -> "FolderFiles":
        name = f"{self.location.name}{suffix}"
        parent = Location(self.location.value.rsplit("/", 1)[0])
        return FolderFiles(self.store, parent / name, self.path.with_name(name))

    def at(self, relative: str) -> Location:
        return self.location.join(*relative.split("/"))


def load_folder(
    *,
    contract: FolderLoadContract,
    destination: FolderFiles,
    staging: FolderFiles,
    deletes=(),
    fault_tolerant: bool = False,
) -> LoadResult:
    """Validate all staged files before modifying the destination."""

    _validate_paths(destination, staging)
    staged_entries, rejected = _classify(staging, contract)
    staged = list(staged_entries)
    current = _inventory(destination, excluded_roots={CHANGES_DIRECTORY})
    deletes = _validate_deletes(deletes, staged, current, contract=contract)
    rows_read = len(staged) + len(rejected)

    reject = destination.sibling(REJECT_SUFFIX)
    _reset_reject_evidence(reject)
    _keep_reject_evidence(rejected, staging, reject)

    if rejected and not fault_tolerant:
        raise LoadError(
            f"{contract.qualified}: {INTOLERANT_MESSAGE}",
            result=LoadResult.failure(
                INTOLERANT_MESSAGE, rows_read=rows_read, rows_rejected=len(rejected)
            ),
        )

    inserted, updated = _publish(staged_entries, staging, destination, current)
    deleted = _reconcile_deletes(
        destination, current, deletes, staged, contract=contract
    )
    _write_change_document(
        destination,
        inserted=inserted,
        updated=updated,
        deleted=deleted,
    )

    result = LoadResult(
        succeeded=True,
        rows_read=rows_read,
        rows_inserted=len(inserted),
        rows_updated=len(updated),
        rows_deleted=len(deleted),
        rows_rejected=len(rejected),
    )
    if rejected:
        return result.rejected(f"{len(rejected)} {TOLERATED_MESSAGE}")
    return result


@dataclass(frozen=True)
class StagingFolder:
    """A staging directory issued for one folder load."""

    path: Path

    def __enter__(self) -> "StagingFolder":
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        return False


def new_staging_folder(destination: FolderFiles, staging: FolderFiles) -> StagingFolder:
    _validate_paths(destination, staging)
    reset_staging(staging)
    return StagingFolder(path=staging.path)


def reset_staging(staging: FolderFiles) -> None:
    """Empty and recreate the fixed staging directory."""

    remove_staging(staging)
    staging.store.make_directory(staging.location)
    # Authored code writes through the mount, which may not yet list the
    # directory the store made.
    staging.path.mkdir(parents=True, exist_ok=True)


def remove_staging(staging: FolderFiles) -> None:
    if staging.store.exists(staging.location):
        staging.store.delete(staging.location, recursive=True)


# --- validation ----------------------------------------------------------------


def _validate_paths(destination: FolderFiles, staging: FolderFiles) -> None:
    destination_location = destination.location.value
    staging_location = staging.location.value
    if destination_location == staging_location:
        raise LoadError(f"staging path is the destination folder: {destination.path}")
    if staging_location.rsplit("/", 1)[0] != destination_location.rsplit("/", 1)[0]:
        raise LoadError(
            f"staging directory {staging.path} is not beside destination folder "
            f"{destination.path}; return self.staging_folder()"
        )
    expected = f"{destination.location.name}{STAGING_SUFFIX}"
    if staging.location.name != expected:
        raise LoadError(
            f"staging directory {staging.path} must be named {expected!r}; return "
            "self.staging_folder()"
        )


def _classify(staging: FolderFiles, contract) -> tuple[dict[str, Any], list[str]]:
    if not staging.store.exists(staging.location):
        raise LoadError(
            f"staging directory not found: {staging.path}; write files into "
            "self.staging_folder() and return it"
        )
    staged: dict[str, Any] = {}
    rejected: list[str] = []
    for relative, entry in _inventory(staging).files.items():
        if _is_changes_path(relative):
            raise LoadError(
                f"{contract.qualified}: {relative!r} is inside Weaver's "
                f"{CHANGES_DIRECTORY}/ directory and cannot be staged"
            )
        if matches_file_key(relative, contract.file_keys):
            staged[relative] = entry
        else:
            rejected.append(relative)
    return staged, rejected


def _validate_deletes(
    deletes, staged, current: "_Inventory", *, contract
) -> tuple[str, ...]:
    if isinstance(deletes, (str, bytes)):
        raise LoadError(
            f"{contract.qualified}: read() must return a sequence of relative "
            "file names to delete, not a single string"
        )
    entries = list(deletes or ())
    if entries and contract.replaces_wholesale:
        raise LoadError(
            f"{contract.qualified}: read() returned explicit deletes for a "
            "non-incremental folder; return only staging or declare Incremental: true"
        )

    staged_set = set(staged)
    normalised: list[str] = []
    for raw in entries:
        if not isinstance(raw, str) or not raw.strip():
            raise LoadError(
                f"{contract.qualified}: a delete entry must be a non-empty "
                f"relative path, got {raw!r}"
            )
        if raw.endswith("/") or "\\" in raw:
            raise LoadError(
                f"{contract.qualified}: a delete must name an exact file, not a "
                f"directory: {raw!r}"
            )
        if any(char in _GLOB_CHARS for char in raw):
            raise LoadError(
                f"{contract.qualified}: a delete must name an exact file, not a "
                f"pattern: {raw!r}"
            )
        path = Path(raw)
        if path.is_absolute() or raw.startswith("/"):
            raise LoadError(
                f"{contract.qualified}: a delete must be relative to the folder, "
                f"not absolute: {raw!r}"
            )
        if ".." in path.parts:
            raise LoadError(
                f"{contract.qualified}: a delete must not traverse out of the "
                f"folder with '..': {raw!r}"
            )
        if _is_changes_path(path.as_posix()):
            raise LoadError(
                f"{contract.qualified}: {raw!r} is inside Weaver's "
                f"{CHANGES_DIRECTORY}/ directory and cannot be deleted"
            )
        relative = path.as_posix()
        if not matches_file_key(relative, contract.file_keys):
            raise LoadError(
                f"{contract.qualified}: {relative!r} does not match the File key, "
                "so it is not this folder's to delete"
            )
        if relative in staged_set:
            raise LoadError(
                f"{contract.qualified}: {relative!r} is both staged and deleted"
            )
        if relative in current.directories:
            raise LoadError(
                f"{contract.qualified}: a delete must name a file, and "
                f"{relative!r} is a directory"
            )
        normalised.append(relative)
    return tuple(normalised)


# --- reconciliation --------------------------------------------------------------


def _publish(
    staged: dict[str, Any],
    staging: FolderFiles,
    destination: FolderFiles,
    current: "_Inventory",
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Do not rewrite byte-identical files or report them as updated."""

    destination.store.make_directory(destination.location)
    inserted: list[str] = []
    updated: list[str] = []
    for relative, entry in staged.items():
        existing = current.files.get(relative)
        if existing is None:
            inserted.append(relative)
        elif not _identical(staging, destination, relative, entry, existing):
            updated.append(relative)
        else:
            continue
        _safe_replace(destination.store, staging.at(relative), destination.at(relative))
    return tuple(inserted), tuple(updated)


def _reconcile_deletes(
    destination: FolderFiles, current: "_Inventory", deletes, staged, *, contract
) -> tuple[str, ...]:
    """Explicit deletes, plus what a replaced folder stopped staging.

    Automatic removal inventories only managed files, so anything the file key
    does not claim survives a replacement it was never part of.
    """

    targets = set(deletes)
    if contract.replaces_wholesale:
        managed = {
            relative
            for relative in current.files
            if matches_file_key(relative, contract.file_keys)
        }
        targets.update(managed - set(staged))

    deleted: list[str] = []
    for relative in sorted(targets):
        if _is_changes_path(relative) or relative not in current.files:
            continue
        destination.store.delete(destination.at(relative))
        deleted.append(relative)
    return tuple(deleted)


# --- evidence and changes ------------------------------------------------------


def _reset_reject_evidence(reject: FolderFiles) -> None:
    if reject.store.exists(reject.location):
        reject.store.delete(reject.location, recursive=True)


def _keep_reject_evidence(rejected, staging: FolderFiles, reject: FolderFiles) -> None:
    for relative in rejected:
        _safe_replace(reject.store, staging.at(relative), reject.at(relative))


def _write_change_document(
    destination: FolderFiles,
    *,
    inserted: tuple[str, ...],
    updated: tuple[str, ...],
    deleted: tuple[str, ...],
) -> None:
    if not (inserted or updated or deleted):
        return
    store = destination.store
    changes = destination.location / CHANGES_DIRECTORY
    at = _utc_now()
    target = changes / f"{_format_change_datetime(at)}.json"
    while store.exists(target):
        at += timedelta(microseconds=1)
        target = changes / f"{_format_change_datetime(at)}.json"
    payload = {
        "inserts": sorted(inserted),
        "updates": sorted(updated),
        "deletes": sorted(deleted),
    }
    _safe_write_text(store, target, json.dumps(payload, indent=2) + "\n")


def _has_change_history(folder: FolderFiles) -> bool:
    """Treat any recorded file as history; validate documents only when reading."""

    try:
        entries = folder.store.list(folder.location / CHANGES_DIRECTORY)
    except StoreNotFoundError:
        return False
    return any(not entry.is_directory for entry in entries)


def adopt_existing_files(destination: FolderFiles) -> tuple[str, ...]:
    """Record every existing file once when the Folder has no change history."""

    if not destination.store.exists(destination.location):
        return ()
    if _has_change_history(destination):
        return ()
    existing = tuple(_inventory(destination, excluded_roots={CHANGES_DIRECTORY}).files)
    if not existing:
        return ()
    _write_change_document(destination, inserted=existing, updated=(), deleted=())
    return existing


def current_files(folder: FolderFiles, patterns=()) -> list[Path]:
    """The files the store lists now, as mounted paths.

    ``patterns`` match as the File key does; none claims every file. Weaver's
    change history is never listed.
    """

    root = folder.path.absolute()
    return [
        root / relative
        for relative in _inventory(folder, excluded_roots={CHANGES_DIRECTORY}).files
        if matches_file_key(relative, tuple(patterns))
    ]


def files_since(folder: FolderFiles, bookmark: datetime) -> dict[Path, datetime]:
    """Current files changed strictly after an aware ``bookmark``, and when."""

    boundary = _change_boundary(bookmark)
    root = folder.path.absolute()
    latest = _collapse_change_events(folder, _change_documents_since(folder, boundary))
    candidates = {
        relative: changed_at
        for relative, (operation, changed_at) in sorted(latest.items())
        if operation in ("inserts", "updates")
    }
    if not candidates:
        return {}
    present = _inventory(folder, excluded_roots={CHANGES_DIRECTORY}).files
    return {
        root / relative: changed_at
        for relative, changed_at in candidates.items()
        if relative in present
    }


def deleted_since(folder: FolderFiles, bookmark: datetime) -> dict[Path, datetime]:
    """Files deleted strictly after an aware ``bookmark``, and when.

    A returned path is the file the deletion retired, so it normally does not
    exist.
    """

    boundary = _change_boundary(bookmark)
    root = folder.path.absolute()
    latest = _collapse_change_events(folder, _change_documents_since(folder, boundary))
    return {
        root / relative: changed_at
        for relative, (operation, changed_at) in sorted(latest.items())
        if operation == "deletes"
    }


def latest_files(folder: FolderFiles) -> dict[Path, datetime]:
    """The current files from the newest change that left files in place.

    A file a newer change deleted is not reported.
    """

    root = folder.path.absolute()
    documents = _available_change_documents(folder)
    if not documents:
        return {}
    present = _inventory(folder, excluded_roots={CHANGES_DIRECTORY}).files
    tombstones: set[str] = set()
    for changed_at, location in reversed(documents):
        document = _read_change_document(folder, location)
        candidates = {*document["inserts"], *document["updates"]}
        tombstones.update(document["deletes"])
        surviving = {
            root / relative: changed_at
            for relative in sorted(candidates - tombstones)
            if relative in present
        }
        if surviving:
            return surviving
    return {}


def _change_boundary(bookmark: datetime) -> datetime:
    """The bookmark as UTC, refusing a naive datetime."""

    if not isinstance(bookmark, datetime) or bookmark.utcoffset() is None:
        raise LoadError("a Folder change bookmark must be a timezone-aware datetime")
    return bookmark.astimezone(timezone.utc)


def _available_change_documents(
    folder: FolderFiles,
) -> list[tuple[datetime, Location]]:
    """Every change document beneath the Folder, oldest first."""

    changes = folder.location / CHANGES_DIRECTORY
    try:
        entries = folder.store.list(changes)
    except StoreNotFoundError:
        return []
    documents = [
        (_parse_change_filename(entry.name), changes / entry.name)
        for entry in entries
        if not entry.is_directory and entry.name.endswith(".json")
    ]
    return sorted(documents, key=lambda item: (item[0], item[1].name))


def _change_documents_since(
    folder: FolderFiles, boundary: datetime
) -> list[tuple[datetime, Location]]:
    """The change documents strictly newer than ``boundary``, oldest first."""

    return [
        entry for entry in _available_change_documents(folder) if entry[0] > boundary
    ]


def _collapse_change_events(folder: FolderFiles, documents):
    latest: dict[str, tuple[str, datetime]] = {}
    for changed_at, location in documents:
        document = _read_change_document(folder, location)
        for operation in ("inserts", "updates", "deletes"):
            for relative in document[operation]:
                latest[relative] = (operation, changed_at)
    return latest


def _read_change_document(
    folder: FolderFiles, location: Location
) -> dict[str, tuple[str, ...]]:
    shown = folder.path / CHANGES_DIRECTORY / location.name
    try:
        raw = json.loads(folder.store.read(location).decode("utf-8"))
    except (StoreError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LoadError(f"cannot read Folder change document {shown}: {exc}") from exc
    if not isinstance(raw, dict) or set(raw) != {"inserts", "updates", "deletes"}:
        raise LoadError(f"Folder change document {shown} has an invalid shape")
    document: dict[str, tuple[str, ...]] = {}
    for operation in ("inserts", "updates", "deletes"):
        values = raw[operation]
        if not isinstance(values, list):
            raise LoadError(f"Folder change document {shown} has invalid {operation}")
        normalised: list[str] = []
        for value in values:
            if not isinstance(value, str) or not value or not _safe_relative(value):
                raise LoadError(
                    f"Folder change document {shown} has invalid path {value!r}"
                )
            normalised.append(Path(value).as_posix())
        document[operation] = tuple(normalised)
    return document


def _parse_change_filename(name: str) -> datetime:
    suffix = ".json"
    if not name.endswith(suffix):
        raise LoadError(f"invalid Folder change document name: {name!r}")
    try:
        parsed = datetime.strptime(name[: -len(suffix)], CHANGE_DATETIME_FORMAT)
    except ValueError as exc:
        raise LoadError(f"invalid Folder change document name: {name!r}") from exc
    return parsed.replace(tzinfo=timezone.utc)


def _format_change_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime(CHANGE_DATETIME_FORMAT)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _is_changes_path(relative: str) -> bool:
    parts = Path(relative).parts
    return bool(parts) and parts[0] == CHANGES_DIRECTORY


def _safe_relative(value: str) -> bool:
    path = Path(value)
    return (
        not path.is_absolute()
        and not value.startswith("/")
        and "\\" not in value
        and ".." not in path.parts
        and not _is_changes_path(path.as_posix())
    )


# --- the file key ----------------------------------------------------------------


def matches_file_key(relative: str, patterns) -> bool:
    """Whether the declared file key claims this path, segment by segment.

    Segment-wise rather than a flat string match, which decides what a
    replacement may delete. ``*`` stops at a directory boundary, so ``*.csv``
    claims ``a.csv`` and not ``archive/old.csv``; ``**`` spans any number of
    segments, so ``**/*`` means everything beneath here.
    """

    if not patterns:
        return True
    parts = tuple(Path(relative).as_posix().split("/"))
    return any(_match_parts(parts, tuple(p.split("/"))) for p in patterns)


def _match_parts(path_parts, pattern_parts) -> bool:
    if not pattern_parts:
        return not path_parts
    head, *tail = pattern_parts
    remaining = tuple(tail)
    if head == "**":
        return _match_parts(path_parts, remaining) or (
            bool(path_parts) and _match_parts(path_parts[1:], pattern_parts)
        )
    return (
        bool(path_parts)
        and fnmatch.fnmatchcase(path_parts[0], head)
        and _match_parts(path_parts[1:], remaining)
    )


def managed_relative_files(folder: FolderFiles, patterns) -> list[str]:
    return [
        relative
        for relative in _inventory(folder, excluded_roots={CHANGES_DIRECTORY}).files
        if matches_file_key(relative, patterns)
    ]


# --- the store ---------------------------------------------------------------------


@dataclass(frozen=True)
class _Inventory:
    """A folder as its store lists it: files by relative path, and directories."""

    files: dict[str, Any]
    directories: frozenset[str]


def _inventory(folder: FolderFiles, *, excluded_roots=frozenset()) -> _Inventory:
    files: dict[str, Any] = {}
    directories: set[str] = set()
    pending = [(folder.location, "")]
    while pending:
        directory, prefix = pending.pop()
        try:
            entries = folder.store.list(directory)
        except StoreNotFoundError:
            continue
        for entry in entries:
            relative = f"{prefix}{entry.name}"
            if entry.is_directory:
                if not prefix and entry.name in excluded_roots:
                    continue
                directories.add(relative)
                pending.append((directory / entry.name, f"{relative}/"))
            else:
                files[relative] = entry
    return _Inventory(dict(sorted(files.items())), frozenset(directories))


def _identical(
    staging: FolderFiles, destination: FolderFiles, relative: str, staged, existing
) -> bool:
    if staged.size is not None and existing.size is not None:
        if staged.size != existing.size:
            return False
    with tempfile.TemporaryDirectory(prefix="weaver-compare-") as scratch:
        source = Path(scratch) / "staged"
        target = Path(scratch) / "current"
        try:
            staging.store.copy_file_to_local(staging.at(relative), source)
            destination.store.copy_file_to_local(destination.at(relative), target)
        except StoreError:
            return False
        return filecmp.cmp(source, target, shallow=False)


def _safe_replace(store, source: Location, target: Location) -> None:
    """Copy into place through a temporary sibling and one move.

    Copying straight over the destination would leave a half-written file if
    anything failed mid-copy.
    """

    _through_sibling(store, target, lambda tmp: store.copy(source, tmp))


def _safe_write_text(store, target: Location, content: str) -> None:
    _through_sibling(store, target, lambda tmp: store.write(tmp, content.encode()))


def _through_sibling(store, target: Location, fill) -> None:
    tmp = Location(target.value.rsplit("/", 1)[0]) / f"{_TMP_PREFIX}{uuid.uuid4().hex}"
    try:
        fill(tmp)
        store.move(tmp, target)
    except BaseException:
        with contextlib.suppress(StoreError):
            if store.exists(tmp):
                store.delete(tmp)
        raise


__all__ = [
    "CHANGES_DIRECTORY",
    "adopt_existing_files",
    "CHANGE_DATETIME_FORMAT",
    "FolderFiles",
    "INTOLERANT_MESSAGE",
    "REJECT_SUFFIX",
    "TOLERATED_MESSAGE",
    "StagingFolder",
    "current_files",
    "deleted_since",
    "files_since",
    "latest_files",
    "load_folder",
    "managed_relative_files",
    "matches_file_key",
    "new_staging_folder",
    "remove_staging",
    "reset_staging",
]
