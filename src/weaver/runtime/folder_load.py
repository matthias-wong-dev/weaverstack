"""Reconcile a Python-defined folder into its destination.

Object code fills a Weaver-issued staging folder and returns files to delete.
Validation confirms staged and deleted paths are within the declared file key
before publishing changes.
"""

from __future__ import annotations

import filecmp
import fnmatch
import json
import os
import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..errors import LoadError
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


def load_folder(
    *,
    contract: FolderLoadContract,
    destination: str | Path,
    staging: str | Path,
    deletes=(),
    fault_tolerant: bool = False,
) -> LoadResult:
    """Reconcile one folder's staged files into its destination.

    Validation runs to completion before the first copy, so a folder that is
    going to be refused is refused with its destination exactly as it was.
    """

    destination_path, staging_path = _validate_paths(destination, staging)
    staged, rejected = _classify(staging_path, contract)
    deletes = _validate_deletes(deletes, staged, destination_path, contract=contract)
    rows_read = len(staged) + len(rejected)

    reject_path = destination_path.with_name(f"{destination_path.name}{REJECT_SUFFIX}")
    _reset_reject_evidence(reject_path)
    _keep_reject_evidence(rejected, staging_path, reject_path)

    if rejected and not fault_tolerant:
        raise LoadError(
            f"{contract.qualified}: {INTOLERANT_MESSAGE}",
            result=LoadResult.failure(
                INTOLERANT_MESSAGE, rows_read=rows_read, rows_rejected=len(rejected)
            ),
        )

    inserted, updated = _publish(staged, staging_path, destination_path)
    deleted = _reconcile_deletes(destination_path, deletes, staged, contract=contract)
    _write_change_document(
        destination_path,
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
    """The staging directory issued for one folder load.

    Folder loading accepts only this instance, preventing object code from
    returning an unprepared directory.
    """

    path: Path

    def __enter__(self) -> "StagingFolder":
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        return False


def new_staging_folder(destination: str | Path, staging: str | Path) -> StagingFolder:
    """Reset and issue the object-local staging directory."""

    _destination_path, staging_path = _validate_paths(destination, staging)
    reset_staging(staging_path)
    return StagingFolder(path=staging_path)


#: Retry a brief remote-filesystem propagation delay during staging cleanup.
RESET_ATTEMPTS = 5
RESET_PAUSE = 0.5


def reset_staging(path: Path) -> None:
    """Empty and recreate the fixed staging directory.

    OneLake mount state can lag DFS deletion, so removal is retried before the
    path is reused.
    """

    _with_retry(lambda: shutil.rmtree(path) if path.exists() else None)
    path.mkdir(parents=True, exist_ok=False)


def remove_staging(path: Path) -> None:
    """Remove staging after a load has published from it, tolerating a race."""

    _with_retry(lambda: shutil.rmtree(path) if path.exists() else None)


def _with_retry(action) -> None:
    import time

    for remaining in range(RESET_ATTEMPTS - 1, -1, -1):
        try:
            action()
            return
        except OSError:
            if remaining == 0:
                raise
            time.sleep(RESET_PAUSE)


# --- validation ----------------------------------------------------------------


def _validate_paths(destination: str | Path, staging: str | Path) -> tuple[Path, Path]:
    """The exact destination/staging relationship, refusing anything else.

    Staging must be the sibling Weaver names: the only directory the object was
    given, and the only one a load reads from. Anything else would publish a
    tree nothing validated.
    """

    destination_path = Path(destination)
    staging_path = Path(staging)
    if destination_path == staging_path:
        raise LoadError("a folder's staging path must not be its destination")
    if staging_path.parent != destination_path.parent:
        raise LoadError(
            "a folder's staging directory must be a sibling of its destination, "
            f"and {staging_path} is not beside {destination_path}"
        )
    expected = f"{destination_path.name}{STAGING_SUFFIX}"
    if staging_path.name != expected:
        raise LoadError(
            f"a folder's staging directory must be named {expected!r}, not "
            f"{staging_path.name!r}. Return self.staging_folder()"
        )
    return destination_path, staging_path


def _classify(staging_path: Path, contract) -> tuple[list[str], list[str]]:
    """Split the staged tree into what may be published and what may not.

    A file the key does not claim is a rejection rather than a quiet skip: the
    author staged it.
    """

    if not staging_path.is_dir():
        raise LoadError(
            f"a folder's staging directory does not exist: {staging_path}. "
            "Write files into self.staging_folder() and return it"
        )
    staged, rejected = [], []
    for relative in _relative_files(staging_path):
        if _is_changes_path(relative):
            raise LoadError(
                f"{contract.qualified}: {relative!r} is inside Weaver's "
                f"{CHANGES_DIRECTORY}/ directory and cannot be staged"
            )
        if matches_file_key(relative, contract.file_keys):
            staged.append(relative)
        else:
            rejected.append(relative)
    return staged, rejected


def _validate_deletes(
    deletes, staged, destination: Path, *, contract
) -> tuple[str, ...]:
    """Every delete entry, checked to be an exact file this folder may remove."""

    if isinstance(deletes, (str, bytes)):
        raise LoadError(
            f"{contract.qualified}: read() must return a sequence of relative "
            "file names to delete, not a single string"
        )
    entries = list(deletes or ())
    if entries and contract.replaces_wholesale:
        raise LoadError(
            f"{contract.qualified}: a non-incremental folder cannot name explicit "
            "deletes. It is replaced whole, so absence from staging is what "
            "retires a file"
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
        if (destination / path).is_dir():
            raise LoadError(
                f"{contract.qualified}: a delete must name a file, and "
                f"{relative!r} is a directory"
            )
        normalised.append(relative)
    return tuple(normalised)


# --- reconciliation --------------------------------------------------------------


def _publish(
    staged, staging_path: Path, destination: Path
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Copy the staged files into place, distinguishing arrival from replacement.

    A file whose bytes already match is neither inserted nor updated and is not
    rewritten, so a folder restaged with identical content reports no change
    rather than a full rewrite.
    """

    destination.mkdir(parents=True, exist_ok=True)
    inserted: list[str] = []
    updated: list[str] = []
    for relative in staged:
        source = staging_path / relative
        target = destination / relative
        if not target.exists():
            inserted.append(relative)
        elif not _identical(source, target):
            updated.append(relative)
        else:
            continue
        _safe_replace(source, target)
    return tuple(inserted), tuple(updated)


def _reconcile_deletes(
    destination: Path, deletes, staged, *, contract
) -> tuple[str, ...]:
    """Explicit deletes, plus what a replaced folder stopped staging.

    Automatic removal inventories only managed files, so anything the file key
    does not claim survives a replacement it was never part of.
    """

    targets = set(deletes)
    if contract.replaces_wholesale:
        managed = set(managed_relative_files(destination, contract.file_keys))
        targets.update(managed - set(staged))

    deleted: list[str] = []
    for relative in sorted(targets):
        if _is_changes_path(relative):
            continue
        target = destination / relative
        if target.is_file():
            target.unlink()
            deleted.append(relative)
    return tuple(deleted)


# --- evidence and changes ------------------------------------------------------


def _reset_reject_evidence(path: Path) -> None:
    """Remove evidence from the preceding Folder load."""

    if path.exists():
        shutil.rmtree(path)


def _keep_reject_evidence(rejected, staging: Path, destination: Path) -> None:
    """Preserve rejected files beneath the Folder's sibling evidence root."""

    for relative in rejected:
        _safe_replace(staging / relative, destination / relative)


def _write_change_document(
    destination: Path,
    *,
    inserted: tuple[str, ...],
    updated: tuple[str, ...],
    deleted: tuple[str, ...],
) -> None:
    """Append one immutable document after a Folder mutation succeeds."""

    if not (inserted or updated or deleted):
        return
    changes = destination / CHANGES_DIRECTORY
    changes.mkdir(parents=True, exist_ok=True)
    at = _utc_now()
    target = changes / f"{_format_change_datetime(at)}.json"
    while target.exists():
        at += timedelta(microseconds=1)
        target = changes / f"{_format_change_datetime(at)}.json"
    payload = {
        "inserts": sorted(inserted),
        "updates": sorted(updated),
        "deletes": sorted(deleted),
    }
    _safe_write_text(target, json.dumps(payload, indent=2) + "\n")


def files_since(destination: str | Path, bookmark: datetime) -> dict[Path, datetime]:
    """Current files changed strictly after an aware ``bookmark``, and when."""

    boundary = _change_boundary(bookmark)
    root = Path(destination).absolute()
    latest = _collapse_change_events(_change_documents_since(destination, boundary))
    return {
        root / relative: changed_at
        for relative, (operation, changed_at) in sorted(latest.items())
        if operation in ("inserts", "updates") and (root / relative).is_file()
    }


def deleted_since(destination: str | Path, bookmark: datetime) -> dict[Path, datetime]:
    """Files deleted strictly after an aware ``bookmark``, and when.

    A returned path is the file the deletion retired, so it normally does not
    exist.
    """

    boundary = _change_boundary(bookmark)
    root = Path(destination).absolute()
    latest = _collapse_change_events(_change_documents_since(destination, boundary))
    return {
        root / relative: changed_at
        for relative, (operation, changed_at) in sorted(latest.items())
        if operation == "deletes"
    }


def latest_files(destination: str | Path) -> dict[Path, datetime]:
    """The current files from the newest change that left files in place.

    A file a newer change deleted is not reported.
    """

    root = Path(destination).absolute()
    documents = _available_change_documents(destination)
    tombstones: set[str] = set()
    for changed_at, path in reversed(documents):
        document = _read_change_document(path)
        candidates = {*document["inserts"], *document["updates"]}
        tombstones.update(document["deletes"])
        surviving = {
            root / relative: changed_at
            for relative in sorted(candidates - tombstones)
            if (root / relative).is_file()
        }
        if surviving:
            return surviving
    return {}


def _change_boundary(bookmark: datetime) -> datetime:
    """The bookmark as UTC, refusing a naive datetime."""

    if not isinstance(bookmark, datetime) or bookmark.utcoffset() is None:
        raise LoadError("a Folder change bookmark must be a timezone-aware datetime")
    return bookmark.astimezone(timezone.utc)


def _change_documents(destination: str | Path) -> list[tuple[datetime, Path]]:
    """Every change document beneath the Folder, oldest first.

    Raises :class:`FileNotFoundError` when there is no ``_changes`` directory.
    """

    changes = Path(destination) / CHANGES_DIRECTORY
    documents = [
        (_parse_change_filename(entry.name), entry)
        for entry in changes.iterdir()
        if entry.is_file() and entry.suffix == ".json"
    ]
    return sorted(documents, key=lambda item: (item[0], item[1].name))


def _available_change_documents(
    destination: str | Path,
) -> list[tuple[datetime, Path]]:
    """The Folder's change history, empty when Weaver has never written it.

    ``_changes`` is the whole of managed history. A file the Folder holds that
    no document records is one Weaver never saw arrive, so it takes no part in
    an incremental read.
    """

    try:
        return _change_documents(destination)
    except FileNotFoundError:
        return []


def _change_documents_since(
    destination: str | Path, boundary: datetime
) -> list[tuple[datetime, Path]]:
    """The change documents strictly newer than ``boundary``, oldest first."""

    return [
        entry
        for entry in _available_change_documents(destination)
        if entry[0] > boundary
    ]


def _collapse_change_events(documents) -> dict[str, tuple[str, datetime]]:
    """Each file's latest operation and the datetime it was recorded."""

    latest: dict[str, tuple[str, datetime]] = {}
    for changed_at, path in documents:
        document = _read_change_document(path)
        for operation in ("inserts", "updates", "deletes"):
            for relative in document[operation]:
                latest[relative] = (operation, changed_at)
    return latest


def _read_change_document(path: Path) -> dict[str, tuple[str, ...]]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LoadError(f"cannot read Folder change document {path}: {exc}") from exc
    if not isinstance(raw, dict) or set(raw) != {"inserts", "updates", "deletes"}:
        raise LoadError(f"Folder change document {path} has an invalid shape")
    document: dict[str, tuple[str, ...]] = {}
    for operation in ("inserts", "updates", "deletes"):
        values = raw[operation]
        if not isinstance(values, list):
            raise LoadError(f"Folder change document {path} has invalid {operation}")
        normalised: list[str] = []
        for value in values:
            if not isinstance(value, str) or not value or not _safe_relative(value):
                raise LoadError(
                    f"Folder change document {path} has invalid path {value!r}"
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


def managed_relative_files(root: Path, patterns) -> list[str]:
    """The files beneath ``root`` the file key claims, as sorted POSIX paths."""

    root = Path(root)
    if not root.is_dir():
        return []
    return sorted(
        relative
        for relative in _relative_files(root, excluded_roots={CHANGES_DIRECTORY})
        if matches_file_key(relative, patterns)
    )


def _relative_files(root: Path, *, excluded_roots=frozenset()) -> list[str]:
    """Every leaf file beneath ``root``. Directories are not CRUD units."""

    files: list[str] = []
    for dirpath, dirnames, filenames in os.walk(root):
        if Path(dirpath) == root:
            dirnames[:] = [name for name in dirnames if name not in excluded_roots]
        for name in filenames:
            full = Path(dirpath) / name
            files.append(full.relative_to(root).as_posix())
    return sorted(files)


def _identical(source: Path, target: Path) -> bool:
    try:
        if source.stat().st_size != target.stat().st_size:
            return False
    except OSError:
        return False
    return filecmp.cmp(source, target, shallow=False)


def _safe_replace(source: Path, target: Path) -> None:
    """Copy into place through a temporary sibling and one atomic rename.

    Copying straight over the destination would leave a half-written file if
    anything failed mid-copy.
    """

    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.parent / f"{_TMP_PREFIX}{uuid.uuid4().hex}"
    try:
        shutil.copyfile(source, tmp)
        os.replace(tmp, target)
    finally:
        if tmp.exists():
            tmp.unlink()


def _safe_write_text(target: Path, content: str) -> None:
    """Write text through a temporary sibling and one atomic rename."""

    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.parent / f"{_TMP_PREFIX}{uuid.uuid4().hex}"
    try:
        tmp.write_text(content, encoding="utf-8")
        os.replace(tmp, target)
    finally:
        if tmp.exists():
            tmp.unlink()


__all__ = [
    "CHANGES_DIRECTORY",
    "CHANGE_DATETIME_FORMAT",
    "INTOLERANT_MESSAGE",
    "REJECT_SUFFIX",
    "RESET_ATTEMPTS",
    "RESET_PAUSE",
    "TOLERATED_MESSAGE",
    "StagingFolder",
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
