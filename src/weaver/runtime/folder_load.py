"""Reconcile a Python-defined folder into its destination.

Object code fills a Weaver-issued staging folder and returns files to delete.
Validation confirms staged and deleted paths are within the declared file key
before publishing changes.
"""

from __future__ import annotations

import filecmp
import fnmatch
import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path

from ..errors import LoadError
from .load_contract import FolderLoadContract
from .load_result import LoadResult

#: Files Weaver owns inside a managed folder. Object code may never stage or
#: delete one: they describe the folder itself, so a load that replaced one
#: would rewrite its own bookkeeping.
RESERVED_NAMES = frozenset({"_weaver.json"})

#: Characters that make a delete entry a pattern rather than a path. A delete is
#: an exact statement about one file; a glob would let an object remove files it
#: never named and could not have known were there.
_GLOB_CHARS = set("*?[]")

_TMP_PREFIX = "._weaver_tmp_"

STAGING_SUFFIX = "_Staging"

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

    if rejected and not fault_tolerant:
        raise LoadError(
            f"{contract.qualified}: {INTOLERANT_MESSAGE}",
            result=LoadResult.failure(
                INTOLERANT_MESSAGE, rows_read=rows_read, rows_rejected=len(rejected)
            ),
        )

    inserted, updated = _publish(staged, staging_path, destination_path)
    deleted = _reconcile_deletes(destination_path, deletes, staged, contract=contract)

    result = LoadResult(
        succeeded=True,
        rows_read=rows_read,
        rows_inserted=inserted,
        rows_updated=updated,
        rows_deleted=deleted,
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


def folder_is_populated(destination: str | Path, patterns) -> bool:
    """Whether the destination contains a file matching this folder's file key."""

    return bool(managed_relative_files(Path(destination), patterns))


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
            f"{staging_path.name!r} — return self.staging_folder()"
        )
    return destination_path, staging_path


def _classify(staging_path: Path, contract) -> tuple[list[str], list[str]]:
    """Split the staged tree into what may be published and what may not.

    A file the key does not claim is a rejection rather than a quiet skip: the
    author staged it deliberately.
    """

    if not staging_path.is_dir():
        raise LoadError(
            f"a folder's staging directory does not exist: {staging_path} — "
            "write files into self.staging_folder() and return it"
        )
    staged, rejected = [], []
    for relative in _relative_files(staging_path):
        if Path(relative).name in RESERVED_NAMES:
            raise LoadError(
                f"{contract.qualified}: {relative!r} is a Weaver file and cannot "
                "be staged"
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
            "deletes — it is replaced whole, so absence from staging is what "
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
        if path.name in RESERVED_NAMES:
            raise LoadError(
                f"{contract.qualified}: {raw!r} is a Weaver file and cannot be deleted"
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


def _publish(staged, staging_path: Path, destination: Path) -> tuple[int, int]:
    """Copy the staged files into place, distinguishing arrival from replacement.

    A file whose bytes already match is neither inserted nor updated and is not
    rewritten, so a folder restaged with identical content reports no change
    rather than a full rewrite.
    """

    destination.mkdir(parents=True, exist_ok=True)
    inserted = updated = 0
    for relative in staged:
        source = staging_path / relative
        target = destination / relative
        if not target.exists():
            inserted += 1
        elif not _identical(source, target):
            updated += 1
        else:
            continue
        _safe_replace(source, target)
    return inserted, updated


def _reconcile_deletes(destination: Path, deletes, staged, *, contract) -> int:
    """Explicit deletes, plus — when the folder is replaced — what it stopped staging.

    Automatic removal inventories only *managed* files, so anything the file key
    does not claim survives a replacement it was never part of.
    """

    targets = set(deletes)
    if contract.replaces_wholesale:
        managed = set(managed_relative_files(destination, contract.file_keys))
        targets.update(managed - set(staged))

    deleted = 0
    for relative in sorted(targets):
        if Path(relative).name in RESERVED_NAMES:
            continue
        target = destination / relative
        if target.is_file():
            target.unlink()
            deleted += 1
    return deleted


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
        for relative in _relative_files(root)
        if Path(relative).name not in RESERVED_NAMES
        and matches_file_key(relative, patterns)
    )


def _relative_files(root: Path) -> list[str]:
    """Every leaf file beneath ``root``. Directories are not CRUD units."""

    files: list[str] = []
    for dirpath, _dirnames, filenames in os.walk(root):
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


__all__ = [
    "INTOLERANT_MESSAGE",
    "RESERVED_NAMES",
    "RESET_ATTEMPTS",
    "RESET_PAUSE",
    "TOLERATED_MESSAGE",
    "StagingFolder",
    "folder_is_populated",
    "load_folder",
    "managed_relative_files",
    "matches_file_key",
    "new_staging_folder",
    "remove_staging",
    "reset_staging",
]
