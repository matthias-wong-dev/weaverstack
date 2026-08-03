"""Loading a Python-defined folder — the mechanics behind ``Folder.load()``.

The folder counterpart of :mod:`weaver.runtime.table_load`, and the same
division holds: ``read()`` writes into the staging directory Weaver issued and
returns ``(staging_folder, files_to_delete)``; everything that reaches the
destination happens here.

Files are the rows. ``rows_read`` is what was staged, ``rows_inserted`` what
arrived that was not there, ``rows_updated`` what replaced something different,
``rows_deleted`` what was removed. Treating files as rows keeps one result model
across all four primitives, which is what lets a caller — and later a logger —
handle them uniformly.

**The file key is what makes replacement safe.** A non-incremental folder is
replaced, and replacement has to know what it is entitled to remove: only files
matching the declared key are Weaver's to manage, so anything else in the
destination is left exactly where it is. Without that a folder declaring
``*.csv`` would delete a README nobody asked it to touch.

``fault_tolerant`` has a narrower job here than for a table. A folder has no
per-row rejection: a file either stages or it does not. What can be rejected is
a staged path that escapes the destination or names a reserved file, and that is
what the setting governs.

Pure filesystem work through the injected store, so nothing here needs Spark.
"""

from __future__ import annotations

import fnmatch
import os
import shutil
from pathlib import Path

from ..errors import LoadError
from .load_contract import FolderLoadContract
from .load_result import LoadResult

#: Files Weaver owns inside a managed folder. Object code may never stage or
#: delete one: they are how the folder itself is described, so a load that
#: replaced one would rewrite its own bookkeeping.
RESERVED_NAMES = frozenset({"_weaver.json"})

INTOLERANT_MESSAGE = (
    "staged files were rejected and fault_tolerant = 0, so the folder was not "
    "modified"
)
TOLERATED_MESSAGE = "staged files were rejected and excluded from the load"


def load_folder(
    *,
    contract: FolderLoadContract,
    destination: str,
    staging: str,
    deletes=(),
    fault_tolerant: bool = False,
) -> LoadResult:
    """Reconcile one folder's staged files into its destination."""

    staging_root = Path(staging)
    target_root = Path(destination)
    if not staging_root.exists():
        raise LoadError(
            f"{contract.qualified}: read() returned the staging folder "
            f"{staging!r}, which does not exist — write files into "
            "self.staging_folder() and return it"
        )

    staged, rejected = _classify(staging_root, contract)
    rows_read = len(staged) + len(rejected)

    if rejected and not fault_tolerant:
        return LoadResult.failure(
            INTOLERANT_MESSAGE, rows_read=rows_read, rows_rejected=len(rejected)
        )

    target_root.mkdir(parents=True, exist_ok=True)
    inserted, updated = _copy(staged, staging_root, target_root)
    deleted = _delete(target_root, deletes, contract)
    if contract.replaces_wholesale:
        deleted += _remove_unstaged(target_root, staged, contract)

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


def _classify(staging_root: Path, contract) -> tuple[list[str], list[str]]:
    """Split the staged tree into what may be copied and what may not.

    A reserved name is refused rather than skipped, because silently ignoring it
    would leave the author believing a file was published when it was not.
    """

    staged, rejected = [], []
    for path in sorted(staging_root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(staging_root).as_posix()
        if Path(relative).name in RESERVED_NAMES:
            rejected.append(relative)
        else:
            staged.append(relative)
    return staged, rejected


def _copy(staged, staging_root: Path, target_root: Path) -> tuple[int, int]:
    """Publish the staged files, distinguishing arrival from replacement.

    A file whose bytes already match is neither: it is counted as neither
    inserted nor updated and is left alone, so a folder that restaged identical
    content reports no change rather than a full rewrite.
    """

    inserted = updated = 0
    for relative in staged:
        source = staging_root / relative
        destination = target_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.exists():
            inserted += 1
        elif _differs(source, destination):
            updated += 1
        else:
            continue
        shutil.copy2(source, destination)
    return inserted, updated


def _differs(source: Path, destination: Path) -> bool:
    if source.stat().st_size != destination.stat().st_size:
        return True
    return source.read_bytes() != destination.read_bytes()


def _delete(target_root: Path, deletes, contract) -> int:
    """Remove the files the object named, and only those.

    Tolerant of absence: a delete is reconciliation toward "this must not
    exist", and something else having already removed it is that state reached.
    """

    removed = 0
    for relative in deletes or ():
        candidate = _within(target_root, relative, contract)
        if candidate.is_file():
            candidate.unlink()
            removed += 1
    return removed


def _remove_unstaged(target_root: Path, staged, contract) -> int:
    """Replacement: drop managed files this load did not produce.

    Managed is decided by the declared file key. A file the key does not match
    was never Weaver's, so it survives a replacement it was never part of.
    """

    keep = set(staged)
    removed = 0
    for path in sorted(target_root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(target_root).as_posix()
        if relative in keep or Path(relative).name in RESERVED_NAMES:
            continue
        if _managed(relative, contract):
            path.unlink()
            removed += 1
    return removed


def _managed(relative: str, contract) -> bool:
    """Whether the declared file key claims this path."""

    if not contract.file_keys:
        return True
    name = Path(relative).name
    return any(
        fnmatch.fnmatch(relative, key) or fnmatch.fnmatch(name, key)
        for key in contract.file_keys
    )


def _within(target_root: Path, relative: str, contract) -> Path:
    """Resolve a named path, refusing one that leaves the folder.

    An object naming ``../../something`` is not a load error to be tolerated —
    it is an attempt to delete outside the object's own destination, and the
    only safe answer is to refuse the whole load.
    """

    candidate = (target_root / relative).resolve()
    root = target_root.resolve()
    if root != candidate and root not in candidate.parents:
        raise LoadError(
            f"{contract.qualified}: read() asked to delete {relative!r}, which "
            "is outside the folder — a load may only touch its own destination"
        )
    return candidate


__all__ = ["INTOLERANT_MESSAGE", "RESERVED_NAMES", "TOLERATED_MESSAGE", "load_folder"]
