"""A deterministic, Fabric-safe wheel version derived from source identity.

Read by Hatch's ``code`` version source (``[tool.hatch.version]`` in
``pyproject.toml``) via :func:`compute_version`.

Three constraints shape the scheme:

* **Moves when the source moves.** No hand-edited version string; a changed
  checkout builds a differently-named wheel on its own.
* **Stable when the source is unchanged.** The same checkout produces the same
  version on every build and on every machine, so ``weaver install`` can skip a
  five-minute republish and a console can compare itself with the published
  wheel for exact equality.
* **No PEP 440 local segment.** Fabric rejects a ``+`` in an uploaded wheel
  filename, so the source identity is encoded as a public decimal dev number.

The result is content-addressed, deliberately not chronological::

    clean checkout exactly on tag v0.1.0   -> 0.1.0
    ahead of, or dirtier than, that tag    -> 0.1.1.dev<fingerprint>

The fingerprint covers the commit and a canonical Git tree of every tracked or
non-ignored source path. The temporary tree makes staging irrelevant and applies
the repository's clean filters, so CRLF on Windows and LF on macOS identify the
same source. Compare these versions for equality, never to infer which source
state is newer.
"""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Mapping

PROJECT_ROOT = Path(__file__).resolve().parent


def _git(
    *args: str,
    env: Mapping[str, str] | None = None,
    check: bool = False,
) -> bytes:
    """One Git answer as bytes, or empty when Git cannot answer here."""

    result = subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        env=env,
    )
    if check and result.returncode != 0:
        detail = result.stderr.decode(errors="replace").strip()
        raise RuntimeError(f"Git could not fingerprint the Weaver source: {detail}")
    return result.stdout if result.returncode == 0 else b""


def _source_tree() -> bytes:
    """Git tree identity for the working source, independent of index state."""

    # A private index lets Git canonicalise the whole working tree without
    # changing the developer's real staging area. This folds tracked, untracked,
    # deleted, binary, file-mode and clean-filtered content into one tree id.
    with tempfile.TemporaryDirectory(prefix="weaver-version-") as directory:
        env = os.environ.copy()
        env["GIT_INDEX_FILE"] = str(Path(directory) / "index")
        _git("read-tree", "HEAD", env=env, check=True)
        _git("add", "-A", "--", ".", env=env, check=True)
        tree = _git("write-tree", env=env, check=True).strip()
    if not tree:
        raise RuntimeError("Git could not identify the Weaver source tree")
    return tree


def _fingerprint(*, head: bytes, source_tree: bytes) -> str:
    """A stable decimal digest of the commit and canonical source tree."""

    digest = hashlib.sha256()
    digest.update(b"head\0")
    digest.update(head)
    digest.update(b"\0source-tree\0")
    digest.update(source_tree)

    # Sixty-four bits keeps the public version compact while leaving collision
    # risk negligible for the number of source states this project can produce.
    return str(int.from_bytes(digest.digest()[:8], "big"))


def _bump_patch(base: str) -> str:
    major, minor, patch = (int(part) for part in base.split("."))
    return f"{major}.{minor}.{patch + 1}"


def compute_version() -> str:
    """The deterministic version for the current checkout state."""

    head = _git("rev-parse", "HEAD").strip()
    head_tree = _git("rev-parse", "HEAD^{tree}").strip()
    if not head or not head_tree:
        raise RuntimeError("building a Weaver version needs a Git checkout")
    source_tree = _source_tree()

    described = (
        _git("describe", "--tags", "--long", "--dirty", "--match", "v*")
        .decode("ascii", errors="replace")
        .strip()
    )
    # e.g. "v0.1.0-0-g7148d2d" (clean at tag) or
    # "v0.1.0-3-gabcdef1-dirty".
    match = re.match(
        r"^v(?P<base>\d+\.\d+\.\d+)-(?P<distance>\d+)-g[0-9a-f]+(?P<dirty>-dirty)?$",
        described,
    )
    if not match:
        # A shallow checkout may have no reachable tag but still has exact
        # source identity through HEAD and the working tree.
        return f"0.0.0.dev{_fingerprint(head=head, source_tree=source_tree)}"
    if (
        int(match.group("distance")) == 0
        and source_tree == head_tree
    ):
        return match.group("base")
    fingerprint = _fingerprint(head=head, source_tree=source_tree)
    return f"{_bump_patch(match.group('base'))}.dev{fingerprint}"
