"""The wheel version, from one authored release line and the source's identity.

Read by Hatch's ``code`` version source (``[tool.hatch.version]`` in
``pyproject.toml``) via :func:`compute_version`.

``VERSION`` at the repository root is the only authored version in the project.
It names the release line under development. Everything else is derived from it
or checked against it::

    VERSION = 0.9.0

    ordinary checkout                 0.9.0.dev<fingerprint>
    clean HEAD tagged v0.9.0          0.9.0
    clean HEAD tagged v0.9.1          error
    clean HEAD tagged v0.8.0          error

A release tag grants permission to drop the ``.dev`` suffix. It never decides
which release line the code is on, so tagging changes no source file and the
tagged source is byte for byte the source that was tested.

Three constraints shape the development fingerprint:

* **Moves when the source moves.** A changed checkout builds a differently
  named wheel on its own.
* **Stable when the source is unchanged.** The same checkout produces the same
  version on every build and on every machine, so ``weaver install`` can skip a
  five-minute republish and a console can compare itself with the published
  wheel for exact equality.
* **No PEP 440 local segment.** Fabric rejects a ``+`` in an uploaded wheel
  filename, so the source identity is encoded as a public decimal dev number.

The fingerprint covers the commit and a canonical Git tree of every tracked or
non-ignored source path. The temporary tree makes staging irrelevant and applies
the repository's clean filters, so CRLF on Windows and LF on macOS identify the
same source. Compare these versions for equality, never to infer which source
state is newer.

Building a wheel from an sdist never reaches this module: Hatchling reads the
version out of ``PKG-INFO``, which the sdist build already wrote from here.
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

#: The one authored version in the repository.
VERSION_FILE = "VERSION"

#: A release tag is the declared version with this in front of it, and nothing
#: else. `tools/release.py` writes them and the release workflow checks them.
TAG_PREFIX = "v"


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


def declared_version() -> str:
    """The release line ``VERSION`` names, refused unless it is canonical.

    ``packaging`` parses it, so every PEP 440 version this project could want is
    accepted. It also has to be spelled the way ``packaging`` spells it back:
    a ``VERSION`` of ``v0.9.0`` or ``0.9.0.0`` would otherwise produce a tag and
    a wheel that disagree about the same release.
    """

    path = PROJECT_ROOT / VERSION_FILE
    try:
        text = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        raise RuntimeError(
            f"{VERSION_FILE} is missing from {PROJECT_ROOT}. It holds the one "
            "authored Weaver version, for example 0.9.0."
        ) from None

    from packaging.version import InvalidVersion, Version

    try:
        parsed = Version(text)
    except InvalidVersion:
        raise RuntimeError(
            f"{VERSION_FILE} does not hold a PEP 440 version: {text!r}"
        ) from None
    if str(parsed) != text:
        raise RuntimeError(
            f"{VERSION_FILE} holds {text!r}, which PEP 440 spells {str(parsed)!r}. "
            "Write the canonical spelling, so the release tag and the wheel "
            "cannot disagree."
        )
    return text


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


def _release_tags_at_head() -> tuple[str, ...]:
    """Every ``v*`` tag pointing at HEAD, in the order Git lists them."""

    listed = _git("tag", "--points-at", "HEAD", "--list", f"{TAG_PREFIX}*")
    return tuple(
        line.strip()
        for line in listed.decode("utf-8", errors="replace").splitlines()
        if line.strip()
    )


def compute_version() -> str:
    """The version this checkout builds, for the release line ``VERSION`` names."""

    declared = declared_version()

    head = _git("rev-parse", "HEAD").strip()
    head_tree = _git("rev-parse", "HEAD^{tree}").strip()
    if not head or not head_tree:
        raise RuntimeError("building a Weaver version needs a Git checkout")
    source_tree = _source_tree()

    # Only a clean checkout can be the release. A dirty one carries source the
    # tag never covered, so it stays a development build whatever it is tagged.
    if source_tree == head_tree:
        tags = _release_tags_at_head()
        expected = f"{TAG_PREFIX}{declared}"
        if expected in tags:
            return declared
        if tags:
            raise RuntimeError(
                f"{VERSION_FILE} says {declared}, so the release tag here would "
                f"be {expected}. This commit is tagged {', '.join(sorted(tags))}."
                "\n\n"
                f"A tag never changes the release line. Correct {VERSION_FILE} or "
                "the tag so the two agree."
            )

    return f"{declared}.dev{_fingerprint(head=head, source_tree=source_tree)}"
