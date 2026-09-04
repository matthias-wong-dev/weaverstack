"""The wheel version for this checkout.

`VERSION` is the authored base version. A clean checkout tagged `v<VERSION>`
builds that version; anything else builds a deterministic
`<VERSION>.dev<fingerprint>` covering the commit and the working source.

See design/releasing.md.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Mapping

PROJECT_ROOT = Path(__file__).resolve().parent

VERSION_FILE = "VERSION"
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
    """Read and validate the version declared in ``VERSION``."""

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

    # A private index canonicalises the working tree without touching the
    # developer's staging area, and applies the repository's clean filters.
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

    # Decimal, not a PEP 440 local segment: Fabric rejects '+' in a wheel name.
    return str(int.from_bytes(digest.digest()[:8], "big"))


def _release_tags_at_head() -> tuple[str, ...]:
    """Every ``v*`` tag pointing at HEAD."""

    listed = _git("tag", "--points-at", "HEAD", "--list", f"{TAG_PREFIX}*")
    return tuple(
        line.strip()
        for line in listed.decode("utf-8", errors="replace").splitlines()
        if line.strip()
    )


def compute_version() -> str:
    """The version this checkout builds."""

    declared = declared_version()

    head = _git("rev-parse", "HEAD").strip()
    head_tree = _git("rev-parse", "HEAD^{tree}").strip()
    if not head or not head_tree:
        raise RuntimeError("building a Weaver version needs a Git checkout")
    source_tree = _source_tree()

    # A release requires a clean checkout.
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
                f"Correct {VERSION_FILE} or the tag so the two agree."
            )

    return f"{declared}.dev{_fingerprint(head=head, source_tree=source_tree)}"
