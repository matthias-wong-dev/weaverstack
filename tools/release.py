#!/usr/bin/env python3
"""Tag the release `VERSION` names, and push it.

    python tools/release.py

There is no version argument. `VERSION` is the release, so there is nothing to
mistype here and nothing to keep in step with it.

Publishing belongs to GitHub Actions. Pushing the tag is the release event: the
workflow checks the tag against `VERSION`, runs the suite, builds, verifies both
artefacts carry that exact version, publishes to PyPI through trusted
publishing, and creates the GitHub Release. This script uploads nothing.

Before tagging it requires a clean working tree on the release branch, level
with its remote, because the tag has to name source that CI can see and that
nobody has edited underneath it.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

#: Where releases are cut from.
RELEASE_BRANCH = "main"
REMOTE = "origin"


class ReleaseError(Exception):
    """A precondition the release cannot proceed without."""


def git(*args: str, check: bool = True) -> str:
    """One Git answer as text."""

    result = subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True)
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise ReleaseError(f"git {' '.join(args)} failed: {detail}")
    return result.stdout.strip()


def declared_version() -> str:
    """The release `VERSION` names, validated the way a build validates it."""

    sys.path.insert(0, str(ROOT))
    from hatch_build import declared_version as read

    return read()


def require_clean_tree() -> None:
    if git("status", "--porcelain"):
        raise ReleaseError(
            "The working tree has uncommitted changes.\n"
            "A tag must name source that is committed and pushed."
        )


def require_release_branch() -> None:
    """On the release branch, and level with the remote CI will build from."""

    branch = git("rev-parse", "--abbrev-ref", "HEAD")
    if branch != RELEASE_BRANCH:
        raise ReleaseError(
            f"Releases are cut from {RELEASE_BRANCH}, and this is {branch}."
        )

    git("fetch", REMOTE, RELEASE_BRANCH, "--tags")
    local = git("rev-parse", "HEAD")
    remote = git("rev-parse", f"{REMOTE}/{RELEASE_BRANCH}")
    if local == remote:
        return
    # Behind is fatal: CI would build a commit this checkout has never seen.
    # Ahead is fatal too, because the tag would point at an unpushed commit.
    behind, ahead = git(
        "rev-list", "--left-right", "--count", f"{remote}...{local}"
    ).split()
    raise ReleaseError(
        f"HEAD is {ahead} commit(s) ahead of and {behind} behind "
        f"{REMOTE}/{RELEASE_BRANCH}.\n"
        f"Push or pull so the two agree, then release."
    )


def require_tag_is_free(tag: str) -> None:
    """Refuse a tag that already exists, unless it already names this commit.

    Re-running after a workflow failure is the safe case: the tag is already
    pointing at exactly this source, so pushing it again is what re-triggers
    the release. Moving a tag onto different source is never safe, because a
    version that reached PyPI cannot be replaced.
    """

    head = git("rev-parse", "HEAD")
    local = git("rev-parse", "--verify", "--quiet", f"refs/tags/{tag}", check=False)
    remote = git("ls-remote", "--tags", REMOTE, f"refs/tags/{tag}")

    remote_commit = remote.split()[0] if remote else ""
    # An annotated tag's ref resolves to the tag object, so compare the commit
    # each one points at.
    local_commit = git("rev-list", "-n", "1", tag, check=False) if local else ""
    remote_head = (
        git("rev-list", "-n", "1", remote_commit, check=False) if remote_commit else ""
    )

    for where, commit in (("locally", local_commit), (f"on {REMOTE}", remote_head)):
        if commit and commit != head:
            raise ReleaseError(
                f"Tag {tag} already exists {where}, on a different commit.\n"
                f"  {tag} -> {commit[:12]}\n"
                f"  HEAD -> {head[:12]}\n\n"
                f"That version may already be published. Set VERSION to the next "
                f"release, and tag that."
            )
    if local_commit == head and remote_head == head:
        print(f"Tag {tag} already names this commit. Pushing it again.")


def release() -> int:
    version = declared_version()
    tag = f"v{version}"

    require_clean_tree()
    require_release_branch()
    require_tag_is_free(tag)

    head = git("rev-parse", "HEAD")
    print(f"Releasing weaverstack {version}")
    print(f"  branch   {RELEASE_BRANCH}")
    print(f"  commit   {head[:12]}")
    print(f"  tag      {tag}")
    print()

    if git("rev-parse", "--verify", "--quiet", f"refs/tags/{tag}", check=False) == "":
        git("tag", "-a", tag, "-m", f"weaverstack {version}")
    git("push", REMOTE, tag)

    print(f"Released source tagged {tag}.")
    print("GitHub Actions will build and publish this tag.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    try:
        return release()
    except ReleaseError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
