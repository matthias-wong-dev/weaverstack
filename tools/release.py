#!/usr/bin/env python3
"""Tag the version declared in `VERSION` and push the tag.

GitHub Actions performs the build and publication. See design/releasing.md.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

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
    """Read and validate the version declared in `VERSION`."""

    sys.path.insert(0, str(ROOT))
    from hatch_build import declared_version as read

    return read()


def require_clean_tree() -> None:
    """Require no uncommitted changes."""

    if git("status", "--porcelain"):
        raise ReleaseError(
            "The working tree has uncommitted changes.\n"
            "A tag must name source that is committed and pushed."
        )


def require_release_branch() -> None:
    """Require local `main` to match `origin/main`."""

    branch = git("rev-parse", "--abbrev-ref", "HEAD")
    if branch != RELEASE_BRANCH:
        raise ReleaseError(
            f"Releases are cut from {RELEASE_BRANCH}, and this is {branch}."
        )

    git("fetch", REMOTE, RELEASE_BRANCH, "--tags")
    if git("rev-parse", "HEAD") != git("rev-parse", f"{REMOTE}/{RELEASE_BRANCH}"):
        raise ReleaseError(
            f"HEAD does not match {REMOTE}/{RELEASE_BRANCH}.\n"
            "Push or pull so the two agree, then release."
        )


def tag_state(tag: str) -> str:
    """Require the release tag to be unused or already point at HEAD.

    Returns ``new``, ``local`` or ``pushed``.
    """

    head = git("rev-parse", "HEAD")
    local = git("rev-list", "-n", "1", tag, check=False)
    listed = git("ls-remote", "--tags", REMOTE, f"refs/tags/{tag}")
    remote = (
        git("rev-list", "-n", "1", listed.split()[0], check=False) if listed else ""
    )

    for where, commit in (("locally", local), (f"on {REMOTE}", remote)):
        if commit and commit != head:
            raise ReleaseError(
                f"Tag {tag} already exists {where}, on a different commit.\n"
                f"  {tag} -> {commit[:12]}\n"
                f"  HEAD -> {head[:12]}\n\n"
                "That version may already be published. Set VERSION to the next "
                "release, and tag that."
            )
    if remote:
        return "pushed"
    return "local" if local else "new"


def release() -> int:
    version = declared_version()
    tag = f"v{version}"

    require_clean_tree()
    require_release_branch()
    state = tag_state(tag)

    if state == "pushed":
        # Re-pushing an identical tag updates no ref, so GitHub raises no event.
        print(f"{tag} is already pushed for this commit, so there is nothing to do.")
        print("Rerun the Publish to PyPI workflow in GitHub Actions to publish it.")
        return 0

    print(f"Releasing weaverstack {version}")
    print(f"  commit   {git('rev-parse', 'HEAD')[:12]}")
    print(f"  tag      {tag}")
    print()

    if state == "new":
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
