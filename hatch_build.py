"""A Git-derived, Fabric-safe wheel version.

Read by Hatch's ``code`` version source (``[tool.hatch.version]`` in
pyproject.toml) via ``compute_version()``.

Three constraints shape the scheme:

* **Moves when the code moves.** No hand-edited version string; a changed
  checkout builds a differently-named wheel on its own.
* **Stable when the code is unchanged.** Rebuilding the *same* source must
  produce the *same* version, so ``weaver install`` can see that nothing changed
  and skip a five-minute republish. (A timestamp fails this — it moves on every
  build, even a no-op one.)
* **No PEP 440 local segment.** Fabric's Environment library upload rejects a
  ``+`` in a wheel filename (it answers 500), so the ``+g<hash>`` that
  ``setuptools_scm`` appends is unusable.

So the version is a *public* dev version whose dev number is a fingerprint of
the source state::

    clean checkout exactly on tag v0.1.0   -> 0.1.0
    ahead of, or dirtier than, that tag    -> 0.1.1.dev<fingerprint>

The fingerprint is a hash of the commit and the working-tree changes, so it is
identical for identical source and different the moment anything changes. It is
content-ordered rather than chronological: a newer build is not guaranteed a
higher dev number, so compare versions for equality (they are exact), not order.
"""

from __future__ import annotations

import hashlib
import re
import subprocess


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], capture_output=True, text=True).stdout.strip()


def _fingerprint() -> str:
    """A stable decimal digest of the exact source state.

    Commit id plus the working tree: ``git status`` catches added/removed and
    untracked files, ``git diff HEAD`` catches the content of tracked edits. The
    same source yields the same number; any change yields a different one.
    """

    head = _git("rev-parse", "HEAD")
    status = _git("status", "--porcelain")
    diff = _git("diff", "HEAD")
    digest = hashlib.sha1(f"{head}\0{status}\0{diff}".encode()).hexdigest()
    return str(int(digest[:9], 16))  # up to ~6.9e10; unique per distinct source


def _bump_patch(base: str) -> str:
    major, minor, patch = (int(part) for part in base.split("."))
    return f"{major}.{minor}.{patch + 1}"


def compute_version() -> str:
    """The version for the current checkout state."""

    described = _git("describe", "--tags", "--long", "--dirty", "--match", "v*")
    # e.g. "v0.1.0-0-g7148d2d" (clean at tag) or "v0.1.0-3-gabcdef1-dirty".
    match = re.match(
        r"^v(?P<base>\d+\.\d+\.\d+)-(?P<distance>\d+)-g[0-9a-f]+(?P<dirty>-dirty)?$",
        described,
    )
    if not match:
        # No reachable tag (shallow clone, fresh repo): still valid and stable.
        return f"0.0.0.dev{_fingerprint()}"
    if int(match.group("distance")) == 0 and not match.group("dirty"):
        return match.group("base")
    return f"{_bump_patch(match.group('base'))}.dev{_fingerprint()}"
