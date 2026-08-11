"""A Git-derived, Fabric-safe wheel version.

Read by Hatch's ``code`` version source (``[tool.hatch.version]`` in
pyproject.toml) via ``compute_version()``.

Three constraints shape the scheme:

* **Moves when the code moves.** No hand-edited version string; a build produces
  a differently-named wheel from the last one on its own.
* **Increases.** A newer wheel sorts *above* the one it replaces, so "which of
  these two is newer" has an answer. The old scheme's docstring had to warn
  readers to compare versions for equality and never for order, which is a
  property every tool that handles a version — pip, a resolver, a person
  reading two wheel names — will eventually be caught by.
* **No PEP 440 local segment.** Fabric's Environment library upload rejects a
  ``+`` in a wheel filename (it answers 500), so the ``+g<hash>`` that
  ``setuptools_scm`` appends is unusable.

So the version is a public dev version whose dev number is the build instant in
UTC::

    clean checkout exactly on tag v0.1.0   -> 0.1.0
    ahead of, or dirtier than, that tag    -> 0.1.1.dev20260812065103

Ordered by construction, because time is.

**What this deliberately gives up.** The dev number used to be a hash of the
source, so rebuilding unchanged code produced the same wheel name and
``weaver install`` could skip a five-minute republish. Content-addressing and
ordering cannot both hold without external state, and ordering is the one worth
keeping: an install is an explicit act, run when you have something to publish,
so paying the republish every time costs little.

One consequence to know about, because it caught a test. **You cannot ask "is
the published wheel from this checkout?" by recomputing this and comparing** —
every call returns a new number. Ask the Environment what it has published
instead, which is the better question anyway: see
``tests/fabric/test_published_weaver.py``.
"""

from __future__ import annotations

import re
import subprocess
from datetime import datetime, timezone


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], capture_output=True, text=True).stdout.strip()


def _stamp() -> str:
    """The build instant, UTC, to the second: ``20260812065103``.

    Seconds are enough. Two builds within one second would have to be two
    processes racing, and they would be building the same source anyway.
    """

    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


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
        # No reachable tag (shallow clone, fresh repo): still valid and ordered.
        return f"0.0.0.dev{_stamp()}"
    if int(match.group("distance")) == 0 and not match.group("dirty"):
        return match.group("base")
    return f"{_bump_patch(match.group('base'))}.dev{_stamp()}"
