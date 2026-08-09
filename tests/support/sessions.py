"""Sessions built around resources a test already holds.

A Session closes only what it opened, so handing it a Spark session, a store or
a resolver is the ordinary way for a caller that already has them to reach the
build path — a notebook does exactly this, and so does a module-scoped fixture
that owns one JVM for a whole file.

That is why the suite needs no test-only installer: there is one Installer, it
takes a Session, and a test gives that Session whatever it already has.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from weaver.session import ConsoleSession
from weaver.workspaces import LocalWorkspace

#: Somewhere for a Session that will never touch a filesystem to call home. Used
#: where the claim is about installer semantics and the workspace is incidental.
NOWHERE = LocalWorkspace(workspace=Path("/tmp/weaver-test-workspace"))


def given_session(
    *,
    workspace: Any = None,
    spark: Any = None,
    store: Any = None,
    resolver: Any = None,
) -> ConsoleSession:
    """A Session around resources the caller owns and will close itself."""

    return ConsoleSession(
        workspace=workspace if workspace is not None else NOWHERE,
        spark=spark,
        store=store,
        resolver=resolver,
    )


def given_installer(
    *,
    workspace: Any = None,
    spark: Any = None,
    store: Any = None,
    resolver: Any = None,
    executors: Any = None,
):
    """An Installer over :func:`given_session`, for an installer-semantics test."""

    from weaver.build_bundle import Installer

    return Installer(
        given_session(
            workspace=workspace, spark=spark, store=store, resolver=resolver
        ),
        executors=executors,
    )


__all__ = ["NOWHERE", "given_installer", "given_session"]
