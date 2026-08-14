"""Sessions built around resources a test already holds.

A Session closes only what it opened, so handing it a store or a resolver is the
ordinary way for a caller that already has them to reach the build path — a
notebook does exactly this.

That is why the suite needs no test-only installer: there is one Installer, it
takes a Session, and a test gives that Session whatever it already has.

Outside ``tests/fabric`` that Session is a
:class:`~weaver.session.testing.TestSession`, which records every statement it
is asked to run and answers from what the test configured. What a statement
*does* is proven against a real workspace; what Weaver *renders* is proven by
reading it back here.
"""

from __future__ import annotations

from typing import Any

from weaver.session import TestSession
from weaver.workspaces import FabricWorkspace

#: Somewhere for a Session that will never reach a workspace to call home. Used
#: where the claim is about installer semantics and the workspace is incidental.
NOWHERE = FabricWorkspace(workspace="Demo", weaver_lakehouse="Weaver")


def given_session(
    *,
    workspace: Any = None,
    store: Any = None,
    resolver: Any = None,
    spark_rows: Any = None,
    executes_here: bool = False,
) -> TestSession:
    """A Session around resources the caller owns and will close itself.

    ``spark_rows`` configures the rows one statement answers with, as
    ``{statement: rows}``, for the reads a build makes before it decides.
    """

    session = TestSession(
        workspace=workspace if workspace is not None else NOWHERE,
        store=store,
        resolver=resolver,
        executes_here=executes_here,
    )
    for statement, rows in (spark_rows or {}).items():
        session.answer_spark_sql(statement, rows)
    return session


def given_installer(
    *,
    workspace: Any = None,
    store: Any = None,
    resolver: Any = None,
    executors: Any = None,
    spark_rows: Any = None,
):
    """An Installer over :func:`given_session`, for an installer-semantics test."""

    from weaver.build_bundle import Installer

    return Installer(
        given_session(
            workspace=workspace,
            store=store,
            resolver=resolver,
            spark_rows=spark_rows,
        ),
        executors=executors,
    )


__all__ = ["NOWHERE", "given_installer", "given_session"]
