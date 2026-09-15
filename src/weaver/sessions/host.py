"""Select a Session for the current process and workspace.

Operations take a Session and never open one twice:

.. code-block:: python

    def build(..., session=None):
        with use_or_create_session(session, workspace=workspace) as session:
            ...

A passed Session is borrowed. A Session created for one operation closes with
that operation.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, Mapping

from ..errors import CommandError
from ..workspaces import Workspace
from .base import Session


def inside_fabric_session(workspace: Workspace) -> bool:
    """Whether this process is running inside the Fabric workspace it addresses.

    A notebook uses its in-Fabric Session only for the attached workspace. It
    uses a ConsoleSession for another workspace.
    """

    try:
        from notebookutils import runtime
    except ImportError:
        return False
    context = runtime.context
    if callable(context):
        context = context()
    if not isinstance(context, Mapping):
        return False
    return context.get("currentWorkspaceName") == workspace.workspace


def current_workspace_name() -> str | None:
    """The workspace reported by the Fabric runtime, or None outside Fabric."""

    try:
        from notebookutils import runtime
    except ImportError:
        return None
    context = runtime.context
    if callable(context):
        context = context()
    if not isinstance(context, Mapping):
        return None
    name = context.get("currentWorkspaceName")
    return str(name) if name else None


def active_spark():
    try:
        from importlib import import_module

        SparkSession = import_module("pyspark.sql").SparkSession
    except ImportError as exc:
        raise CommandError(
            "An active Spark session is required for this operation."
        ) from exc
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise CommandError("An active Spark session is required for this operation.")
    return spark


def session_for(workspace: Workspace | None, **kwargs) -> Session:
    """Select the Session implementation for ``workspace``.

    Inside the Fabric session being addressed, that is a
    :class:`~weaver.sessions.notebook.NotebookSession`; from a desktop reaching
    into Fabric, a :class:`~weaver.sessions.console.ConsoleSession`.
    """

    if workspace is not None and inside_fabric_session(workspace):
        from .notebook import NotebookSession

        # Fabric supplies notebook authentication; desktop credentials do not
        # apply inside the notebook session.
        kwargs.pop("credential", None)
        return NotebookSession(workspace=workspace, **kwargs)
    from .console import ConsoleSession

    return ConsoleSession(workspace=workspace, **kwargs)


@contextmanager
def use_or_create_session(
    session: Session | None, *, workspace: Workspace | None = None
) -> Iterator[Session]:
    """The caller's Session, or one owned by this operation and closed with it."""

    if session is not None:
        if session.closed:
            raise CommandError("The Session is closed.")
        yield session
        return
    created = session_for(workspace)
    try:
        yield created
    finally:
        created.close()


__all__ = [
    "active_spark",
    "inside_fabric_session",
    "session_for",
    "use_or_create_session",
]
