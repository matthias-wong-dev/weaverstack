"""Which workspace an operation means, resolved once for all of them.

Every operation answers the same question before it does anything, and answers
it from names: an explicit argument, then workspace configuration, then the
Session's own context, then the workspace a Fabric notebook is running in.
Shared, because four operations answering it four ways is four places for it to
drift.
"""

from __future__ import annotations

from typing import Mapping

from ..errors import CommandError
from ..workspaces import Workspace


def _operation_workspace(
    *, workspace, workspace_config, catalogue=None, environment=None, session=None
) -> Workspace:
    """Which workspace this operation means.

    .. code-block:: text

        an explicit workspace name
          → a workspace configuration file
            → the Session's own workspace
              → the workspace the notebook runs in
                → a configuration error naming what is missing

    The Session's default context lets a command inside ``weaver session`` omit
    what the session already holds:

    .. code-block:: text

        weaver session --workspace "Weaver Example"
        weaver> build .
        weaver> load Lakehouse/Sales

    It is a default, so an explicit argument still outranks it.
    """

    if isinstance(workspace, Workspace):
        raise CommandError(
            "an operation takes a workspace name; open a Session for an "
            "already-resolved Workspace and pass session= instead:\n"
            "    with weaver.session(workspace=workspace) as session:\n"
            "        weaver.build('.', session=session)"
        )
    # The base context first, then what this call named on top of it. Split in
    # two because the same overrides apply however the base was found, a
    # Session's workspace, a configuration file, or a notebook's own context.
    if workspace is not None or workspace_config is not None:
        from ..config import resolve_workspace

        base = resolve_workspace(
            workspace=workspace,
            catalogue=catalogue,
            environment=environment,
            workspace_config=workspace_config,
        )
    else:
        inherited = getattr(session, "workspace", None)
        base = inherited if inherited is not None else _current_fabric_workspace()

    changes = {}
    if catalogue is not None and base.catalogue != catalogue:
        changes["catalogue"] = catalogue
    if environment is not None and base.environment != environment:
        changes["environment"] = environment
    if not changes:
        return base

    from dataclasses import replace

    return replace(base, **changes)


def operation_workspace(
    operation: str,
    *,
    workspace=None,
    catalogue=None,
    environment=None,
    workspace_config=None,
    session=None,
    needs_catalogue: bool = True,
) -> Workspace:
    """The workspace one operation means, resolved once for all of them.

    What differs between operations is only whether the catalogue is
    required: a wipe can empty a target without one, everything else reads or
    writes the catalogue.
    """

    resolved = _operation_workspace(
        workspace=workspace,
        workspace_config=workspace_config,
        catalogue=catalogue,
        environment=environment,
        session=session,
    )
    if needs_catalogue and not resolved.catalogue:
        raise CommandError(
            f"{operation} needs a Weaver catalogue: pass "
            "catalogue='Warehouse/Weaver', or give one in workspace "
            "configuration"
        )
    return resolved


def current_workspace() -> Workspace:
    """The workspace this code is running in, discovered rather than named.

    Inside a Fabric notebook the session already holds the answer. This is the
    discovery every operation does for ``workspace=None``, reachable on its own
    for a caller that needs a resolver rather than an operation.

    Outside a session there is nothing to discover, and this says so rather than
    guessing.
    """

    return _operation_workspace(workspace=None, workspace_config=None)


def _current_fabric_workspace() -> Workspace:
    try:
        from notebookutils import runtime
    except ImportError as exc:
        raise CommandError(
            "give workspace or workspace_config outside a Fabric notebook"
        ) from exc
    context = runtime.context
    if callable(context):
        context = context()
    if not isinstance(context, Mapping):
        raise CommandError("Fabric runtime context is not a mapping")
    name = context.get("currentWorkspaceName")
    if not name:
        raise CommandError("Fabric runtime context carries no current workspace")
    return Workspace(workspace=str(name))
