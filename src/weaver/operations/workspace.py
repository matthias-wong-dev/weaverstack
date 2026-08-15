"""Which workspace an operation means, resolved once for all of them.

Every operation answers the same question before it does anything: an explicit
argument, then an already-resolved Workspace, then workspace configuration,
then — inside a Fabric notebook only — the session's own context. Shared
because four operations answering it four ways is four places for it to drift.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Mapping

from ..errors import CommandError
from ..sessions.host import active_spark as _active_spark
from ..sessions.host import inside_fabric_session as _inside_fabric_session
from ..workspaces import Workspace


def _operation_workspace(
    *, workspace, workspace_config, catalogue=None, environment=None, session=None
) -> Workspace:
    """Which workspace this operation means.

    .. code-block:: text

        an explicit workspace argument
          → a workspace configuration file
            → the Session's default context
              → what the notebook is attached to
                → a configuration error naming what is missing

    The Session's default context lets a command inside ``weaver session`` omit
    what the session already knows:

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
    # two because the same overrides apply however the base was found — a
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

    Every operation asks the same question and used to answer it in its own
    words. What differs between them is only whether the control Lakehouse is
    required: a wipe can empty a target without one, everything else reads or
    writes the catalogue.
    """

    resolved = _with_inferred_control_lakehouse(
        _operation_workspace(
            workspace=workspace,
            workspace_config=workspace_config,
            catalogue=catalogue,
            environment=environment,
            session=session,
        )
    )
    if needs_catalogue and not resolved.catalogue:
        raise CommandError(
            f"{operation} needs a Weaver control Lakehouse: pass catalogue=, "
            "give one in workspace configuration, or run inside a Fabric "
            "notebook with one attached as the default Lakehouse"
        )
    return resolved


def current_workspace() -> Workspace:
    """The workspace this code is running in, discovered rather than named.

    Inside a Fabric notebook the session already knows the answer. This is the
    discovery every operation does for ``workspace=None``, reachable on its own
    for a caller that needs a resolver rather than an operation.

    Outside a session there is nothing to discover, and this says so rather than
    guessing.
    """

    return _with_inferred_control_lakehouse(
        _operation_workspace(workspace=None, workspace_config=None)
    )


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


def _with_inferred_control_lakehouse(workspace: Workspace) -> Workspace:
    if workspace.catalogue:
        return workspace
    if not _inside_fabric_session(workspace):
        return workspace
    from ..lakehouse import default_lakehouse

    spark = _active_spark()
    # Typed on the way in, because what a notebook's attachment gives is a
    # bare name and the field is a typed one.
    from ..workspaces import CATALOGUE_KIND

    return replace(
        workspace, catalogue=f"{CATALOGUE_KIND}/{default_lakehouse(spark).name}"
    )
