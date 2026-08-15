"""Which workspace an operation means, resolved once for all of them.

Every operation answers the same question before it does anything: an explicit
argument, then an already-resolved Workspace, then workspace configuration,
then — inside a Fabric notebook only — the session's own context. Shared
because four operations answering it four ways is four places for it to drift.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Mapping

from ..errors import CommandError
from ..sessions.host import active_spark as _active_spark
from ..sessions.host import inside_fabric_session as _inside_fabric_session
from ..workspaces import FabricWorkspace, Workspace


def _operation_workspace(*, workspace, workspace_config, session=None) -> Workspace:
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
        if workspace_config is not None:
            raise CommandError(
                "workspace_config cannot be combined with an already resolved Workspace"
            )
        return workspace
    if workspace is None and workspace_config is None:
        inherited = getattr(session, "workspace", None)
        if inherited is not None:
            return inherited
        return _current_fabric_workspace()
    from ..config import resolve_workspace

    return resolve_workspace(workspace=workspace, workspace_config=workspace_config)




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




def _current_fabric_workspace() -> FabricWorkspace:
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
    return FabricWorkspace(workspace=str(name))




def _with_inferred_control_lakehouse(workspace: Workspace) -> Workspace:
    if workspace.catalogue or not isinstance(workspace, FabricWorkspace):
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




