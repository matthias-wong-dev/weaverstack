"""Shared workspace resolution for public operations."""

from __future__ import annotations

from typing import Mapping

from ..errors import CommandError
from ..workspaces import Workspace


def _operation_workspace(
    *, workspace, workspace_config, catalogue=None, environment=None, session=None
) -> Workspace:
    """Resolve the workspace for an operation.

    .. code-block:: text

        an explicit workspace name
          → a workspace configuration file
            → the Session's own workspace
              → workspace-config.yml in the working directory
                → the workspace the notebook runs in
                  → a configuration error naming what is missing

    A discovered project configuration ranks below the Session's workspace.
    """

    if isinstance(workspace, Workspace):
        raise CommandError(
            "workspace must be a name, not a Workspace object. Open a Session "
            "for the resolved Workspace and pass session= instead:\n"
            "    with weaver.session(workspace=workspace) as session:\n"
            "        weaver.build('.', session=session)"
        )
    # Apply explicit catalogue and Environment values over any resolved base.
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
        base = inherited if inherited is not None else _discovered_or_current()

    changes = {}
    if catalogue is not None and base.catalogue != catalogue:
        changes["catalogue"] = catalogue
    if environment is not None:
        from ..workspaces import EnvironmentRef

        environment_ref = EnvironmentRef.parse(environment)
        if base.environment != environment_ref:
            changes["environment"] = environment_ref
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
    """Resolve an operation's workspace and required catalogue."""

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
    """Discover the current project or Fabric notebook workspace."""

    return _operation_workspace(workspace=None, workspace_config=None)


def _discovered_or_current() -> Workspace:
    """Resolve a local project before the current Fabric workspace.

    A Fabric notebook's working directory is separate from Notebook Resources,
    so project discovery cannot shadow the notebook's workspace.
    """

    from ..config import discovered_workspace_config, load_workspace

    discovered = discovered_workspace_config()
    if discovered is not None:
        return load_workspace(discovered)
    return _current_fabric_workspace()


def _current_fabric_workspace() -> Workspace:
    try:
        from notebookutils import runtime
    except ImportError as exc:
        raise CommandError(
            "give workspace or workspace_config outside a Fabric notebook, or "
            "run from a project directory holding workspace-config.yml"
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
