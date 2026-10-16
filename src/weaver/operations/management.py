"""Native notebook entry points for Fabric management operations."""

from __future__ import annotations

from contextlib import contextmanager

from .workspace import operation_workspace


@contextmanager
def _management_client(
    operation,
    *,
    workspace,
    workspace_config,
    environment=None,
    session=None,
    client=None,
):
    configured = operation_workspace(
        operation,
        workspace=workspace,
        workspace_config=workspace_config,
        environment=environment,
        session=session,
        needs_catalogue=False,
    )
    if client is not None:
        yield configured, client
        return
    from ..sessions.host import use_or_create_session

    with use_or_create_session(session, workspace=configured) as opened:
        yield configured, opened.resolver(configured).client


def publish_environment(
    environment=None,
    *,
    workspace=None,
    workspace_config=None,
    path=None,
    dev=False,
    session=None,
    client=None,
):
    """Publish a named Environment or its local definition."""
    from ..errors import CommandError
    from ..fabric.environment import publish_environment as publish
    from ..fabric.environment import resolve_environment_owner
    from ..workspaces import EnvironmentRef

    if path is not None and environment is not None:
        raise CommandError("path and environment cannot be used together")
    selected = EnvironmentRef.parse(environment) if environment is not None else None
    if (
        selected is not None
        and selected.workspace is not None
        and workspace is None
        and workspace_config is None
    ):
        workspace = selected.workspace
    with _management_client(
        "publish_environment",
        workspace=workspace,
        workspace_config=workspace_config,
        session=session,
        client=client,
    ) as (configured, rest):
        if path is None and selected is None:
            selected = configured.environment
        if selected is not None:
            resolve_environment_owner(configured.workspace, selected)
        return publish(
            configured.workspace,
            selected,
            path=path,
            dev=dev,
            session=session,
            client=rest,
        )


def push_notebook(
    source,
    *,
    workspace=None,
    workspace_config=None,
    name=None,
    description=None,
    session=None,
    client=None,
):
    """Create or update a Notebook definition in the selected workspace."""
    from ..fabric.notebooks import push_notebook as push

    with _management_client(
        "push_notebook",
        workspace=workspace,
        workspace_config=workspace_config,
        session=session,
        client=client,
    ) as (configured, rest):
        return push(
            source,
            workspace=configured.workspace,
            name=name,
            description=description,
            client=rest,
        )


def run_notebook(
    name,
    *,
    workspace=None,
    workspace_config=None,
    lakehouse=None,
    environment=None,
    wait=True,
    timeout=7200.0,
    poll_interval=10.0,
    session=None,
    client=None,
):
    """Run a Notebook with the selected workspace's attachments and wait policy."""
    from ..errors import CommandError
    from ..fabric.notebooks import run_notebook as run

    with _management_client(
        "run_notebook",
        workspace=workspace,
        workspace_config=workspace_config,
        environment=environment,
        session=session,
        client=client,
    ) as (configured, rest):
        attached = lakehouse
        if attached is None and len(configured.configured_lakehouses) == 1:
            attached = configured.configured_lakehouses[0]
        if not attached:
            raise CommandError(
                "A Lakehouse is required to run this notebook. Pass lakehouse or configure exactly one Lakehouse."
            )
        if not configured.environment:
            raise CommandError(
                "A Fabric Environment is required to run this notebook. Pass environment or configure one."
            )
        return run(
            name,
            workspace=configured.workspace,
            lakehouse=attached,
            environment=configured.environment,
            wait=wait,
            timeout=timeout,
            poll_interval=poll_interval,
            client=rest,
        )
