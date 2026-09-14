"""Public construction of a reusable Session.

``weaver.session(...)`` is the reusable form of every operation: names in, one
Session out, and each operation given it rather than opening its own.

.. code-block:: python

    import weaver

    with weaver.session(
        workspace="Sales Analytics",
        catalogue="Warehouse/Weaver",
        environment="weaver",
    ) as session:
        weaver.build(".", items="Lakehouse/Sales=Lakehouse/SalesDev", session=session)
        weaver.load("Lakehouse/Sales", session=session)
        weaver.test("Lakehouse/Sales", session=session)

The Session reuses credentials, resolved items, clients, the Livy session, and
one TDS connection per Warehouse.

Opening a Session resolves no items, starts no Livy session, opens no connection,
and publishes nothing.

Construction selects a NotebookSession inside the target workspace and a
ConsoleSession elsewhere.
"""

from __future__ import annotations

from typing import Any

from ..errors import CommandError


def session(
    *,
    workspace: Any = None,
    catalogue: str | None = None,
    environment: str | None = None,
    workspace_config: Any = None,
    credential: Any = None,
):
    """One reusable Session for a workspace named the way a caller names it.

    ``workspace`` is a Fabric workspace name, or an already-resolved
    :class:`~weaver.workspaces.Workspace` when a caller has one.
    ``workspace_config`` reads the same file the CLI's ``--workspace-config``
    does, and explicit arguments win over it.

    ``credential`` accepts an object with the ``azure.core`` ``TokenCredential``
    shape. Without one, the library default is used; the core does not select a
    credential chain. Credentials are validated here and acquired on first use.

    Construct a concrete Session directly to set implementation-specific options.
    """

    from ..config import resolve_workspace
    from ..workspaces import Workspace
    from .host import session_for

    if isinstance(workspace, Workspace):
        if workspace_config is not None:
            raise CommandError(
                "workspace_config cannot be combined with a resolved Workspace."
            )
        resolved = workspace
    else:
        resolved = resolve_workspace(
            workspace=workspace,
            catalogue=catalogue,
            environment=environment,
            workspace_config=workspace_config,
        )

    return session_for(resolved, credential=credential)


__all__ = ["session"]
