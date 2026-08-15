"""The Session a caller opens by name.

``weaver.session(...)`` is the reusable form of every operation: names in, one
Session out, and each operation given it rather than opening its own.

.. code-block:: python

    import weaver

    with weaver.session(
        workspace="Sales Analytics",
        catalogue="Weaver",
        environment="weaver",
    ) as session:
        weaver.build(".", bind="Lakehouse/SalesDev=Sales", session=session)
        weaver.load("Lakehouse/SalesDev", session=session)
        weaver.test("Lakehouse/SalesDev", session=session)

What the Session holds is what is expensive: the credential, the resolved items
and their cache, the REST client, the OneLake transport, the Livy session and one
TDS connection per Warehouse. Four operations that share one pay for them once.

Everything is lazy. Opening a Session resolves no items, starts no Livy session,
opens no connection and publishes nothing — a caller that opens one and does
nothing has cost a credential object.

Which host it is depends on where this runs, and the caller does not choose: a
notebook inside the workspace gets the session it is already in, and a desktop
gets one that reaches across.
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
    :class:`~weaver.workspaces.FabricWorkspace` when a caller has one.
    ``workspace_config`` reads the same file the CLI's ``--workspace-config``
    does, and explicit arguments win over it.

    ``credential`` accepts anything offering a callable ``get_token``, which is
    the ``azure.core`` ``TokenCredential`` shape. Without one the library
    default is used and no chain is pinned: which credential to authenticate
    with is a caller's policy, never the core's. It is validated here and
    acquired later, so a wrong object is refused at the call that supplied it
    rather than during whichever operation first reaches Fabric.

    Other host-specific options are deliberately absent. Whether a Livy session
    needs the published wheel, and where a timing tree is drawn, are a console's
    business; a caller who needs to set them constructs that host directly.
    """

    from ..config import resolve_workspace
    from ..workspaces import Workspace
    from .host import session_for

    if isinstance(workspace, Workspace):
        if workspace_config is not None:
            raise CommandError(
                "a resolved Workspace arrives complete, so workspace_config "
                "would have nothing to add to it"
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
