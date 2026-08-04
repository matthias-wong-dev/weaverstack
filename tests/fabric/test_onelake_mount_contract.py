"""What a OneLake mount does, asked of Fabric rather than of Weaver.

This is a ``remote`` Fabric test, not a ``hosted`` one, and the distinction is
the point. Nothing here imports the installed package — it asks the *platform*
what a mount is, which needs a session and nothing else. So it runs in the fast
loop, with no ``weaver install`` in front of it.

That matters because the mount is what broke. A Folder's authored code writes
ordinary files, and ``folder_path()`` was handing it an ``abfss://`` URL that
``pathlib`` cannot parse — so on Fabric the files went into a local directory
literally named ``abfss:/…``, the load reported success, and the table that read
them failed. The behaviour that settles it is entirely Fabric's:

.. code-block:: text

    mount        turns a remote root into a POSIX path
    the path     /synfs/notebook/<session id>/…, scoped to the job
    a write      lands in OneLake, immediately, with nothing copied
    two mounts   coexist, so an estate can span Lakehouses

None of that is a question about Weaver, and none of it needed the wheel. It was
found with a throwaway probe and should have been left behind as this.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.fabric, pytest.mark.remote]

MOUNT_CONTRACT = r'''
import notebookutils, os
from pathlib import Path

out = {}
notebookutils.fs.mount(ROOT, "/weaver_contract")
local = notebookutils.fs.getMountPath("/weaver_contract")
out["local_path"] = local

# Ordinary Python, which is the whole question: a Folder's authored code writes
# with open() and cannot address a URL.
target = Path(local) / "Files" / "weaver_mount_probe" / "nested"
target.mkdir(parents=True, exist_ok=True)
(target / "hello.txt").write_text("written with pathlib", encoding="utf-8")
out["read_back"] = (target / "hello.txt").read_text(encoding="utf-8")

# And it is the same bytes at the abfss address — a view, not a copy.
out["seen_via_abfss"] = [
    f.name for f in notebookutils.fs.ls(ROOT + "/Files/weaver_mount_probe/nested")
]
out["scopes"] = [str(m.scope) for m in notebookutils.fs.mounts()
                 if m.mountPoint == "/weaver_contract"]
emit(out)
'''


def test_a_mount_makes_onelake_addressable_by_ordinary_python(
    livy_session, fabric_workspace_item, fabric_target_lakehouse
):
    """The contract a Folder load depends on, asked of the platform directly.

    Weaver mounts a root it *resolved by name*, so this works detached — it is
    not ``/lakehouse/default``, which only ever names whatever a notebook
    attached and could never serve an orchestrator loading somewhere else.
    """

    workspace = fabric_workspace_item
    item = fabric_target_lakehouse
    root = (
        f"abfss://{workspace.id}@onelake.dfs.fabric.microsoft.com/{item.id}"
    )

    seen = livy_session.run(
        f"ROOT = {root!r}\n{MOUNT_CONTRACT}", label="mount contract"
    ).payload

    # A POSIX path, so pathlib and open() work — which is what a Folder needs.
    assert seen["local_path"].startswith("/synfs/")
    assert seen["read_back"] == "written with pathlib"
    # The same bytes at the abfss address: a view of OneLake, not a copy of it.
    assert seen["seen_via_abfss"] == ["hello.txt"]
    # Scoped to the job, which is why the path is derived on use and never
    # stored: the session id is in it, and the next session's differs.
    assert seen["scopes"] == ["job"]
