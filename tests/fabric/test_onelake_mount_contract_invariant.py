"""What a OneLake mount does, asked of Fabric rather than of Weaver.

This is a ``remote`` Fabric test, not a ``hosted`` one, and the distinction is
the point. Nothing here imports the installed package. It asks the platform
what a mount is, which needs a session and nothing else. So it runs in the fast
loop, with no Environment publication in front of it.

That matters because the mount is what broke. A Folder's authored code writes
ordinary files, and ``folder_path()`` was handing it an ``abfss://`` URL that
``pathlib`` cannot parse, so on Fabric the files went into a local directory
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

from support.weaver_test import weaver_test

MOUNT_CONTRACT = r"""
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

# And it is the same bytes at the abfss address, a view, not a copy.
out["seen_via_abfss"] = [
    f.name for f in notebookutils.fs.ls(ROOT + "/Files/weaver_mount_probe/nested")
]
out["scopes"] = [str(m.scope) for m in notebookutils.fs.mounts()
                 if m.mountPoint == "/weaver_contract"]
emit(out)
"""


@weaver_test(remote=True)
def test_a_mount_makes_onelake_addressable_by_ordinary_python(
    livy_session, fabric_workspace_item, fabric_target_lakehouse
):
    """The contract a Folder load depends on, asked of the platform directly.

    Weaver mounts a root it resolved by name, so this works detached. It is
    not ``/lakehouse/default``, which only ever names whatever a notebook
    attached and could never serve an orchestrator loading somewhere else.
    """

    workspace = fabric_workspace_item
    item = fabric_target_lakehouse
    root = f"abfss://{workspace.id}@onelake.dfs.fabric.microsoft.com/{item.id}"

    seen = livy_session.run(f"ROOT = {root!r}\n{MOUNT_CONTRACT}").payload

    # A POSIX path, so pathlib and open() work, which is what a Folder needs.
    assert seen["local_path"].startswith("/synfs/")
    assert seen["read_back"] == "written with pathlib"
    # The same bytes at the abfss address: a view of OneLake, not a copy of it.
    assert seen["seen_via_abfss"] == ["hello.txt"]
    # Scoped to the job, which is why the path is derived on use and never
    # stored: the session id is in it, and the next session's differs.
    assert seen["scopes"] == ["job"]


#: The second contract, and the one that cost a defect. A mount is a view of
#: remote storage, and a view can be stale: its listing can still hold an entry
#: deleted through OneLake. So Weaver lists, resets and publishes a Folder
#: through ``notebookutils.fs`` and leaves the mount to authored code, which
#: writes staged files through it.
#:
#: Only reproducible when one session outlives a change made behind it, which is
#: the Fabric suite's shape.
MOUNT_COHERENCE = r"""
import notebookutils
from pathlib import Path

out = {}
notebookutils.fs.mount(ROOT, POINT, {"fileCacheTimeout": 0})
local = Path(notebookutils.fs.getMountPath(POINT))

staging = local / "Files" / PROBE / "CustomerCsv_Staging"
staging.mkdir(parents=True, exist_ok=True)
(staging / "customers.csv").write_text("a,b\n1,2\n", encoding="utf-8")
out["before"] = sorted(p.name for p in staging.iterdir())
emit(out)
"""

MOUNT_AFTER_WIPE = r"""
import notebookutils
from pathlib import Path

out = {}
# The same mount, in the same session, not remounted. Fabric refuses a second
# mount of one point, so this is the state a real load meets.
local = Path(notebookutils.fs.getMountPath(POINT))
staging = local / "Files" / PROBE / "CustomerCsv_Staging"
onelake = ROOT + "/Files/" + PROBE + "/CustomerCsv_Staging"

# What each view says about a directory whose storage is gone. Recorded rather
# than asserted where it is the mount's answer: the mount may lag.
out["mount_exists_after_wipe"] = staging.exists()
out["mount_listed_after_wipe"] = (
    sorted(p.name for p in staging.iterdir()) if staging.exists() else None
)
out["store_exists_after_wipe"] = notebookutils.fs.exists(onelake)

# The reset a folder load performs, inlined because the Environment carries the
# published wheel, which may predate the change under test: remove and make the
# directory through OneLake, make it through the mount for authored code, which
# then writes through the mount.
try:
    if notebookutils.fs.exists(onelake):
        notebookutils.fs.rm(onelake, True)
    notebookutils.fs.mkdirs(onelake)
    staging.mkdir(parents=True, exist_ok=True)
    (staging / "fresh.csv").write_text("c,d\n3,4\n", encoding="utf-8")
    out["reset"] = "ok"
    out["store_after_reset"] = sorted(
        info.name for info in notebookutils.fs.ls(onelake)
    )
except OSError as exc:
    out["reset"] = f"{type(exc).__name__}: {exc}"
emit(out)
"""


@weaver_test(remote=True)
def test_a_folder_reset_through_onelake_survives_a_dfs_wipe_behind_the_mount(
    livy_session, fabric_workspace, fabric_client, fabric_target_lakehouse
):
    """The Folder reset, proved where a stale mount is possible.

    One Livy session spans the whole thing: the defect needs a mount that
    outlives a change made outside it, and a test that remounted between the
    two halves would prove nothing. The wipe goes over DFS from here, which is
    how ``weaver wipe`` reaches a Lakehouse from a desktop. The session, holding
    the same mount, must see the removal through OneLake, reset the directory,
    and have authored writes through the mount land in it.
    """

    from weaver.fabric import FabricResolver, OneLakeDfsClient
    from weaver.targets import ItemRef

    item = fabric_target_lakehouse
    resolver = FabricResolver(fabric_workspace, client=fabric_client)
    root = resolver.spark_root(ItemRef(item.name))
    probe = "weaver_mount_coherence"
    point = "/weaver_coherence"
    preamble = f"ROOT = {root!r}\nPOINT = {point!r}\nPROBE = {probe!r}\n"

    before = livy_session.run(preamble + MOUNT_COHERENCE).payload
    assert before["before"] == ["customers.csv"]

    # Outside the mount, and outside the session: the desktop's own transport.
    dfs = OneLakeDfsClient()
    staged = resolver.files_root(ItemRef(item.name)) / probe / "CustomerCsv_Staging"
    dfs.delete(staged, recursive=True)

    after = livy_session.run(preamble + MOUNT_AFTER_WIPE).payload

    # OneLake reports the delete at once, which is why Weaver lists through it.
    assert after["store_exists_after_wipe"] is False
    assert after["reset"] == "ok", after["reset"]
    # A write through the mount lands in the directory OneLake made, and nothing
    # the storage had already lost survives into it.
    assert after["store_after_reset"] == ["fresh.csv"]
    # The mount's own view, asserted narrowly because it is Fabric's behaviour:
    # it may still list the deleted file for a moment.
    assert after["mount_listed_after_wipe"] in (None, [], ["customers.csv"])
