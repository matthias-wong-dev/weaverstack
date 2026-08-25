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
#: remote storage, and a view can be stale: Weaver reaches one Files area two
#: ways, ``abfss://`` over DFS for storage work, the mount for authored Python
#:, so anything that changes OneLake outside the mount has to be visible
#: through it. With caching on it is not, and the symptom is a listing that
#: still holds entries the storage no longer has: ``shutil.rmtree`` deletes what
#: it was told about and then fails to remove the directory, as ``ENOTEMPTY``.
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
import notebookutils, shutil, time
from pathlib import Path

out = {}
# The same mount, in the same session, not remounted. Fabric refuses a second
# mount of one point, so this is the state a real load meets.
local = Path(notebookutils.fs.getMountPath(POINT))
staging = local / "Files" / PROBE / "CustomerCsv_Staging"

# What the mount says about a directory whose storage is gone. Recorded rather
# than asserted: it is Fabric's answer, and the two halves of it are odd enough
# to be worth reading in a failure. `exists()` stays true while the listing goes
# empty, and the listing settles a moment after the delete rather than with it.
out["exists_after_wipe"] = staging.exists()
out["listed_after_wipe"] = sorted(p.name for p in staging.iterdir()) if staging.exists() else None

# The reset a folder load performs, verbatim from `reset_staging`, inlined
# because the Environment carries the published wheel, which may predate the
# change under test. This is the operation that failed with ENOTEMPTY.
def with_retry(action, attempts=RESET_ATTEMPTS, pause=RESET_PAUSE):
    for remaining in range(attempts - 1, -1, -1):
        try:
            return action()
        except OSError:
            if remaining == 0:
                raise
            time.sleep(pause)

try:
    with_retry(lambda: shutil.rmtree(staging) if staging.exists() else None)
    staging.mkdir(parents=True, exist_ok=False)
    (staging / "fresh.csv").write_text("c,d\n3,4\n", encoding="utf-8")
    out["reset"] = "ok"
    out["after_reset"] = sorted(p.name for p in staging.iterdir())
except OSError as exc:
    out["reset"] = f"{type(exc).__name__}: {exc}"
emit(out)
"""


@weaver_test(remote=True)
def test_a_zero_cache_mount_sees_a_dfs_wipe_made_behind_it(
    livy_session, fabric_workspace, fabric_client, fabric_target_lakehouse
):
    """The mount-coherence repair, proved where it is the only place it fails.

    One Livy session spans the whole thing: the defect needs a
    mount that outlives a change made outside it, and a test that remounted
    between the two halves would prove nothing. The wipe goes over DFS from
    here, which is exactly how ``weaver wipe`` reaches a Lakehouse from a
    desktop, and then the session, holding the same mount, must both see the
    removal and be able to reset the directory over it.
    """

    from weaver.fabric import FabricResolver, OneLakeDfsClient
    from weaver.runtime.folder_load import RESET_ATTEMPTS, RESET_PAUSE
    from weaver.targets import ItemRef

    item = fabric_target_lakehouse
    resolver = FabricResolver(fabric_workspace, client=fabric_client)
    root = resolver.spark_root(ItemRef(item.name))
    probe = "weaver_mount_coherence"
    point = "/weaver_coherence"
    preamble = (
        f"ROOT = {root!r}\nPOINT = {point!r}\nPROBE = {probe!r}\n"
        f"RESET_ATTEMPTS = {RESET_ATTEMPTS!r}\nRESET_PAUSE = {RESET_PAUSE!r}\n"
    )

    before = livy_session.run(preamble + MOUNT_COHERENCE).payload
    assert before["before"] == ["customers.csv"]

    # Outside the mount, and outside the session: the desktop's own transport.
    # The location is resolved rather than composed, a DFS client addresses
    # OneLake by URL, and an item by id, not by the name a person types.
    dfs = OneLakeDfsClient()
    staged = resolver.files_root(ItemRef(item.name)) / probe / "CustomerCsv_Staging"
    dfs.delete(staged, recursive=True)

    after = livy_session.run(preamble + MOUNT_AFTER_WIPE).payload

    # The claim, and it is about the outcome rather than about any listing on
    # the way to it. Before the repair this was
    # `OSError: [Errno 39] Directory not empty`.
    assert after["reset"] == "ok", after["reset"]
    # And what the next load publishes from holds only what this run staged, so
    # no entry the storage had already lost survived into it.
    assert after["after_reset"] == ["fresh.csv"]

    # Two observations about the mount, asserted narrowly because they are
    # Fabric's behaviour and not Weaver's, and because getting them wrong is
    # what the retry exists for.
    #
    # A directory whose storage is gone still answers `exists()`, so a reset
    # cannot decide there is nothing to remove and skip straight to `mkdir`.
    assert after["exists_after_wipe"] is True
    # And the listing is eventually consistent, not immediately: it may still
    # name the deleted file for a moment. That is precisely why the removal is
    # retried and the `mkdir` that follows is not.
    assert after["listed_after_wipe"] in ([], ["customers.csv"])
