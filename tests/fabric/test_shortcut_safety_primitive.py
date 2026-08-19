"""What a destructive operation may do to what a shortcut points at.

The invariant these tests hold, which blocks a release:

    Binding, unbinding, pruning, dropping, rebuilding and wiping may alter the
    local shortcut, but must never mutate or delete the referenced object.

It needs Fabric, because it is a claim about OneLake rather than about Weaver's
decisions. A shortcut is a read-write window into the item it points at: writing
beneath one lands in that item, and so does deleting. Removing the shortcut root
is safe, and these tests hold Weaver to touching only that.

The source is read directly in the external workspace afterwards rather than
through the local shortcut, because a removed shortcut reports an absent source
and a present one alike.
"""

from __future__ import annotations

import pytest
from support import external_estate
from support.weaver_test import weaver_test

from weaver.targets import DeltaTarget, FolderTarget, ItemRef

#: Where these tests put their shortcuts. A schema of their own, so anything an
#: interrupted run leaves behind is recognisable rather than looking declared,
#: and one per test, because removing a shortcut leaves its directory behind and
#: a later schema shortcut of that name would find the path occupied.
PROBE_ROOT = "ShortcutProbe"


@pytest.fixture
def probe(
    rest_session, fabric_workspace, fabric_target_lakehouse, external_source, request
):
    """The destination Lakehouse, and the shortcuts one test made in it.

    Every shortcut is removed afterwards through the workspace. Deleting anything
    inside one would reach the item it points at.
    """

    resolver = rest_session.resolver(fabric_workspace)
    store = rest_session.transport_store(fabric_workspace)
    target = ItemRef(fabric_target_lakehouse.name)
    made: list[tuple[str, str]] = []

    class Probe:
        workspace = fabric_workspace
        source = external_source
        session = rest_session
        #: This test's own schema, so one test's residue cannot occupy another's
        #: path.
        schema = f"{PROBE_ROOT}{abs(hash(request.node.name)) % 10000:04d}"

        def __init__(self) -> None:
            self.resolver = resolver
            self.store = store
            self.target = target

        def create(self, *, path: str, name: str, source_path: str) -> dict:
            """One shortcut, through the product path the installer uses."""

            made.append((path, name))
            return resolver.create_onelake_shortcut(
                target,
                path=path,
                name=name,
                source=external_source.item,
                source_path=source_path,
            )

        def local(self, relative: str):
            return resolver.lakehouse(target) / relative

        def shortcuts(self) -> set[str]:
            return {
                f"{shortcut.path}/{shortcut.name}"
                for shortcut in resolver.onelake_shortcuts(target)
            }

    try:
        yield Probe()
    finally:
        for path, name in made:
            try:
                resolver.remove_onelake_shortcut(target, path=path, name=name)
            except Exception as exc:  # cleanup must not mask a failure
                print(f"warning: could not remove shortcut {path}/{name}: {exc}")


def _source_tables(probe) -> set[str]:
    return {
        entry.location.name
        for entry in probe.store.list(
            probe.source.location(f"Tables/{external_estate.SCHEMA}")
        )
    }


@weaver_test(remote=True, resources={"rest", "onelake"})
def test_a_table_shortcut_reaches_another_workspace(probe):
    """A direct shortcut may point outside the workspace the build is bound to."""

    made = probe.create(
        path=f"Tables/{probe.schema}",
        name="Customer",
        source_path=external_estate.table_path("Customer"),
    )

    assert made["status"] in (200, 201)
    assert f"Tables/{probe.schema}/Customer" in probe.shortcuts()
    assert probe.store.exists(probe.local(f"Tables/{probe.schema}/Customer/_delta_log"))


@weaver_test(remote=True, resources={"rest", "onelake"})
def test_a_schema_shortcut_presents_the_source_namespace(probe):
    """Created directly under ``Tables`` and named for the schema it presents.

    Its contents are the source item's, and they are dynamic: nothing here
    declares them and no rebuild is needed for them to change.
    """

    probe.create(
        path="Tables",
        name=probe.schema,
        source_path=f"Tables/{external_estate.SCHEMA}",
    )

    assert f"Tables/{probe.schema}" in probe.shortcuts()
    seen = {
        entry.location.name
        for entry in probe.store.list(probe.local(f"Tables/{probe.schema}"))
    }
    assert set(external_estate.TABLES) <= seen


@weaver_test(remote=True, resources={"rest", "onelake"})
def test_a_folder_shortcut_reads_the_external_sentinel(probe):
    probe.create(
        path="Files",
        name=probe.schema,
        source_path=f"Files/{external_estate.SCHEMA}",
    )

    read = probe.store.read(probe.local(f"Files/{probe.schema}/{external_estate.FILE}"))
    assert read == external_estate.FILE_BYTES


@weaver_test(remote=True, resources={"rest", "onelake"})
def test_removing_a_shortcut_leaves_the_source_alone(probe):
    """The local name goes; the data belongs to the item that produced it."""

    before = _source_tables(probe)
    probe.create(
        path=f"Tables/{probe.schema}",
        name="Customer",
        source_path=external_estate.table_path("Customer"),
    )
    probe.resolver.remove_onelake_shortcut(
        probe.target, path=f"Tables/{probe.schema}", name="Customer"
    )

    assert f"Tables/{probe.schema}/Customer" not in probe.shortcuts()
    assert _source_tables(probe) == before
    assert probe.store.exists(probe.source.table("Customer"))


@weaver_test(remote=True, resources={"rest", "onelake"})
def test_a_wipe_removes_the_shortcut_and_not_what_it_points_at(probe):
    """Why :mod:`weaver.physical_wipe` removes shortcuts through the workspace.

    A recursive delete that went through one would reach the producer's data, so
    the pointers are taken away before storage is swept.
    """

    from weaver.physical_wipe import wipe_delta_target, wipe_folder_target

    before_tables = _source_tables(probe)
    before_bytes = probe.store.read(probe.source.file())

    probe.create(
        path=f"Tables/{probe.schema}",
        name="Customer",
        source_path=external_estate.table_path("Customer"),
    )
    probe.create(
        path="Tables",
        name=f"{probe.schema}Schema",
        source_path=f"Tables/{external_estate.SCHEMA}",
    )
    probe.create(
        path="Files",
        name=probe.schema,
        source_path=f"Files/{external_estate.SCHEMA}",
    )

    wipe_delta_target(
        DeltaTarget(lakehouse=probe.target),
        probe.workspace,
        store=probe.store,
        session=probe.session,
    )
    wipe_folder_target(
        FolderTarget(lakehouse=probe.target),
        probe.workspace,
        store=probe.store,
        session=probe.session,
    )

    assert not probe.shortcuts()
    # Read in the external workspace, not through the shortcut that is now gone.
    assert _source_tables(probe) == before_tables
    assert probe.store.read(probe.source.file()) == before_bytes
