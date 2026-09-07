"""One bulk request, a mixed batch of shortcuts, and what the destination holds.

Fabric takes a whole batch at ``shortcuts/bulkCreate`` and settles each member
separately, so a batch can come back part succeeded. This is the narrow claim
that the batch Weaver sends is the batch Fabric makes: table shortcuts and a
folder shortcut in one bulk create, every member reported, and each one readable
in the destination afterwards.

Where a member's outcome is read from is the part worth proving against a tenant.
Bulk creation is long-running, so the accepting response carries no outcomes and
the members arrive at the operation's result address.
"""

from __future__ import annotations

import pytest
from support import external_estate
from support.weaver_test import weaver_test

from weaver.targets import ItemRef

#: Where these tests put their shortcuts. A schema of their own, so anything an
#: interrupted run leaves behind is recognisable rather than looking declared,
#: and one per test, because removing a shortcut leaves its directory behind and
#: a later shortcut of that name would find the path occupied.
PROBE_ROOT = "BulkShortcutProbe"


@pytest.fixture
def batch(
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

    class Batch:
        schema = f"{PROBE_ROOT}{abs(hash(request.node.name)) % 10000:04d}"

        def __init__(self) -> None:
            self.store = store

        def create(self, requests) -> tuple[dict, ...]:
            """One bulk request, through the product path the installer uses."""

            made.extend((each["path"], each["name"]) for each in requests)
            return resolver.create_onelake_shortcuts(target, requests)

        def member(self, *, path: str, name: str, source_path: str) -> dict:
            return {
                "path": path,
                "name": name,
                "source": external_source.item,
                "source_kind": None,
                "source_path": source_path,
            }

        def local(self, relative: str):
            return resolver.lakehouse(target) / relative

        def held(self) -> set[str]:
            return {
                f"{shortcut.path}/{shortcut.name}"
                for shortcut in resolver.onelake_shortcuts(target)
            }

    try:
        yield Batch()
    finally:
        for path, name in made:
            try:
                resolver.remove_onelake_shortcut(target, path=path, name=name)
            except Exception as exc:  # cleanup must not mask a failure
                print(f"warning: could not remove shortcut {path}/{name}: {exc}")


@weaver_test(remote=True, resources={"rest", "onelake"})
def test_one_bulk_request_creates_tables_and_a_folder(batch):
    """Two table shortcuts and a folder shortcut, in one bulk create.

    A folder shortcut sits directly under ``Files`` and a table shortcut under
    ``Tables/<schema>``, so the batch carries both shapes and Fabric is asked to
    settle them together.
    """

    tables = f"Tables/{batch.schema}"
    requests = [
        batch.member(
            path=tables,
            name="Customer",
            source_path=external_estate.table_path("Customer"),
        ),
        batch.member(
            path=tables,
            name="Product",
            source_path=external_estate.table_path("Product"),
        ),
        batch.member(
            path="Files",
            name=batch.schema,
            source_path=f"Files/{external_estate.SCHEMA}",
        ),
    ]

    created = batch.create(requests)

    # One detail per member, in request order, so a caller can match an outcome
    # back to what it asked for.
    assert [detail["path"] for detail in created] == [
        f"{tables}/Customer",
        f"{tables}/Product",
        f"Files/{batch.schema}",
    ]
    assert {
        f"{tables}/Customer",
        f"{tables}/Product",
        f"Files/{batch.schema}",
    } <= batch.held()
    # Readable through the destination, which is what a shortcut is for. A table
    # shortcut answers at its Delta log, a folder shortcut at the source's bytes.
    assert batch.store.exists(batch.local(f"{tables}/Customer/_delta_log"))
    assert batch.store.exists(batch.local(f"{tables}/Product/_delta_log"))
    read = batch.store.read(batch.local(f"Files/{batch.schema}/{external_estate.FILE}"))
    assert read == external_estate.FILE_BYTES


@weaver_test(remote=True, resources={"rest", "onelake"})
def test_a_bulk_batch_overwrites_the_names_it_already_made(batch):
    """A build has to run twice, so the second batch stands on the first.

    ``CreateOrOverwrite`` is what makes that true for a whole batch: under the
    default ``Abort`` every member of a rerun would fail on its own name.
    """

    tables = f"Tables/{batch.schema}"
    requests = [
        batch.member(
            path=tables,
            name="Customer",
            source_path=external_estate.table_path("Customer"),
        )
    ]
    batch.create(requests)

    again = batch.create(requests)

    assert [detail["path"] for detail in again] == [f"{tables}/Customer"]
    assert f"{tables}/Customer" in batch.held()
    assert batch.store.exists(batch.local(f"{tables}/Customer/_delta_log"))
