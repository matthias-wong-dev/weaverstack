"""Clearing a Lakehouse: both OneLake areas, driven from this checkout.

`wipe_lakehouse` takes its store as an argument and removes directories — a Delta
table is a directory, there is no catalogue to enumerate, and shortcuts go over
REST. It never needed the installed package, only a real OneLake.

Only the seed needs a session, because a Delta table is the one thing a desktop
cannot make. That body imports nothing.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.fabric, pytest.mark.remote]


def test_a_wipe_clears_both_onelake_areas(
    livy_session, fabric_workspace, fabric_client, fabric_target_lakehouse
):
    """What only OneLake can answer about a wipe: it clears Tables *and* Files.

    `wipe_lakehouse` takes its store as an argument and removes directories, so
    it runs from here against the real workspace. Only the seed needs a session —
    a Delta table has to be made by Spark — and that body imports nothing, so
    this no longer waits on a wheel.
    """

    from weaver import ItemRef, wipe_lakehouse
    from weaver.fabric import FabricResolver, OneLakeDfsClient

    resolver = FabricResolver(fabric_workspace, client=fabric_client)
    store = OneLakeDfsClient()
    target = ItemRef(fabric_target_lakehouse.name)
    destination = resolver.spark_destination(target)

    # Raw Spark: a Delta table is the one thing a desktop cannot make.
    livy_session.run(
        f"spark.sql('CREATE SCHEMA IF NOT EXISTS {destination.qualified_schema('Sales')}')\n"
        f"spark.sql('CREATE TABLE IF NOT EXISTS {destination.qualify('Sales', 'Customer')} "
        "(Id string) USING delta')\n"
        "emit(True)\n",
        label="seed",
    )
    store.make_directory(resolver.files_root(target) / "Sales" / "Customer")

    seeded = {
        "Files": [e.name for e in store.list(resolver.files_root(target))],
        "Tables": [e.name for e in store.list(resolver.tables_root(target))],
    }
    # The seed has to have landed, or an empty wipe would pass for a working one.
    assert seeded["Files"], seeded
    assert seeded["Tables"], seeded

    reports = wipe_lakehouse(target, fabric_workspace, store=store)

    assert sorted(item.target.split(":", 1)[0] for item in reports) == [
        "delta",
        "folder",
    ]
    assert [e.name for e in store.list(resolver.files_root(target))] == []
    assert [e.name for e in store.list(resolver.tables_root(target))] == []
