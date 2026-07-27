"""One session, two destination Lakehouses, one schema name.

This is the local half of the multi-target claim, and it is the case the old
addressing got silently wrong. Both Lakehouses declare a schema called ``DWG``.
Under two-part names the first build to run ``CREATE SCHEMA IF NOT EXISTS DWG``
won: the second Lakehouse's tables were then written into the first Lakehouse's
storage, no statement failed, and every assertion that read ``DWG.Customer``
back through the same session catalogue passed.

Fabric keeps the two apart with its own namespace. Local Spark has one namespace
level and cannot be given another, so the proxy folds the Lakehouse into the
database name — different syntax, same property, and that property is what is
tested here.

The catalogue is exercised too, because it is the third destination in the room:
the session is attached to nothing in particular, the objects go to two target
Lakehouses, and the record of them goes to the Weaver Lakehouse.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from weaver import ItemRef, LocalHost, LocalResolver, LocalStore, RepositoryRef
from weaver.build_bundle import (
    InstallationEnvironment,
    LakehouseBinding,
    TargetBindings,
    generate_build_bundle,
    install_bundle,
    load_bundle,
)
from weaver.catalogue import INSTALLATION, REGISTRY, InstallationScope
from weaver.catalogue.reader import read_table
from weaver.setup import initialise_weaver_lakehouse
from weaver.spark import SparkCatalogue

pytestmark = pytest.mark.spark

FIXTURE = Path(__file__).parent.parent / "fixtures" / "build-lakehouse"
WEAVER = "Weaver"
FIRST = "Sales_LH"
SECOND = "Inventory_LH"
SCOPE = InstallationScope(repository="MyRepo", target_type="lakehouse")


@pytest.fixture
def estate(tmp_path, spark):
    """A Weaver Lakehouse and two destinations, all reachable from one session."""

    host = LocalHost(root=tmp_path, weaver_lakehouse=WEAVER)
    store, resolver = LocalStore(), LocalResolver(host)
    for name in (WEAVER, FIRST, SECOND):
        store.make_directory(resolver.files_root(ItemRef(name)))
        store.make_directory(resolver.tables_root(ItemRef(name)))
    store.make_directory(resolver.repos_root)
    shutil.copytree(FIXTURE, resolver.repository(RepositoryRef("MyRepo")).path)

    initialise_weaver_lakehouse(
        weaver_lakehouse=ItemRef(WEAVER), host=host, store=store, spark=spark
    )
    try:
        yield host, store, resolver
    finally:
        # A succession of temporary directories share one logical Lakehouse name
        # in one long-lived session, so the harness forgets each schema it
        # registered. Production has one Weaver Lakehouse for the session's life.
        for name in (WEAVER, FIRST, SECOND):
            place = resolver.spark_destination(ItemRef(name))
            for schema in ("_", "DWG", "Raw"):
                spark.sql(
                    f"DROP SCHEMA IF EXISTS {place.qualified_schema(schema)} CASCADE"
                )


def _build(host, store, resolver, spark, lakehouse: str):
    bundle = generate_build_bundle(
        weaver_lakehouse=ItemRef(WEAVER),
        repository_name="MyRepo",
        targets=TargetBindings(lakehouse=LakehouseBinding(lakehouse=ItemRef(lakehouse))),
        output=resolver.build_bundle(f"into-{lakehouse}"),
        host=host,
        store=store,
        prune=True,
        catalogue=True,
        spark=spark,
    )
    report = install_bundle(
        load_bundle(bundle.location, store=store),
        environment=InstallationEnvironment(store=store, resolver=resolver, spark=spark),
    )
    assert report.status == "succeeded", [
        f"{a.action_id}: {a.error_message}"
        for s in report.sequences
        for a in s.actions
        if a.status == "failed"
    ]
    return bundle


def test_two_lakehouses_declaring_one_schema_get_two_tables(estate, spark):
    host, store, resolver = estate
    _build(host, store, resolver, spark, FIRST)
    _build(host, store, resolver, spark, SECOND)

    first = SparkCatalogue(spark, resolver.spark_destination(ItemRef(FIRST)))
    second = SparkCatalogue(spark, resolver.spark_destination(ItemRef(SECOND)))

    assert first.qualify("DWG", "Customer") != second.qualify("DWG", "Customer")
    assert first.exists("DWG", "Customer")
    assert second.exists("DWG", "Customer")

    # Separate tables, not one table seen twice: a row written to one is not in
    # the other. This is the assertion the shared-schema defect would fail.
    first.sql(
        "INSERT INTO {{object:DWG.Customer}} "
        "SELECT 1, 'Only in the first', true, "
        "current_timestamp(), current_timestamp(), current_timestamp()"
    )
    assert first.sql("SELECT count(*) AS n FROM {{object:DWG.Customer}}").collect()[0][0] == 1
    assert second.sql("SELECT count(*) AS n FROM {{object:DWG.Customer}}").collect()[0][0] == 0


def test_each_destination_keeps_its_own_storage(estate, spark):
    """The fold is in the name; the bytes still land under each Lakehouse."""

    host, store, resolver = estate
    _build(host, store, resolver, spark, FIRST)
    _build(host, store, resolver, spark, SECOND)

    for lakehouse in (FIRST, SECOND):
        table = resolver.tables_root(ItemRef(lakehouse)).join("DWG", "Customer")
        assert store.exists(table), lakehouse
        assert store.exists(table / "_delta_log")
        # Do not let macOS's case-insensitive filesystem hide Spark folding the
        # physical directory to ``customer``; local emulates Fabric's casing.
        names = {path.name for path in table.path.parent.iterdir()}
        assert "Customer" in names
        assert "customer" not in names


def test_the_catalogue_records_the_installation_it_is_currently_bound_to(estate, spark):
    """Both builds are the same repository, so there is one installation, rebound.

    The catalogue is in a third Lakehouse throughout, and the rows are read back
    from it by name rather than from wherever the session might be pointed.
    """

    host, store, resolver = estate
    _build(host, store, resolver, spark, FIRST)
    catalogue = SparkCatalogue(spark, resolver.spark_destination(ItemRef(WEAVER)))

    (installation,) = read_table(catalogue, INSTALLATION, scope=SCOPE)
    assert installation["target_name"] == FIRST

    _build(host, store, resolver, spark, SECOND)

    # One row still: the item bound is an attribute of the installation, not its
    # identity, so rebinding updates rather than inserting.
    (rebound,) = read_table(catalogue, INSTALLATION, scope=SCOPE)
    assert rebound["target_name"] == SECOND

    catalogued = {
        (row["schema_name"], row["object_name"])
        for row in read_table(catalogue, REGISTRY, scope=SCOPE)
    }
    assert ("DWG", "Customer") in catalogued


def test_the_catalogue_is_not_written_into_either_destination(estate, spark):
    host, store, resolver = estate
    _build(host, store, resolver, spark, FIRST)
    _build(host, store, resolver, spark, SECOND)

    for lakehouse in (FIRST, SECOND):
        place = SparkCatalogue(spark, resolver.spark_destination(ItemRef(lakehouse)))
        assert not place.schema_exists("_"), lakehouse
        assert not store.exists(resolver.tables_root(ItemRef(lakehouse)).join("_"))
