"""One repo, the whole Lakehouse chain, built and installed in a single call.

The mixed-estate repository chains a Folder, a Python Delta table, an inferred
Spark SQL table and a Spark view — plus Warehouse T-SQL objects that read the
Lakehouse by hard-coded three-part name. This test binds only the Lakehouse and
proves the entire Lakehouse side materialises in one build→install, with the
Warehouse objects transparently omitted (they are built in a separate call — see
``test_mixed_estate_warehouse``). The Warehouse T-SQL's three-part read means it
carries no SES dependency on the Lakehouse, so the two sides build independently.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from weaver import FolderTarget, ItemRef, Location, RepositoryRef
from weaver.build_bundle import (
    InstallationEnvironment,
    LakehouseBinding,
    TargetBindings,
    generate_build_bundle,
    install_bundle,
)

pytestmark = pytest.mark.spark

FIXTURE = Path(__file__).parent.parent / "fixtures" / "mixed-estate"


@pytest.fixture
def clean_catalog(spark):
    spark.sql("DROP SCHEMA IF EXISTS Sales CASCADE")
    yield
    spark.sql("DROP SCHEMA IF EXISTS Sales CASCADE")


def _schema_of(spark, qualified: str) -> list[str]:
    return [field.name for field in spark.table(qualified).schema]


def test_the_whole_lakehouse_chain_builds_and_installs_in_one_call(
    lakehouses, spark, tmp_path, clean_catalog
):
    destination = lakehouses.resolver.repository(RepositoryRef("Mixed"))
    shutil.copytree(FIXTURE, destination.path)

    bundle = generate_build_bundle(
        weaver_lakehouse=lakehouses.weaver,
        repository_name="Mixed",
        targets=TargetBindings(lakehouse=LakehouseBinding(lakehouse=lakehouses.target)),
        output=Location(str(tmp_path / "bundle")),
        host=lakehouses.host,
        store=lakehouses.store,
        spark=spark,
    )
    report = install_bundle(
        bundle,
        environment=InstallationEnvironment(
            store=lakehouses.store, resolver=lakehouses.resolver, spark=spark
        ),
    )
    assert report.status == "succeeded", report.to_yaml()

    # The Folder is a real directory under Files.
    folder = lakehouses.resolver.folder_object(
        FolderTarget(lakehouse=lakehouses.target), "Raw", "Orders"
    )
    assert lakehouses.store.is_directory(folder)

    # The Python Delta base table and the inferred Spark table both exist, empty,
    # with business columns plus the audit columns.
    for qualified in ("Sales.Customer", "Sales.CustomerEnriched"):
        columns = _schema_of(spark, qualified)
        assert columns[:2] == ["CustomerId", "CustomerName"]
        assert "Row_insert_datetime" in columns
        assert spark.table(qualified).count() == 0

    # The Spark view resolves through the chain and is queryable.
    view_columns = spark.sql("SELECT * FROM Sales.ActiveCustomer").columns
    assert view_columns[:2] == ["CustomerId", "CustomerName"]

    # The Warehouse leaves were omitted from this Lakehouse-only build.
    omitted = {node.node_id for node in bundle.plan.omitted_nodes}
    assert omitted == {"sql:Wh.CustomerReport", "sql:Wh.ActiveReport"}
