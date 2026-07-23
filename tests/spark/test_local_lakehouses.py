"""The local fixtures themselves: Lakehouses stand up, Delta lands where resolved."""

from __future__ import annotations

import pytest

from weaver import DeltaTarget, FolderTarget, RepositoryRef

pytestmark = pytest.mark.spark


def test_a_lakehouse_presents_files_and_tables(lakehouses):
    assert lakehouses.tree() == [
        "Sales_LH",
        "Sales_LH/Files",
        "Sales_LH/Tables",
        "Weaver",
        "Weaver/Files",
        "Weaver/Files/repos",
        "Weaver/Tables",
    ]


def test_the_repository_installs_into_the_weaver_lakehouse(installed_repository, lakehouses):
    resolved = lakehouses.resolver.repository(RepositoryRef("sales-etl"))
    assert resolved.value == installed_repository.value
    assert (resolved / "Sales__Order.py").path.is_file()


def test_an_installed_repository_reads_back(installed_repository):
    from weaver.ses import read_repository

    repo = read_repository(installed_repository)
    assert repo.name == "sales-etl"
    assert "delta:Sales.Order" in repo.graph.nodes


def test_delta_writes_to_the_resolved_table_path(spark, lakehouses):
    location = lakehouses.resolver.delta_table(
        DeltaTarget.parse("Sales_LH"), "Sales", "Order"
    )
    spark.createDataFrame(
        [("A1", 10.0), ("A2", 20.0)], "Order_id string, Amount double"
    ).write.format("delta").mode("overwrite").save(location.value)

    assert (lakehouses.root / "Sales_LH/Tables/Sales/Order/_delta_log").is_dir()
    assert spark.read.format("delta").load(location.value).count() == 2


def test_two_tables_in_one_lakehouse_stay_separate(spark, lakehouses):
    target = DeltaTarget.parse("Sales_LH")
    for name, rows in (("Order", 2), ("Customer", 3)):
        location = lakehouses.resolver.delta_table(target, "Sales", name)
        spark.createDataFrame(
            [(i,) for i in range(rows)], "id int"
        ).write.format("delta").mode("overwrite").save(location.value)

    for name, rows in (("Order", 2), ("Customer", 3)):
        location = lakehouses.resolver.delta_table(target, "Sales", name)
        assert spark.read.format("delta").load(location.value).count() == rows


def test_a_folder_object_materialises_under_its_target(lakehouses):
    target = FolderTarget.parse("Sales_LH/Files/Extracts")
    destination = lakehouses.resolver.folder_object(target, "Sales", "OrderExport")
    lakehouses.store.write(destination / "order_20260723.csv", b"id,amount\n1,10\n")

    assert destination.path.is_dir()
    assert (destination / "order_20260723.csv").path.read_bytes().startswith(b"id,")
    assert lakehouses.resolver.folder_staging(target, "Sales", "OrderExport").value.endswith(
        "OrderExport_Staging"
    )


def test_each_test_gets_a_fresh_lakehouse(lakehouses):
    """The other tests wrote tables; this one starts empty."""
    assert not (lakehouses.root / "Sales_LH/Tables/Sales").exists()
