"""An authored object against a real session and real Delta files.

``tests/test_objects.py`` proves *which* address each accessor composes. This
proves the addresses are the right ones: a table written where the resolver says
it goes is the table ``dataframe()`` reads, through nothing but a Spark session
and a resolved Lakehouse.
"""

from __future__ import annotations

import pytest

from weaver import DeltaTarget, Folder, FolderTarget, ItemRef, Table, View, lakehouse_for

pytestmark = pytest.mark.spark

TARGET = "Sales_LH"


class Sales__Customer(Table):
    def read(self):
        return [], []


class Sales__Order(Table):
    """Reads a dependency and its own current contents, as authored code does."""

    def read(self):
        customers = Sales__Customer(self).dataframe()
        return self.dataframe().join(customers, "CustomerId"), []


class Sales__OrderExport(Folder):
    def read(self):
        return self.staging_folder(), []


@pytest.fixture
def lakehouse(lakehouses):
    return lakehouse_for(lakehouses.resolver, ItemRef(TARGET))


def _write_delta(spark, resolver, schema: str, name: str, rows, columns: str) -> None:
    """One Delta table, exactly where the resolver says the build puts it."""

    path = resolver.delta_table(DeltaTarget.parse(TARGET), schema, name).value
    spark.createDataFrame(rows, columns).write.format("delta").mode("overwrite").save(path)


def test_a_table_reads_the_delta_files_the_resolver_addresses(spark, lakehouses, lakehouse):
    _write_delta(
        spark, lakehouses.resolver, "Sales", "Order",
        [(1, "A"), (2, "B")], "OrderId int, CustomerId string",
    )

    frame = Sales__Order(spark, lakehouse=lakehouse).dataframe()

    assert frame.count() == 2
    assert frame.columns == ["OrderId", "CustomerId"]


def test_a_dependency_reads_its_own_table_through_the_same_session(spark, lakehouses, lakehouse):
    _write_delta(
        spark, lakehouses.resolver, "Sales", "Order",
        [(1, "A")], "OrderId int, CustomerId string",
    )
    _write_delta(
        spark, lakehouses.resolver, "Sales", "Customer",
        [("A", "Ada"), ("B", "Bo")], "CustomerId string, CustomerName string",
    )

    order = Sales__Order(spark, lakehouse=lakehouse)
    joined, deletes = order.read()

    assert deletes == []
    assert joined.collect()[0]["CustomerName"] == "Ada"
    assert Sales__Customer(order).dataframe().count() == 2


def test_an_empty_dataframe_keeps_the_shape_and_drops_the_rows(spark, lakehouses, lakehouse):
    _write_delta(
        spark, lakehouses.resolver, "Sales", "Order",
        [(1, "A"), (2, "B")], "OrderId int, CustomerId string",
    )

    order = Sales__Order(spark, lakehouse=lakehouse)
    empty = order.empty_dataframe()

    assert empty.count() == 0
    assert empty.schema == order.dataframe().schema


def test_a_view_is_read_by_the_name_its_destination_gives_it(spark, lakehouses, lakehouse):
    _write_delta(
        spark, lakehouses.resolver, "Sales", "Customer",
        [("A", "Ada")], "CustomerId string, CustomerName string",
    )
    destination = lakehouses.resolver.spark_destination(ItemRef(TARGET))
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {destination.qualified_schema('Sales')}")
    spark.sql(
        f"CREATE OR REPLACE VIEW {destination.qualify('Sales', 'Named')} AS "
        f"SELECT CustomerName FROM delta.`{lakehouse.table_path('Sales', 'Customer')}`"
    )

    class Sales__Named(View):
        pass

    try:
        assert Sales__Named(spark, lakehouse=lakehouse).dataframe().collect()[0][0] == "Ada"
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {destination.qualified_schema('Sales')} CASCADE")


def test_a_folder_stages_beside_its_destination(spark, lakehouses, lakehouse):
    """Locally the Lakehouse root is a directory, so the Spark-addressed folder
    path and the resolver's own answer are the same string."""

    target = FolderTarget(lakehouse=ItemRef(TARGET))
    export = Sales__OrderExport(spark, lakehouse=lakehouse)

    staging, deletes = export.read()

    assert deletes == []
    assert export.path() == lakehouses.resolver.folder_object(
        target, "Sales", "OrderExport"
    ).value
    assert staging == lakehouses.resolver.folder_staging(
        target, "Sales", "OrderExport"
    ).value
