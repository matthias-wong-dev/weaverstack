"""A genuine build → load → wipe → rebuild lifecycle, against local Delta.

This is the intended development foundation: saved Spark SQL creates and loads
real Delta tables, the *actual* wipe implementation removes them, and the whole
thing recovers on a second pass. It executes DDL and DML from files rather than
building tables through createDataFrame, so it exercises the path build and load
will take.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from weaver import DeltaTarget, ItemRef, wipe_delta_target

pytestmark = pytest.mark.spark

FIXTURES = Path(__file__).parent.parent / "fixtures" / "local-lakehouse"
TABLES = ("Sales.Order", "Sales.Customer")


def run_script(spark, name: str, tables_root: str) -> None:
    raw = (FIXTURES / name).read_text(encoding="utf-8").format(tables=tables_root)
    # Drop comment lines first, so a statement sitting under a comment is not
    # itself mistaken for one.
    code = "\n".join(
        line for line in raw.splitlines() if not line.lstrip().startswith("--")
    )
    for statement in (s.strip() for s in code.split(";")):
        if statement:
            spark.sql(statement)


def table_path(lakehouses, schema: str, name: str) -> str:
    return lakehouses.resolver.delta_table(
        DeltaTarget.parse("Sales_LH"), schema, name
    ).value


def tables_root(lakehouses) -> str:
    return lakehouses.resolver.tables_root(ItemRef("Sales_LH")).value


def count(spark, path: str) -> int:
    return spark.read.format("delta").load(path).count()


def schema_of(spark, path: str) -> list[str]:
    return spark.read.format("delta").load(path).columns


def test_build_creates_declared_tables_empty(spark, lakehouses):
    run_script(spark, "build.spark.sql", tables_root(lakehouses))

    order = table_path(lakehouses, "Sales", "Order")
    assert count(spark, order) == 0
    assert schema_of(spark, order) == ["Order id", "Customer id", "Amount"]
    assert count(spark, table_path(lakehouses, "Sales", "Customer")) == 0


def test_load_populates_the_built_tables(spark, lakehouses):
    run_script(spark, "build.spark.sql", tables_root(lakehouses))
    run_script(spark, "load.spark.sql", tables_root(lakehouses))

    assert count(spark, table_path(lakehouses, "Sales", "Order")) == 3
    assert count(spark, table_path(lakehouses, "Sales", "Customer")) == 2


def test_wipe_removes_the_real_tables(spark, lakehouses):
    run_script(spark, "build.spark.sql", tables_root(lakehouses))
    run_script(spark, "load.spark.sql", tables_root(lakehouses))

    report = wipe_delta_target(DeltaTarget.parse("Sales_LH"), lakehouses.host)
    assert set(report.removed) == {"Sales"}

    tables = lakehouses.resolver.tables_root(ItemRef("Sales_LH"))
    assert tables.path.is_dir()
    assert list(tables.path.iterdir()) == []


def test_the_environment_recovers_on_a_second_pass(spark, lakehouses):
    """build → load → wipe → build → load, and the rows come back."""
    root = tables_root(lakehouses)

    run_script(spark, "build.spark.sql", root)
    run_script(spark, "load.spark.sql", root)
    assert count(spark, table_path(lakehouses, "Sales", "Order")) == 3

    wipe_delta_target(DeltaTarget.parse("Sales_LH"), lakehouses.host)
    assert not (lakehouses.root / "Sales_LH" / "Tables" / "Sales").exists()

    run_script(spark, "build.spark.sql", root)
    assert count(spark, table_path(lakehouses, "Sales", "Order")) == 0
    run_script(spark, "load.spark.sql", root)
    assert count(spark, table_path(lakehouses, "Sales", "Order")) == 3
    assert count(spark, table_path(lakehouses, "Sales", "Customer")) == 2


def test_a_direct_dataframe_write_still_proves_raw_path_resolution(spark, lakehouses):
    """Kept deliberately: this one proves the resolved Delta path is writable,
    independent of the saved-SQL lifecycle above."""
    path = table_path(lakehouses, "Ad", "Hoc")
    spark.createDataFrame([("x",)], "id string").write.format("delta").save(path)
    assert count(spark, path) == 1
