"""Wiping Delta tables. A table is a directory, so there is no catalogue to mind."""

from __future__ import annotations

import pytest

from weaver import DeltaTarget, FolderTarget, wipe, wipe_delta_target

pytestmark = pytest.mark.spark


def delta_target() -> DeltaTarget:
    return DeltaTarget.parse("Sales_LH")


def test_the_fixture_really_has_delta_tables(spark, populated_lakehouse):
    location = populated_lakehouse.resolver.delta_table(delta_target(), "Sales", "Order")
    assert spark.read.format("delta").load(location.value).count() == 3


def test_a_wipe_removes_every_table(populated_lakehouse):
    report = wipe_delta_target(delta_target(), populated_lakehouse.host)
    assert set(report.removed) == {"Sales", "Reporting"}
    tables = populated_lakehouse.resolver.tables_root(populated_lakehouse.target)
    assert tables.path.is_dir()
    assert list(tables.path.iterdir()) == []


def test_the_transaction_log_goes_with_the_table(populated_lakehouse):
    location = populated_lakehouse.resolver.delta_table(delta_target(), "Sales", "Order")
    assert (location / "_delta_log").path.is_dir()
    wipe_delta_target(delta_target(), populated_lakehouse.host)
    assert not location.path.exists()


def test_a_wiped_table_can_be_written_again(spark, populated_lakehouse):
    """Nothing is left behind to conflict with the next build."""
    location = populated_lakehouse.resolver.delta_table(delta_target(), "Sales", "Order")
    wipe_delta_target(delta_target(), populated_lakehouse.host)

    spark.createDataFrame([("B1",)], "Order_id string").write.format("delta").save(
        location.value
    )
    assert spark.read.format("delta").load(location.value).count() == 1


def test_a_dry_run_leaves_the_tables_readable(spark, populated_lakehouse):
    report = wipe_delta_target(delta_target(), populated_lakehouse.host, dry_run=True)
    assert set(report.removed) == {"Sales", "Reporting"}
    location = populated_lakehouse.resolver.delta_table(delta_target(), "Sales", "Order")
    assert spark.read.format("delta").load(location.value).count() == 3


def test_wiping_delta_leaves_the_folders_alone(populated_lakehouse):
    wipe_delta_target(delta_target(), populated_lakehouse.host)
    files = populated_lakehouse.resolver.files_root(populated_lakehouse.target)
    assert (files / "notes.txt").path.exists()
    assert (files / "Sales" / "OrderExport").path.is_dir()


def test_both_targets_wipe_together_when_both_are_given(populated_lakehouse):
    reports = wipe(
        populated_lakehouse.host,
        folder_target=FolderTarget.parse("Sales_LH/Files"),
        delta_target=delta_target(),
    )
    assert [report.target for report in reports] == [
        "folder:Sales_LH/Files",
        "delta:Sales_LH",
    ]
    assert all(report.count for report in reports)
