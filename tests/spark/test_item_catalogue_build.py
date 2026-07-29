"""The repository/item catalogue seam through the real local Spark installer."""

from __future__ import annotations

import pytest

from weaver import ItemRef, Location
from weaver.build_bundle import (
    InstallationEnvironment,
    ItemBinding,
    ItemBindings,
    LakehouseBinding,
    build_uploaded_item_repository,
    effective_item_bindings,
)
from weaver.catalogue.tables import CATALOGUE_TABLES, INSTALLATION, REGISTRY
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverItemId
from weaver.spark import SparkCatalogue

from test_item_repository import _estate

pytestmark = pytest.mark.spark


def _failures(report) -> str:
    return "\n".join(
        f"{action.action_id}: {action.error_type}: {action.error_message}"
        for sequence in report.sequences
        for action in sequence.actions
        if action.status == "failed"
    )


def test_builtin_item_is_built_and_published_by_one_item_bundle(
    lakehouses, spark, weaver_catalogue
):
    """No integration-only shortcut: discovery, planning and install are real."""

    repository_root = lakehouses.location("Estate")
    lakehouses.store.make_directory(repository_root)
    from weaver.setup import initialise_weaver_lakehouse

    result = initialise_weaver_lakehouse(
        weaver_lakehouse=lakehouses.weaver,
        workspace=lakehouses.workspace,
        store=lakehouses.store,
        spark=spark,
        output=lakehouses.location("bundle"),
    )
    report = result.report

    assert report.status == "succeeded", _failures(report)
    assert set(weaver_catalogue.tables("_")) == {
        table.name for table in CATALOGUE_TABLES
    }

    installation = spark.table(
        weaver_catalogue.qualify("_", INSTALLATION.name)
    ).select("item_type", "item_name", "target_name").collect()
    assert [tuple(row) for row in installation] == [
        ("Lakehouse", "_weaver", lakehouses.weaver.name)
    ]

    registry = spark.table(weaver_catalogue.qualify("_", REGISTRY.name))
    assert registry.where(
        "item_type = 'Lakehouse' AND item_name = '_weaver'"
    ).count() == len(CATALOGUE_TABLES)


def test_public_workflow_materialises_then_installs_from_driver_local_files(
    tmp_path, lakehouses, spark
):
    repository_root = Location(str(_estate(tmp_path)))
    binding = LakehouseBinding(lakehouses.target)

    target = SparkCatalogue(
        spark, lakehouses.resolver.spark_destination(lakehouses.target)
    )
    try:
        selected = ItemBindings(
            (ItemBinding(WeaverItemId.parse("Lakehouse/Raw"), binding),)
        )
        result = build_uploaded_item_repository(
            repository_root,
            bindings=effective_item_bindings(
                selected, weaver_lakehouse=lakehouses.weaver.name
            ),
            environment=InstallationEnvironment(
                store=lakehouses.store,
                resolver=lakehouses.resolver,
                spark=spark,
                workspace=lakehouses.workspace,
            ),
            prune=False,
            control_lakehouse=LakehouseBinding(lakehouses.weaver),
        )

        assert result.report.status == "succeeded", _failures(result.report)
        assert target.exists("Sales", "Customer")
        assert lakehouses.store.exists(
            lakehouses.resolver.files_root(lakehouses.target) / "Sales" / "Customer"
        )
    finally:
        spark.sql(
            f"DROP SCHEMA IF EXISTS {target.qualified_schema('Sales')} CASCADE"
        )


def test_item_build_prunes_tables_and_files_then_lakehouse_wipe_clears_both(
    tmp_path, lakehouses, spark
):
    from weaver import wipe_lakehouse

    target_catalogue = SparkCatalogue(
        spark, lakehouses.resolver.spark_destination(lakehouses.target)
    )
    target_catalogue.create_schema("Sales")
    target_catalogue.sql(
        "CREATE TABLE {{object:Sales.Gworkspace}} (`Id` string) USING delta"
    )
    old_folder = lakehouses.resolver.files_root(lakehouses.target) / "Sales" / "OldFolder"
    lakehouses.store.make_directory(old_folder)

    repository_root = Location(str(_estate(tmp_path)))
    logical = WeaverItemId.parse("Lakehouse/Raw")
    binding = LakehouseBinding(lakehouses.target)
    selected = ItemBindings((ItemBinding(logical, binding),))
    bindings = effective_item_bindings(
        selected, weaver_lakehouse=lakehouses.weaver.name
    )

    try:
        result = build_uploaded_item_repository(
            repository_root,
            bindings=bindings,
            environment=InstallationEnvironment(
                store=lakehouses.store,
                resolver=lakehouses.resolver,
                spark=spark,
                workspace=lakehouses.workspace,
            ),
            prune=True,
            control_lakehouse=LakehouseBinding(lakehouses.weaver),
        )
        report = result.report
        assert report.status == "succeeded", _failures(report)
        assert {name.lower() for name in target_catalogue.tables("Sales")} == {
            "customer"
        }
        customer_folder = (
            lakehouses.resolver.files_root(lakehouses.target) / "Sales" / "Customer"
        )
        assert lakehouses.store.exists(customer_folder)
        assert not lakehouses.store.exists(old_folder)

        reports = wipe_lakehouse(
            lakehouses.target, lakehouses.workspace, store=lakehouses.store
        )
        assert {report.target.split(":", 1)[0] for report in reports} == {
            "folder",
            "delta",
        }
        assert lakehouses.store.list(
            lakehouses.resolver.files_root(lakehouses.target)
        ) == []
        assert lakehouses.store.list(
            lakehouses.resolver.tables_root(lakehouses.target)
        ) == []
    finally:
        spark.sql(
            f"DROP SCHEMA IF EXISTS {target_catalogue.qualified_schema('Sales')} CASCADE"
        )
